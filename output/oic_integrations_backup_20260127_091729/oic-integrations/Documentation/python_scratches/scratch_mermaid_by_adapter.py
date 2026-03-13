import os
import re
import pandas as pd

# ---------- CONFIG ----------
IN_DIR = "./output/oic_integrations_backup_20260127_091729/integrations/src"
IN_FILE = os.path.join(IN_DIR, "AA-INVOKE-XREFv4.csv")  # CSV input

# Output markdown with all Mermaid diagrams
OUT_MD = os.path.join(IN_DIR, "invoke_mermaid_backwards.md")

# Optional: write a separate .mmd file per adapter_code
OUT_SNIPPETS_DIR = os.path.join(IN_DIR, "invoke_by_adapter")

# ---------- UTILS ----------
def norm_id_keep_case(s: str) -> str:
    """
    Create a Mermaid-safe node id while PRESERVING CASE.
    Allowed: letters, numbers, underscore.
    Steps:
      - convert to str
      - replace non [A-Za-z0-9_] with _
      - collapse multiple underscores
      - strip leading/trailing underscores
    """
    if s is None:
        return "unnamed"
    s = str(s)
    s = re.sub(r"[^A-Za-z0-9_]", "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s or "unnamed"

def safe_label(val) -> str:
    """Return a label for node/edge text (preserve case), escaping double quotes."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val).replace('"', '\\"')

def pick_source_label(row: pd.Series) -> str:
    """
    Label for SOURCE node:
      1) source_integration_name (preferred)
      2) fallback: source_integration_code
    """
    name = row.get("source_integration_name", None)
    if name is None or (isinstance(name, float) and pd.isna(name)) or str(name).strip() == "":
        name = row.get("source_integration_code", None)
    return "" if name is None or (isinstance(name, float) and pd.isna(name)) else str(name).strip()

def build_edges_adapter_to_source(df: pd.DataFrame):
    """
    Build unique edges:
      (adapter_id, adapter_label, source_id, source_label, edge_label)
    Direction: adapter_code --> source_integration
    Edge label: application_name
    Only include rows with both adapter_code and source_integration_code.
    """
    edges = set()
    for _, row in df.iterrows():
        adapter_code = row.get("adapter_code")
        source_code = row.get("source_integration_code")
        app_name = row.get("application_name")

        if pd.isna(adapter_code) or pd.isna(source_code):
            continue

        adapter_code = str(adapter_code).strip()
        source_code = str(source_code).strip()
        app_name = None if pd.isna(app_name) else str(app_name).strip()
        if not adapter_code or not source_code:
            continue

        a_id = norm_id_keep_case(adapter_code)
        s_id = norm_id_keep_case(source_code)

        a_label = adapter_code  # adapter label is itself
        s_label = pick_source_label(row)  # source friendly name (fallback to code)
        e_label = app_name  # edge label is application_name

        edges.add((a_id, a_label, s_id, s_label, e_label))
    return edges

def emit_mermaid(edges):
    """
    Emit a Mermaid 'flowchart LR' with:
      - Left node (rect) = Adapter (adapter_code)
      - Right node (rect) = Source (source_integration_name or code)
      - Edge label = application_name
      - Direction: adapter --> source
    """
    lines = []
    lines.append("flowchart LR")

    # Collect nodes
    nodes = {}
    for a_id, a_lbl, s_id, s_lbl, _ in edges:
        nodes[a_id] = a_lbl
        nodes[s_id] = s_lbl

    # Declare nodes (rectangles). Preserve case in labels and IDs.
    for nid in sorted(nodes.keys(), key=lambda x: x):
        nlabel = safe_label(nodes[nid]) or nid
        lines.append(f'    {nid}["{nlabel}"]')

    # Edges (adapter -> source), labeled by application_name when present
    for a_id, _, s_id, _, edge_lbl in sorted(edges, key=lambda x: (x[0], x[2], x[4] or "")):
        if edge_lbl:
            el = safe_label(edge_lbl)
            lines.append(f'    {a_id} -- "{el}" --> {s_id}')
        else:
            lines.append(f'    {a_id} --> {s_id}')
    return "\n".join(lines)

# ---------- MAIN ----------
def main():
    # Robust CSV read
    read_attempts = [
        dict(engine="c"),
        dict(engine="python"),
        dict(engine="python", on_bad_lines="skip"),
    ]
    last_err = None
    df = None
    for opts in read_attempts:
        try:
            df = pd.read_csv(IN_FILE, **opts)
            break
        except Exception as e:
            last_err = e
            continue
    if df is None:
        raise RuntimeError(f"Failed to read CSV: {IN_FILE} ; last error: {last_err}")

    # Clean headers
    df.columns = [str(c).strip() for c in df.columns]

    # Validate required columns
    required = {"source_integration_code", "adapter_code"}  # application_name is optional
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input is missing required columns: {missing}")

    # Filter to rows that have adapter and source code
    df_pairs = df[df["adapter_code"].notna() & df["source_integration_code"].notna()].copy()
    if df_pairs.empty:
        os.makedirs(os.path.dirname(OUT_MD), exist_ok=True)
        with open(OUT_MD, "w", encoding="utf-8") as f:
            f.write("# OIC Adapter → Source Landscape\n\n```mermaid\nflowchart LR\n```\n")
        print("No adapter→source rows found. Wrote an empty diagram.")
        return

    # Build global edges
    edges_all = build_edges_adapter_to_source(df_pairs)

    # Prepare outputs
    os.makedirs(os.path.dirname(OUT_MD), exist_ok=True)
    os.makedirs(OUT_SNIPPETS_DIR, exist_ok=True)

    md_sections = []
    md_sections.append("# OIC Landscape (Adapter → Source integration)")
    md_sections.append("_Node labels preserve case; adapter label = adapter_code; source label = source_integration_name (fallback to code); edge label = application_name_")
    md_sections.append("")

    # 1) Global diagram
    md_sections.append("## Global view")
    md_sections.append("")
    md_sections.append("```mermaid")
    md_sections.append(emit_mermaid(edges_all) if edges_all else "flowchart LR\n    %% (no edges found)")
    md_sections.append("```")
    md_sections.append("")

    # 2) Per-adapter diagrams
    for adapter_code, sub in sorted(df_pairs.groupby("adapter_code"), key=lambda x: str(x[0])):
        edges_sub = build_edges_adapter_to_source(sub)
        if not edges_sub:
            continue

        md_sections.append(f"## Adapter: {adapter_code}")
        md_sections.append("")
        md_sections.append("```mermaid")
        md_sections.append(emit_mermaid(edges_sub))
        md_sections.append("```")
        md_sections.append("")

        # Optional per-adapter .mmd snippet
        safe_adapter_id = norm_id_keep_case(adapter_code)
        out_mmd = os.path.join(OUT_SNIPPETS_DIR, f"{safe_adapter_id}.mmd")
        with open(out_mmd, "w", encoding="utf-8") as f:
            f.write(emit_mermaid(edges_sub))

    # Write the consolidated Markdown
    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(md_sections))

    print(f"Mermaid markdown written to: {OUT_MD}")
    print(f"Per-adapter .mmd snippets at: {OUT_SNIPPETS_DIR}")

if __name__ == "__main__":
    main()