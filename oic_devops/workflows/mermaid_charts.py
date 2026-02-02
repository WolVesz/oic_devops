import glob
import os
import re
import pandas as pd
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List


def create_mermaid_integration_dependencies(
    chains_dir: str,
    csv_pattern: str = "invoke_chain_group_*.csv",
    render_adapter_when_no_target: bool = False,  # integration-only by default
    mermaid_format_output = True
) -> Dict[str, Any]:
    """
    Generate a Markdown (.md) Mermaid flowchart for each CSV in chains_dir.

    Default behavior:
      - Draw integration → integration edges only.
      - If render_adapter_when_no_target=True, also draw adapter nodes when a row has no
        target_integration_code; adapter labels prefer adapter_name (fallback to adapter_code).

    The .md files are written next to their CSVs (same base name, .md suffix).
    """
    if pd is None:
        raise RuntimeError(
            "pandas is required for Mermaid generation; install pandas or disable the step."
        )

    pattern = str(Path(chains_dir) / csv_pattern)
    csv_files = glob.glob(pattern)

    def _first_existing_column(df, candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"None of the expected columns exist: {candidates}")

    def _clean_invoke(label):
        if pd.isna(label):
            return ""
        # Remove trailing "(iX)" counters
        return re.sub(r"\(i\d+\)", "", str(label)).strip()

    def _sanitize_id(s: str) -> str:
        s = str(s).replace("\n", "").replace("\\", "")
        s = re.sub(r"[^0-9A-Za-z_]", "_", s)
        if not s:
            return "node"
        if s[0].isdigit():
            s = f"n_{s}"
        return s

    def _esc_text(s: str) -> str:
        return str(s).replace("\n", " ").replace("\\", "").replace('"', '\\"')

    out_md_files: List[str] = []

    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        df.columns = [c.strip() for c in df.columns]

        # Required columns (present in your XREF CSVs)
        src_code_col = _first_existing_column(df, ["source_integration_code", "source_integration_name"])
        tgt_code_col = _first_existing_column(df, ["target_integration_code", "target_integration_name"])
        src_label_col = _first_existing_column(df, ["source_integration_name", "source_integration_code"])
        tgt_label_col = _first_existing_column(df, ["target_integration_name", "target_integration_code"])
        invoke_col = _first_existing_column(df, ["invoke"])

        if mermaid_format_output:
            lines = [
                "```mermaid",
                "flowchart LR",
                f"%% Auto-generated from {os.path.basename(csv_file)}",
                "classDef integ fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20;",
                "classDef adapt fill:#e3f2fd,stroke:#1565c0,color:#0d47a1;",
            ]
        else:
            lines = [
                "flowchart LR",
                f"%% Auto-generated from {os.path.basename(csv_file)}",
                "classDef integ fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20;",
                "classDef adapt fill:#e3f2fd,stroke:#1565c0,color:#0d47a1;",
            ]

        declared_nodes: set = set()
        edges_seen: set = set()

        def declare_integ(node_id: str, label: str):
            key = (node_id, "integ")
            if key in declared_nodes:
                return
            lines.append(f' {node_id}["{label}"]:::integ')
            declared_nodes.add(key)

        def declare_adapter(node_id: str, label: str):
            key = (node_id, "adapt")
            if key in declared_nodes:
                return
            # Hexagon: {{ "Label" }}
            lines.append(f' {node_id}{{{{"{label}"}}}}:::adapt')
            declared_nodes.add(key)

        for _, row in df.iterrows():
            src_code = row.get(src_code_col)
            tgt_code = row.get(tgt_code_col)
            adapter_code = str(row.get("adapter_code", "") or "").strip()
            adapter_name = str(row.get("adapter_name", "") or "").strip()

            # Skip rows with no meaningful source
            if pd.isna(src_code) or not str(src_code).strip():
                continue

            src_id = _sanitize_id(src_code)
            src_label = _esc_text(row.get(src_label_col, src_code))
            declare_integ(src_id, src_label)

            # Integration target present -> draw integration target (default)
            if not pd.isna(tgt_code) and str(tgt_code).strip():
                tgt_id = _sanitize_id(tgt_code)
                tgt_label = _esc_text(row.get(tgt_label_col, tgt_code))
                declare_integ(tgt_id, tgt_label)
            # No integration target: optionally draw the adapter (using adapter_name)
            elif render_adapter_when_no_target and adapter_code:
                tgt_id = _sanitize_id(adapter_code)  # keep ID stable on the code
                label = adapter_name if adapter_name else adapter_code
                declare_adapter(tgt_id, _esc_text(label))
            else:
                # nothing to draw
                continue

            # Edge (single line)
            edge_label = _clean_invoke(row.get(invoke_col, ""))
            key = (src_id, tgt_id, edge_label)
            if key in edges_seen:
                continue
            edges_seen.add(key)
            if edge_label:
                lines.append(f' {src_id} -->|{edge_label}| {tgt_id}')
            else:
                lines.append(f' {src_id} --> {tgt_id}')

        if mermaid_format_output:
            lines.append("```")
            out_path = Path(csv_file).with_suffix(".md")
        else:
            out_path = Path(csv_file).with_suffix(".mmd")


        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("\n".join(lines), encoding="utf-8")
        out_md_files.append(str(out_path))

    return {
        "markdown_files": out_md_files,
        "input_csv_count": len(csv_files),
        "output_md_count": len(out_md_files),
        "output_dir": chains_dir,
    }


def create_mermaid_integration_dependencies_per_root(
    chains_dir: str,
    render_adapter_when_no_target: bool = True,
    csv_pattern: str = "invoke_chain_group_*.csv",
    mermaid_format_output = True,
) -> Dict[str, Any]:
    """
    Generate one Mermaid (.md) per *root* integration for each dependency CSV.

    Root definition (per CSV): source_integration_code that never appears as target_integration_code.

    Behavior:
      - Integration → Integration edges: included when reachable from the root.
      - Adapter edges: included only when the row’s source is reachable AND the target_integration_code is blank,
        if render_adapter_when_no_target=True.
      - Adapter node labels prefer 'adapter_name'; fallback to 'adapter_code' when name is missing.
      - Filenames: <csv_base>__root_<source_integration_name>.md (sanitized; fallback to code).
    """
    if pd is None:
        raise RuntimeError("pandas is required for Mermaid generation; install pandas or disable the step.")

    pattern = str(Path(chains_dir) / csv_pattern)
    csv_files = glob.glob(pattern)

    def _first_existing_column(df, candidates):
        for c in candidates:
            if c in df.columns:
                return c
        raise KeyError(f"None of the expected columns exist: {candidates}")

    def _clean_invoke(label):
        if pd.isna(label):
            return ""
        return re.sub(r"\(i\d+\)", "", str(label)).strip()

    def _sanitize_id(s: str) -> str:
        s = str(s).replace("\n", "").replace("\\", "")
        s = re.sub(r"[^0-9A-Za-z_]", "_", s)
        if not s:
            return "node"
        if s[0].isdigit():
            s = f"n_{s}"
        return s

    def _esc_text(s: str) -> str:
        return str(s).replace("\n", " ").replace("\\", "").replace('"', '\\"')

    def _sanitize_filename(s: str) -> str:
        s = str(s).strip()
        s = re.sub(r"\s+", " ", s)
        s = s.replace("/", "-").replace("\\", "-")
        s = re.sub(r'[<>:"|?*\x00-\x1F]', "_", s)  # Windows-forbidden + control chars
        s = s.strip(" .")
        return s or "root"

    out_md_files: List[str] = []
    roots_total = 0

    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        df.columns = [c.strip() for c in df.columns]

        # Column mapping
        src_code_col = _first_existing_column(df, ["source_integration_code", "source_integration_name"])
        tgt_code_col = _first_existing_column(df, ["target_integration_code", "target_integration_name"])
        src_label_col = _first_existing_column(df, ["source_integration_name", "source_integration_code"])
        tgt_label_col = _first_existing_column(df, ["target_integration_name", "target_integration_code"])
        invoke_col = _first_existing_column(df, ["invoke"])

        # Normalized strings
        src_series = df[src_code_col].astype(str).str.strip()
        tgt_series = df[tgt_code_col].astype(str).str.strip()

        def _valid(v: str) -> bool:
            return bool(v) and v.lower() != "nan"

        sources = set(v for v in src_series if _valid(v))
        targets = set(v for v in tgt_series if _valid(v))

        # Roots: appear as source, never as target
        roots = sorted(sources - targets)
        roots_total += len(roots)

        # adjacency for integration→integration
        adj: Dict[str, set] = defaultdict(set)
        for _, row in df.iterrows():
            s = str(row.get(src_code_col, "")).strip()
            t = str(row.get(tgt_code_col, "")).strip()
            if _valid(s) and _valid(t):
                adj[s].add(t)

        # map code -> first seen source name (for filename & root label)
        code_to_srcname: Dict[str, str] = {}
        for _, row in df.iterrows():
            s = str(row.get(src_code_col, "")).strip()
            nm = row.get(src_label_col, None)
            if _valid(s) and (not pd.isna(nm)) and str(nm).strip():
                code_to_srcname.setdefault(s, str(nm).strip())

        used_basenames: set = set()  # avoid filename collisions within a group

        for root_code in roots:
            # collect reachable codes from this root
            reachable = set()
            q = [root_code]
            while q:
                u = q.pop()
                if u in reachable:
                    continue
                reachable.add(u)
                for v in adj.get(u, []):
                    if v not in reachable:
                        q.append(v)

            # rows to plot (sources in reachable set)
            filtered_rows = []
            for _, row in df.iterrows():
                s = str(row.get(src_code_col, "")).strip()
                if not _valid(s) or s not in reachable:
                    continue
                t = str(row.get(tgt_code_col, "")).strip()
                adapter_code = str(row.get("adapter_code", "") or "").strip()

                if _valid(t):
                    filtered_rows.append(row)
                elif render_adapter_when_no_target and adapter_code:
                    filtered_rows.append(row)

            if not filtered_rows:
                continue

            # Mermaid lines
            if mermaid_format_output:

                lines = [
                    "```mermaid",
                    "flowchart LR",
                    f"%% Auto-generated from {os.path.basename(csv_file)} — root {root_code}",
                    "classDef integ fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20;",
                    "classDef adapt fill:#e3f2fd,stroke:#1565c0,color:#0d47a1;",
                ]
            else:
                lines = [
                    "flowchart LR",
                    f"%% Auto-generated from {os.path.basename(csv_file)} — root {root_code}",
                    "classDef integ fill:#e8f5e9,stroke:#2e7d32,color:#1b5e20;",
                    "classDef adapt fill:#e3f2fd,stroke:#1565c0,color:#0d47a1;",
                ]

            declared_nodes: set = set()
            edges_seen: set = set()

            def declare_integ(node_id: str, label: str):
                key = (node_id, "integ")
                if key in declared_nodes:
                    return
                lines.append(f' {node_id}["{label}"]:::integ')
                declared_nodes.add(key)

            def declare_adapter(node_id: str, label: str):
                key = (node_id, "adapt")
                if key in declared_nodes:
                    return
                lines.append(f' {node_id}{{{{"{label}"}}}}:::adapt')  # hexagon
                declared_nodes.add(key)

            # root node label (prefer source_integration_name)
            root_label = code_to_srcname.get(root_code, root_code)
            declare_integ(_sanitize_id(root_code), _esc_text(root_label))

            # edges
            for row in filtered_rows:
                s_raw = str(row.get(src_code_col, "")).strip()
                t_raw = str(row.get(tgt_code_col, "")).strip()
                adapter_code = str(row.get("adapter_code", "") or "").strip()
                adapter_name = str(row.get("adapter_name", "") or "").strip()

                src_id = _sanitize_id(s_raw)
                src_label = _esc_text(row.get(src_label_col, s_raw))
                declare_integ(src_id, src_label)

                tgt_id = None
                if _valid(t_raw):
                    tgt_id = _sanitize_id(t_raw)
                    tgt_label = _esc_text(row.get(tgt_label_col, t_raw))
                    declare_integ(tgt_id, tgt_label)
                elif render_adapter_when_no_target and adapter_code:
                    tgt_id = _sanitize_id(adapter_code)  # keep ID stable on the code
                    label = adapter_name if adapter_name else adapter_code
                    declare_adapter(tgt_id, _esc_text(label))
                else:
                    continue

                edge_label = _clean_invoke(row.get(invoke_col, ""))
                key = (src_id, tgt_id, edge_label)
                if key in edges_seen:
                    continue
                edges_seen.add(key)
                if edge_label:
                    lines.append(f' {src_id} -->|{edge_label}| {tgt_id}')
                else:
                    lines.append(f' {src_id} --> {tgt_id}')
            if mermaid_format_output:
                lines.append("```")

            # filename: <csvbase>__root_<source_integration_name>.md (sanitized; unique)
            base = Path(csv_file).with_suffix("")
            root_name_for_file = _sanitize_filename(root_label or root_code)
            out_base_name = f"{base.name}__root_{root_name_for_file}"

            final_base_name = out_base_name
            suffix_i = 2
            while final_base_name in used_basenames:
                final_base_name = f"{out_base_name}__{suffix_i}"
                suffix_i += 1
            used_basenames.add(final_base_name)

            if mermaid_format_output:
                out_path = base.parent / f"{final_base_name}.md"
            else:
                out_path = base.parent / f"{final_base_name}.mmd"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text("\n".join(lines), encoding="utf-8")
            out_md_files.append(str(out_path))

    return {
        "markdown_files": out_md_files,
        "input_csv_count": len(csv_files),
        "output_md_count": len(out_md_files),
        "roots_total": roots_total,
        "output_dir": chains_dir,
    }