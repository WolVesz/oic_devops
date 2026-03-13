import pandas as pd
import glob
import re
import os
from pathlib import Path

base_path = "./output/oic_integrations_backup_20260127_091729/integrations/src/invoke_chains"
csv_files = glob.glob(f"{base_path}/chain_group_*.csv")

# ----------------- helpers -----------------

def first_existing_column(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"None of the expected columns exist: {candidates}")

def clean_invoke(label):
    if pd.isna(label):
        return ""
    return re.sub(r"\(.*?\)", "", str(label)).strip()

def sanitize_id(s: str) -> str:
    s = str(s).replace("\n", "").replace("\\", "")
    s = re.sub(r"[^0-9A-Za-z_]", "_", s)
    if not s:
        return "node"
    if s[0].isdigit():
        s = f"n_{s}"
    return s

def esc_text(s: str) -> str:
    return str(s).replace("\n", " ").replace("\\", "").replace('"', '\\"')

# -------------------------------------------

for csv_file in csv_files:
    df = pd.read_csv(csv_file)
    df.columns = [c.strip() for c in df.columns]

    # IDs use codes; labels use names
    src_code_col = first_existing_column(
        df, ["source_integration_code", "source_integration_name"]
    )
    tgt_code_col = first_existing_column(
        df, ["target_integration_code", "target_integration_name"]
    )

    src_label_col = first_existing_column(
        df, ["source_integration_name", "source_integration_code"]
    )
    tgt_label_col = first_existing_column(
        df, ["target_integration_name", "target_integration_code"]
    )

    invoke_col = first_existing_column(df, ["invoke"])

    lines = [
        "```mermaid",
        "flowchart LR",
        f"%% Auto-generated from {os.path.basename(csv_file)}",
    ]

    edges_seen = set()

    for _, row in df.iterrows():
        src_code = row.get(src_code_col)
        tgt_code = row.get(tgt_code_col)

        if pd.isna(src_code) or pd.isna(tgt_code):
            continue

        src_id = sanitize_id(src_code)
        tgt_id = sanitize_id(tgt_code)

        src_label = esc_text(row.get(src_label_col, src_code))
        tgt_label = esc_text(row.get(tgt_label_col, tgt_code))

        edge_label = clean_invoke(row.get(invoke_col, ""))

        key = (src_id, tgt_id, edge_label)
        if key in edges_seen:
            continue
        edges_seen.add(key)

        if edge_label:
            lines.append(
                f'    {src_id}["{src_label}"] -->|{edge_label}| {tgt_id}["{tgt_label}"]'
            )
        else:
            lines.append(
                f'    {src_id}["{src_label}"] --> {tgt_id}["{tgt_label}"]'
            )

    lines.append("```")

    md_text = "\n".join(lines)

    # ✅ Write ONLY .md
    out_path = Path(csv_file).with_suffix(".md")
    out_path.write_text(md_text, encoding="utf-8")

print(f"Generated {len(csv_files)} Markdown Mermaid files (.md only).")
