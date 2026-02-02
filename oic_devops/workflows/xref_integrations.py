# oic_devops/workflows/xref_integrations.py
"""
Cross-reference (Xref) workflows for Oracle Integration Cloud (OIC).
This module scans IAR-derived YAMLs to discover Invoke steps and classifies calls:
- IntegrationAction (by integrationId property)
- RESTToOIC (by flows REST/SOAP URI)
- Adapter_Invoke (everything else)

It exposes one workflow operation via execute(operation=...):
 - operation='build_xref':
   Build a consolidated XREF (optionally write CSV), then:
   1) Split the consolidated rows into "invoke chain" groups
      (weakly connected components) and emit one CSV per group + manifest.
   2) Generate Mermaid flowcharts.
"""

import csv
import os
import re
import pandas as _pd
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Tuple

import yaml
from oic_devops.workflows.base import BaseWorkflow, WorkflowResult
from oic_devops.client import OICClient
from oic_devops.workflows.mermaid_charts import (
    create_mermaid_integration_dependencies_per_root,
    create_mermaid_integration_dependencies,
)

try:
    import pandas as pd  # optional; required for split_by_invoke_chain and Mermaid generation
except Exception:
    pd = None

# Detect direct REST/SOAP calls to OIC flows API
INTEGRATION_RESOURCE_RE = re.compile(
    r"/api/integration/v1/flows/(?:rest|soap)/(?P<code>[^/]+)/(?P<version>[^/]+)/?",
    re.IGNORECASE,
)


class XrefIntegrationWorkflows(BaseWorkflow):
    def execute(self, *args, **kwargs) -> WorkflowResult:
        op = kwargs.pop("operation", None)
        if op == "build_xref":
            return self.build_xref(**kwargs)
        result = WorkflowResult(success=False, message=f"Unknown Xref operation: {op}")
        result.add_error(f"Unknown operation: {op}")
        return result

    def build_xref(
        self,
        input_dir: str,
        pattern: str = ".yaml",
        output_csv: Optional[str] = './output/xref_integrations/INVOKE-XREF.csv',
        use_pandas: bool = False,
        oic_profile: Optional[str] = None,
        create_flow_charts: bool = True,
        chains_output_dir: Optional[str] = None,
    ) -> WorkflowResult:
        result = WorkflowResult()
        result.message = "Building invoke cross-reference (XREF)"

        # Resolve {integration_code: name}
        try:
            code_to_name = self._build_integration_code_to_name_map(oic_profile)
            result.details["resolved_integration_names"] = True
        except Exception as e:
            self.logger.warning(f"Could not resolve code->name map: {e!s}")
            code_to_name = {}
            result.details["resolved_integration_names"] = False

        # Resolve {adapter_code: adapter_name}
        try:
            adapter_code_to_name = self.build_adapter_code_to_name_map(oic_profile)
            result.details["resolved_adapter_names"] = True
        except Exception as e:
            self.logger.warning(f"Could not resolve adapter_code->adapter_name map: {e!s}")
            adapter_code_to_name = {}
            result.details["resolved_adapter_names"] = False

        rows: List[Dict[str, Any]] = []
        total_files = 0
        parsed_files = 0
        failed_files = 0

        for root, _, files in os.walk(input_dir):
            for fn in files:
                if not fn.lower().endswith(pattern.lower()):
                    continue
                total_files += 1
                full_path = os.path.join(root, fn)
                self.logger.info(f"Parsing file: {full_path}")
                try:
                    file_rows = self._parse_yaml_file(full_path, code_to_name, adapter_code_to_name)
                    rows.extend(file_rows)
                    parsed_files += 1
                    result.add_resource(
                        "file",
                        fn,
                        {"path": full_path, "rows": len(file_rows), "status": "parsed"},
                    )
                except Exception as e:
                    failed_files += 1
                    self.logger.exception(f"Failed parsing {full_path}")
                    result.add_error(f"Failed to parse {fn}", e, resource_id=fn)
                    result.add_resource(
                        "file",
                        fn,
                        {"path": full_path, "rows": 0, "status": "error", "error": str(e)},
                    )

        result.details["file_counts"] = {
            "total": total_files,
            "parsed": parsed_files,
            "failed": failed_files,
        }

        pattern_counts: Dict[str, int] = {
            "IntegrationAction": 0,
            "RESTToOIC": 0,
            "Adapter_Invoke": 0,
            "ParseError": 0,
        }
        for r in rows:
            key = r.get("call_type") or "ParseError"
            pattern_counts[key] = pattern_counts.get(key, 0) + 1
        result.details["pattern_counts"] = pattern_counts
        result.details["total_rows"] = len(rows)

        if output_csv:
            try:
                self._write_csv(rows, output_csv, use_pandas=use_pandas)
                result.details["output_csv"] = output_csv
            except Exception as e:
                self.logger.exception(f"Failed writing CSV to {output_csv}")
                result.add_error(f"Failed writing CSV: {output_csv}", e)


        try:
            base_dir = os.path.dirname(output_csv) if output_csv else input_dir
            chains_dir = (
                chains_output_dir
                or os.path.join(base_dir or ".", "Documentation", "Flows_by_invoke_chains")
            )

            split_res = self._split_by_invoke_chain(rows=rows, chains_output_dir=chains_dir)
            result.details["chains"] = split_res

            if "manifest_path" in split_res:
                result.add_resource(
                    "chains",
                    "manifest",
                    {"path": split_res["manifest_path"], "groups": split_res.get("component_count", 0)},
                )

            if "out_files" in split_res:
                for p in split_res["out_files"]:
                    result.add_resource("chains_file", os.path.basename(p), {"path": p})

            if create_flow_charts:
                mermaid_res = create_mermaid_integration_dependencies(chains_dir=chains_dir, render_adapter_when_no_target=False)
                result.details["mermaid_dependencies"] = mermaid_res
                for md in mermaid_res.get("markdown_files", []):
                    result.add_resource("mermaid_md", os.path.basename(md), {"path": md})

                mermaid_roots_res = create_mermaid_integration_dependencies_per_root(
                    chains_dir=chains_dir,
                    render_adapter_when_no_target=True,
                    csv_pattern="invoke_chain_group_*.csv",
                )
                result.details["mermaid_dependencies_per_root"] = mermaid_roots_res
                for md in mermaid_roots_res.get("markdown_files", []):
                    result.add_resource("mermaid_md_root", os.path.basename(md), {"path": md})
        except Exception as e:
            self.logger.exception("Chain splitting / Mermaid step failed")
            result.add_error("Chain splitting / Mermaid step failed", e)

        if failed_files:
            result.success = False
            result.message = (
                f"Built XREF: {len(rows)} rows from {parsed_files}/{total_files} files "
                f"({failed_files} failed)"
            )
        else:
            result.message = f"Built XREF: {len(rows)} rows from {parsed_files} files"

        sample_cap = 50
        result.details["rows_sample"] = rows[:sample_cap]
        result.details["rows_sample_count"] = len(result.details["rows_sample"])
        result.details["rows_full_attached"] = False

        return result

    # ---------- Helpers: resolution & IO ----------
    def _build_integration_code_to_name_map(self, oic_profile: Optional[str]) -> Dict[str, str]:
        client: OICClient
        if oic_profile:
            client = OICClient(profile=oic_profile)
        else:
            client = self.client

        try:
            df = client.integrations.df()
        except Exception as e:
            self.logger.warning(f"Falling back to list() for code->name mapping: {e!s}")
            items = client.integrations.list()
            return {i.get("code"): i.get("name") for i in items if isinstance(i, dict) and i.get("code")}

        if "status" in df.columns:
            df = df[df["status"] == "ACTIVATED"].copy()
        for col in ("code", "name"):
            if col not in df.columns:
                df[col] = None
        df = df.dropna(subset=["code"])
        return dict(zip(df["code"], df["name"]))

    def build_adapter_code_to_name_map(self, oic_profile: Optional[str]) -> Dict[str, str]:
        """
        Build a {adapter_code: adapter_display_name} map using OIC Connections via list_all().
        Friendly name precedence: adapterType.displayName > connection.name > adapterType.type
        """
        try:
            client = OICClient(profile=oic_profile) if oic_profile else self.client
            items = client.connections.list_all()
            mapping: Dict[str, str] = {}
            for it in items or []:
                adapter_name = str(it["name"]).strip()
                possible_codes: List[str] = []
                possible_codes.append(str(it["name"]).strip())
                possible_codes.append(str(it["id"]).strip())
                for code in {c for c in possible_codes if c}:
                    mapping[code] = str(adapter_name)
            # Add OIC Adapter for Integration Action PRESEEDED_COLLOCATED_CONN_1741
            mapping['PRESEEDED_COLLOCATED_CONN_1741'] = "OIC Integration"
            return mapping
        except Exception as e:
            self.logger.warning(f"Could not build adapter_code->adapter_name map (list_all): {e!s}")
            return {}

    def _write_csv(self, rows: List[Dict[str, Any]], output_csv: str, use_pandas: bool = False) -> None:
        columns = [
            "source_file",
            "source_integration",
            "source_integration_code",
            "source_integration_name",
            "source_integration_version",
            "invoke",
            "call_type",
            "target_integration_code",
            "target_version",
            "target_integration_name",
            "resource_uri",
            "adapter_code",
            "adapter_name",
            "application_name",
            "error",
        ]

        if use_pandas and pd is not None:
            df = pd.DataFrame(rows, columns=columns)
            df = df[columns]
            os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
            df.to_csv(output_csv, index=False)
            return

        os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

    # ---------- Core parsing ----------
    @staticmethod
    def _normalize_key(k: Any) -> str:
        s = str(k) if not isinstance(k, str) else k
        return re.sub(r"[^a-z0-9]", "", s.lower())

    @classmethod
    def _to_properties_map(cls, section: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(section, dict):
            return {}
        props = section.get("properties", [])
        out: Dict[str, Any] = {}
        if isinstance(props, list):
            for item in props:
                if isinstance(item, dict):
                    name = item.get("name")
                    val = item.get("value")
                    if isinstance(name, str):
                        out[name] = val
        return out

    @staticmethod
    def _norm_integration_id(raw: Any) -> Tuple[str, Optional[str]]:
        """
        Normalize an IntegrationAction target id into (name, version).

        Handles:
          - NAME + backslash + (optional spaces) + newline + VERSION
          - NAME\\nVERSION  (literal)
          - NAME\nVERSION   (real newline)
          - NAME|VERSION    (pipe-delimited variant)
          - NAME <space> VERSION
        """
        s = str(raw) if not isinstance(raw, str) else raw
        # Normalize line endings
        s = s.replace("\r\n", "\n").replace("\r", "\n")
        # Collapse backslash + optional spaces + newline into a single newline
        # (covers "\\\n", "\\ \n", "\\\r\n", etc.)
        s = re.sub(r"\\\s*\n", "\n", s)
        s = s.strip()
        if not s:
            return "", None

        # 1) NAME\nVERSION
        if "\n" in s:
            name, ver = s.rsplit("\n", 1)
            name = name.rstrip("\\").strip()
            return name, ver.strip()

        # 2) NAME\\nVERSION (two-character literal)
        if "\\n" in s:
            name, ver = s.rsplit("\\n", 1)
            name = name.rstrip("\\").strip()
            return name, ver.strip()

        # 3) NAME|VERSION (pipe-delimited)
        if "|" in s:
            name, ver = s.rsplit("|", 1)
            return name.rstrip("\\").strip(), ver.strip()

        # 4) NAME <space> VERSION
        if " " in s:
            name, ver = s.rsplit(" ", 1)
            name, ver = name.strip(), ver.strip()
            if ver:
                return name.rstrip("\\").strip(), ver

        # 5) fallback
        return s.rstrip("\\").strip(), None

    @staticmethod
    def _lookup_integration_name(code_to_name: Dict[str, str], code: Optional[str]) -> Optional[str]:
        if not code:
            return None
        return code_to_name.get(code)

    @classmethod
    def _get_from_props(cls, props: Dict[str, Any], *candidates: str) -> Optional[Any]:
        if not props:
            return None
        normalized = {cls._normalize_key(k): v for k, v in props.items() if isinstance(k, str)}
        for c in candidates:
            v = normalized.get(cls._normalize_key(c))
            if v is not None:
                return v
        return None

    @classmethod
    def _extract_integration_id(cls, props_map: Dict[str, Any]) -> Optional[str]:
        for k, v in list(props_map.items()):
            if cls._normalize_key(k) == "integrationid":
                return v
        return None

    def _find_invokes(self, obj: Any, results: List[Dict[str, Any]], context: Dict[str, Any]) -> None:
        if isinstance(obj, dict):
            if "Invoke" in obj:
                invoke_name = obj.get("Invoke")
                application = obj.get("application", {}) or {}
                adapter = application.get("adapter", {}) or {}

                props_map = self._to_properties_map(application)
                adapter_code = adapter.get("code")
                application_name = application.get("name")

                call_type: Optional[str] = None
                target_code: Optional[str] = None
                target_version: Optional[str] = None

                resource_uri = self._get_from_props(props_map, "ResourceURI", "resourceUri", "resource_uri")

                # Pattern 1: IntegrationAction by integrationId
                integ_id = self._extract_integration_id(props_map)
                if isinstance(integ_id, (str, int, float)) and str(integ_id).strip():
                    code, ver = self._norm_integration_id(str(integ_id))
                    call_type = "IntegrationAction"
                    target_code, target_version = code, ver

                # Pattern 2: flows REST/SOAP URI
                if not call_type and isinstance(resource_uri, str):
                    m = INTEGRATION_RESOURCE_RE.search(resource_uri)
                    if m:
                        call_type = "RESTToOIC"
                        target_code = m.group("code")
                        target_version = m.group("version")

                # Pattern 3: default
                if not call_type:
                    call_type = "Adapter_Invoke"

                # Final sanitization
                if isinstance(target_code, str):
                    target_code = target_code.rstrip("\\").strip()

                code_to_name: Dict[str, str] = context["code_to_name"]
                adapter_code_to_name: Dict[str, str] = context["adapter_code_to_name"]
                source_integration_code = context.get("source_integration_code")
                results.append(
                    {
                        "source_file": context.get("source_file"),
                        "source_integration": context.get("source_integration"),
                        "source_integration_code": source_integration_code,
                        "source_integration_name": self._lookup_integration_name(code_to_name, source_integration_code),
                        "source_integration_version": context.get("source_integration_version"),
                        "invoke": invoke_name,
                        "call_type": call_type,
                        "target_integration_code": target_code,
                        "target_version": target_version,
                        "target_integration_name": self._lookup_integration_name(code_to_name, target_code),
                        "resource_uri": resource_uri,
                        "adapter_code": adapter_code,
                        "adapter_name": adapter_code_to_name.get(adapter_code),
                        "application_name": application_name,
                        "error": None,
                    }
                )

            for v in obj.values():
                self._find_invokes(v, results, context)

        elif isinstance(obj, list):
            for item in obj:
                self._find_invokes(item, results, context)

    def _parse_yaml_file(
        self,
        file_path: str,
        code_to_name: Dict[str, str],
        adapter_code_to_name: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()

        try:
            data = yaml.safe_load(text)
        except Exception:
            data = None  # fall back to regex if YAML contains odd constructs

        results: List[Dict[str, Any]] = []

        base = os.path.basename(file_path).replace(".iar.yaml", "")
        parts = base.rsplit("_", 1)
        code = parts[0]
        version = parts[1] if len(parts) == 2 else None

        source_integration = "\n".join(os.path.basename(file_path).replace(".iar.yaml", "").rsplit("_", 1))

        context = {
            "source_file": os.path.basename(file_path),
            "source_integration": source_integration,
            "source_integration_code": code,
            "source_integration_version": version,
            "code_to_name": code_to_name,
            "adapter_code_to_name": adapter_code_to_name,
        }

        if data is not None:
            self._find_invokes(data, results, context)
            return results

        # Fallback regex path (best-effort)
        for m in re.finditer(r"^\s*\-\s*Invoke:\s*(.*)$", text, flags=re.MULTILINE):
            invoke_name = (m.group(1) or "").strip()
            block_start = m.end()
            block_text = text[block_start : block_start + 3000]

            res_uri_match = re.search(
                r"name:\s*ResourceURI\s*\n\s*value:\s*(.*)", block_text, flags=re.IGNORECASE
            )
            integ_match = re.search(
                r"name:\s*integration[_-]?id\s*\n\s*value:\s*(.*)", block_text, flags=re.IGNORECASE
            )

            call_type: Optional[str] = None
            target_code: Optional[str] = None
            target_version: Optional[str] = None
            resource_uri: Optional[str] = None

            if res_uri_match:
                resource_uri = (res_uri_match.group(1) or "").strip()
                m2 = INTEGRATION_RESOURCE_RE.search(resource_uri)
                if m2:
                    call_type = "RESTToOIC"
                    target_code = m2.group("code")
                    target_version = m2.group("version")

            if integ_match and not call_type:
                call_type = "IntegrationAction"
                code_norm, ver = self._norm_integration_id((integ_match.group(1) or "").strip())
                target_code, target_version = code_norm, ver

            if not call_type:
                call_type = "Adapter_Invoke"

            if isinstance(target_code, str):
                target_code = target_code.rstrip("\\").strip()

            results.append(
                {
                    "source_file": context.get("source_file"),
                    "source_integration": context.get("source_integration"),
                    "source_integration_code": context.get("source_integration_code"),
                    "source_integration_name": self._lookup_integration_name(
                        code_to_name, context.get("source_integration_code")
                    ),
                    "source_integration_version": version,
                    "invoke": invoke_name,
                    "call_type": call_type,
                    "target_integration_code": target_code,
                    "target_version": target_version,
                    "target_integration_name": self._lookup_integration_name(code_to_name, target_code),
                    "resource_uri": resource_uri,
                    "adapter_code": None,
                    "adapter_name": None,
                    "application_name": None,
                    "error": None,
                }
            )
        return results

    # ---------- Split by invoke chain ----------
    def _split_by_invoke_chain(
        self,
        rows: List[Dict[str, Any]],
        chains_output_dir: str,
    ) -> Dict[str, Any]:
        if pd is None:
            raise RuntimeError(
                "pandas is required for split_by_invoke_chain; install pandas or disable the step."
            )

        df = _pd.DataFrame(rows)
        df.columns = [str(c).strip() for c in df.columns]

        src_col, tgt_col = "source_integration_code", "target_integration_code"
        edges_df = df[(df.get(src_col).notna()) & (df.get(tgt_col).notna())].copy()

        for col in [src_col, tgt_col]:
            if col in edges_df.columns:
                edges_df[col] = (
                    edges_df[col].astype(str)
                    .str.replace("\n", "", regex=False)
                    .str.replace("\\", "", regex=False)
                    .str.strip()
                )

        adj: Dict[str, set] = defaultdict(set)
        for _, row in edges_df.iterrows():
            s = row.get(src_col)
            t = row.get(tgt_col)
            if not s or not t or s == "nan" or t == "nan":
                continue
            adj[s].add(t)
            adj[t].add(s)

        visited = set()
        components: List[List[str]] = []
        for node in list(adj.keys()):
            if node in visited:
                continue
            comp = set()
            q = deque([node])
            visited.add(node)
            while q:
                u = q.popleft()
                comp.add(u)
                for v in adj[u]:
                    if v not in visited:
                        visited.add(v)
                        q.append(v)
            components.append(sorted(comp))

        node_to_comp: Dict[str, int] = {}
        for idx, comp in enumerate(components, start=1):
            for n in comp:
                node_to_comp[n] = idx

        comp_rows: Dict[int, List[int]] = defaultdict(list)
        assigned_indices: set = set()
        for i, row in df.iterrows():
            s = str(row.get(src_col, "")).replace("\n", "").replace("\\", "").strip()
            t = str(row.get(tgt_col, "")).replace("\n", "").replace("\\", "").strip()
            comp_id = None
            if s in node_to_comp:
                comp_id = node_to_comp[s]
            if t in node_to_comp:
                comp_id = node_to_comp[t] if comp_id is None else comp_id
            if comp_id is not None:
                comp_rows[comp_id].append(i)
                assigned_indices.add(i)

        all_indices = set(df.index.tolist())
        unassigned = sorted(all_indices - assigned_indices)
        if unassigned:
            catchall_id = (max(comp_rows.keys()) if comp_rows else 0) + 1
            comp_rows[catchall_id] = unassigned
            components.append(["__CATCH_ALL__"])

        os.makedirs(chains_output_dir, exist_ok=True)
        out_files: List[str] = []
        for comp_id, idxs in comp_rows.items():
            sub = df.loc[idxs]
            sort_cols = [c for c in [src_col, tgt_col, "call_type", "invoke"] if c in sub.columns]
            if sort_cols:
                sub = sub.sort_values(by=sort_cols, kind="stable")
            out_name = os.path.join(chains_output_dir, f"invoke_chain_group_{comp_id:03d}.csv")
            sub.to_csv(out_name, index=False)
            out_files.append(out_name)

        manifest_rows = []
        for comp_id, nodes in enumerate(components, start=1):
            manifest_rows.append(
                {
                    "chain_group": comp_id,
                    "members_count": len(nodes),
                    "members": ";".join(nodes),
                    "rows_in_csv": len(comp_rows.get(comp_id, [])),
                }
            )
        manifest = _pd.DataFrame(manifest_rows).sort_values(by="rows_in_csv", ascending=False)
        manifest_path = os.path.join(chains_output_dir, "manifest.csv")
        manifest.to_csv(manifest_path, index=False)

        return {
            "out_files": out_files,
            "manifest_path": manifest_path,
            "component_count": len(components),
            "unassigned_count": len(unassigned),
            "output_dir": chains_output_dir,
        }