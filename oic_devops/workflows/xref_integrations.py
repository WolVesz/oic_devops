"""
Cross-reference (Xref) workflows for OIC: discover and classify invokes in IAR YAMLs.

This module converts the scratch script that parsed Invoke steps into a reusable workflow
class consistent with the project's BaseWorkflow/WorkflowResult pattern.
"""

import csv
import os
import re
from typing import Any, Dict, List, Optional, Tuple

try:
    import pandas as pd  # optional; used only if available and user requests DataFrame/CSV via pandas
except Exception:  # pragma: no cover - keep optional
    pd = None

import yaml

from oic_devops.workflows.base import BaseWorkflow, WorkflowResult  # project-local base classes
from oic_devops.client import OICClient  # used to resolve code->name (optional profile)


# Reused from the scratch logic: detect direct REST calls to OIC flows api (rest/soap).
INTEGRATION_RESOURCE_RE = re.compile(
    r"/api/integration/v1/flows/(?:rest|soap)/(?P<code>[^/]+)/(?P<version>[^/]+)/?",
    re.IGNORECASE,
)


class XrefIntegrationWorkflows(BaseWorkflow):
    """
    Workflow operations to build an invoke cross-reference (XREF) from IAR YAML exports.

    Operations exposed via execute(operation=...):
      - operation='build_xref':
          Scan an input directory for *.yaml, parse invokes, classify patterns, and optionally
          write a CSV. Returns a WorkflowResult with counts and per-file resource entries.

    Example:
        wf = XrefWorkflows(client=my_client, logger=my_logger)
        res = wf.execute(
            operation="build_xref",
            input_dir="./output/.../integrations/src",
            pattern=".yaml",
            output_csv="./output/invoke_xref.csv",          # optional
            use_pandas=True,                                 # optional; requires pandas
            oic_profile="prod"                               # optional; used for code->name mapping
        )
    """

    # --------------------------
    # Public dispatcher
    # --------------------------
    def execute(self, *args, **kwargs) -> WorkflowResult:
        op = kwargs.pop("operation", None)
        if op == "build_xref":
            return self.build_xref(**kwargs)

        result = WorkflowResult(success=False, message=f"Unknown Xref operation: {op}")
        result.add_error(f"Unknown operation: {op}")
        return result

    # --------------------------
    # Main Workflow
    # --------------------------
    def build_xref(
        self,
        input_dir: str,
        pattern: str = ".yaml",
        output_csv: Optional[str] = None,
        use_pandas: bool = False,
        oic_profile: Optional[str] = None,
    ) -> WorkflowResult:
        """
        Build an OIC invoke cross-reference from IAR YAML files.

        Args:
            input_dir: Root directory to walk.
            pattern: Filename suffix to include (default: ".yaml").
            output_csv: Optional path to write a CSV file of all discovered invokes.
            use_pandas: If True and pandas is available, write CSV via pandas for consistent
                        column ordering and NA handling. Falls back to csv module otherwise.
            oic_profile: Optional profile name for OICClient to resolve integration code->name.

        Returns:
            WorkflowResult: details include total rows, by-pattern counts, and file counts.
                            resources['file'][<file>] contains parse summary and per-file rows count.
        """
        result = WorkflowResult()
        result.message = "Building invoke cross-reference (XREF)"

        # Resolve integration code->name map (optional; if client already has a profile, use that)
        try:
            code_to_name = self._build_code_to_name_map(oic_profile)
            result.details["resolved_names"] = True
        except Exception as e:
            # Non-fatal: we can still proceed without names
            self.logger.warning(f"Could not resolve code->name map: {e!s}")
            code_to_name = {}
            result.details["resolved_names"] = False

        rows: List[Dict[str, Any]] = []
        total_files = 0
        parsed_files = 0
        failed_files = 0

        # Walk files
        for root, _, files in os.walk(input_dir):
            for fn in files:
                if not fn.lower().endswith(pattern.lower()):
                    continue

                total_files += 1
                full_path = os.path.join(root, fn)
                self.logger.info(f"Parsing file: {full_path}")

                try:
                    file_rows = self._parse_yaml_file(full_path, code_to_name)
                    rows.extend(file_rows)
                    parsed_files += 1

                    # Per-file resource summary
                    result.add_resource(
                        "file",
                        fn,
                        {
                            "path": full_path,
                            "rows": len(file_rows),
                            "status": "parsed",
                        },
                    )
                except Exception as e:
                    failed_files += 1
                    self.logger.exception(f"Failed parsing {full_path}")
                    result.add_error(f"Failed to parse {fn}", e, resource_id=fn)
                    result.add_resource(
                        "file",
                        fn,
                        {
                            "path": full_path,
                            "rows": 0,
                            "status": "error",
                            "error": str(e),
                        },
                    )

        # Summaries
        result.details["file_counts"] = {
            "total": total_files,
            "parsed": parsed_files,
            "failed": failed_files,
        }

        # Compute pattern counts and write CSV if requested
        pattern_counts = {"IntegrationAction": 0, "RESTToOIC": 0, "Adapter_Invoke": 0, "ParseError": 0}
        for r in rows:
            key = r.get("call_type") or "ParseError"
            if key not in pattern_counts:
                pattern_counts[key] = 0
            pattern_counts[key] += 1

        result.details["pattern_counts"] = pattern_counts
        result.details["total_rows"] = len(rows)

        # Optional CSV output
        if output_csv:
            try:
                self._write_csv(rows, output_csv, use_pandas=use_pandas)
                result.details["output_csv"] = output_csv
            except Exception as e:
                self.logger.exception(f"Failed writing CSV to {output_csv}")
                result.add_error(f"Failed writing CSV: {output_csv}", e)

        # Final message
        if failed_files:
            result.success = False
            result.message = (
                f"Built XREF: {len(rows)} rows from {parsed_files}/{total_files} files "
                f"({failed_files} failed)"
            )
        else:
            result.message = f"Built XREF: {len(rows)} rows from {parsed_files} files"

        # Optionally attach the rows (if not too big) — we’ll keep just the first N to avoid bloat
        sample_cap = 50
        result.details["rows_sample"] = rows[:sample_cap]
        result.details["rows_sample_count"] = len(result.details["rows_sample"])
        result.details["rows_full_attached"] = False  # keep result light-weight

        return result

    # --------------------------
    # Helpers: resolution & IO
    # --------------------------
    def _build_code_to_name_map(self, oic_profile: Optional[str]) -> Dict[str, str]:
        """
        Build {code: name} map using the project's OICClient.

        Uses the provided profile if given; otherwise uses the client's current profile/context.
        """
        # Prefer the workflow's client instance; if a specific profile was requested and differs,
        # we create a transient client for lookup.
        client: OICClient
        if oic_profile:
            client = OICClient(profile=oic_profile)
        else:
            client = self.client  # use provided client

        # Expect client.integrations.df() to be available (like in the scratch)
        try:
            df = client.integrations.df()
        except Exception as e:
            # Fall back: try list(), if present
            self.logger.warning(f"Falling back to list() for code->name mapping: {e!s}")
            items = client.integrations.list()
            # items expected to be dicts with 'code' and 'name'
            return {i.get("code"): i.get("name") for i in items if isinstance(i, dict) and i.get("code")}

        # Keep only rows with code and name; (optionally) filter by status==ACTIVATED if present
        if "status" in df.columns:
            df = df[df["status"] == "ACTIVATED"].copy()
        for col in ("code", "name"):
            if col not in df.columns:
                df[col] = None
        df = df.dropna(subset=["code"])
        return dict(zip(df["code"], df["name"]))

    def _write_csv(self, rows: List[Dict[str, Any]], output_csv: str, use_pandas: bool = False) -> None:
        """
        Write the discovered rows to CSV.
        Uses pandas if requested and available; otherwise uses csv module.
        """
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
            "application_name",
            "error",
        ]

        if use_pandas and pd is not None:
            df = pd.DataFrame(rows, columns=columns)
            # Ensure deterministic column order
            df = df[columns]
            os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
            df.to_csv(output_csv, index=False)
            return

        # csv module fallback
        os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

    # --------------------------
    # Core parsing (adapted from scratch)
    # --------------------------
    @staticmethod
    def _normalize_key(k: Any) -> str:
        s = str(k) if not isinstance(k, str) else k
        return re.sub(r"[^a-z0-9]", "", s.lower())

    @classmethod
    def _to_properties_map(cls, section: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten *.properties (list of {name, value}) into {name: value}."""
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
        """Accept 'CODE\nVERSION', 'CODE_VERSION', or just 'CODE'."""
        s = str(raw) if not isinstance(raw, str) else raw
        if "\n" in s:
            code, ver = s.rsplit("\n", 1)
            return code, ver
        if "_" in s:
            code, ver = s.rsplit("_", 1)
            return code, ver
        return s, None

    def _lookup_integration_name(self, code_to_name: Dict[str, str], code: Optional[str]) -> Optional[str]:
        if not code:
            return None
        return code_to_name.get(code)

    @classmethod
    def _get_from_props(cls, props: Dict[str, Any], *candidates: str) -> Optional[Any]:
        """
        Case-/style-insensitive property lookup:
        - lowercases and removes non-alphanumerics on keys
        """
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
        """
        Recursive descent over the YAML structure:
        - Collect Invoke nodes.
        - Classify calls into IntegrationAction, RESTToOIC, or Adapter_Invoke.
        """
        if isinstance(obj, dict):
            if "Invoke" in obj:
                invoke_name = obj.get("Invoke")
                application = obj.get("application", {}) or {}
                adapter = application.get("adapter", {}) or {}

                # Property flattening
                props_map = self._to_properties_map(application)

                # Collect common fields
                adapter_code = adapter.get("code")
                application_name = application.get("name")

                call_type = None
                target_code = None
                target_version = None
                resource_uri = self._get_from_props(props_map, "ResourceURI", "resourceUri", "resource_uri")

                # Pattern 1: integration_id variant present
                integ_id = self._extract_integration_id(props_map)
                if isinstance(integ_id, (str, int, float)) and str(integ_id).strip():
                    code, ver = self._norm_integration_id(str(integ_id))
                    call_type = "IntegrationAction"
                    target_code, target_version = code, ver

                # Pattern 2: REST URI into OIC flows API
                if not call_type and isinstance(resource_uri, str):
                    m = INTEGRATION_RESOURCE_RE.search(resource_uri)
                    if m:
                        call_type = "RESTToOIC"
                        target_code = m.group("code")
                        target_version = m.group("version")

                # Pattern 3: Otherwise → Adapter_Invoke
                if not call_type:
                    call_type = "Adapter_Invoke"

                code_to_name: Dict[str, str] = context["code_to_name"]
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
                        "application_name": application_name,
                        "error": None,
                    }
                )

            # Recurse
            for v in obj.values():
                self._find_invokes(v, results, context)

        elif isinstance(obj, list):
            for item in obj:
                self._find_invokes(item, results, context)

    # Single-file parser (YAML + best-effort regex fallback)
    def _parse_yaml_file(self, file_path: str, code_to_name: Dict[str, str]) -> List[Dict[str, Any]]:
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
        }

        if data is not None:
            self._find_invokes(data, results, context)
            return results

        # --------
        # Fallback regex parsing (best effort)
        # --------
        for m in re.finditer(r"^\s*-\s*Invoke:\s*(.*)$", text, flags=re.MULTILINE):
            invoke_name = (m.group(1) or "").strip()
            block_start = m.end()
            block_text = text[block_start : block_start + 3000]  # lookahead window

            res_uri_match = re.search(
                r"name:\s*ResourceURI\s*\n\s*value:\s*(.*)", block_text, flags=re.IGNORECASE
            )
            integ_match = re.search(
                r"name:\s*integration[_-]?id\s*\n\s*value:\s*(.*)", block_text, flags=re.IGNORECASE
            )

            call_type = None
            target_code = None
            target_version = None
            resource_uri = None

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
                    "adapter_code": None,  # unknown in fallback
                    "application_name": None,  # unknown in fallback
                    "error": None,
                }
            )

        return results