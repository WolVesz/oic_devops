# oic_invoke_parser.py
import os
import re
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
import yaml

from oic_devops.client import OICClient  # assumes you have this available


# Match REST/SOAP calls to OIC flows: /api/integration/v1/flows/rest/<code>/<version>/...
INTEGRATION_RESOURCE_RE = re.compile(
    r"/api/integration/v1/flows/(?:rest|soap)/(?P<code>[^/]+)/(?P<version>[^/]+)/?",
    re.IGNORECASE
)


def _normalize_key(k: Any) -> str:
    """Lowercase key and remove non-alphanumerics to normalize variants (e.g., integrationId, integration-id)."""
    s = str(k) if not isinstance(k, str) else k
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _to_properties_map(section: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten *.properties (list of {name, value}) into {name: value}.
    Some IAR exports put properties under `application.properties`.
    """
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


def _norm_integration_id(raw: Any) -> Tuple[str, Optional[str]]:
    """Accept 'CODE|VERSION', 'CODE_VERSION', or just 'CODE'."""
    s = str(raw) if not isinstance(raw, str) else raw
    if "|" in s:
        code, ver = s.rsplit("|", 1)
        return code, ver
    if "_" in s:
        code, ver = s.rsplit("_", 1)
        return code, ver
    return s, None


def _get_integrations_code_names() -> pd.DataFrame:
    """
    Returns DataFrame of ACTIVATED integrations with at least columns: code, name.
    """
    client = OICClient(profile="prod")
    df = client.integrations.df()
    df = df[df["status"] == "ACTIVATED"].copy()
    for col in ("code", "name"):
        if col not in df.columns:
            df[col] = None
    return df[["code", "name"]].drop_duplicates()


def _build_code_to_name_map(integ_df: pd.DataFrame) -> Dict[str, str]:
    """Build fast lookup {code: name}."""
    integ_df = integ_df.dropna(subset=["code"])
    return dict(zip(integ_df["code"], integ_df["name"]))


def _lookup_integration_name(code_to_name: Dict[str, str], code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    return code_to_name.get(code)


def _get_from_props(props: Dict[str, Any], *candidates: str) -> Optional[Any]:
    """
    Case-/style-insensitive property lookup.
    Normalize each prop key by lowercasing and removing non-alphanumerics.
    """
    if not props:
        return None
    normalized = {_normalize_key(k): v for k, v in props.items() if isinstance(k, str)}
    for c in candidates:
        v = normalized.get(_normalize_key(c))
        if v is not None:
            return v
    return None


def _extract_integration_id(props_map: Dict[str, Any]) -> Optional[str]:
    """Find integration id under common variants."""
    for k, v in list(props_map.items()):
        if _normalize_key(k) == "integrationid":
            return v
    return None


def _find_invokes(obj: Any, results: List[Dict[str, Any]], context: Dict[str, Any]):
    """
    Recursive descent over the YAML structure to collect Invoke nodes
    and detect OIC-to-OIC invocations (Pattern 1 and 2), or classify
    remaining invokes as Pattern 3 (Adapter_Invoke).
    """
    if isinstance(obj, dict):
        if "Invoke" in obj:
            invoke_name = obj.get("Invoke")
            application = obj.get("application", {}) or {}
            adapter = application.get("adapter", {}) or {}

            # Property flattening
            props_map = _to_properties_map(application)

            # Collect common fields first (always included)
            adapter_code = adapter.get("code")
            application_name = application.get("name")

            call_type = None
            target_code = None
            target_version = None
            target_name = None  # kept for completeness; not required in CSV but available if you choose to add
            resource_uri = _get_from_props(props_map, "ResourceURI", "resourceUri", "resource_uri")

            # ---------------------------
            # Pattern 1: integration_id
            # ---------------------------
            integ_id = _extract_integration_id(props_map)
            if isinstance(integ_id, (str, int, float)) and str(integ_id).strip():
                code, ver = _norm_integration_id(str(integ_id))
                call_type = "IntegrationAction"
                target_code, target_version = code, ver

            # -----------------------------------------------------------
            # Pattern 2: REST URI into OIC flows API (/flows/<type>/..)
            # -----------------------------------------------------------
            if not call_type and isinstance(resource_uri, str):
                m = INTEGRATION_RESOURCE_RE.search(resource_uri)
                if m:
                    call_type = "RESTToOIC"
                    target_code = m.group("code")
                    target_version = m.group("version")

            # -----------------------------------------------------------
            # Pattern 3: Otherwise → Adapter_Invoke
            # -----------------------------------------------------------
            if not call_type:
                call_type = "Adapter_Invoke"
                # target_* remain None; resource_uri stays whatever we have (possibly None)

            # Emit row for any Invoke we encounter (all patterns)
            code_to_name: Dict[str, str] = context["code_to_name"]
            source_integration_code = context.get("source_integration_code")

            results.append({
                "source_file": context.get("source_file"),
                "source_integration": context.get("source_integration"),
                "source_integration_code": source_integration_code,
                "source_integration_name": _lookup_integration_name(code_to_name, source_integration_code),
                "source_integration_version": context.get("source_integration_version"),
                "invoke": invoke_name,
                "call_type": call_type,
                "target_integration_code": target_code,
                "target_version": target_version,
                "target_integration_name": _lookup_integration_name(code_to_name, target_code),
                "resource_uri": resource_uri,
                "adapter_code": adapter_code,
                "application_name": application_name,
                "error": None,
            })

        # Recurse through mapping
        for v in obj.values():
            _find_invokes(v, results, context)

    elif isinstance(obj, list):
        for item in obj:
            _find_invokes(item, results, context)


def parse_yaml_file(file_path: str, integ_df: pd.DataFrame, code_to_name: Dict[str, str]) -> List[Dict[str, Any]]:
    """Parse one IAR YAML and return discovered invocations as rows."""
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

    source_integration = "|".join(os.path.basename(file_path).replace(".iar.yaml", "").rsplit("_", 1))

    context = {
        "source_file": os.path.basename(file_path),
        "source_integration": source_integration,
        "source_integration_code": code,
        "source_integration_version": version,
        "code_to_name": code_to_name
    }

    if data is not None:
        _find_invokes(data, results, context)
    else:
        # -----------------------------
        # Fallback (best-effort) regex
        # -----------------------------
        # NOTE: Fixed double-escaped patterns -> use \s in raw strings.
        for m in re.finditer(r"^\s*-\s*Invoke:\s*(.*)$", text, flags=re.MULTILINE):
            invoke_name = (m.group(1) or "").strip()
            block_start = m.end()
            block_text = text[block_start:block_start + 3000]

            # Try to extract ResourceURI and integration_id from the following block text
            res_uri_match = re.search(
                r"name:\s*ResourceURI\s*\n\s*value:\s*(.*)",
                block_text, flags=re.IGNORECASE
            )
            integ_match = re.search(
                r"name:\s*integration[_-]?id\s*\n\s*value:\s*(.*)",
                block_text, flags=re.IGNORECASE
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
                code_norm, ver = _norm_integration_id((integ_match.group(1) or "").strip())
                target_code, target_version = code_norm, ver

            # Pattern 3: Otherwise
            if not call_type:
                call_type = "Adapter_Invoke"

            results.append({
                "source_file": context.get("source_file"),
                "source_integration": context.get("source_integration"),
                "source_integration_code": context.get("source_integration_code"),
                "source_integration_name": _lookup_integration_name(code_to_name, context.get("source_integration_code")),
                "source_integration_version": version,
                "invoke": invoke_name,
                "call_type": call_type,
                "target_integration_code": target_code,
                "target_version": target_version,
                "target_integration_name": _lookup_integration_name(code_to_name, target_code),
                "resource_uri": resource_uri,
                "adapter_code": None,           # unknown in fallback
                "application_name": None,       # unknown in fallback
                "error": None,
            })

    return results


def build_oic_call_dataframe(input_dir: str, pattern: str = ".yaml") -> pd.DataFrame:
    """
    Walk a directory, parse *.yaml files, and return a DataFrame with columns:
      source_file, source_integration, source_integration_code, source_integration_name, source_integration_version,
      invoke, call_type, target_integration_code, target_version, target_integration_name,
      resource_uri, adapter_code, application_name, error
    """
    integrations_df = _get_integrations_code_names()
    code_to_name = _build_code_to_name_map(integrations_df)

    rows: List[Dict[str, Any]] = []
    for root, _dirs, files in os.walk(input_dir):
        for fn in files:
            if fn.lower().endswith(pattern.lower()):
                full = os.path.join(root, fn)
                print("parsing file:", full)
                try:
                    rows.extend(parse_yaml_file(full, integrations_df, code_to_name))
                except Exception as e:
                    rows.append({
                        "source_file": fn,
                        "source_integration": fn.replace(".iar.yaml", ""),
                        "source_integration_code": None,
                        "source_integration_name": None,
                        "source_integration_version": None,
                        "invoke": None,
                        "call_type": "ParseError",
                        "target_integration_code": None,
                        "target_version": None,
                        "target_integration_name": None,
                        "resource_uri": None,
                        "adapter_code": None,
                        "application_name": None,
                        "error": str(e),
                    })

    df = pd.DataFrame(rows, columns=[
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
    ])

    return df


if __name__ == "__main__":
    in_dir = "./output/oic_integrations_backup_20260127_091729/integrations/src"
    df = build_oic_call_dataframe(in_dir, pattern=".yaml")
    output_path = os.path.join(in_dir, "AA-INVOKE-XREFv4.csv")
    df.to_csv(output_path, index=False)
    print(f"Wrote {len(df):,} rows to {output_path}")