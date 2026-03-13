# oic_invoke_parser.py
import os, re, sys
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
import yaml

# Match REST calls to OIC flows: /api/integration/v1/flows/rest/<code>/<ver>/...
INTEGRATION_RESOURCE_RE = re.compile(
    r"/api/integration/v1/flows/(?:rest|soap)/(?P<code>[^/]+)/(?P<version>[^/]+)/?",
    re.IGNORECASE
)

# Accept common variants of the integration-id property
INTEG_ID_KEYS = {"integration_id"}

def _to_properties_map(adapter_section: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten adapter.properties (list of name/value dicts) into {name: value}."""
    if not isinstance(adapter_section, dict):
        return {}
    props = adapter_section.get("properties", [])
    out = {}
    if isinstance(props, list):
        for item in props:
            if isinstance(item, dict):
                name = item.get("name")
                val = item.get("value")
                if isinstance(name, str):
                    out[name] = val
    return out

def _norm_integration_id(raw: str) -> Tuple[str, Optional[str]]:
    """
    Normalize a value like 'TSGXXX_SERVICE_01.00.0000'
    into (code, version?). If no dotted version is present, version is None.
    """
    if not isinstance(raw, str):
        return str(raw), None
    cleaned = re.sub(r"\\s+", "", raw.strip())  # strip spaces/newlines

    # m = re.search(r"^(?P<code>.*?)(?:[_-]?)(?P<ver>\\d+\\.\\d+(?:\\.\\d+)*)$", cleaned)
    # if m:
    #     return m.group("code"), m.group("ver")
    # return cleaned, None

    # Split at the last underscore
    integration_code, version = cleaned.rsplit("_", 1)
    return integration_code, version

def _find_invokes(obj: Any, results: List[Dict[str, Any]], context: Dict[str, Any]):
    """
    Recursive descent over the YAML structure to collect Invoke nodes
    and detect OIC-to-OIC invocations.
    """

    if isinstance(obj, dict):
        # Found an Invoke block?

        if "Invoke" in obj:
            invoke_name = obj.get("Invoke")
            adapter = obj.get("adapter", {})
            application = obj.get("application", {})
            props_map = _to_properties_map(application)

            # if application['Application'] == 'InvokeErrorNotificationService(PRESEEDED_COLLOCATED_CONN_1741)':
            #     print ("Application:", application)
            #     print("\n_find_invokes() - Invoke found. properties: ", properties)
            #     print("\nResourceURI ", props_map.get("ResourceURI"))
            #     print("\nintegration_id ", props_map.get("integration_id"))
            #     print("\nprops_map ", props_map)

            # Lower-case keys for robust lookup
            # lowered = {str(k).lower(): v for k, v in props_map.items()}


            call_type = None
            target_code = None
            target_version = None
            resource_uri = props_map.get("ResourceURI")


            # Pattern 1: integration_id / integrationId
            integ_id = None
            for k, v in list(props_map.items()):
                # k_norm = k.replace("_", "").replace("-", "")
                if k in INTEG_ID_KEYS:
                    integ_id = v
                    break
            if integ_id:
                code, ver = _norm_integration_id(str(integ_id))
                call_type = "IntegrationAction"
                target_code, target_version = code, ver


            # Pattern 2: REST URI into OIC flows API
            if not call_type and isinstance(resource_uri, str):
                m = INTEGRATION_RESOURCE_RE.search(resource_uri)
                if m:
                    call_type = "RESTToOIC"
                    target_code = m.group("code")
                    target_version = m.group("version")

            if call_type:
                results.append({
                    "source_file": context.get("source_file"),
                    "source_integration": context.get("source_integration"),
                    "source_integration_code": context.get("source_integration_code"),
                    "source_integration_version": context.get("source_integration_version"),
                    "invoke": invoke_name,
                    "call_type": call_type,
                    "target_integration_code": target_code,
                    "target_version": target_version,
                    "resource_uri": resource_uri,
                    "adapter_code": adapter.get("code"),
                    "application_name": application.get("name"),
                })

        # Recurse through mapping
        for v in obj.values():
            _find_invokes(v, results, context)

    elif isinstance(obj, list):
        for item in obj:
            _find_invokes(item, results, context)

def parse_yaml_file(file_path: str) -> List[Dict[str, Any]]:
    """Parse one IAR YAML and return discovered OIC invocations as rows."""
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        text = f.read()
    try:
        data = yaml.safe_load(text)
    except Exception:
        data = None  # fall back to regex if YAML contains odd constructs

    results: List[Dict[str, Any]] = []
    base = os.path.basename(file_path).replace(".iar.yaml", "")
    source_integration_name, version = _norm_integration_id(base)
    # source_integration= "|".join(os.path.basename(file_path).replace(".iar.yaml", "").rsplit("_", 1),)
    source_integration = source_integration_name + "|" +version
    context = {
        "source_file": os.path.basename(file_path),
        "source_integration":source_integration,
        "source_integration_code": source_integration_name,
        "source_integration_version": version
    }

    if data is not None:
        _find_invokes(data, results, context)
    else:
        # Fallback (best-effort) regex for really quirky YAML
        for m in re.finditer(r"^\\s*-\\s*Invoke:\\s*(.*)$", text, flags=re.MULTILINE):
            invoke_name = m.group(1).strip()
            block_start = m.end()
            block_text = text[block_start:block_start + 2000]
            res_uri_match = re.search(r"name:\\s*ResourceURI\\s*\\n\\s*value:\\s*(.*)", block_text)
            integ_match = re.search(r"name:\\s*integration[_-]?id\\s*\\n\\s*value:\\s*(.*)",
                                    block_text, flags=re.IGNORECASE)
            call_type = None
            target_code = None
            target_version = None
            resource_uri = None
            if res_uri_match:
                resource_uri = res_uri_match.group(1).strip()
                m2 = INTEGRATION_RESOURCE_RE.search(resource_uri)
                if m2:
                    call_type = "RESTToOIC"
                    target_code = m2.group("code")
                    target_version = m2.group("version")
            if integ_match and not call_type:
                call_type = "IntegrationAction"
                code, ver = _norm_integration_id(integ_match.group(1).strip())
                target_code, target_version = code, ver
            if call_type:
                results.append({
                    "source_file": context.get("source_file"),
                    "source_integration": context.get("source_integration"),
                    "source_integration_code":context.get("source_integration_code"),
                    "source_integration_version": context.get("source_integration_version"),
                    "invoke": invoke_name,
                    "call_type": call_type,
                    "target_integration_code": target_code,
                    "target_version": target_version,
                    "resource_uri": resource_uri,
                    "adapter_code": None,
                    "application_name": None,
                })
    return results

def build_oic_call_dataframe(input_dir: str, pattern: str = ".yaml") -> pd.DataFrame:
    """
    Walk a directory, parse *.yaml files, and return a DataFrame with columns:
      source_file, source_integration, invoke, call_type,
      target_integration_code, target_version, resource_uri, adapter_code, application_name, error
    """
    rows: List[Dict[str, Any]] = []
    for root, _dirs, files in os.walk(input_dir):
        for fn in files:
            if fn.lower().endswith(pattern):
                full = os.path.join(root, fn)
                print("parsing file:", full)
                try:
                    rows.extend(parse_yaml_file(full))
                except Exception as e:
                    rows.append({
                        "source_file": fn,
                        "source_integration": fn.replace(".iar.yaml", ""),
                        "source_integration_code": None,
                        "source_integration_version": None,
                        "invoke": None,
                        "call_type": "ParseError",
                        "target_integration_code": None,
                        "target_version": None,
                        "resource_uri": None,
                        "adapter_code": None,
                        "application_name": None,
                        "error": str(e),
                    })
    df = pd.DataFrame(rows, columns=[
        "source_file",
        "source_integration",
        "source_integration_code",
        "source_integration_version",
        "invoke",
        "call_type",
        "target_integration_code",
        "target_version",
        "resource_uri",
        "adapter_code",
        "application_name",
        "error",
    ])
    return df


####################################################
in_dir = "./output/oic_integrations_backup_20260127_091729/integrations/src"
df = build_oic_call_dataframe(in_dir)
output_path = in_dir + "/AA-INVOKE-XREFv2.csv"
df.to_csv(output_path, index=False)

