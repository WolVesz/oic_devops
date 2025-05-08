import re


def camel_to_snake(name: str) -> str:
    """
    concerts camelCaseItems to snake_case_items like god intended
    """
    s = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s = re.sub(r'([a-z0-9])([A-Z)])', r'\1_\2', s)
    s = re.sub(r'([a-zA-Z])([0-9)])', r'\1_\2', s)
    s = re.sub(r'([0-9])([a-zA-Z])', r'\1_\2', s)
    return s.replace('-', '_').lower()
