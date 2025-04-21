"""
Validators module for the OIC DevOps package.

This module provides validation functions for various inputs.
"""

import re
from typing import Optional, Dict, Any, Union

from oic_devops.exceptions import OICValidationError


def validate_identifier(identifier: str) -> bool:
    """
    Validate an OIC resource identifier.
    
    Args:
        identifier: The identifier to validate.
        
    Returns:
        bool: True if the identifier is valid.
        
    Raises:
        OICValidationError: If the identifier is invalid.
    """
    # OIC identifiers must start with a letter and can only contain
    # letters, numbers, and underscores
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', identifier):
        raise OICValidationError(
            "Invalid identifier. Identifiers must start with a letter and "
            "can only contain letters, numbers, and underscores."
        )
    
    return True


def validate_name(name: str) -> bool:
    """
    Validate an OIC resource name.
    
    Args:
        name: The name to validate.
        
    Returns:
        bool: True if the name is valid.
        
    Raises:
        OICValidationError: If the name is invalid.
    """
    # OIC names can contain most characters but cannot be empty
    if not name or not name.strip():
        raise OICValidationError("Name cannot be empty.")
    
    # Maximum length is typically 255 characters
    if len(name) > 255:
        raise OICValidationError("Name is too long. Maximum length is 255 characters.")
    
    return True


def validate_required_fields(data: Dict[str, Any], required_fields: list) -> bool:
    """
    Validate that required fields are present in data.
    
    Args:
        data: The data to validate.
        required_fields: List of required field names.
        
    Returns:
        bool: True if all required fields are present.
        
    Raises:
        OICValidationError: If any required fields are missing.
    """
    for field in required_fields:
        if field not in data:
            raise OICValidationError(f"Missing required field: {field}")
    
    return True


def validate_field_type(
    data: Dict[str, Any],
    field: str,
    expected_type: Union[type, tuple],
    required: bool = True,
) -> bool:
    """
    Validate that a field has the expected type.
    
    Args:
        data: The data to validate.
        field: The name of the field to validate.
        expected_type: The expected type or tuple of types.
        required: Whether the field is required.
        
    Returns:
        bool: True if the field has the expected type or is not present and not required.
        
    Raises:
        OICValidationError: If the field has the wrong type.
    """
    if field not in data:
        if required:
            raise OICValidationError(f"Missing required field: {field}")
        return True
    
    if not isinstance(data[field], expected_type):
        raise OICValidationError(
            f"Field '{field}' has incorrect type. Expected {expected_type}, "
            f"got {type(data[field])}."
        )
    
    return True


def validate_enum_field(
    data: Dict[str, Any],
    field: str,
    allowed_values: list,
    required: bool = True,
) -> bool:
    """
    Validate that a field has one of the allowed values.
    
    Args:
        data: The data to validate.
        field: The name of the field to validate.
        allowed_values: List of allowed values.
        required: Whether the field is required.
        
    Returns:
        bool: True if the field has an allowed value or is not present and not required.
        
    Raises:
        OICValidationError: If the field has a disallowed value.
    """
    if field not in data:
        if required:
            raise OICValidationError(f"Missing required field: {field}")
        return True
    
    if data[field] not in allowed_values:
        raise OICValidationError(
            f"Field '{field}' has invalid value. Allowed values: {allowed_values}, "
            f"got: {data[field]}."
        )
    
    return True


def validate_url(url: str) -> bool:
    """
    Validate a URL.
    
    Args:
        url: The URL to validate.
        
    Returns:
        bool: True if the URL is valid.
        
    Raises:
        OICValidationError: If the URL is invalid.
    """
    # Basic URL validation - starts with http:// or https://
    if not url.startswith(('http://', 'https://')):
        raise OICValidationError("Invalid URL. URL must start with http:// or https://")
    
    return True


def validate_email(email: str) -> bool:
    """
    Validate an email address.
    
    Args:
        email: The email address to validate.
        
    Returns:
        bool: True if the email address is valid.
        
    Raises:
        OICValidationError: If the email address is invalid.
    """
    # Basic email validation
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_pattern, email):
        raise OICValidationError(
            "Invalid email address. Email addresses must be in the format "
            "username@domain.tld"
        )
    
    return True


def validate_date_format(date_str: str, format_str: str = "%Y-%m-%dT%H:%M:%S") -> bool:
    """
    Validate a date string against a format.
    
    Args:
        date_str: The date string to validate.
        format_str: The expected format of the date string.
        
    Returns:
        bool: True if the date string matches the format.
        
    Raises:
        OICValidationError: If the date string does not match the format.
    """
    import datetime
    
    try:
        datetime.datetime.strptime(date_str, format_str)
        return True
    except ValueError:
        raise OICValidationError(
            f"Invalid date format. Expected format: {format_str}, "
            f"got: {date_str}"
        )
