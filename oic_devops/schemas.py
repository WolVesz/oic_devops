from typing import Dict, List, Any, Optional

# Base schema definitions
API_SCHEMAS = {
    "integrations": {
        "id": "integration_id",
        "name": "integration_name",
        "identifier": "integration_identifier",
        "version": "integration_version",
        "status": "integration_status",
        "integrationType": "integration_integration_type",
        "created": "integration_created_date",
        "createdBy": "integration_created_by",
        "lastUpdated": "integration_last_updated",
        "lastUpdatedBy": "integration_last_updated_by",
        "description": "integration_description",
    },
    "connections": {
        "id": "connection_id",
        "name": "connection_name",
        "identifier": "connection_identifier",
        "connectionType": "connection_connection_type",
        "status": "connection_status",
        "created": "connection_created_date",
        "createdBy": "connection_created_by",
        "lastUpdated": "connection_last_updated",
        "lastUpdatedBy": "connection_last_updated_by",
        "description": "connection_description",
    },
    "instances": {
        "id": "instance_id",
        "integrationId": "instance_integration_id",
        "integrationVersion": "instance_integration_version",
        "status": "instance_status",
        "startTime": "instance_start_time",
        "endTime": "instance_end_time",
        "duration": "instance_duration",
        "errorMessage": "instance_error_message",
    },
    # Add more schemas as needed
}


def get_schema(resource_type: str) -> Dict[str, str]:
    """
    Get the schema definition for a specific resource type.

    Args:
        resource_type: The type of resource to get the schema for.

    Returns:
        Dict[str, str]: The schema definition.
    """
    return API_SCHEMAS.get(resource_type, {})