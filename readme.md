# OIC DevOps

A production-grade Python package for Oracle Integration Cloud (OIC) DevOps that utilizes the OIC REST API to manage all integration tasks including Connections, Integrations, Libraries, Lookups, Monitoring, and Packages.

## Installation

```bash
pip install oic-devops
```

## Configuration

Before using the package, you need to set up a configuration file with your OIC credentials.

Create a file named `.oic_config.yaml` in your home directory or at a custom location using the following template:

```yaml
default:
  instance_url: "https://your-instance-name.integration.ocp.oraclecloud.com"
  identity_domain: "your-identity-domain"
  username: "your-username"
  password: "your-password"
  scope: "your-scope"  # Optional

dev:
  instance_url: "https://your-dev-instance.integration.ocp.oraclecloud.com"
  identity_domain: "your-dev-identity-domain"
  username: "your-dev-username"
  password: "your-dev-password"
  scope: "your-dev-scope"  # Optional

prod:
  instance_url: "https://your-prod-instance.integration.ocp.oraclecloud.com"
  identity_domain: "your-prod-identity-domain"
  username: "your-prod-username"
  password: "your-prod-password"
  scope: "your-prod-scope"  # Optional
```

Alternatively, you can create a configuration file in a custom location and specify that path when initializing the client.

## Usage

### Python API

```python
from oic_devops.client import OICClient

# Use the default profile
client = OICClient()

# Or specify a profile
client = OICClient(profile="dev")

# Or specify a custom configuration file
client = OICClient(config_file="/path/to/config-template.yaml", profile="prod")

# Working with integrations
integrations = client.integrations.list()
integration = client.integrations.get("INTEGRATION_ID")
client.integrations.activate("INTEGRATION_ID")
client.integrations.deactivate("INTEGRATION_ID")
client.integrations.export("INTEGRATION_ID", "/path/to/export/file.iar")
client.integrations.import_integration("/path/to/import/file.iar")

# Working with connections
connections = client.connections.list()
connection = client.connections.get("CONNECTION_ID")
client.connections.create(connection_data)
client.connections.update("CONNECTION_ID", updated_connection_data)
client.connections.delete("CONNECTION_ID")
client.connections.test("CONNECTION_ID")

# Similar patterns for other resources
```

### Command Line Interface

The package also provides a command-line interface for common operations:

```bash
# List all integrations
oic-devops integrations list

# Get integration details
oic-devops integrations get INTEGRATION_ID

# Activate an integration
oic-devops integrations activate INTEGRATION_ID

# Deactivate an integration
oic-devops integrations deactivate INTEGRATION_ID

# Export an integration
oic-devops integrations export INTEGRATION_ID -o /path/to/export/file.iar

# Import an integration
oic-devops integrations import /path/to/import/file.iar

# List all connections
oic-devops connections list

# Similar commands for other resources
```

### Using Multiple Profiles

You can switch between profiles using the `--profile` or `-p` option:

```bash
oic-devops --profile dev integrations list
oic-devops -p prod connections list
```

## Documentation

For more detailed documentation, please refer to:

- [OIC DevOps Python API Documentation](https://oic-devops.readthedocs.io/)
- [OIC REST API Documentation](https://docs.oracle.com/en/cloud/paas/integration-cloud/rest-api/index.html)

## License

This project is licensed under the MIT License - see the LICENSE file for details.
