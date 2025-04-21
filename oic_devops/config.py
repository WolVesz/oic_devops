"""
Configuration management module for the OIC DevOps package.

This module handles loading and validating configuration from yaml files,
with support for multiple profiles.
"""

import os
import yaml
import jsonschema
from pathlib import Path
from typing import Dict, Any, Optional

from oic_devops.exceptions import OICConfigurationError

# Configuration schema for validation
CONFIG_SCHEMA = {
    "type": "object",
    "additionalProperties": {
        "type": "object",
        "required": ["instance_url", "identity_domain", "username", "password"],
        "properties": {
            "instance_url": {"type": "string", "format": "uri"},
            "identity_domain": {"type": "string"},
            "username": {"type": "string"},
            "password": {"type": "string"},
            "scope": {"type": "string"},
            "timeout": {"type": "integer", "minimum": 1},
            "verify_ssl": {"type": "boolean"},
        },
        "additionalProperties": False,
    },
}

DEFAULT_CONFIG_PATHS = [
    os.path.join(os.path.expanduser("~"), ".oic_config.yaml"),
    os.path.join(os.getcwd(), ".oic_config.yaml"),
]

class OICConfig:
    """
    Class for handling OIC configuration management.
    
    Supports loading configuration from YAML files with multiple profiles
    for different environments.
    """
    
    def __init__(self, config_file: Optional[str] = None, profile: str = "default"):
        """
        Initialize OIC configuration.
        
        Args:
            config_file: Path to the configuration file. If None, will look in default locations.
            profile: The profile to use from the configuration file.
        
        Raises:
            OICConfigurationError: If configuration cannot be loaded or is invalid.
        """
        self.config_file = config_file
        self.profile = profile
        self.config = self._load_config()
        
        # Ensure the specified profile exists
        if self.profile not in self.config:
            available_profiles = list(self.config.keys())
            raise OICConfigurationError(
                f"Profile '{self.profile}' not found in config. Available profiles: {available_profiles}"
            )
        
        # Extract the configuration for the specified profile
        self.profile_config = self.config[self.profile]
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load and validate the configuration file.
        
        Returns:
            Dict: The loaded configuration.
            
        Raises:
            OICConfigurationError: If the configuration file cannot be found or is invalid.
        """
        # If config_file is specified, use it
        if self.config_file:
            if not os.path.exists(self.config_file):
                raise OICConfigurationError(f"Configuration file not found: {self.config_file}")
            try:
                with open(self.config_file, "r") as f:
                    config = yaml.safe_load(f)
                    self._validate_config(config)
                    return config
            except Exception as e:
                raise OICConfigurationError(f"Error loading configuration file: {str(e)}")
        
        # Otherwise, try default locations
        for path in DEFAULT_CONFIG_PATHS:
            if os.path.exists(path):
                try:
                    with open(path, "r") as f:
                        config = yaml.safe_load(f)
                        self._validate_config(config)
                        return config
                except Exception as e:
                    raise OICConfigurationError(f"Error loading configuration file {path}: {str(e)}")
        
        # If no configuration file is found, provide helpful error
        template_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config_template.yaml")
        raise OICConfigurationError(
            "No configuration file found. Please create a .oic_config.yaml file in your home directory "
            f"or current working directory. See {template_path} for a template."
        )
    
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate the configuration against the schema.
        
        Args:
            config: The configuration to validate.
            
        Raises:
            OICConfigurationError: If the configuration is invalid.
        """
        try:
            jsonschema.validate(config, CONFIG_SCHEMA)
        except jsonschema.exceptions.ValidationError as e:
            raise OICConfigurationError(f"Invalid configuration: {str(e)}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a value from the current profile configuration.
        
        Args:
            key: The key to get.
            default: The default value to return if the key doesn't exist.
            
        Returns:
            The value for the key, or the default if the key doesn't exist.
        """
        return self.profile_config.get(key, default)
    
    def get_all_profiles(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all available profiles.
        
        Returns:
            Dict: All profiles in the configuration.
        """
        return self.config
    
    def get_available_profiles(self) -> list:
        """
        Get a list of available profile names.
        
        Returns:
            List: Names of available profiles.
        """
        return list(self.config.keys())
    
    @property
    def instance_url(self) -> str:
        """Get the instance URL for the current profile."""
        return self.profile_config["instance_url"]
    
    @property
    def identity_domain(self) -> str:
        """Get the identity domain for the current profile."""
        return self.profile_config["identity_domain"]
    
    @property
    def username(self) -> str:
        """Get the username for the current profile."""
        return self.profile_config["username"]
    
    @property
    def password(self) -> str:
        """Get the password for the current profile."""
        return self.profile_config["password"]
    
    @property
    def scope(self) -> Optional[str]:
        """Get the scope for the current profile, if specified."""
        return self.profile_config.get("scope")
    
    @property
    def timeout(self) -> int:
        """Get the timeout for the current profile, defaults to 300 seconds."""
        return self.profile_config.get("timeout", 300)
    
    @property
    def verify_ssl(self) -> bool:
        """Get whether to verify SSL certificates, defaults to True."""
        return self.profile_config.get("verify_ssl", True)
