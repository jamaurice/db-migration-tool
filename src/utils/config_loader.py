"""
Configuration loading and validation
"""

import yaml
import logging
from typing import Dict, Any
from pathlib import Path


class ConfigLoader:
    """Loads and validates migration configuration"""
    
    @staticmethod
    def load(config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        config_file = Path(config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        
        # Validate configuration
        ConfigLoader._validate_config(config)
        
        return config
    
    @staticmethod
    def _validate_config(config: Dict[str, Any]):
        """Validate configuration structure"""
        required_sections = ['source', 'target']
        
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Required configuration section missing: {section}")
        
        # Validate source configuration
        source_required = ['server', 'database', 'username', 'password']
        for field in source_required:
            if field not in config['source']:
                raise ValueError(f"Required source field missing: {field}")
        
        # Validate target configuration
        target_required = ['host', 'database', 'username', 'password']
        for field in target_required:
            if field not in config['target']:
                raise ValueError(f"Required target field missing: {field}")
