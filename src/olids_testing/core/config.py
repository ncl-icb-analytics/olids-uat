"""Configuration management for OLIDS testing framework."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, validator


class DatabaseConfig(BaseModel):
    """Database configuration."""
    source: str = Field(..., description="Source database name")
    terminology: str = Field(..., description="Terminology database name")
    results: str = Field(..., description="Results database name")
    dictionary: str = Field(..., description="Dictionary database name")


class SchemaConfig(BaseModel):
    """Schema configuration."""
    masked: str = Field(..., description="Masked data schema")
    terminology: str = Field(..., description="Terminology schema")
    tests: str = Field(..., description="Test results schema")


class ConnectionConfig(BaseModel):
    """Snowflake connection configuration."""
    account: Optional[str] = Field(None, description="Snowflake account (optional if using Snow CLI connection)")
    host: Optional[str] = Field(None, description="Snowflake host (for display purposes)")
    role: Optional[str] = Field(None, description="Snowflake role (optional if using Snow CLI connection)")
    warehouse: Optional[str] = Field(None, description="Snowflake warehouse (optional if using Snow CLI connection)")
    snow_cli_connection: Optional[str] = Field(None, description="Snow CLI connection name (uses default connection if not specified)")


class ExecutionConfig(BaseModel):
    """Test execution configuration."""
    parallel_workers: int = Field(4, description="Number of parallel workers")
    timeout_seconds: int = Field(300, description="Test timeout in seconds")
    retry_attempts: int = Field(2, description="Number of retry attempts")


class OutputConfig(BaseModel):
    """Output configuration."""
    default_format: str = Field("table", description="Default output format")
    export_formats: List[str] = Field(["json", "csv"], description="Available export formats")
    max_display_rows: int = Field(100, description="Maximum rows to display")


class EnvironmentConfig(BaseModel):
    """Environment-specific configuration."""
    name: str = Field(..., description="Environment name")
    description: str = Field(..., description="Environment description")
    databases: DatabaseConfig = Field(..., description="Database configuration")
    schemas: SchemaConfig = Field(..., description="Schema configuration")
    connection: ConnectionConfig = Field(..., description="Connection configuration")
    execution: ExecutionConfig = Field(..., description="Execution configuration")
    output: OutputConfig = Field(..., description="Output configuration")

    @validator('name')
    def name_must_be_valid(cls, v):
        """Validate environment name."""
        valid_names = ['dev', 'uat', 'prod']
        if v not in valid_names:
            raise ValueError(f'Environment name must be one of {valid_names}')
        return v


class TestSuiteConfig(BaseModel):
    """Test suite configuration."""
    description: str = Field(..., description="Suite description")
    categories: Optional[List[str]] = Field(None, description="Test categories")
    tests: Optional[List[str]] = Field(None, description="Individual tests")


class TestConfig(BaseModel):
    """Individual test configuration."""
    description: str = Field(..., description="Test description")
    timeout: int = Field(60, description="Test timeout in seconds")
    priority: str = Field("medium", description="Test priority")
    
    # Optional test metadata fields
    relationships_count: Optional[int] = Field(None, description="Number of relationships tested")
    concept_columns: Optional[int] = Field(None, description="Number of concept columns tested")
    pattern_count: Optional[int] = Field(None, description="Number of patterns tested")
    test_count: Optional[int] = Field(None, description="Number of sub-tests")
    
    class Config:
        extra = "allow"  # Allow additional fields not defined in the model
    
    @validator('priority')
    def priority_must_be_valid(cls, v):
        """Validate test priority."""
        valid_priorities = ['low', 'medium', 'high', 'critical']
        if v not in valid_priorities:
            raise ValueError(f'Priority must be one of {valid_priorities}')
        return v


class Config:
    """Main configuration manager."""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize configuration manager.
        
        Args:
            config_dir: Path to configuration directory
        """
        self.config_dir = config_dir or self._get_default_config_dir()
        self.environments_dir = self.config_dir / "environments"
        self.test_suites_file = self.config_dir / "test_suites.yml"
        
        self._environment_configs: Dict[str, EnvironmentConfig] = {}
        self._test_suites: Dict[str, TestSuiteConfig] = {}
        self._test_configs: Dict[str, TestConfig] = {}
        
        self._load_configurations()
    
    def _get_default_config_dir(self) -> Path:
        """Get default configuration directory."""
        # Try to find config directory relative to this file
        current_file = Path(__file__)
        project_root = current_file.parent.parent.parent.parent
        config_dir = project_root / "config"
        
        if config_dir.exists():
            return config_dir
        
        # Fallback to current directory
        return Path.cwd() / "config"
    
    def _load_configurations(self) -> None:
        """Load all configuration files."""
        self._load_environment_configs()
        self._load_test_suites_config()
    
    def _load_environment_configs(self) -> None:
        """Load environment configurations."""
        if not self.environments_dir.exists():
            raise FileNotFoundError(f"Environments directory not found: {self.environments_dir}")
        
        # Consider both .yml and .yaml files, and skip template files
        env_files = list(self.environments_dir.glob("*.yml")) + list(self.environments_dir.glob("*.yaml"))
        for env_file in env_files:
            if env_file.stem.lower() == "template":
                continue
            try:
                with open(env_file, 'r') as f:
                    env_data = yaml.safe_load(f)
                
                env_config = EnvironmentConfig(**env_data)
                self._environment_configs[env_config.name] = env_config
                
            except Exception as e:
                raise ValueError(f"Error loading environment config {env_file}: {e}")
    
    def _load_test_suites_config(self) -> None:
        """Load test suites configuration."""
        if not self.test_suites_file.exists():
            raise FileNotFoundError(f"Test suites config not found: {self.test_suites_file}")
        
        try:
            with open(self.test_suites_file, 'r') as f:
                config_data = yaml.safe_load(f)
            
            # Load test suites
            for suite_name, suite_data in config_data.get('test_suites', {}).items():
                self._test_suites[suite_name] = TestSuiteConfig(**suite_data)
            
            # Load individual test configs
            for test_name, test_data in config_data.get('test_config', {}).items():
                self._test_configs[test_name] = TestConfig(**test_data)
                
        except Exception as e:
            raise ValueError(f"Error loading test suites config: {e}")
    
    def get_environment(self, name: str) -> EnvironmentConfig:
        """Get environment configuration by name.
        
        Args:
            name: Environment name
            
        Returns:
            Environment configuration
            
        Raises:
            KeyError: If environment not found
        """
        if name not in self._environment_configs:
            available = list(self._environment_configs.keys())
            raise KeyError(f"Environment '{name}' not found. Available: {available}")
        
        return self._environment_configs[name]
    
    def get_test_suite(self, name: str) -> TestSuiteConfig:
        """Get test suite configuration by name.
        
        Args:
            name: Test suite name
            
        Returns:
            Test suite configuration
            
        Raises:
            KeyError: If test suite not found
        """
        if name not in self._test_suites:
            available = list(self._test_suites.keys())
            raise KeyError(f"Test suite '{name}' not found. Available: {available}")
        
        return self._test_suites[name]
    
    def get_test_config(self, name: str) -> TestConfig:
        """Get test configuration by name.
        
        Args:
            name: Test name
            
        Returns:
            Test configuration
            
        Raises:
            KeyError: If test not found
        """
        if name not in self._test_configs:
            available = list(self._test_configs.keys())
            raise KeyError(f"Test '{name}' not found. Available: {available}")
        
        return self._test_configs[name]
    
    def list_environments(self) -> List[str]:
        """List available environments.
        
        Returns:
            List of environment names
        """
        return list(self._environment_configs.keys())
    
    def list_test_suites(self) -> List[str]:
        """List available test suites.
        
        Returns:
            List of test suite names
        """
        return list(self._test_suites.keys())
    
    def list_tests(self) -> List[str]:
        """List available individual tests.
        
        Returns:
            List of test names
        """
        return list(self._test_configs.keys())
    
    def validate(self) -> bool:
        """Validate all configurations.
        
        Returns:
            True if all configurations are valid
        """
        try:
            # Validate that all required directories exist
            required_dirs = [self.config_dir, self.environments_dir]
            for dir_path in required_dirs:
                if not dir_path.exists():
                    print(f"Missing directory: {dir_path}")
                    return False
            
            # Validate that we have at least one environment
            if not self._environment_configs:
                print("No environment configurations found")
                return False
            
            # Validate that test suites reference valid tests
            for suite_name, suite_config in self._test_suites.items():
                if suite_config.tests:
                    for test_name in suite_config.tests:
                        if test_name not in self._test_configs:
                            print(f"Suite '{suite_name}' references unknown test '{test_name}'")
                            return False
            
            return True
            
        except Exception as e:
            print(f"Configuration validation error: {e}")
            return False