"""Snowflake connection management for OLIDS testing framework."""

from __future__ import annotations

import os
from typing import Dict, Optional

from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSessionException

from .config import EnvironmentConfig


class SnowflakeConnection:
    """Manages Snowflake connections for testing."""
    
    def __init__(self, env_config: EnvironmentConfig):
        """Initialize connection manager.
        
        Args:
            env_config: Environment configuration
        """
        self.env_config = env_config
        self._session: Optional[Session] = None
    
    def create_session(self) -> Session:
        """Create a new Snowflake session using Snow CLI authentication.
        
        Returns:
            Snowflake session
            
        Raises:
            SnowparkSessionException: If connection fails
        """
        connection_params = {}
        
        # Only add environment config values if they exist
        if self.env_config.connection.account:
            connection_params["account"] = self.env_config.connection.account
        if self.env_config.connection.role:
            connection_params["role"] = self.env_config.connection.role
        if self.env_config.connection.warehouse:
            connection_params["warehouse"] = self.env_config.connection.warehouse
        
        # Use Snow CLI connection if available
        try:
            from snowflake.cli._app.snow_connector import get_connection_dict
            import subprocess
            import json
            
            # Use environment-specific connection name if specified, otherwise fallback to env var or find default
            connection_name = (
                self.env_config.connection.snow_cli_connection or 
                os.getenv("SNOWFLAKE_CONNECTION")
            )
            
            # If no specific connection specified, find the default Snow CLI connection
            if not connection_name:
                try:
                    result = subprocess.run(['snow', 'connection', 'list', '--format', 'json'], 
                                          capture_output=True, text=True, check=True)
                    connections = json.loads(result.stdout)
                    
                    # Find default connection
                    for conn in connections:
                        if conn.get('is_default', False):
                            connection_name = conn['connection_name']
                            break
                    
                    # If no default found, use the first available connection
                    if not connection_name and connections:
                        connection_name = connections[0]['connection_name']
                        
                except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError):
                    # Fallback: if Snow CLI commands fail, we'll handle it in the except block below
                    pass
            
            if connection_name:
                # Get connection configuration from Snow CLI
                cli_conn = get_connection_dict(connection_name)
                
                # Use Snow CLI connection parameters
                connection_params.update({
                    "account": cli_conn.get("account", connection_params.get("account")),
                    "user": cli_conn.get("user"),
                    "authenticator": cli_conn.get("authenticator", "externalbrowser"),  # Default to SSO
                    "role": cli_conn.get("role", connection_params.get("role")),
                    "warehouse": cli_conn.get("warehouse", connection_params.get("warehouse")),
                })
            else:
                # No Snow CLI connection found, fallback to SSO
                connection_params["authenticator"] = "externalbrowser"
                
        except ImportError:
            # Snow CLI not available, use environment variables or SSO
            connection_params["authenticator"] = os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")
            
            # Only add user if provided - SSO will handle authentication
            if os.getenv("SNOWFLAKE_USER"):
                connection_params["user"] = os.getenv("SNOWFLAKE_USER")
        
        try:
            session = Session.builder.configs(connection_params).create()
            self._session = session
            return session
            
        except Exception as e:
            raise SnowparkSessionException(f"Failed to create Snowflake session: {e}")
    
    def get_session(self) -> Session:
        """Get current session or create new one.
        
        Returns:
            Snowflake session
        """
        if self._session is None:
            self._session = self.create_session()
        
        return self._session
    
    def close_session(self) -> None:
        """Close current session."""
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass  # Ignore errors during cleanup
            finally:
                self._session = None
    
    def test_connection(self) -> Dict[str, str]:
        """Test the connection and return status.
        
        Returns:
            Connection status information
        """
        try:
            session = self.get_session()
            
            # Test basic query
            result = session.sql("SELECT CURRENT_VERSION() as version").collect()
            version = result[0]["VERSION"] if result else "Unknown"
            
            # Get current context
            context_result = session.sql("""
                SELECT 
                    CURRENT_ACCOUNT() as account,
                    CURRENT_USER() as user,
                    CURRENT_ROLE() as role,
                    CURRENT_WAREHOUSE() as warehouse,
                    CURRENT_DATABASE() as database,
                    CURRENT_SCHEMA() as schema
            """).collect()
            
            if context_result:
                context = context_result[0]
                return {
                    "status": "OK",
                    "version": version,
                    "account": context["ACCOUNT"],
                    "user": context["USER"],
                    "role": context["ROLE"],
                    "warehouse": context["WAREHOUSE"],
                    "database": context["DATABASE"] or "not set",
                    "schema": context["SCHEMA"] or "not set",
                    "host": getattr(self.env_config.connection, 'host', None) or "not set",
                }
            else:
                return {
                    "status": "ERROR",
                    "error": "Failed to get connection context",
                }
                
        except Exception as e:
            return {
                "status": "ERROR", 
                "error": str(e),
            }
    
    def set_context(self, database: str, schema: str) -> None:
        """Set database and schema context.
        
        Args:
            database: Database name
            schema: Schema name
        """
        session = self.get_session()
        session.sql(f'USE DATABASE "{database}"').collect()
        session.sql(f'USE SCHEMA "{schema}"').collect()
    
    def execute_sql(self, sql: str) -> list:
        """Execute SQL and return results.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            Query results
        """
        session = self.get_session()
        return session.sql(sql).collect()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_session()