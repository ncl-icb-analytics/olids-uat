"""SQL query logging utility for OLIDS testing framework."""

import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional


class SQLLogger:
    """Logs SQL queries executed during tests to files for debugging and analysis."""
    
    def __init__(self, output_dir: Optional[Path] = None):
        """Initialize SQL logger.
        
        Args:
            output_dir: Directory to save SQL files. Defaults to 'sql_logs' in project root.
        """
        if output_dir is None:
            # Default to sql_logs directory in project root
            project_root = Path(__file__).parent.parent.parent.parent
            output_dir = project_root / "sql_logs"
        
        self.output_dir = Path(output_dir)
        self.query_counter = 0
        self._ensure_clean_directory()
    
    def _ensure_clean_directory(self):
        """Ensure SQL logs directory exists and is clean."""
        if self.output_dir.exists():
            # Clear existing SQL files
            shutil.rmtree(self.output_dir)
        
        # Create fresh directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a README explaining the files
        readme_content = f"""# SQL Query Logs

Generated: {datetime.now().isoformat()}

This directory contains SQL queries executed during the last test run.
Files are organized by test and numbered in execution order.

## File Naming Convention:
- `001_<test_name>_<description>.sql`
- Numbers indicate execution order
- Test name identifies which test generated the query
- Description provides context about the query purpose

## Contents:
- Each .sql file contains the exact query sent to Snowflake
- Comments at the top provide metadata (timestamp, test context, etc.)
"""
        
        (self.output_dir / "README.md").write_text(readme_content)
    
    def log_query(self, query: str, test_name: str, description: str = "", metadata: Optional[dict] = None) -> Path:
        """Log a SQL query to a file.
        
        Args:
            query: The SQL query to log
            test_name: Name of the test executing the query
            description: Brief description of what the query does
            metadata: Optional metadata to include in the file header
            
        Returns:
            Path to the created SQL file
        """
        self.query_counter += 1
        
        # Clean up test name and description for filename
        clean_test_name = "".join(c if c.isalnum() or c in '-_' else '_' for c in test_name)
        clean_description = "".join(c if c.isalnum() or c in '-_' else '_' for c in description)
        
        # Create filename
        filename = f"{self.query_counter:03d}_{clean_test_name}"
        if clean_description:
            filename += f"_{clean_description}"
        filename += ".sql"
        
        file_path = self.output_dir / filename
        
        # Prepare file content with metadata header
        content_lines = [
            "-- SQL Query Log",
            f"-- Generated: {datetime.now().isoformat()}",
            f"-- Test: {test_name}",
            f"-- Description: {description}" if description else "-- Description: N/A",
            f"-- Execution Order: {self.query_counter}",
        ]
        
        if metadata:
            content_lines.append("-- Metadata:")
            for key, value in metadata.items():
                content_lines.append(f"--   {key}: {value}")
        
        content_lines.extend([
            "--",
            "-- Query:",
            "",
            query.strip(),
            ""
        ])
        
        # Write to file
        file_path.write_text("\n".join(content_lines), encoding='utf-8')
        
        return file_path
    
    def get_log_summary(self) -> str:
        """Get a summary of logged queries.
        
        Returns:
            Summary string with query counts and file locations
        """
        sql_files = list(self.output_dir.glob("*.sql"))
        
        summary = f"SQL Logging Summary:\n"
        summary += f"  Directory: {self.output_dir}\n"
        summary += f"  Queries logged: {self.query_counter}\n"
        summary += f"  Files created: {len(sql_files)}\n"
        
        if sql_files:
            summary += f"  Latest file: {sql_files[-1].name}\n"
        
        return summary


# Global logger instance
_global_logger: Optional[SQLLogger] = None


def get_sql_logger() -> SQLLogger:
    """Get the global SQL logger instance."""
    global _global_logger
    if _global_logger is None:
        _global_logger = SQLLogger()
    return _global_logger


def log_sql_query(query: str, test_name: str, description: str = "", metadata: Optional[dict] = None) -> Path:
    """Convenience function to log a SQL query using the global logger.
    
    Args:
        query: The SQL query to log
        test_name: Name of the test executing the query
        description: Brief description of what the query does
        metadata: Optional metadata to include in the file header
        
    Returns:
        Path to the created SQL file
    """
    return get_sql_logger().log_query(query, test_name, description, metadata)


def reset_sql_logger():
    """Reset the global SQL logger (clears directory and starts fresh)."""
    global _global_logger
    _global_logger = SQLLogger()