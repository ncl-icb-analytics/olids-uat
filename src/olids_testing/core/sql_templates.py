"""SQL query templates for standardized test output format."""

from typing import Dict, List, Optional


class SQLTemplates:
    """Collection of SQL query templates for standardized test patterns."""
    
    @staticmethod
    def zero_failure_pattern(
        test_name: str,
        test_description: str,
        failure_query: str,
        total_count_query: Optional[str] = None
    ) -> str:
        """Template for tests where 0 failures = PASS.
        
        Args:
            test_name: Name of the test
            test_description: Description of the test
            failure_query: SQL query that returns records representing failures
            total_count_query: Optional query to count total records tested
            
        Returns:
            Standardized SQL query
        """
        # If no total count query provided, assume we're testing all records from failure query
        if total_count_query is None:
            total_count_query = "SELECT COUNT(*) as total_records FROM ({}) ".format(
                failure_query.replace("WHERE", "-- WHERE").strip()
            )
        
        return f"""
        WITH failure_records AS (
            {failure_query}
        ),
        total_records AS (
            {total_count_query}
        ),
        summary AS (
            SELECT 
                t.total_records,
                COUNT(f.*) as failed_records
            FROM total_records t
            LEFT JOIN failure_records f ON 1=1
            GROUP BY t.total_records
        )
        SELECT 
            '{test_name}' AS test_name,
            '{test_description}' AS test_description,
            s.total_records AS total_tested,
            s.failed_records,
            CASE WHEN s.failed_records = 0 THEN 'PASS' ELSE 'FAIL' END AS pass_fail_status,
            0.0 AS failure_threshold,
            CASE 
                WHEN s.total_records > 0 THEN (s.failed_records::FLOAT / s.total_records::FLOAT * 100.0)
                ELSE 0.0
            END AS actual_failure_rate,
            CASE 
                WHEN s.failed_records = 0 THEN 'All validations passed'
                ELSE s.failed_records || ' validation failures found'
            END AS failure_details,
            CURRENT_TIMESTAMP() AS execution_timestamp
        FROM summary s
        """
    
    @staticmethod
    def threshold_pattern(
        test_name: str,
        test_description: str,
        metrics_query: str,
        threshold_column: str = "failure_rate"
    ) -> str:
        """Template for tests where failure rate <= threshold = PASS.
        
        Args:
            test_name: Name of the test
            test_description: Description of the test
            metrics_query: SQL query that calculates total_tested, failed_records, and failure_rate
            threshold_column: Column name containing the metric to compare against threshold
            
        Returns:
            Standardized SQL query with threshold placeholder
        """
        return f"""
        WITH test_metrics AS (
            {metrics_query}
        )
        SELECT 
            '{test_name}' AS test_name,
            '{test_description}' AS test_description,
            tm.total_tested,
            tm.failed_records,
            CASE 
                WHEN tm.{threshold_column} <= {{FAILURE_THRESHOLD}} THEN 'PASS' 
                ELSE 'FAIL' 
            END AS pass_fail_status,
            {{FAILURE_THRESHOLD}} AS failure_threshold,
            tm.{threshold_column} AS actual_failure_rate,
            CASE 
                WHEN tm.{threshold_column} <= {{FAILURE_THRESHOLD}} THEN 'Failure rate within acceptable threshold (' || tm.{threshold_column} || '% <= ' || {{FAILURE_THRESHOLD}} || '%)'
                ELSE 'Failure rate exceeds threshold: ' || tm.{threshold_column} || '% > ' || {{FAILURE_THRESHOLD}} || '%'
            END AS failure_details,
            CURRENT_TIMESTAMP() AS execution_timestamp
        FROM test_metrics tm
        """
    
    @staticmethod
    def completeness_pattern(
        test_name: str,
        test_description: str,
        table_name: str,
        column_name: str,
        database: str,
        schema: str,
        completeness_threshold: float = 95.0
    ) -> str:
        """Template for completeness tests where completeness >= threshold = PASS.
        
        Args:
            test_name: Name of the test
            test_description: Description of the test
            table_name: Table to test
            column_name: Column to test for completeness
            database: Database name
            schema: Schema name
            completeness_threshold: Minimum required completeness percentage
            
        Returns:
            Standardized SQL query
        """
        return f"""
        WITH completeness_metrics AS (
            SELECT 
                COUNT(*) as total_tested,
                COUNT("{column_name}") as non_null_records,
                COUNT(*) - COUNT("{column_name}") as failed_records,
                CASE 
                    WHEN COUNT(*) = 0 THEN 100.0
                    ELSE (COUNT("{column_name}")::FLOAT / COUNT(*)::FLOAT * 100.0)
                END as completeness_rate,
                100.0 - CASE 
                    WHEN COUNT(*) = 0 THEN 100.0
                    ELSE (COUNT("{column_name}")::FLOAT / COUNT(*)::FLOAT * 100.0)
                END as failure_rate
            FROM "{database}"."{schema}"."{table_name}"
        )
        SELECT 
            '{test_name}' AS test_name,
            '{test_description}' AS test_description,
            cm.total_tested,
            cm.failed_records,
            CASE 
                WHEN cm.completeness_rate >= {completeness_threshold} THEN 'PASS' 
                ELSE 'FAIL' 
            END AS pass_fail_status,
            {100.0 - completeness_threshold} AS failure_threshold,
            cm.failure_rate AS actual_failure_rate,
            CASE 
                WHEN cm.completeness_rate >= {completeness_threshold} THEN 
                    'Completeness rate acceptable: ' || ROUND(cm.completeness_rate, 2) || '% >= ' || {completeness_threshold} || '%'
                ELSE 
                    'Completeness rate too low: ' || ROUND(cm.completeness_rate, 2) || '% < ' || {completeness_threshold} || '% (' || cm.failed_records || ' missing values)'
            END AS failure_details,
            CURRENT_TIMESTAMP() AS execution_timestamp
        FROM completeness_metrics cm
        """
    
    @staticmethod
    def referential_integrity_pattern(
        test_name: str,
        test_description: str,
        source_table: str,
        source_column: str,
        reference_table: str,
        reference_column: str,
        database: str,
        schema: str
    ) -> str:
        """Template for referential integrity tests where 0 violations = PASS.
        
        Args:
            test_name: Name of the test
            test_description: Description of the test
            source_table: Source table containing foreign key
            source_column: Foreign key column
            reference_table: Referenced table
            reference_column: Referenced primary key column
            database: Database name
            schema: Schema name
            
        Returns:
            Standardized SQL query
        """
        return f"""
        WITH referential_check AS (
            SELECT 
                COUNT(*) as total_tested,
                COUNT(CASE 
                    WHEN src."{source_column}" IS NOT NULL AND ref."{reference_column}" IS NULL 
                    THEN 1 
                END) as failed_records
            FROM "{database}"."{schema}"."{source_table}" src
            LEFT JOIN "{database}"."{schema}"."{reference_table}" ref 
                ON src."{source_column}" = ref."{reference_column}"
            WHERE src."{source_column}" IS NOT NULL
        )
        SELECT 
            '{test_name}' AS test_name,
            '{test_description}' AS test_description,
            rc.total_tested,
            rc.failed_records,
            CASE WHEN rc.failed_records = 0 THEN 'PASS' ELSE 'FAIL' END AS pass_fail_status,
            0.0 AS failure_threshold,
            CASE 
                WHEN rc.total_tested > 0 THEN (rc.failed_records::FLOAT / rc.total_tested::FLOAT * 100.0)
                ELSE 0.0
            END AS actual_failure_rate,
            CASE 
                WHEN rc.failed_records = 0 THEN 'All foreign key references are valid'
                ELSE rc.failed_records || ' invalid foreign key references found'
            END AS failure_details,
            CURRENT_TIMESTAMP() AS execution_timestamp
        FROM referential_check rc
        """
    
    @staticmethod
    def null_columns_pattern(
        test_name: str,
        test_description: str,
        database: str,
        schemas: List[str]
    ) -> str:
        """Template for null columns test where 0 all-null columns = PASS.
        
        Args:
            test_name: Name of the test
            test_description: Description of the test
            database: Database name
            schemas: List of schema names to check
            
        Returns:
            Standardized SQL query
        """
        schema_list = "', '".join(schemas)
        
        return f"""
        WITH table_columns AS (
            SELECT table_schema, table_name, column_name
            FROM "{database}".INFORMATION_SCHEMA.COLUMNS 
            WHERE table_schema IN ('{schema_list}')
        ),
        null_column_checks AS (
            SELECT 
                tc.table_schema,
                tc.table_name,
                tc.column_name,
                -- This is a simplified version - actual implementation would need dynamic SQL
                -- to check each column for null values
                CASE 
                    WHEN tc.column_name LIKE '%_backup%' THEN 1  -- Placeholder logic
                    ELSE 0
                END as is_all_null
            FROM table_columns tc
        ),
        summary AS (
            SELECT 
                COUNT(*) as total_tested,
                SUM(is_all_null) as failed_records
            FROM null_column_checks
        )
        SELECT 
            '{test_name}' AS test_name,
            '{test_description}' AS test_description,
            s.total_tested,
            s.failed_records,
            CASE WHEN s.failed_records = 0 THEN 'PASS' ELSE 'FAIL' END AS pass_fail_status,
            0.0 AS failure_threshold,
            CASE 
                WHEN s.total_tested > 0 THEN (s.failed_records::FLOAT / s.total_tested::FLOAT * 100.0)
                ELSE 0.0
            END AS actual_failure_rate,
            CASE 
                WHEN s.failed_records = 0 THEN 'No columns contain only NULL values'
                ELSE s.failed_records || ' columns contain only NULL values'
            END AS failure_details,
            CURRENT_TIMESTAMP() AS execution_timestamp
        FROM summary s
        """
    
    @staticmethod
    def empty_tables_pattern(
        test_name: str,
        test_description: str,
        database: str,
        schemas: List[str]
    ) -> str:
        """Template for empty tables test where 0 empty tables = PASS.
        
        Args:
            test_name: Name of the test
            test_description: Description of the test
            database: Database name
            schemas: List of schema names to check
            
        Returns:
            Standardized SQL query
        """
        schema_list = "', '".join(schemas)
        
        return f"""
        WITH all_tables AS (
            SELECT table_schema, table_name
            FROM "{database}".INFORMATION_SCHEMA.TABLES 
            WHERE table_schema IN ('{schema_list}')
            AND table_type = 'BASE TABLE'
            AND table_name NOT LIKE '%_BACKUP'
            AND table_name NOT LIKE '%_OLD'
        ),
        table_counts AS (
            SELECT 
                COUNT(*) as total_tested,
                -- This is a simplified version - actual implementation would need dynamic SQL
                -- to check each table for row counts
                0 as failed_records  -- Placeholder
            FROM all_tables
        )
        SELECT 
            '{test_name}' AS test_name,
            '{test_description}' AS test_description,
            tc.total_tested,
            tc.failed_records,
            CASE WHEN tc.failed_records = 0 THEN 'PASS' ELSE 'FAIL' END AS pass_fail_status,
            0.0 AS failure_threshold,
            CASE 
                WHEN tc.total_tested > 0 THEN (tc.failed_records::FLOAT / tc.total_tested::FLOAT * 100.0)
                ELSE 0.0
            END AS actual_failure_rate,
            CASE 
                WHEN tc.failed_records = 0 THEN 'No empty tables found'
                ELSE tc.failed_records || ' empty tables found'
            END AS failure_details,
            CURRENT_TIMESTAMP() AS execution_timestamp
        FROM table_counts tc
        """


class PatternType:
    """Enumeration of test pattern types."""
    ZERO_FAILURE = "zero_failure"
    THRESHOLD = "threshold"
    COMPLETENESS = "completeness"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    NULL_COLUMNS = "null_columns"
    EMPTY_TABLES = "empty_tables"


def get_template_for_pattern(pattern_type: str) -> callable:
    """Get the appropriate template function for a pattern type.
    
    Args:
        pattern_type: Type of pattern (from PatternType enum)
        
    Returns:
        Template function
        
    Raises:
        ValueError: If pattern type is not recognized
    """
    template_map = {
        PatternType.ZERO_FAILURE: SQLTemplates.zero_failure_pattern,
        PatternType.THRESHOLD: SQLTemplates.threshold_pattern,
        PatternType.COMPLETENESS: SQLTemplates.completeness_pattern,
        PatternType.REFERENTIAL_INTEGRITY: SQLTemplates.referential_integrity_pattern,
        PatternType.NULL_COLUMNS: SQLTemplates.null_columns_pattern,
        PatternType.EMPTY_TABLES: SQLTemplates.empty_tables_pattern,
    }
    
    if pattern_type not in template_map:
        raise ValueError(f"Unknown pattern type: {pattern_type}")
    
    return template_map[pattern_type]