"""Data completeness checks for OLIDS testing framework."""

from typing import List, Dict, Any, Optional
from snowflake.snowpark import Session

from olids_testing.core.test_base import BaseTest, TestResult, TestStatus, TestContext
from olids_testing.core.sql_logger import log_sql_query

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn


class AllNullColumnsTest(BaseTest):
    """Test to identify columns with 100% NULL values across specified schemas."""
    
    def __init__(self, schemas: Optional[List[str]] = None):
        """Initialize the test.
        
        Args:
            schemas: List of schema names to check. If None, uses default schemas.
        """
        super().__init__(
            name="null_columns",
            description="Identifies columns that contain only NULL values",
            category="data_quality"
        )
        self.schemas = schemas or ["OLIDS_MASKED", "OLIDS_TERMINOLOGY"]
    
    def execute(self, context: TestContext) -> TestResult:
        """Execute the all NULL columns test.
        
        Args:
            context: Test execution context
            
        Returns:
            TestResult with details about columns containing only NULL values
        """
        session = context.session
        source_db = context.databases["source"]
        
        try:
            # Get all tables and their columns grouped by table
            schema_list = "', '".join(self.schemas)
            tables_query = f"""
            SELECT table_schema, table_name, 
                   LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY ordinal_position) as columns
            FROM "{source_db}".INFORMATION_SCHEMA.COLUMNS 
            WHERE table_schema IN ('{schema_list}')
            GROUP BY table_schema, table_name
            ORDER BY table_schema, table_name
            """
            
            # Log the schema tables query
            log_sql_query(
                tables_query,
                self.name,
                "get_tables_with_columns",
                {"database": source_db, "schemas": self.schemas}
            )
            
            tables = session.sql(tables_query).collect()
            
            all_null_columns = []
            total_checked = 0
            errors = []
            
            # Check if we should show progress
            show_progress = not context.config.get('parallel_execution', False)
            
            # Simple progress reporting with just x/total status
            import sys
            
            for i, row in enumerate(tables):
                schema_name = row['TABLE_SCHEMA']
                table_name = row['TABLE_NAME']
                columns_list = row['COLUMNS'].split(',') if row['COLUMNS'] else []
                
                # Show simple progress status with proper overwrite
                if show_progress:
                    sys.stdout.write(f"\r  Checking tables for NULL-only columns [{i+1}/{len(tables)}]")
                    sys.stdout.flush()
                
                # Skip tables with no columns or empty column list
                if not columns_list or not row['COLUMNS']:
                    continue
                
                try:
                    # Build a single query to check all columns in the table at once
                    column_checks = []
                    for col in columns_list:
                        col = col.strip()
                        if col:  # Make sure column name is not empty
                            column_checks.append(f'COUNT("{col}") as "{col}_non_null"')
                    
                    # Skip if no valid columns found
                    if not column_checks:
                        continue
                    
                    check_query = f'''
                    SELECT 
                        COUNT(*) as total_rows,
                        {', '.join(column_checks)}
                    FROM "{source_db}"."{schema_name}"."{table_name}"
                    '''
                    
                    # Log the table null check query
                    log_sql_query(
                        check_query,
                        self.name,
                        f"check_table_nulls_{schema_name}_{table_name}",
                        {
                            "schema": schema_name,
                            "table": table_name,
                            "columns_checked": len(columns_list),
                            "check_type": "table_null_analysis"
                        }
                    )
                    
                    result = session.sql(check_query).collect()[0]
                    
                    total_rows = result['TOTAL_ROWS']
                    
                    # Check each column's null count
                    for col in columns_list:
                        col = col.strip()
                        if not col:  # Skip empty column names
                            continue
                        
                        try:
                            # Column names in result are uppercase
                            result_key = f'{col.upper()}_NON_NULL'
                            non_null_count = result[result_key]
                            total_checked += 1
                            
                            # If no non-null values and table has rows, it's all NULL
                            if non_null_count == 0 and total_rows > 0:
                                all_null_columns.append({
                                    'schema': schema_name,
                                    'table': table_name,
                                    'column': col,
                                    'total_rows': total_rows,
                                    'non_null_count': non_null_count
                                })
                        except KeyError as ke:
                            # Log which column key was missing for debugging
                            errors.append({
                                'schema': schema_name,
                                'table': table_name,
                                'column': col,
                                'error': f"Missing result key: {result_key}, available keys: {list(result.keys())}"
                            })
                        
                except Exception as e:
                    # Track errors for debugging with more detail
                    import traceback
                    errors.append({
                        'schema': schema_name,
                        'table': table_name,
                        'columns': len(columns_list),
                        'columns_list': columns_list[:5],  # First 5 columns for debugging
                        'error': str(e),
                        'traceback': traceback.format_exc()
                    })
                    # Still count the columns that would have been checked
                    total_checked += len(columns_list)
                
            # Clear progress line completely
            if show_progress:
                clear_line = " " * 120  # Clear up to 120 characters
                sys.stdout.write(f"\r{clear_line}\r")  # Clear the entire line
                sys.stdout.flush()
            
            # Determine test status and failure details
            failed_records = len(all_null_columns)
            failure_details = []
            
            if all_null_columns:
                failure_details.append(f"Found {failed_records} columns with 100% NULL values:")
                for col in all_null_columns[:10]:  # Show first 10
                    failure_details.append(
                        f"  • {col['schema']}.{col['table']}.{col['column']} "
                        f"({col['total_rows']} rows, {col['non_null_count']} non-null)"
                    )
                if len(all_null_columns) > 10:
                    failure_details.append(f"  ... and {len(all_null_columns) - 10} more")
            
            if errors:
                failure_details.append(f"\nErrors encountered during checking ({len(errors)} tables):")
                for error in errors[:3]:  # Show first 3 errors with more detail
                    if 'column' in error:
                        # Column-level error
                        failure_details.append(
                            f"  • {error['schema']}.{error['table']}.{error['column']}: {error['error']}"
                        )
                    else:
                        # Table-level error
                        failure_details.append(
                            f"  • {error['schema']}.{error['table']} ({error['columns']} columns): {error['error']}"
                        )
                        if 'columns_list' in error and error['columns_list']:
                            failure_details.append(f"    Sample columns: {error['columns_list']}")
                if len(errors) > 3:
                    failure_details.append(f"  ... and {len(errors) - 3} more table errors")
            
            # Test passes if no all-NULL columns found (some NULL columns might be expected)
            status = TestStatus.PASSED if failed_records == 0 else TestStatus.FAILED
            
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=status,
                total_tested=total_checked,
                failed_records=failed_records,
                failure_rate=(failed_records / total_checked * 100) if total_checked > 0 else 0.0,
                failure_details="\n".join(failure_details) if failure_details else None,
                metadata={
                    'schemas_checked': self.schemas,
                    'columns_checked': total_checked,
                    'null_columns': all_null_columns,
                    'errors': errors
                }
            )
            
        except Exception as e:
            # Add more detailed error information
            import traceback
            error_details = f"Failed to execute all NULL columns test: {str(e)}\nTraceback: {traceback.format_exc()}"
            
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=TestStatus.ERROR,
                error_message=error_details,
                metadata={'schemas': self.schemas}
            )


class EmptyTablesTest(BaseTest):
    """Test to identify tables with zero rows across specified schemas."""
    
    def __init__(self, schemas: Optional[List[str]] = None):
        """Initialize the test.
        
        Args:
            schemas: List of schema names to check. If None, uses default schemas.
        """
        super().__init__(
            name="empty_tables",
            description="Identifies tables that contain no data (zero rows)",
            category="data_quality"
        )
        self.schemas = schemas or ["OLIDS_MASKED", "OLIDS_TERMINOLOGY"]
    
    def execute(self, context: TestContext) -> TestResult:
        """Execute the empty tables test.
        
        Args:
            context: Test execution context
            
        Returns:
            TestResult with details about empty tables
        """
        session = context.session
        source_db = context.databases["source"]
        
        try:
            # Get all tables from specified schemas, excluding backup tables
            schema_list = "', '".join(self.schemas)
            tables_query = f"""
            SELECT table_schema, table_name
            FROM "{source_db}".INFORMATION_SCHEMA.TABLES 
            WHERE table_schema IN ('{schema_list}')
            AND table_type = 'BASE TABLE'
            AND table_name NOT LIKE '%_BACKUP'
            AND table_name NOT LIKE '%_OLD'
            ORDER BY table_schema, table_name
            """
            
            # Log the schema tables query
            log_sql_query(
                tables_query,
                self.name,
                "get_all_tables",
                {"database": source_db, "schemas": self.schemas}
            )
            
            tables = session.sql(tables_query).collect()
            
            empty_tables = []
            non_empty_tables = []
            total_checked = 0
            errors = []
            
            # Check if we should show progress
            show_progress = not context.config.get('parallel_execution', False)
            
            # Simple progress reporting with just x/total status
            import sys
            
            for i, row in enumerate(tables):
                schema_name = row['TABLE_SCHEMA']
                table_name = row['TABLE_NAME']
                
                # Show simple progress status
                if show_progress:
                    sys.stdout.write(f"\r  Checking tables for empty data [{i+1}/{len(tables)}]")
                    sys.stdout.flush()
                
                try:
                    # Check if table has any rows
                    count_query = f'''
                    SELECT COUNT(*) as row_count
                    FROM "{source_db}"."{schema_name}"."{table_name}"
                    '''
                    
                    # Log the table count query
                    log_sql_query(
                        count_query,
                        self.name,
                        f"check_empty_{schema_name}_{table_name}",
                        {
                            "schema": schema_name,
                            "table": table_name,
                            "check_type": "row_count"
                        }
                    )
                    
                    result = session.sql(count_query).collect()[0]
                    row_count = result['ROW_COUNT']
                    total_checked += 1
                    
                    # If table has zero rows, it's empty
                    if row_count == 0:
                        empty_tables.append({
                            'schema': schema_name,
                            'table': table_name,
                            'row_count': row_count
                        })
                    else:
                        # Table has data - record as non-empty
                        non_empty_tables.append({
                            'schema': schema_name,
                            'table': table_name,
                            'row_count': row_count
                        })
                        
                except Exception as e:
                    # Track errors for debugging
                    import traceback
                    errors.append({
                        'schema': schema_name,
                        'table': table_name,
                        'error': str(e),
                        'traceback': traceback.format_exc()
                    })
                    total_checked += 1
                
            # Clear progress line completely
            if show_progress:
                clear_line = " " * 120  # Clear up to 120 characters
                sys.stdout.write(f"\r{clear_line}\r")  # Clear the entire line
                sys.stdout.flush()
            
            # Determine test status and failure details
            failed_records = len(empty_tables)
            failure_details = []
            
            if empty_tables:
                failure_details.append(f"Found {failed_records} empty tables:")
                for table in empty_tables[:15]:  # Show first 15
                    failure_details.append(
                        f"  • {table['schema']}.{table['table']} (0 rows)"
                    )
                if len(empty_tables) > 15:
                    failure_details.append(f"  ... and {len(empty_tables) - 15} more empty tables")
            
            if errors:
                failure_details.append(f"\nErrors encountered during checking ({len(errors)} tables):")
                for error in errors[:3]:  # Show first 3 errors
                    failure_details.append(
                        f"  • {error['schema']}.{error['table']}: {error['error']}"
                    )
                if len(errors) > 3:
                    failure_details.append(f"  ... and {len(errors) - 3} more table errors")
            
            # Test passes if no empty tables found (some empty tables might be expected in test environments)
            status = TestStatus.PASSED if failed_records == 0 else TestStatus.FAILED
            
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=status,
                total_tested=total_checked,
                failed_records=failed_records,
                failure_rate=(failed_records / total_checked * 100) if total_checked > 0 else 0.0,
                failure_details="\n".join(failure_details) if failure_details else None,
                metadata={
                    'schemas_checked': self.schemas,
                    'tables_checked': total_checked,
                    'empty_tables': empty_tables,
                    'non_empty_tables': non_empty_tables,
                    'errors': errors
                }
            )
            
        except Exception as e:
            # Add more detailed error information
            import traceback
            error_details = f"Failed to execute empty tables test: {str(e)}\nTraceback: {traceback.format_exc()}"
            
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=TestStatus.ERROR,
                error_message=error_details,
                metadata={'schemas': self.schemas}
            )


class ColumnCompletenessTest(BaseTest):
    """Test to check completeness rates for specific columns."""
    
    def __init__(self, completeness_rules: Optional[Dict[str, Dict]] = None):
        """Initialize the test.
        
        Args:
            completeness_rules: Dictionary mapping table.column to expected completeness thresholds
                Format: {
                    "PATIENT.nhs_number_hash": {"min_completeness": 95.0, "schema": "OLIDS_MASKED"},
                    "ENCOUNTER.patient_id": {"min_completeness": 100.0, "schema": "OLIDS_MASKED"}
                }
        """
        super().__init__(
            name="column_completeness",
            description="Checks completeness rates for critical columns",
            category="data_quality"
        )
        self.completeness_rules = completeness_rules or {
            "PATIENT.nhs_number_hash": {"min_completeness": 95.0, "schema": "OLIDS_MASKED"},
            "PATIENT.birth_year": {"min_completeness": 98.0, "schema": "OLIDS_MASKED"},
            "PATIENT.birth_month": {"min_completeness": 98.0, "schema": "OLIDS_MASKED"},
            "ENCOUNTER.patient_id": {"min_completeness": 100.0, "schema": "OLIDS_MASKED"},
            "OBSERVATION.patient_id": {"min_completeness": 100.0, "schema": "OLIDS_MASKED"},
            "PERSON.id": {"min_completeness": 100.0, "schema": "OLIDS_MASKED"},
        }
    
    def execute(self, context: TestContext) -> TestResult:
        """Execute the column completeness test.
        
        Args:
            context: Test execution context
            
        Returns:
            TestResult with details about column completeness rates
        """
        session = context.session
        source_db = context.databases["source"]
        
        try:
            # First, get available columns to validate our rules
            available_columns = {}
            for table_column, rule in self.completeness_rules.items():
                table_name, column_name = table_column.split('.')
                schema_name = rule['schema']
                
                if f"{schema_name}.{table_name}" not in available_columns:
                    # Get actual column names for this table
                    columns_query = f"""
                    SELECT column_name
                    FROM "{source_db}".INFORMATION_SCHEMA.COLUMNS 
                    WHERE table_schema = '{schema_name}' 
                    AND table_name = '{table_name}'
                    """
                    try:
                        cols_result = session.sql(columns_query).collect()
                        available_columns[f"{schema_name}.{table_name}"] = [row['COLUMN_NAME'] for row in cols_result]
                    except Exception:
                        available_columns[f"{schema_name}.{table_name}"] = []
            
            completeness_results = []
            failed_checks = []
            total_checks = len(self.completeness_rules)
            current_check = 0
            
            # Check if we should show progress
            show_progress = not context.config.get('parallel_execution', False)
            
            import sys
            
            for table_column, rule in self.completeness_rules.items():
                current_check += 1
                # Show progress indicator
                if show_progress:
                    sys.stdout.write(f"\r  Checking column completeness [{current_check}/{total_checks}]: {table_column}")
                    sys.stdout.flush()
                try:
                    table_name, column_name = table_column.split('.')
                    schema_name = rule['schema']
                    min_completeness = rule['min_completeness']
                    
                    # Check if column exists in the table
                    table_key = f"{schema_name}.{table_name}"
                    if table_key in available_columns:
                        actual_columns = available_columns[table_key]
                        if column_name not in actual_columns:
                            # Column doesn't exist - report what columns are available
                            similar_columns = [col for col in actual_columns if column_name.lower() in col.lower() or col.lower() in column_name.lower()]
                            error_msg = f"Column '{column_name}' not found in {table_key}. "
                            if similar_columns:
                                error_msg += f"Similar columns available: {similar_columns[:5]}"
                            else:
                                error_msg += f"Available columns: {actual_columns[:10]}"
                            
                            failed_checks.append({
                                'table_column': table_column,
                                'error': error_msg
                            })
                            continue
                    
                    # Calculate completeness rate
                    completeness_query = f'''
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT("{column_name}") as non_null_count,
                        ROUND(COUNT("{column_name}") / COUNT(*) * 100, 2) as completeness_rate
                    FROM "{source_db}"."{schema_name}"."{table_name}"
                    '''
                    
                    result = session.sql(completeness_query).collect()[0]
                    
                    total_rows = result['TOTAL_ROWS']
                    non_null_count = result['NON_NULL_COUNT']
                    completeness_rate = float(result['COMPLETENESS_RATE'] or 0.0)
                    
                    completeness_results.append({
                        'table_column': table_column,
                        'schema': schema_name,
                        'total_rows': total_rows,
                        'non_null_count': non_null_count,
                        'completeness_rate': completeness_rate,
                        'min_required': min_completeness,
                        'passed': completeness_rate >= min_completeness
                    })
                    
                    if completeness_rate < min_completeness:
                        failed_checks.append({
                            'table_column': table_column,
                            'completeness_rate': completeness_rate,
                            'min_required': min_completeness,
                            'total_rows': total_rows,
                            'missing_rows': total_rows - non_null_count
                        })
                        
                except Exception as e:
                    failed_checks.append({
                        'table_column': table_column,
                        'error': str(e)
                    })
            
            # Clear progress line completely
            if show_progress:
                clear_line = " " * 120  # Clear up to 120 characters
                sys.stdout.write(f"\r{clear_line}\r")  # Clear the entire line
                sys.stdout.flush()
            
            # Build failure details
            failure_details = []
            if failed_checks:
                failure_details.append(f"Found {len(failed_checks)} completeness failures:")
                for failure in failed_checks:
                    if 'error' in failure:
                        failure_details.append(f"  • {failure['table_column']}: Error - {failure['error']}")
                    else:
                        failure_details.append(
                            f"  • {failure['table_column']}: {failure['completeness_rate']:.1f}% "
                            f"(required: {failure['min_required']:.1f}%, "
                            f"missing: {failure['missing_rows']:,} rows)"
                        )
            
            failed_records = len(failed_checks)
            status = TestStatus.PASSED if failed_records == 0 else TestStatus.FAILED
            
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=status,
                total_tested=total_checks,
                failed_records=failed_records,
                failure_rate=(failed_records / total_checks * 100) if total_checks > 0 else 0.0,
                failure_details="\n".join(failure_details) if failure_details else None,
                metadata={
                    'completeness_results': completeness_results,
                    'failed_checks': failed_checks,
                    'rules_checked': list(self.completeness_rules.keys())
                }
            )
            
        except Exception as e:
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=TestStatus.ERROR,
                error_message=f"Failed to execute column completeness test: {str(e)}",
                metadata={'rules': list(self.completeness_rules.keys())}
            )