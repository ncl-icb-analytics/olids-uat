"""Person pattern validation tests for OLIDS testing framework."""

import os
import yaml
import sys
from typing import List, Dict, Any, Optional
from snowflake.snowpark import Session

from olids_testing.core.test_base import BaseTest, TestResult, TestStatus, TestContext
from olids_testing.core.sql_logger import log_sql_query


class PersonPatternTest(BaseTest):
    """Test to validate person patterns based on YAML configuration."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the test.
        
        Args:
            config_path: Path to person pattern mappings YAML file
        """
        super().__init__(
            name="person_patterns",
            description="Validates person data patterns based on business rules",
            category="person_validation"
        )
        
        # Default config path
        if config_path is None:
            # Assume config is in project root/config directory
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.join(current_dir, '..', '..', '..', '..')
            config_path = os.path.join(project_root, 'config', 'person_pattern_mappings.yml')
        
        self.config_path = config_path
        self.pattern_config = self._load_pattern_config()
    
    def _load_pattern_config(self) -> Dict[str, Any]:
        """Load person pattern configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Warning: Could not load person pattern config from {self.config_path}: {e}")
            return {}
    
    def execute(self, context: TestContext) -> TestResult:
        """Execute all person pattern tests.
        
        Args:
            context: Test execution context
            
        Returns:
            TestResult with combined results from all pattern tests
        """
        if not self.pattern_config:
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=TestStatus.ERROR,
                error_message="No pattern configuration available",
                metadata={'config_path': self.config_path}
            )
        
        session = context.session
        source_db = context.databases["source"]
        
        all_test_results = []
        total_tests = 0
        failed_tests = 0
        
        # Count total tests for progress reporting
        for category, config in self.pattern_config.items():
            if 'tests' in config:
                total_tests += len(config['tests'])
        
        current_test = 0
        
        try:
            # Execute each category of tests
            for category, config in self.pattern_config.items():
                if 'tests' not in config:
                    continue
                    
                category_description = config.get('description', f'{category} tests')
                
                for test_config in config['tests']:
                    current_test += 1
                    
                    # Show progress
                    test_name = test_config.get('name', f'unnamed_{category}_test')
                    sys.stdout.write(f"\r  Running person pattern tests [{current_test}/{total_tests}]: {test_name}")
                    sys.stdout.flush()
                    
                    # Execute the configured test based on its type
                    test_result = self._execute_configured_test(
                        test_config, session, source_db
                    )
                    
                    all_test_results.append(test_result)
                    
                    if not test_result['passed']:
                        failed_tests += 1
            
            # Clear progress line completely
            clear_line = " " * 120  # Clear up to 120 characters
            sys.stdout.write(f"\r{clear_line}\r")  # Clear the entire line
            sys.stdout.flush()
            
            # Build failure details
            failure_details = []
            if failed_tests > 0:
                failure_details.append(f"Failed {failed_tests} out of {total_tests} person pattern tests:")
                
                for result in all_test_results:
                    if not result['passed']:
                        failure_details.append(
                            f"  • {result['test_name']}: {result['failure_message']} "
                            f"({result['failed_count']:,} failures out of {result['total_tested']:,} records)"
                        )
            
            status = TestStatus.PASSED if failed_tests == 0 else TestStatus.FAILED
            
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=status,
                total_tested=total_tests,
                failed_records=failed_tests,
                failure_rate=(failed_tests / total_tests * 100) if total_tests > 0 else 0.0,
                failure_details="\n".join(failure_details) if failure_details else None,
                metadata={
                    'pattern_tests_executed': total_tests,
                    'pattern_tests_failed': failed_tests,
                    'detailed_results': all_test_results,
                    'config_path': self.config_path
                }
            )
            
        except Exception as e:
            import traceback
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=TestStatus.ERROR,
                error_message=f"Failed to execute person pattern tests: {str(e)}\nTraceback: {traceback.format_exc()}",
                metadata={
                    'config_path': self.config_path,
                    'tests_completed': current_test,
                    'total_tests': total_tests
                }
            )
    
    def _execute_configured_test(self, test_config: Dict[str, Any], 
                                session: Session, source_db: str) -> Dict[str, Any]:
        """Execute a single test based on its configuration.
        
        Args:
            test_config: Test configuration from YAML
            session: Snowflake session
            source_db: Source database name
            
        Returns:
            Dictionary with test results
        """
        test_name = test_config.get('name', 'unnamed_test')
        test_description = test_config.get('description', 'No description')
        test_type = test_config.get('type', 'unknown')
        
        try:
            # Route to appropriate test handler based on type
            if test_type == 'uniqueness':
                return self._execute_uniqueness_test(test_config, session, source_db)
            elif test_type == 'relationship':
                return self._execute_relationship_test(test_config, session, source_db)
            elif test_type == 'completeness':
                return self._execute_completeness_test(test_config, session, source_db)
            elif test_type == 'range_validation':
                return self._execute_range_validation_test(test_config, session, source_db)
            elif test_type == 'referential_integrity':
                return self._execute_referential_integrity_test(test_config, session, source_db)
            elif test_type == 'field_comparison':
                return self._execute_field_comparison_test(test_config, session, source_db)
            elif test_type == 'count_check':
                return self._execute_count_check_test(test_config, session, source_db)
            else:
                return {
                    'test_name': test_name,
                    'test_description': test_description,
                    'test_type': test_type,
                    'passed': False,
                    'total_tested': 0,
                    'failed_count': 0,
                    'failure_message': f"Unknown test type: {test_type}"
                }
                
        except Exception as e:
            return {
                'test_name': test_name,
                'test_description': test_description,
                'test_type': test_type,
                'passed': False,
                'total_tested': 0,
                'failed_count': 0,
                'failure_message': f"Test execution error: {str(e)}"
            }
    
    def _execute_uniqueness_test(self, test_config: Dict[str, Any], session: Session, source_db: str) -> Dict[str, Any]:
        """Execute uniqueness validation test."""
        table = test_config['table']
        unique_column = test_config['unique_column']
        test_name = test_config['name']
        
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT "{unique_column}") as unique_records,
            COUNT(*) - COUNT(DISTINCT "{unique_column}") as duplicate_count
        FROM "{source_db}"."OLIDS_MASKED"."{table}"
        """
        
        log_sql_query(query, self.name, f"uniqueness_{test_name}", {
            "table": table, "column": unique_column, "test_type": "uniqueness"
        })
        
        result = session.sql(query).collect()[0]
        total_records = result['TOTAL_RECORDS']
        duplicate_count = result['DUPLICATE_COUNT']
        
        return {
            'test_name': test_name,
            'test_description': test_config.get('description', ''),
            'test_type': 'uniqueness',
            'passed': duplicate_count == 0,
            'total_tested': total_records,
            'failed_count': duplicate_count,
            'failure_message': f"Found {duplicate_count:,} duplicate person IDs (should be unique)" if duplicate_count > 0 else None
        }
    
    def _execute_relationship_test(self, test_config: Dict[str, Any], session: Session, source_db: str) -> Dict[str, Any]:
        """Execute relationship validation test."""
        base_table = test_config['base_table']
        join_table = test_config['join_table']
        join_condition = test_config['join_condition']
        test_name = test_config['name']
        filter_condition = test_config.get('filter')
        
        # Determine aliases based on table names and join condition
        if 'per.' in join_condition and 'pp.' in join_condition:
            # Person-PatientPerson relationship
            base_alias = 'per'
            join_alias = 'pp'
            check_field = f'{join_alias}."person_id"'
        elif 'p.' in join_condition and 'prr.' in join_condition:
            # Patient-PATIENT_REGISTERED_PRACTITIONER_IN_ROLE relationship
            base_alias = 'p'
            join_alias = 'prr'
            check_field = f'{join_alias}."patient_id"'
        elif 'p.' in join_condition and 'pr.' in join_condition:
            # Patient-PractitionerRole relationship (legacy)
            base_alias = 'p'
            join_alias = 'pr'
            check_field = f'{join_alias}."patient_id"'
        else:
            # Default aliases
            base_alias = 'base'
            join_alias = 'joined'
            check_field = f'{join_alias}."id"'
        
        query = f"""
        SELECT 
            COUNT(DISTINCT {base_alias}."id") as total_tested,
            COUNT(DISTINCT CASE WHEN {check_field} IS NULL THEN {base_alias}."id" END) as failed_records
        FROM "{source_db}"."OLIDS_MASKED"."{base_table}" {base_alias}
        LEFT JOIN "{source_db}"."OLIDS_MASKED"."{join_table}" {join_alias} ON {join_condition}
        """
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        log_sql_query(query, self.name, f"relationship_{test_name}", {
            "base_table": base_table, "join_table": join_table, "test_type": "relationship"
        })
        
        result = session.sql(query).collect()[0]
        total_tested = result['TOTAL_TESTED']
        failed_records = result['FAILED_RECORDS']
        
        return {
            'test_name': test_name,
            'test_description': test_config.get('description', ''),
            'test_type': 'relationship',
            'passed': failed_records == 0,
            'total_tested': total_tested,
            'failed_count': failed_records,
            'failure_message': self._get_relationship_failure_message(test_name, failed_records) if failed_records > 0 else None
        }
    
    def _execute_completeness_test(self, test_config: Dict[str, Any], session: Session, source_db: str) -> Dict[str, Any]:
        """Execute field completeness validation test."""
        table = test_config['table']
        required_fields = test_config['required_fields']
        test_name = test_config['name']
        filter_condition = test_config.get('filter')
        check_empty_strings = test_config.get('check_empty_strings', False)
        
        # Build conditions for checking null/empty values
        field_conditions = []
        for field in required_fields:
            if check_empty_strings:
                field_conditions.append(f'"{field}" IS NULL OR TRIM("{field}") = \'\'')
            else:
                field_conditions.append(f'"{field}" IS NULL')
        
        null_condition = ' OR '.join(field_conditions)
        
        query = f"""
        SELECT 
            COUNT(*) as total_tested,
            COUNT(CASE WHEN {null_condition} THEN 1 END) as failed_records
        FROM "{source_db}"."OLIDS_MASKED"."{table}"
        """
        
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        log_sql_query(query, self.name, f"completeness_{test_name}", {
            "table": table, "fields": required_fields, "test_type": "completeness"
        })
        
        result = session.sql(query).collect()[0]
        total_tested = result['TOTAL_TESTED']
        failed_records = result['FAILED_RECORDS']
        
        return {
            'test_name': test_name,
            'test_description': test_config.get('description', ''),
            'test_type': 'completeness',
            'passed': failed_records == 0,
            'total_tested': total_tested,
            'failed_count': failed_records,
            'failure_message': self._get_completeness_failure_message(test_name, failed_records, required_fields) if failed_records > 0 else None
        }
    
    def _execute_range_validation_test(self, test_config: Dict[str, Any], session: Session, source_db: str) -> Dict[str, Any]:
        """Execute range validation test."""
        table = test_config['table']
        field = test_config['field']
        min_value = test_config['min_value']
        max_value = test_config['max_value']
        test_name = test_config['name']
        filter_condition = test_config.get('filter')
        cast_to = test_config.get('cast_to', 'INTEGER')
        exclude_nulls = test_config.get('exclude_nulls', False)
        exclude_empty = test_config.get('exclude_empty', False)
        
        # Build casting and validation conditions
        cast_field = f'TRY_CAST("{field}" AS {cast_to})' if cast_to else f'"{field}"'
        
        # Handle dynamic max values
        if isinstance(max_value, str) and 'CURRENT_DATE' in max_value:
            max_condition = f'{cast_field} > {max_value}'
        else:
            max_condition = f'{cast_field} > {max_value}'
            
        range_condition = f'{cast_field} < {min_value} OR {max_condition}'
        
        # Build WHERE clause for what to test
        where_conditions = []
        if filter_condition:
            where_conditions.append(filter_condition)
        if exclude_nulls:
            where_conditions.append(f'"{field}" IS NOT NULL')
        if exclude_empty:
            where_conditions.append(f'TRIM("{field}") != \'\'')    
            
        where_clause = ' AND '.join(where_conditions) if where_conditions else 'TRUE'
        
        query = f"""
        SELECT 
            COUNT(CASE WHEN {where_clause} THEN 1 END) as total_tested,
            COUNT(CASE WHEN ({where_clause}) AND ({range_condition}) THEN 1 END) as failed_records
        FROM "{source_db}"."OLIDS_MASKED"."{table}"
        """
        
        log_sql_query(query, self.name, f"range_validation_{test_name}", {
            "table": table, "field": field, "test_type": "range_validation"
        })
        
        result = session.sql(query).collect()[0]
        total_tested = result['TOTAL_TESTED']
        failed_records = result['FAILED_RECORDS']
        
        return {
            'test_name': test_name,
            'test_description': test_config.get('description', ''),
            'test_type': 'range_validation',
            'passed': failed_records == 0,
            'total_tested': total_tested,
            'failed_count': failed_records,
            'failure_message': self._get_range_failure_message(test_name, failed_records, field, min_value, max_value) if failed_records > 0 else None
        }
    
    def _execute_referential_integrity_test(self, test_config: Dict[str, Any], session: Session, source_db: str) -> Dict[str, Any]:
        """Execute referential integrity validation test."""
        source_table = test_config['source_table']
        source_key = test_config['source_key']
        reference_table = test_config['reference_table']
        reference_key = test_config['reference_key']
        test_name = test_config['name']
        filter_condition = test_config.get('filter')
        exclude_null_keys = test_config.get('exclude_null_keys', False)
        
        query = f"""
        SELECT 
            COUNT(*) as total_tested,
            COUNT(CASE WHEN r."{reference_key}" IS NULL THEN 1 END) as failed_records
        FROM "{source_db}"."OLIDS_MASKED"."{source_table}" s
        LEFT JOIN "{source_db}"."OLIDS_MASKED"."{reference_table}" r 
            ON s."{source_key}" = r."{reference_key}"
        WHERE TRUE
        """
        
        # Add filter conditions
        conditions = []
        if filter_condition:
            conditions.append(filter_condition)
        if exclude_null_keys:
            conditions.append(f's."{source_key}" IS NOT NULL')
            
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        log_sql_query(query, self.name, f"referential_integrity_{test_name}", {
            "source_table": source_table, "reference_table": reference_table, "test_type": "referential_integrity"
        })
        
        result = session.sql(query).collect()[0]
        total_tested = result['TOTAL_TESTED']
        failed_records = result['FAILED_RECORDS']
        
        return {
            'test_name': test_name,
            'test_description': test_config.get('description', ''),
            'test_type': 'referential_integrity',
            'passed': failed_records == 0,
            'total_tested': total_tested,
            'failed_count': failed_records,
            'failure_message': f"Found {failed_records:,} patients with registered practices that don't exist in ORGANISATION table" if failed_records > 0 else None
        }
    
    def _execute_field_comparison_test(self, test_config: Dict[str, Any], session: Session, source_db: str) -> Dict[str, Any]:
        """Execute field comparison validation test."""
        table = test_config['table']
        field1 = test_config['field1']
        field2 = test_config['field2']
        comparison = test_config['comparison']
        test_name = test_config['name']
        filter_condition = test_config.get('filter')
        cast_to = test_config.get('cast_to', 'INTEGER')
        exclude_empty = test_config.get('exclude_empty', False)
        
        # Build casting
        if cast_to:
            cast_field1 = f'TRY_CAST("{field1}" AS {cast_to})'
            cast_field2 = f'TRY_CAST("{field2}" AS {cast_to})'
        else:
            cast_field1 = f'"{field1}"'
            cast_field2 = f'"{field2}"'
        
        # Build comparison condition
        if comparison == 'greater_than_or_equal':
            comparison_condition = f'{cast_field1} < {cast_field2}'
        elif comparison == 'greater_than':
            comparison_condition = f'{cast_field1} <= {cast_field2}'
        elif comparison == 'less_than_or_equal':
            comparison_condition = f'{cast_field1} > {cast_field2}'
        elif comparison == 'less_than':
            comparison_condition = f'{cast_field1} >= {cast_field2}'
        elif comparison == 'equal':
            comparison_condition = f'{cast_field1} != {cast_field2}'
        else:
            comparison_condition = f'{cast_field1} != {cast_field2}'
        
        # Build WHERE conditions
        where_conditions = []
        if filter_condition:
            where_conditions.append(filter_condition)
        if exclude_empty:
            where_conditions.append(f'TRIM("{field1}") != \'\'')
            where_conditions.append(f'TRIM("{field2}") != \'\'')
            
        where_clause = ' AND '.join(where_conditions) if where_conditions else 'TRUE'
        
        query = f"""
        SELECT 
            COUNT(CASE WHEN {where_clause} THEN 1 END) as total_tested,
            COUNT(CASE WHEN ({where_clause}) AND ({comparison_condition}) THEN 1 END) as failed_records
        FROM "{source_db}"."OLIDS_MASKED"."{table}"
        """
        
        log_sql_query(query, self.name, f"field_comparison_{test_name}", {
            "table": table, "fields": [field1, field2], "test_type": "field_comparison"
        })
        
        result = session.sql(query).collect()[0]
        total_tested = result['TOTAL_TESTED']
        failed_records = result['FAILED_RECORDS']
        
        return {
            'test_name': test_name,
            'test_description': test_config.get('description', ''),
            'test_type': 'field_comparison',
            'passed': failed_records == 0,
            'total_tested': total_tested,
            'failed_count': failed_records,
            'failure_message': f"Found {failed_records:,} patients with death year before birth year (impossible dates)" if failed_records > 0 else None
        }
    
    def _get_relationship_failure_message(self, test_name: str, failed_records: int) -> str:
        """Get specific failure message for relationship tests."""
        if 'person_to_patient' in test_name:
            return f"Found {failed_records:,} persons not linked to any patient record via PATIENT_PERSON table"
        elif 'registered_practitioner' in test_name and 'any_gp_registration_history' not in test_name:
            return f"Found {failed_records:,} patients with no practitioner relationships in PATIENT_REGISTERED_PRACTITIONER_IN_ROLE"
        elif 'any_gp_registration_history' in test_name:
            return f"Found {failed_records:,} patients with NO GP registration history (never registered to any practice - concerning data gap)"
        elif 'active_gp_registration' in test_name:
            return f"Found {failed_records:,} patients with NO ACTIVE GP registration (all registrations ended or missing)"
        else:
            return f"Found {failed_records:,} records with missing required relationships"
    
    def _get_completeness_failure_message(self, test_name: str, failed_records: int, required_fields: List[str]) -> str:
        """Get specific failure message for completeness tests."""
        if 'birth_year' in test_name:
            return f"Found {failed_records:,} patients with missing or empty birth year"
        elif 'practice' in test_name:
            return f"Found {failed_records:,} patients with missing registered practice ID (no GP practice assigned)"
        elif 'flags' in test_name:
            return f"Found {failed_records:,} patients with missing boolean flags (is_confidential, is_spine_sensitive)"
        elif 'record_owner' in test_name:
            return f"Found {failed_records:,} patients with missing or empty record owner organisation code"
        else:
            return f"Found {failed_records:,} records with missing required fields: {', '.join(required_fields)}"
    
    def _get_range_failure_message(self, test_name: str, failed_records: int, field: str, min_value: Any, max_value: Any) -> str:
        """Get specific failure message for range validation tests."""
        if 'birth_year' in test_name:
            return f"Found {failed_records:,} patients with invalid birth years (outside {min_value}-{max_value} range)"
        elif 'birth_month' in test_name:
            return f"Found {failed_records:,} patients with invalid birth months (outside 1-12 range)"
        else:
            return f"Found {failed_records:,} records with {field} outside valid range [{min_value}, {max_value}]"
    
    def _execute_count_check_test(self, test_config: Dict[str, Any], session: Session, source_db: str) -> Dict[str, Any]:
        """Execute count check test (e.g., patients with multiple practitioners)."""
        table = test_config['table']
        test_name = test_config['name']
        filter_condition = test_config.get('filter')
        count_query = test_config.get('count_query', '')
        
        # Replace the source_db placeholder in the query
        formatted_query = count_query.format(source_db=source_db)
        
        # Get total patients for context
        total_query = f"""
        SELECT COUNT(*) as total_tested
        FROM "{source_db}"."OLIDS_MASKED"."{table}"
        """
        if filter_condition:
            total_query += f" WHERE {filter_condition}"
        
        log_sql_query(total_query, self.name, f"count_check_total_{test_name}", {
            "table": table, "test_type": "count_check_total"
        })
        
        total_result = session.sql(total_query).collect()[0]
        total_tested = total_result['TOTAL_TESTED']
        
        # Execute the count query to find records that meet the criteria
        log_sql_query(formatted_query, self.name, f"count_check_{test_name}", {
            "table": table, "test_type": "count_check"
        })
        
        count_results = session.sql(formatted_query).collect()
        failed_count = len(count_results)
        
        # For multiple practitioners, this is more informational than a failure
        # We'll mark it as passed but report the count for visibility
        is_multiple_practitioners = 'multiple' in test_name.lower() and 'practitioner' in test_name.lower()
        
        if is_multiple_practitioners:
            # For multiple practitioners, we want to report but not necessarily fail
            passed = True
            if failed_count > 0:
                practitioner_counts = [row['PRACTITIONER_COUNT'] for row in count_results if 'PRACTITIONER_COUNT' in row]
                avg_practitioners = sum(practitioner_counts) / len(practitioner_counts) if practitioner_counts else 0
                failure_message = f"Found {failed_count} patients with multiple active practitioners (avg: {avg_practitioners:.1f} practitioners per patient)"
            else:
                failure_message = "All patients have single practitioner registrations"
        else:
            # For other count checks, treat non-zero as failure
            passed = failed_count == 0
            failure_message = f"Found {failed_count} records meeting criteria" if failed_count > 0 else None
        
        return {
            'test_name': test_name,
            'test_description': test_config.get('description', ''),
            'test_type': 'count_check',
            'passed': passed,
            'total_tested': total_tested,
            'failed_count': failed_count,
            'failure_message': failure_message
        }