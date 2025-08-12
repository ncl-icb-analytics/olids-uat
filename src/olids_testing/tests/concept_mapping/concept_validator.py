"""Concept mapping validation tests for OLIDS testing framework."""

import os
import yaml
import sys
from typing import List, Dict, Any, Optional
from snowflake.snowpark import Session

from olids_testing.core.test_base import BaseTest, TestResult, TestStatus, TestContext
from olids_testing.core.sql_logger import log_sql_query


class ConceptMappingTest(BaseTest):
    """Test to validate concept ID mappings through CONCEPT_MAP to CONCEPT tables."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the test.
        
        Args:
            config_path: Path to concept mapping tests YAML file
        """
        super().__init__(
            name="concept_mapping",
            description="Validates concept ID mappings from source tables through CONCEPT_MAP to CONCEPT",
            category="concept_mapping"
        )
        
        # Default config path
        if config_path is None:
            # Assume config is in project root/config directory
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.join(current_dir, '..', '..', '..', '..')
            config_path = os.path.join(project_root, 'config', 'concept_mapping_tests.yml')
        
        self.config_path = config_path
        self.mapping_config = self._load_mapping_config()
    
    def _load_mapping_config(self) -> Dict[str, Any]:
        """Load concept mapping configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Warning: Could not load concept mapping config from {self.config_path}: {e}")
            return {}
    
    def execute(self, context: TestContext) -> TestResult:
        """Execute all concept mapping tests.
        
        Args:
            context: Test execution context
            
        Returns:
            TestResult with combined results from all mapping tests
        """
        if not self.mapping_config:
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=TestStatus.ERROR,
                error_message="No concept mapping configuration available",
                metadata={'config_path': self.config_path}
            )
        
        session = context.session
        source_db = context.databases["source"]
        
        all_test_results = []
        total_tests = 0
        failed_tests = 0
        
        # Count total tests for progress reporting
        config = self.mapping_config.get('concept_mapping_tests', {})
        tests = config.get('tests', [])
        total_tests = len(tests)
        
        current_test = 0
        
        try:
            # Execute each concept mapping test
            for test_config in tests:
                current_test += 1
                
                # Show progress
                source_table = test_config.get('source_table', 'unknown')
                concept_field = test_config.get('concept_field', 'unknown')
                test_name = f"{source_table}.{concept_field}"
                
                sys.stdout.write(f"\r  Running concept mapping tests [{current_test}/{total_tests}]: {test_name}")
                sys.stdout.flush()
                
                # Execute the concept mapping test
                test_result = self._execute_concept_mapping_test(
                    test_config, session, source_db
                )
                
                all_test_results.append(test_result)
                
                if not test_result['passed']:
                    failed_tests += 1
            
            # Clear progress line completely
            # Use a much larger space buffer to ensure we clear any long test names
            clear_line = " " * 120  # Clear up to 120 characters
            sys.stdout.write(f"\r{clear_line}\r")  # Clear the entire line
            sys.stdout.flush()
            
            # Build failure details with separated sections
            failure_details = []
            
            # Separate mapping failures from data quality failures
            mapping_failures = []
            data_quality_failures = []
            
            for result in all_test_results:
                if not result['passed']:
                    breakdown = result.get('breakdown', {})
                    test_name = result['test_name']
                    
                    # Check if this has mapping issues (missing CONCEPT_MAP, CONCEPT entries, or NULL codes)
                    has_mapping_issues = (breakdown.get('distinct_no_concept_map_match', 0) > 0 or 
                                        breakdown.get('distinct_no_concept_match', 0) > 0 or
                                        breakdown.get('distinct_null_code', 0) > 0)
                    
                    # Check if this has data quality issues (NULL display only)
                    has_data_quality_issues = (breakdown.get('distinct_null_display', 0) > 0)
                    
                    # Add to mapping failures if it has mapping issues
                    if has_mapping_issues:
                        mapping_failures.append(result)
                    
                    # Add to data quality failures if it has data quality issues
                    if has_data_quality_issues:
                        data_quality_failures.append(result)
            
            if failed_tests > 0:
                failure_details.append(f"Failed {failed_tests} out of {total_tests} concept mapping tests")
                failure_details.append("")  # Empty line
                
                # Mapping failures section
                if mapping_failures:
                    failure_details.append("MAPPING FAILURES (missing CONCEPT_MAP, CONCEPT entries, or NULL codes):")
                    for result in mapping_failures:
                        breakdown = result.get('breakdown', {})
                        mapping_issues = []
                        
                        total_records = result.get('total_tested', 1)  # Avoid division by zero
                        
                        # Only mapping issues in this section
                        if breakdown.get('distinct_no_concept_map_match', 0) > 0:
                            distinct_count = breakdown['distinct_no_concept_map_match']
                            record_count = breakdown.get('no_concept_map_match', 0)
                            percentage = (record_count / total_records * 100) if total_records > 0 else 0
                            mapping_issues.append(f"{distinct_count:,} concept IDs missing CONCEPT_MAP ({record_count:,} records, {percentage:.1f}%)")
                        if breakdown.get('distinct_no_concept_match', 0) > 0:
                            distinct_count = breakdown['distinct_no_concept_match']
                            record_count = breakdown.get('no_concept_match', 0)
                            percentage = (record_count / total_records * 100) if total_records > 0 else 0
                            mapping_issues.append(f"{distinct_count:,} concept IDs missing CONCEPT ({record_count:,} records, {percentage:.1f}%)")
                        if breakdown.get('distinct_null_code', 0) > 0:
                            distinct_count = breakdown['distinct_null_code']
                            record_count = breakdown.get('null_code', 0)
                            percentage = (record_count / total_records * 100) if total_records > 0 else 0
                            mapping_issues.append(f"{distinct_count:,} concept IDs with NULL code ({record_count:,} records, {percentage:.1f}%)")
                        
                        if mapping_issues:  # Only show if there are actual mapping issues
                            failure_details.append(f"  • {result['test_name']}: {', '.join(mapping_issues)}")
                    
                    failure_details.append("")  # Empty line between sections
                
                # Data quality failures section  
                if data_quality_failures:
                    failure_details.append("DATA QUALITY FAILURES (NULL display values):")
                    for result in data_quality_failures:
                        breakdown = result.get('breakdown', {})
                        quality_issues = []
                        
                        total_records = result.get('total_tested', 1)  # Avoid division by zero
                        
                        if breakdown.get('distinct_null_display', 0) > 0:
                            distinct_count = breakdown['distinct_null_display']
                            record_count = breakdown.get('null_display', 0)
                            percentage = (record_count / total_records * 100) if total_records > 0 else 0
                            quality_issues.append(f"{distinct_count:,} concept IDs with NULL display ({record_count:,} records, {percentage:.1f}%)")
                        
                        failure_details.append(f"  • {result['test_name']}: {', '.join(quality_issues)}")
            
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
                    'concept_mapping_tests_executed': total_tests,
                    'concept_mapping_tests_failed': failed_tests,
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
                error_message=f"Failed to execute concept mapping tests: {str(e)}\nTraceback: {traceback.format_exc()}",
                metadata={
                    'config_path': self.config_path,
                    'tests_completed': current_test,
                    'total_tests': total_tests
                }
            )
    
    def _execute_concept_mapping_test(self, test_config: Dict[str, Any], 
                                    session: Session, source_db: str) -> Dict[str, Any]:
        """Execute a single concept mapping test.
        
        Args:
            test_config: Test configuration from YAML
            session: Snowflake session
            source_db: Source database name
            
        Returns:
            Dictionary with test results
        """
        source_table = test_config['source_table']
        concept_field = test_config['concept_field']
        description = test_config.get('description', 'Concept mapping validation')
        test_name = f"{source_table}.{concept_field}"
        
        try:
            # Build the concept mapping validation query with distinct source ID counts
            query = f"""
            SELECT
                COUNT(*) as total_records_with_concept_ids,
                COUNT(DISTINCT src."{concept_field}") as total_distinct_concept_ids,
                COUNT(CASE 
                    WHEN cm."source_code_id" IS NULL THEN 1
                    WHEN c."id" IS NULL THEN 1 
                    WHEN c."display" IS NULL THEN 1
                    WHEN c."code" IS NULL THEN 1
                    ELSE NULL
                END) as failed_mappings,
                COUNT(DISTINCT CASE 
                    WHEN cm."source_code_id" IS NULL THEN src."{concept_field}"
                    WHEN c."id" IS NULL THEN src."{concept_field}"
                    WHEN c."display" IS NULL THEN src."{concept_field}"
                    WHEN c."code" IS NULL THEN src."{concept_field}"
                    ELSE NULL
                END) as failed_distinct_concept_ids,
                COUNT(CASE WHEN cm."source_code_id" IS NULL THEN 1 END) as no_concept_map_match,
                COUNT(DISTINCT CASE WHEN cm."source_code_id" IS NULL THEN src."{concept_field}" END) as distinct_no_concept_map_match,
                COUNT(CASE WHEN cm."source_code_id" IS NOT NULL AND c."id" IS NULL THEN 1 END) as no_concept_match,
                COUNT(DISTINCT CASE WHEN cm."source_code_id" IS NOT NULL AND c."id" IS NULL THEN src."{concept_field}" END) as distinct_no_concept_match,
                COUNT(CASE WHEN c."id" IS NOT NULL AND c."display" IS NULL THEN 1 END) as null_display,
                COUNT(DISTINCT CASE WHEN c."id" IS NOT NULL AND c."display" IS NULL THEN src."{concept_field}" END) as distinct_null_display,
                COUNT(CASE WHEN c."id" IS NOT NULL AND c."code" IS NULL THEN 1 END) as null_code,
                COUNT(DISTINCT CASE WHEN c."id" IS NOT NULL AND c."code" IS NULL THEN src."{concept_field}" END) as distinct_null_code
            FROM "{source_db}"."OLIDS_MASKED"."{source_table}" src
            LEFT JOIN "{source_db}"."OLIDS_TERMINOLOGY"."CONCEPT_MAP" cm 
                ON src."{concept_field}" = cm."source_code_id"
            LEFT JOIN "{source_db}"."OLIDS_TERMINOLOGY"."CONCEPT" c 
                ON cm."target_code_id" = c."id"
            WHERE src."{concept_field}" IS NOT NULL
            """
            
            log_sql_query(query, self.name, f"concept_mapping_{source_table}_{concept_field}", {
                "source_table": source_table, 
                "concept_field": concept_field,
                "test_type": "concept_mapping"
            })
            
            result = session.sql(query).collect()[0]
            
            total_tested = result['TOTAL_RECORDS_WITH_CONCEPT_IDS']
            total_distinct_concept_ids = result['TOTAL_DISTINCT_CONCEPT_IDS']
            failed_mappings = result['FAILED_MAPPINGS']
            failed_distinct_concept_ids = result['FAILED_DISTINCT_CONCEPT_IDS']
            
            no_concept_map_match = result['NO_CONCEPT_MAP_MATCH']
            distinct_no_concept_map_match = result['DISTINCT_NO_CONCEPT_MAP_MATCH']
            no_concept_match = result['NO_CONCEPT_MATCH']
            distinct_no_concept_match = result['DISTINCT_NO_CONCEPT_MATCH']
            null_display = result['NULL_DISPLAY']
            distinct_null_display = result['DISTINCT_NULL_DISPLAY']
            null_code = result['NULL_CODE']
            distinct_null_code = result['DISTINCT_NULL_CODE']
            
            # Build failure message with separated categories
            failure_message = None
            if failed_mappings > 0:
                mapping_issues = []
                data_quality_issues = []
                
                # Categorize mapping vs data quality issues (using distinct counts for brevity)
                if distinct_no_concept_map_match > 0:
                    mapping_issues.append(f"{distinct_no_concept_map_match:,} missing CONCEPT_MAP")
                if distinct_no_concept_match > 0:
                    mapping_issues.append(f"{distinct_no_concept_match:,} missing CONCEPT") 
                if distinct_null_display > 0:
                    data_quality_issues.append(f"{distinct_null_display:,} NULL display")
                if distinct_null_code > 0:
                    mapping_issues.append(f"{distinct_null_code:,} NULL code")
                
                # Build combined message
                message_parts = []
                if mapping_issues:
                    message_parts.append(f"MAPPING ISSUES: {', '.join(mapping_issues)}")
                if data_quality_issues:
                    message_parts.append(f"DATA QUALITY: {', '.join(data_quality_issues)}")
                
                failure_message = f"Found {failed_distinct_concept_ids:,}/{total_distinct_concept_ids:,} failed concept IDs ({failed_mappings:,} records) - {' | '.join(message_parts)}"
            
            return {
                'test_name': test_name,
                'test_description': description,
                'test_type': 'concept_mapping',
                'passed': failed_mappings == 0,
                'total_tested': total_tested,
                'failed_count': failed_mappings,
                'failure_message': failure_message,
                'breakdown': {
                    'no_concept_map_match': no_concept_map_match,
                    'distinct_no_concept_map_match': distinct_no_concept_map_match,
                    'no_concept_match': no_concept_match,
                    'distinct_no_concept_match': distinct_no_concept_match,
                    'null_display': null_display,
                    'distinct_null_display': distinct_null_display,
                    'null_code': null_code,
                    'distinct_null_code': distinct_null_code
                }
            }
                
        except Exception as e:
            return {
                'test_name': test_name,
                'test_description': description,
                'test_type': 'concept_mapping',
                'passed': False,
                'total_tested': 0,
                'failed_count': 0,
                'failure_message': f"Test execution error: {str(e)}"
            }