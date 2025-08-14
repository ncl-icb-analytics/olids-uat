"""Base classes for OLIDS testing framework."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from snowflake.snowpark import Session


class TestStatus(Enum):
    """Test execution status."""
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"


class TestSeverity(Enum):
    """Test failure severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class TestResult:
    """Test execution result."""
    test_name: str
    test_description: str
    status: TestStatus
    total_tested: int = 0
    failed_records: int = 0
    failure_rate: float = 0.0
    failure_details: str = ""
    execution_time: float = 0.0
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_tested == 0:
            return 0.0
        return ((self.total_tested - self.failed_records) / self.total_tested) * 100
    
    @property
    def passed(self) -> bool:
        """Check if test passed."""
        return self.status == TestStatus.PASSED
    
    @property
    def failed(self) -> bool:
        """Check if test failed."""
        return self.status in [TestStatus.FAILED, TestStatus.ERROR]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "test_name": self.test_name,
            "test_description": self.test_description,
            "status": self.status.value,
            "total_tested": self.total_tested,
            "failed_records": self.failed_records,
            "failure_rate": round(self.failure_rate, 2),
            "success_rate": round(self.success_rate, 2),
            "failure_details": self.failure_details,
            "execution_time": round(self.execution_time, 3),
            "error_message": self.error_message,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


@dataclass
class TestContext:
    """Test execution context."""
    environment: str
    databases: Dict[str, str]
    schemas: Dict[str, str]
    session: Session
    config: Dict[str, Any]
    progress_callback: Optional[Callable[[int], None]] = None
    
    def get_full_table_name(self, database_key: str, schema_key: str, table_name: str) -> str:
        """Get fully qualified table name.
        
        Args:
            database_key: Database key from config (e.g., 'source', 'terminology')
            schema_key: Schema key from config (e.g., 'masked', 'terminology')
            table_name: Table name
            
        Returns:
            Fully qualified table name
        """
        database = self.databases.get(database_key)
        schema = self.schemas.get(schema_key)
        
        if not database or not schema:
            raise ValueError(f"Invalid database/schema keys: {database_key}.{schema_key}")
        
        return f'"{database}"."{schema}"."{table_name}"'


class BaseTest(ABC):
    """Base class for all OLIDS tests."""
    
    def __init__(self, name: str, description: str, category: str = "general"):
        """Initialize test.
        
        Args:
            name: Test name
            description: Test description
            category: Test category
        """
        self.name = name
        self.description = description
        self.category = category
        self.severity = TestSeverity.MEDIUM
        self.timeout = 300  # 5 minutes default
        
    @abstractmethod
    def execute(self, context: TestContext) -> TestResult:
        """Execute the test.
        
        Args:
            context: Test execution context
            
        Returns:
            Test result
        """
        pass
    
    def run(self, context: TestContext) -> TestResult:
        """Run the test with timing and error handling.
        
        Args:
            context: Test execution context
            
        Returns:
            Test result
        """
        result = TestResult(
            test_name=self.name,
            test_description=self.description,
            status=TestStatus.PENDING,
            started_at=datetime.now()
        )
        
        try:
            result.status = TestStatus.RUNNING
            start_time = time.time()
            
            # Execute the actual test
            test_result = self.execute(context)
            
            # Update timing
            end_time = time.time()
            test_result.execution_time = end_time - start_time
            test_result.started_at = result.started_at
            test_result.completed_at = datetime.now()
            
            # Ensure status is set
            if test_result.status == TestStatus.PENDING:
                test_result.status = TestStatus.PASSED if test_result.failed_records == 0 else TestStatus.FAILED
            
            return test_result
            
        except Exception as e:
            result.status = TestStatus.ERROR
            result.error_message = str(e)
            result.completed_at = datetime.now()
            result.execution_time = time.time() - start_time if 'start_time' in locals() else 0
            return result
    
    def validate_context(self, context: TestContext) -> bool:
        """Validate test context before execution.
        
        Args:
            context: Test execution context
            
        Returns:
            True if context is valid
        """
        required_keys = ['session', 'databases', 'schemas']
        return all(hasattr(context, key) for key in required_keys)


class SQLTest(BaseTest):
    """Base class for SQL-based tests."""
    
    def __init__(self, name: str, description: str, sql_query: str, category: str = "sql"):
        """Initialize SQL test.
        
        Args:
            name: Test name
            description: Test description
            sql_query: SQL query to execute
            category: Test category
        """
        super().__init__(name, description, category)
        self.sql_query = sql_query
    
    def execute(self, context: TestContext) -> TestResult:
        """Execute SQL test.
        
        Args:
            context: Test execution context
            
        Returns:
            Test result
        """
        try:
            # Execute the SQL query
            df = context.session.sql(self.sql_query).collect()
            
            # Process results based on expected format
            if len(df) == 0:
                return TestResult(
                    test_name=self.name,
                    test_description=self.description,
                    status=TestStatus.PASSED,
                    total_tested=0,
                    failed_records=0,
                    failure_details="No data returned"
                )
            
            # Assume first row contains test results
            row = df[0]
            
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=TestStatus.PASSED,
                total_tested=getattr(row, 'TOTAL_TESTED', 0),
                failed_records=getattr(row, 'FAILED_RECORDS', 0),
                failure_rate=getattr(row, 'FAILURE_RATE', 0.0),
                failure_details=getattr(row, 'FAILURE_DETAILS', 'Test completed')
            )
            
        except Exception as e:
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=TestStatus.ERROR,
                error_message=f"SQL execution failed: {str(e)}"
            )


class StandardSQLTest(BaseTest):
    """Base class for SQL-based tests with consistent output format."""
    
    def __init__(self, name: str, description: str, sql_query: str, category: str = "sql", 
                 failure_threshold: Optional[float] = None):
        """Initialize SQL test.
        
        Args:
            name: Test name
            description: Test description
            sql_query: SQL query that returns consistent output format
            category: Test category
            failure_threshold: Optional failure threshold override (0.0-100.0 as percentage)
        """
        super().__init__(name, description, category)
        self.sql_query = sql_query
        self.failure_threshold = failure_threshold
    
    def get_failure_threshold(self, context: TestContext) -> float:
        """Get failure threshold for this test.
        
        Args:
            context: Test execution context
            
        Returns:
            Failure threshold as percentage (0.0-100.0)
        """
        # Check for test-specific threshold override
        if self.failure_threshold is not None:
            return self.failure_threshold
        
        # Check context config for test-specific threshold
        thresholds = context.config.get('failure_thresholds', {})
        if self.name in thresholds:
            return float(thresholds[self.name])
        
        # Check for category-specific threshold
        if self.category in thresholds:
            return float(thresholds[self.category])
        
        # Default threshold: 0% (no failures allowed by default)
        return 0.0
    
    def execute(self, context: TestContext) -> TestResult:
        """Execute SQL test.
        
        Args:
            context: Test execution context
            
        Returns:
            Test result
        """
        try:
            # Get failure threshold for this test
            threshold = self.get_failure_threshold(context)
            
            # Inject threshold and database into SQL query
            final_query = self.sql_query
            if '{FAILURE_THRESHOLD}' in final_query:
                final_query = final_query.replace('{FAILURE_THRESHOLD}', str(threshold))
            if '{DATABASE}' in final_query:
                final_query = final_query.replace('{DATABASE}', context.databases["source"])
            
            # Execute the SQL query using the existing session from context
            df = context.session.sql(final_query).collect()
            
            # Process results based on consistent format
            if len(df) == 0:
                return TestResult(
                    test_name=self.name,
                    test_description=self.description,
                    status=TestStatus.ERROR,
                    error_message="No data returned from SQL test"
                )
            
            # Extract result columns
            row = df[0]
            
            # Required columns in consistent format
            total_tested = getattr(row, 'TOTAL_TESTED', 0)
            failed_records = getattr(row, 'FAILED_RECORDS', 0)
            pass_fail_status = getattr(row, 'PASS_FAIL_STATUS', 'FAIL')
            failure_threshold_used = getattr(row, 'FAILURE_THRESHOLD', threshold)
            actual_failure_rate = getattr(row, 'ACTUAL_FAILURE_RATE', 0.0)
            failure_details = getattr(row, 'FAILURE_DETAILS', '')
            
            # Convert pass/fail status to TestStatus
            if pass_fail_status.upper() == 'PASS':
                status = TestStatus.PASSED
            elif pass_fail_status.upper() == 'FAIL':
                status = TestStatus.FAILED
            else:
                status = TestStatus.ERROR
            
            # Calculate failure rate if not provided
            if actual_failure_rate == 0.0 and total_tested > 0:
                actual_failure_rate = (failed_records / total_tested) * 100.0
            
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=status,
                total_tested=total_tested,
                failed_records=failed_records,
                failure_rate=actual_failure_rate,
                failure_details=failure_details,
                metadata={
                    'failure_threshold_used': failure_threshold_used
                }
            )
            
        except Exception as e:
            return TestResult(
                test_name=self.name,
                test_description=self.description,
                status=TestStatus.ERROR,
                error_message=f"SQL execution failed: {str(e)}"
            )
    
    @staticmethod
    def build_zero_failure_query(base_query: str, test_name: str, test_description: str) -> str:
        """Build a query for zero-failure pattern tests.
        
        Args:
            base_query: Base SQL query that returns failure records
            test_name: Name of the test
            test_description: Description of the test
            
        Returns:
            SQL query with consistent output format
        """
        return f"""
        WITH test_results AS (
            {base_query}
        ),
        summary AS (
            SELECT 
                COUNT(*) as failed_records,
                -- Assume total tested is derived from a separate count query or provided
                -- This will need to be customized per test
                0 as total_tested
            FROM test_results
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
                WHEN s.failed_records = 0 THEN 'All validations passed'
                ELSE s.failed_records || ' validation failures found'
            END AS failure_details,
            CURRENT_TIMESTAMP() AS execution_timestamp
        FROM summary s
        """
    
    @staticmethod
    def build_threshold_query(base_query: str, test_name: str, test_description: str, 
                            threshold_column: str = 'failure_rate') -> str:
        """Build a query for threshold-based pattern tests.
        
        Args:
            base_query: Base SQL query that calculates metrics
            test_name: Name of the test
            test_description: Description of the test
            threshold_column: Column name containing the metric to compare against threshold
            
        Returns:
            SQL query with threshold comparison and consistent output format
        """
        return f"""
        WITH test_results AS (
            {base_query}
        )
        SELECT 
            '{test_name}' AS test_name,
            '{test_description}' AS test_description,
            tr.total_tested,
            tr.failed_records,
            CASE 
                WHEN tr.{threshold_column} <= {{FAILURE_THRESHOLD}} THEN 'PASS' 
                ELSE 'FAIL' 
            END AS pass_fail_status,
            {{FAILURE_THRESHOLD}} AS failure_threshold,
            tr.{threshold_column} AS actual_failure_rate,
            CASE 
                WHEN tr.{threshold_column} <= {{FAILURE_THRESHOLD}} THEN 'Failure rate within acceptable threshold'
                ELSE 'Failure rate exceeds threshold: ' || tr.{threshold_column} || '% > ' || {{FAILURE_THRESHOLD}} || '%'
            END AS failure_details,
            CURRENT_TIMESTAMP() AS execution_timestamp
        FROM test_results tr
        """


class TestSuite:
    """Collection of related tests."""
    
    def __init__(self, name: str, description: str):
        """Initialize test suite.
        
        Args:
            name: Suite name
            description: Suite description
        """
        self.name = name
        self.description = description
        self.tests: List[BaseTest] = []
    
    def add_test(self, test: BaseTest) -> None:
        """Add test to suite.
        
        Args:
            test: Test to add
        """
        self.tests.append(test)
    
    def run_all(self, context: TestContext, parallel: bool = False) -> List[TestResult]:
        """Run all tests in the suite.
        
        Args:
            context: Test execution context
            parallel: Whether to run tests in parallel
            
        Returns:
            List of test results
        """
        results = []
        
        if parallel:
            # TODO: Implement parallel execution
            # For now, run sequentially
            pass
        
        for test in self.tests:
            result = test.run(context)
            results.append(result)
            
        return results
    
    def get_test_count(self) -> int:
        """Get number of tests in suite.
        
        Returns:
            Number of tests
        """
        return len(self.tests)
    
    def get_tests_by_category(self, category: str) -> List[BaseTest]:
        """Get tests by category.
        
        Args:
            category: Test category
            
        Returns:
            List of tests in category
        """
        return [test for test in self.tests if test.category == category]