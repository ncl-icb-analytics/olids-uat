"""Base classes for OLIDS testing framework."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

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