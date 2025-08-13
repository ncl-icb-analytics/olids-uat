"""Test execution engine for OLIDS testing framework."""

from __future__ import annotations

import concurrent.futures
import time
from typing import Dict, List, Optional, Set

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from .config import Config, EnvironmentConfig
from .connection import SnowflakeConnection
from .test_base import BaseTest, TestContext, TestResult, TestStatus
from .sql_logger import reset_sql_logger, get_sql_logger


class TestRunner:
    """Manages test execution and reporting."""
    
    def __init__(self, config: Config, environment: str = "uat"):
        """Initialize test runner.
        
        Args:
            config: Configuration manager
            environment: Environment name to use
        """
        self.config = config
        self.environment = environment
        self.env_config = config.get_environment(environment)
        self.console = Console()
        
        # Initialize SQL logging (clears previous logs)
        reset_sql_logger()
        
        # Test registry
        self._tests: Dict[str, BaseTest] = {}
        self._test_suites: Dict[str, List[str]] = {}
        
        # Auto-load tests from test modules
        self._load_tests()
    
    def _load_tests(self) -> None:
        """Load tests from test modules."""
        try:
            from ..tests import TEST_REGISTRY, TEST_CATEGORIES
            
            # Register all tests
            for test_name, test_class in TEST_REGISTRY.items():
                test_instance = test_class()
                self.register_test(test_instance)
            
            # Register test suites by category
            for category, test_names in TEST_CATEGORIES.items():
                if test_names:  # Only register non-empty categories
                    self.register_test_suite(category, test_names)
                    
        except ImportError as e:
            # Graceful fallback if test modules aren't available yet
            self.console.print(f"[dim]Warning: Could not load test modules: {e}[/dim]")
        
    def register_test(self, test: BaseTest) -> None:
        """Register a test for execution.
        
        Args:
            test: Test to register
        """
        self._tests[test.name] = test
        
    def register_test_suite(self, suite_name: str, test_names: List[str]) -> None:
        """Register a test suite.
        
        Args:
            suite_name: Name of the test suite
            test_names: List of test names in the suite
        """
        self._test_suites[suite_name] = test_names
        
    def list_tests(self, main_only: bool = False) -> List[str]:
        """List registered tests.
        
        Args:
            main_only: If True, only return main tests (for 'run all')
        
        Returns:
            List of test names
        """
        if main_only:
            from ..tests import TEST_REGISTRY
            return list(TEST_REGISTRY.keys())
        return list(self._tests.keys())
        
    def list_test_suites(self) -> List[str]:
        """List all registered test suites.
        
        Returns:
            List of test suite names
        """
        return list(self._test_suites.keys())
    
    def run_test(self, test_name: str, show_progress: bool = True) -> TestResult:
        """Run a single test.
        
        Args:
            test_name: Name of test to run
            show_progress: Whether to show progress indicators
            
        Returns:
            Test result
            
        Raises:
            KeyError: If test not found
        """
        if test_name not in self._tests:
            raise KeyError(f"Test '{test_name}' not found")
        
        test = self._tests[test_name]
        
        with SnowflakeConnection(self.env_config) as conn:
            context = TestContext(
                environment=self.environment,
                databases={
                    "source": self.env_config.databases.source,
                    "terminology": self.env_config.databases.terminology,
                    "results": self.env_config.databases.results,
                    "dictionary": self.env_config.databases.dictionary,
                },
                schemas={
                    "masked": self.env_config.schemas.masked,
                    "terminology": self.env_config.schemas.terminology,
                    "tests": self.env_config.schemas.tests,
                },
                session=conn.get_session(),
                config={}
            )
            
            # Tests now handle their own progress indicators
            result = test.run(context)
            
            return result
    
    def run_test_suite(self, suite_name: str, parallel: bool = False, show_progress: bool = True) -> List[TestResult]:
        """Run a test suite.
        
        Args:
            suite_name: Name of test suite to run
            parallel: Whether to run tests in parallel
            show_progress: Whether to show progress indicators
            
        Returns:
            List of test results
            
        Raises:
            KeyError: If test suite not found
        """
        if suite_name not in self._test_suites:
            raise KeyError(f"Test suite '{suite_name}' not found")
        
        test_names = self._test_suites[suite_name]
        return self.run_tests(test_names, parallel=parallel, show_progress=show_progress, suite_name=suite_name)
    
    def run_tests(self, test_names: List[str], parallel: bool = False, show_progress: bool = True, suite_name: str = "tests") -> List[TestResult]:
        """Run multiple tests.
        
        Args:
            test_names: List of test names to run
            parallel: Whether to run tests in parallel
            show_progress: Whether to show progress indicators
            suite_name: Name of the test suite being run
            
        Returns:
            List of test results
        """
        # Validate all tests exist
        missing_tests = [name for name in test_names if name not in self._tests]
        if missing_tests:
            raise KeyError(f"Tests not found: {missing_tests}")
        
        if parallel:
            return self._run_tests_parallel(test_names, show_progress, suite_name)
        else:
            return self._run_tests_sequential(test_names, show_progress, suite_name)
    
    def _run_tests_sequential(self, test_names: List[str], show_progress: bool, suite_name: str = "tests") -> List[TestResult]:
        """Run tests sequentially.
        
        Args:
            test_names: List of test names to run
            show_progress: Whether to show progress indicators
            
        Returns:
            List of test results
        """
        results = []
        
        if show_progress:
            import sys
            print(f"Running {suite_name}")
            print(f"Running {len(test_names)} tests")
            for i, test_name in enumerate(test_names):
                sys.stdout.write(f"\r  [{i+1}/{len(test_names)}] {test_name}")
                sys.stdout.flush()
                result = self.run_test(test_name, show_progress=False)
                results.append(result)
                # Clear the line completely and show completion
                spaces = " " * 80  # Clear any remaining characters
                sys.stdout.write(f"\r  [{i+1}/{len(test_names)}] {test_name} - completed{spaces[:max(0, 80-len(test_name)-30)]}")
                print()  # Move to next line
            
            # Show total SQL queries logged at the end
            logger = get_sql_logger()
            if hasattr(logger, 'query_counter') and logger.query_counter > 0:
                print(f"SQL queries logged: {logger.query_counter} queries saved to {logger.output_dir}")
            print()  # Extra line before results
        else:
            for test_name in test_names:
                result = self.run_test(test_name, show_progress=False)
                results.append(result)
        
        return results
    
    def _run_tests_parallel(self, test_names: List[str], show_progress: bool, suite_name: str = "tests") -> List[TestResult]:
        """Run tests in parallel with enhanced progress tracking.
        
        Args:
            test_names: List of test names to run
            show_progress: Whether to show progress indicators
            
        Returns:
            List of test results
        """
        from .parallel_runner import ParallelTestRunner
        
        # Use enhanced parallel runner for better progress tracking
        parallel_runner = ParallelTestRunner(
            self.env_config,
            max_workers=min(self.env_config.execution.parallel_workers, len(test_names))
        )
        
        # Organize tests by suite for the parallel runner
        test_suites = {suite_name: [(name, self._tests[name]) for name in test_names]}
        
        # Run with enhanced progress display
        if show_progress:
            results = parallel_runner.run_all(test_suites)
        else:
            # Fall back to simple parallel execution without progress
            max_workers = min(self.env_config.execution.parallel_workers, len(test_names))
            results = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_test = {
                    executor.submit(self.run_test, test_name, False): test_name
                    for test_name in test_names
                }
                
                for future in concurrent.futures.as_completed(future_to_test):
                    test_name = future_to_test[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        error_result = TestResult(
                            test_name=test_name,
                            test_description=f"Test {test_name}",
                            status=TestStatus.ERROR,
                            error_message=str(e)
                        )
                        results.append(error_result)
        
        # Sort results to match original order
        test_order = {name: i for i, name in enumerate(test_names)}
        results.sort(key=lambda r: test_order.get(r.test_name, float('inf')))
        
        return results
    
    def run_all_tests(self, parallel: bool = False, show_progress: bool = True) -> List[TestResult]:
        """Run all registered tests.
        
        Args:
            parallel: Whether to run tests in parallel
            show_progress: Whether to show progress indicators
            
        Returns:
            List of test results
        """
        if parallel and show_progress:
            # Use enhanced parallel runner for "run all" with better suite organization
            from .parallel_runner import ParallelTestRunner
            
            parallel_runner = ParallelTestRunner(
                self.env_config,
                max_workers=self.env_config.execution.parallel_workers
            )
            
            # Organize each individual test as its own suite for better tracking
            test_suites = {}
            for test_name, test_instance in self._tests.items():
                test_suites[test_name] = [(test_name, test_instance)]
            
            # Debug: print what suites we're running
            # self.console.print(f"[dim]Debug: Running suites: {list(test_suites.keys())}[/dim]")
            
            return parallel_runner.run_all(test_suites)
        else:
            # Fall back to simple execution
            test_names = list(self._tests.keys())
            return self.run_tests(test_names, parallel=parallel, show_progress=show_progress)
    
    def get_summary(self, results: List[TestResult]) -> Dict[str, int]:
        """Get test execution summary.
        
        Args:
            results: List of test results
            
        Returns:
            Summary statistics
        """
        summary = {
            "total": len(results),
            "passed": 0,
            "failed": 0,
            "error": 0,
            "skipped": 0,
        }
        
        for result in results:
            if result.status == TestStatus.PASSED:
                summary["passed"] += 1
            elif result.status == TestStatus.FAILED:
                summary["failed"] += 1
            elif result.status == TestStatus.ERROR:
                summary["error"] += 1
            elif result.status == TestStatus.SKIPPED:
                summary["skipped"] += 1
        
        return summary
    
    def validate_environment(self) -> bool:
        """Validate the test environment.
        
        Returns:
            True if environment is valid
        """
        try:
            with SnowflakeConnection(self.env_config) as conn:
                status = conn.test_connection()
                return status.get("status") == "OK"
        except Exception:
            return False