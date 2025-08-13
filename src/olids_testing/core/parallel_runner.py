"""Enhanced parallel test execution with better progress tracking."""

from __future__ import annotations

import concurrent.futures
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from rich.layout import Layout
from rich.panel import Panel
from rich.columns import Columns
from rich.text import Text

from .config import Config, EnvironmentConfig
from .connection import SnowflakeConnection
from .test_base import BaseTest, TestContext, TestResult, TestStatus
from .sql_logger import get_sql_logger


@dataclass
class TestItem:
    """Represents an individual test to be executed."""
    suite_name: str
    test_name: str
    test_instance: BaseTest
    sub_test_name: Optional[str] = None
    
    @property
    def display_name(self):
        if self.sub_test_name:
            return f"{self.test_name}.{self.sub_test_name}"
        return self.test_name


class ParallelTestRunner:
    """Enhanced parallel test runner with better progress tracking."""
    
    def __init__(self, env_config: EnvironmentConfig, max_workers: int = 4):
        """Initialize the parallel test runner.
        
        Args:
            env_config: Environment configuration
            max_workers: Maximum number of parallel workers
        """
        self.env_config = env_config
        self.max_workers = max_workers
        self.console = Console()
        
        # Tracking state
        self.test_queue: List[TestItem] = []
        self.results: List[TestResult] = []
        self.worker_status: Dict[int, str] = {}
        self.suite_progress: Dict[str, Tuple[int, int]] = {}  # suite -> (completed, total)
        self.lock = threading.Lock()
        
        # Spinner for active workers
        from rich.spinner import Spinner
        self.spinner = Spinner("dots", style="yellow")
        
    def prepare_execution_plan(self, test_suites: Dict[str, List[Tuple[str, BaseTest]]]) -> None:
        """Prepare the execution plan with all tests to run.
        
        Args:
            test_suites: Dictionary of suite name -> list of (test_name, test_instance) tuples
        """
        self.test_queue.clear()
        self.suite_progress.clear()
        
        # Build the test queue and count tests per suite
        for suite_name, tests in test_suites.items():
            suite_test_count = 0
            
            for test_name, test_instance in tests:
                # For now, treat each test as a single item
                # Future enhancement: break down tests with many sub-tests
                self.test_queue.append(TestItem(
                    suite_name=suite_name,
                    test_name=test_name,
                    test_instance=test_instance
                ))
                
                # Count data tests within this test
                data_test_count = count_data_tests(test_instance)
                suite_test_count += data_test_count
            
            self.suite_progress[suite_name] = (0, suite_test_count)
    
    def display_execution_plan(self) -> None:
        """Display the execution plan before starting."""
        total_data_tests = sum(count for _, count in self.suite_progress.values())
        
        self.console.print("\n[bold cyan]🚀 Execution Plan[/bold cyan]")
        self.console.print(f"Test suites: [bold]{len(self.test_queue)}[/bold]")
        self.console.print(f"Total data tests: [bold]{total_data_tests}[/bold]")
        self.console.print(f"Parallel workers: [bold]{self.max_workers}[/bold]\n")
        
        # Show test suite breakdown
        table = Table(title="Test Suite Breakdown", show_header=True, header_style="bold magenta")
        table.add_column("Suite", style="cyan")
        table.add_column("Data Tests", justify="right")
        table.add_column("Status", justify="center")
        
        for item in self.test_queue:
            data_test_count = count_data_tests(item.test_instance)
            table.add_row(
                item.test_name,
                str(data_test_count),
                "[dim]Pending[/dim]"
            )
        
        self.console.print(table)
        self.console.print()
    
    def run_test_item(self, item: TestItem, worker_id: int, shared_session) -> TestResult:
        """Execute a single test item using a shared session.
        
        Args:
            item: Test item to execute
            worker_id: ID of the worker executing this test
            shared_session: Shared Snowflake session for all workers
            
        Returns:
            Test result
        """
        # Update worker status
        with self.lock:
            self.worker_status[worker_id] = f"Running {item.display_name}"
        
        try:
            # Use the shared session instead of creating a new connection
            context = TestContext(
                environment=self.env_config.name if hasattr(self.env_config, 'name') else 'uat',
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
                session=shared_session,
                config={}
            )
            
            # Run the test (suppress its own progress output in parallel mode)
            # Set a context flag to indicate parallel execution
            context.config['parallel_execution'] = True
            context.config['show_progress'] = False
            
            if item.sub_test_name:
                # Run specific sub-test
                result = item.test_instance.run_sub_test(context, item.sub_test_name)
            else:
                # Run full test
                result = item.test_instance.run(context)
            
            return result
                
        except Exception as e:
            # Create error result
            return TestResult(
                test_name=item.display_name,
                test_description=f"Test {item.display_name}",
                status=TestStatus.ERROR,
                error_message=str(e)
            )
        finally:
            # Clear worker status
            with self.lock:
                if worker_id in self.worker_status:
                    del self.worker_status[worker_id]
    
    def update_suite_progress(self, suite_name: str, test_instance: BaseTest) -> None:
        """Update the progress for a suite.
        
        Args:
            suite_name: Name of the suite to update
            test_instance: The test instance that completed
        """
        with self.lock:
            if suite_name in self.suite_progress:
                completed, total = self.suite_progress[suite_name]
                # Add the number of data tests this test represents
                data_test_count = count_data_tests(test_instance)
                self.suite_progress[suite_name] = (completed + data_test_count, total)
    
    def create_compact_display(self, completed: int, total: int) -> Table:
        """Create a compact progress display with worker rows.
        
        Args:
            completed: Number of completed data tests
            total: Total number of data tests
            
        Returns:
            Rich Table with progress information
        """
        # Create main table
        table = Table(show_header=False, show_edge=False, padding=(0, 1))
        table.add_column("Item", style="cyan", width=80)
        
        # Overall progress bar (compact, no timer)
        percentage = (completed / total * 100) if total > 0 else 0
        progress_text = f"Progress: {completed}/{total} data tests ({percentage:.1f}%)"
        
        # Create a simple progress bar representation
        filled = int(50 * completed / total) if total > 0 else 0
        bar = "█" * filled + "░" * (50 - filled)
        
        table.add_row(f"[bold cyan]{progress_text}[/bold cyan]")
        table.add_row(f"[cyan]│{bar}│[/cyan]")
        table.add_row("")  # Spacing
        
        # Worker status rows with spinners
        any_active = False
        for worker_id in range(self.max_workers):
            if worker_id in self.worker_status:
                status = self.worker_status[worker_id]
                # Extract just the test name from "Running test_name"
                if status.startswith("Running "):
                    test_name = status[8:]  # Remove "Running "
                else:
                    test_name = status
                
                # Truncate long test names
                if len(test_name) > 50:
                    test_name = test_name[:47] + "..."
                
                # Use animated spinner for active workers
                spinner_text = self.spinner.render(time=__import__('time').time())
                table.add_row(f"{spinner_text} Worker {worker_id}: [white]{test_name}[/white]")
                any_active = True
            else:
                table.add_row(f"[dim]●[/dim] Worker {worker_id}: [dim]Idle[/dim]")
        
        if not any_active and completed < total:
            table.add_row("[dim]Starting workers...[/dim]")
        
        return table
    
    def run_all(self, test_suites: Dict[str, List[Tuple[str, BaseTest]]]) -> List[TestResult]:
        """Run all tests in parallel with enhanced progress tracking.
        
        Args:
            test_suites: Dictionary of suite name -> list of (test_name, test_instance) tuples
            
        Returns:
            List of test results
        """
        # Prepare execution plan
        self.prepare_execution_plan(test_suites)
        
        # Display execution plan
        self.display_execution_plan()
        
        # Clear results
        self.results.clear()
        
        # Calculate total data tests for progress bar
        total_data_tests = sum(count for _, count in self.suite_progress.values())
        
        # Create a single shared connection for all workers to avoid multiple auth popups
        with SnowflakeConnection(self.env_config) as conn:
            shared_session = conn.get_session()
            
            # Show initial state
            self.console.print("\n[bold cyan]Executing Tests[/bold cyan]")
            completed_count = 0
            
            with Live(console=self.console, refresh_per_second=3, transient=False) as live:
                # Start with initial display
                initial_display = self.create_compact_display(0, total_data_tests)
                live.update(initial_display)
                
                # Run tests in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submit all tests
                    future_to_item = {}
                    for i, item in enumerate(self.test_queue):
                        worker_id = i % self.max_workers
                        future = executor.submit(self.run_test_item, item, worker_id, shared_session)
                        future_to_item[future] = item
                
                    # Process results as they complete
                    for future in concurrent.futures.as_completed(future_to_item):
                        item = future_to_item[future]
                        
                        # Get result
                        result = future.result()
                        self.results.append(result)
                        
                        # Update progress
                        self.update_suite_progress(item.suite_name, item.test_instance)
                        
                        # Update completed count
                        data_test_count = count_data_tests(item.test_instance)
                        completed_count += data_test_count
                        
                        # Create and update display
                        display_content = self.create_compact_display(completed_count, total_data_tests)
                        live.update(display_content)
        
        # Show completion summary
        self.console.print("\n[bold green]✓ All tests completed![/bold green]")
        
        # Show SQL query count
        logger = get_sql_logger()
        if hasattr(logger, 'query_counter') and logger.query_counter > 0:
            self.console.print(f"[dim]SQL queries logged: {logger.query_counter} queries saved to {logger.output_dir}[/dim]")
        
        return self.results


def count_data_tests(test_instance: BaseTest) -> int:
    """Count the number of data tests in a test instance.
    
    Args:
        test_instance: Test instance to count
        
    Returns:
        Number of data tests
    """
    # Check for known test types with sub-tests
    if test_instance.name == 'referential_integrity':
        return 85  # Known count from YAML
    elif test_instance.name == 'concept_mapping':
        return 28  # Known count from YAML
    elif test_instance.name == 'person_patterns':
        return 13  # Known count from YAML
    elif test_instance.name == 'null_columns':
        return 710  # Approximate based on columns
    elif test_instance.name == 'empty_tables':
        return 28  # Known count for empty tables
    elif test_instance.name == 'column_completeness':
        return 6  # Known count for column completeness
    else:
        return 1  # Default to 1 for simple tests