"""Enhanced parallel test execution with better progress tracking."""

from __future__ import annotations

import concurrent.futures
import signal
import sys
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
    status: str = "pending"  # pending, running, completed, failed
    
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
        self.test_status: Dict[str, str] = {}  # test_name -> status
        self.completed_suites: List[str] = []  # Track completed test suites
        self.completed_tests: set = set()  # Track completed individual tests
        self.lock = threading.Lock()
        self.start_time = None
        
        # Spinner for active workers
        from rich.spinner import Spinner
        self.spinner = Spinner("dots", style="yellow")
        
        # Signal handling for graceful shutdown
        self.shutdown_requested = False
        self.executor = None
        self.live_display = None
        self.completed_count = 0
        self.total_data_tests = 0
        
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.console.print("\n[yellow]⚠️  Shutdown requested... Stopping workers gracefully...[/yellow]")
            self.shutdown_requested = True
            if self.executor:
                self.executor.shutdown(wait=False, cancel_futures=True)
            sys.exit(1)
        
        # Handle Ctrl+C and other termination signals
        signal.signal(signal.SIGINT, signal_handler)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, signal_handler)
    
    def _start_display_updater(self):
        """Start background thread to continuously update the display for spinner animation."""
        def update_display():
            while not self.shutdown_requested and self.live_display:
                try:
                    # Update display every 0.125 seconds (8 times per second)
                    display_content = self.create_compact_display(self.completed_count, self.total_data_tests)
                    if self.live_display:
                        self.live_display.update(display_content)
                    time.sleep(0.125)
                except Exception:
                    # Ignore exceptions in display updater
                    break
        
        display_thread = threading.Thread(target=update_display, daemon=True)
        display_thread.start()
        return display_thread
        
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
                # Count the actual data tests for this test instance (regardless of chunking)
                data_test_count = count_data_tests(test_instance)
                suite_test_count += data_test_count
                
                # For complex tests with many sub-tests, break them down into smaller work items
                if self._should_break_down_test(test_instance):
                    sub_items = self._break_down_test(suite_name, test_name, test_instance)
                    self.test_queue.extend(sub_items)
                else:
                    # Simple test - treat as single item
                    self.test_queue.append(TestItem(
                        suite_name=suite_name,
                        test_name=test_name,
                        test_instance=test_instance
                    ))
            
            self.suite_progress[suite_name] = (0, suite_test_count)
    
    def _should_break_down_test(self, test_instance: BaseTest) -> bool:
        """Determine if a test should be broken down into smaller work items.
        
        Args:
            test_instance: Test instance to check
            
        Returns:
            True if test should be broken down for better parallelization
        """
        # Enable chunking for tests with many sub-tests to better utilize workers
        complex_tests = ['referential_integrity', 'concept_mapping', 'person_patterns']
        return test_instance.name in complex_tests
    
    def _break_down_test(self, suite_name: str, test_name: str, test_instance: BaseTest) -> List[TestItem]:
        """Break down a complex test into smaller work items.
        
        Args:
            suite_name: Suite name
            test_name: Test name  
            test_instance: Test instance to break down
            
        Returns:
            List of TestItem work items
        """
        items = []
        
        if test_name == 'referential_integrity':
            # Break into chunks of ~5 relationships each for better distribution
            chunk_size = 5
            total_relationships = 85
            for i in range(0, total_relationships, chunk_size):
                chunk_name = f"{test_name}_chunk_{i//chunk_size + 1}"
                items.append(TestItem(
                    suite_name=suite_name,
                    test_name=chunk_name,
                    test_instance=test_instance,
                    sub_test_name=f"relationships_{i+1}-{min(i+chunk_size, total_relationships)}"
                ))
        
        elif test_name == 'concept_mapping':
            # Break into chunks of ~3 concept fields each
            chunk_size = 3
            total_concepts = 28
            for i in range(0, total_concepts, chunk_size):
                chunk_name = f"{test_name}_chunk_{i//chunk_size + 1}"
                items.append(TestItem(
                    suite_name=suite_name,
                    test_name=chunk_name,
                    test_instance=test_instance,
                    sub_test_name=f"concepts_{i+1}-{min(i+chunk_size, total_concepts)}"
                ))
        
        elif test_name == 'person_patterns':
            # Break into chunks of ~2 patterns each
            chunk_size = 2
            total_patterns = 13
            for i in range(0, total_patterns, chunk_size):
                chunk_name = f"{test_name}_chunk_{i//chunk_size + 1}"
                items.append(TestItem(
                    suite_name=suite_name,
                    test_name=chunk_name,
                    test_instance=test_instance,
                    sub_test_name=f"patterns_{i+1}-{min(i+chunk_size, total_patterns)}"
                ))
        
        else:
            # Fallback: just return the original test
            items.append(TestItem(
                suite_name=suite_name,
                test_name=test_name,
                test_instance=test_instance
            ))
        
        return items
    
    def display_execution_plan(self) -> None:
        """Display the execution plan before starting."""
        total_data_tests = sum(count for _, count in self.suite_progress.values())
        
        self.console.print("\n[bold cyan]Execution Plan[/bold cyan]")
        self.console.print(f"Work items: [bold]{len(self.test_queue)}[/bold]")
        self.console.print(f"Total data tests: [bold]{total_data_tests}[/bold]")
        self.console.print(f"Parallel workers: [bold]{self.max_workers}[/bold]\n")
        
        # Show simple test suite list
        self.console.print("Test suites planned for execution:")
        
        # Group work items by suite and get unique test names
        suite_tests = {}
        for item in self.test_queue:
            if item.suite_name not in suite_tests:
                suite_tests[item.suite_name] = set()
            # Extract base test name (remove chunk info)
            base_test_name = item.test_name.split('_chunk_')[0] if '_chunk_' in item.test_name else item.test_name
            suite_tests[item.suite_name].add(base_test_name)
        
        # Display each unique test in each suite as a simple list
        for suite_name, test_names in suite_tests.items():
            for test_name in sorted(test_names):
                # Get data test count for this test
                data_count = 0
                for item in self.test_queue:
                    if item.suite_name == suite_name and (item.test_name == test_name or item.test_name.startswith(test_name + '_chunk_')):
                        data_count = count_data_tests(item.test_instance)
                        break
                
                self.console.print(f"  • {test_name} ({data_count} data tests)")
        
        self.console.print()
    
    def run_test_item(self, item: TestItem, shared_session) -> TestResult:
        """Execute a single test item using a shared session.
        
        Args:
            item: Test item to execute
            shared_session: Shared Snowflake session for all workers
            
        Returns:
            Test result
        """
        # Get current thread ID and find available worker slot
        import threading
        thread_id = threading.current_thread().ident
        
        # Find an available worker slot or assign this thread to one
        worker_id = None
        with self.lock:
            # Check if this thread already has a worker ID assigned
            for wid, status in self.worker_status.items():
                if hasattr(status, 'thread_id') and status.thread_id == thread_id:
                    worker_id = wid
                    break
            
            # If no existing assignment, find next available worker slot
            if worker_id is None:
                for wid in range(self.max_workers):
                    if wid not in self.worker_status:
                        worker_id = wid
                        break
                        
                # Fallback: if all slots taken, find oldest completed worker
                if worker_id is None:
                    worker_id = len(self.worker_status) % self.max_workers
            
            # Update worker status with thread tracking
            self.worker_status[worker_id] = type('WorkerStatus', (), {
                'display': f"Running {item.display_name}",
                'thread_id': thread_id,
                'test_name': item.display_name
            })()
            
            # Update test status using base test name
            base_test_name = item.test_name.split('_chunk_')[0] if '_chunk_' in item.test_name else item.test_name
            self.test_status[base_test_name] = "running"
        
        result = None
        try:
            # Use the shared session instead of creating a new connection
            # Create a progress callback that updates the parallel runner's progress
            def progress_callback(completed: int):
                with self.lock:
                    # For null_columns, track its specific progress
                    if item.test_name == 'null_columns':
                        # Calculate the base completed count (everything except null_columns)
                        null_columns_data_tests = count_data_tests(item.test_instance)
                        base_completed = self.completed_count - getattr(self, '_null_columns_progress', 0)
                        
                        # Update null_columns progress
                        self._null_columns_progress = completed
                        
                        # Set total completed count
                        self.completed_count = base_completed + completed
            
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
                config={},
                progress_callback=progress_callback
            )
            
            # Run the test (suppress its own progress output in parallel mode)
            # Set a context flag to indicate parallel execution
            context.config['parallel_execution'] = True
            context.config['show_progress'] = False
            
            # Execute specific chunk if sub_test_name is provided
            if item.sub_test_name:
                # Add chunk information to context so the test knows which subset to run
                context.config['chunk_info'] = item.sub_test_name
                context.config['chunk_test_name'] = item.test_name
            
            result = item.test_instance.run(context)
            
            return result
                
        except Exception as e:
            # Create error result
            result = TestResult(
                test_name=item.display_name,
                test_description=f"Test {item.display_name}",
                status=TestStatus.ERROR,
                error_message=str(e)
            )
            return result
        finally:
            # Clear worker status for this thread and update test status
            with self.lock:
                if worker_id is not None and worker_id in self.worker_status:
                    del self.worker_status[worker_id]
                
                # Update test status based on result using base test name
                if result and hasattr(result, 'status'):
                    base_test_name = item.test_name.split('_chunk_')[0] if '_chunk_' in item.test_name else item.test_name
                    if result.status == TestStatus.PASSED:
                        self.test_status[base_test_name] = "completed"
                    elif result.status in [TestStatus.FAILED, TestStatus.ERROR]:
                        self.test_status[base_test_name] = "failed"
                    else:
                        self.test_status[base_test_name] = "completed"  # Default to completed
                    
                    # Check if this base test is now fully completed (all chunks done)
                    self._check_test_completion(base_test_name, item.suite_name)
    
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
                new_completed = completed + data_test_count
                self.suite_progress[suite_name] = (new_completed, total)
                
                # Check if this suite is now complete
                if new_completed >= total and suite_name not in self.completed_suites:
                    self.completed_suites.append(suite_name)
    
    def _check_test_completion(self, base_test_name: str, suite_name: str) -> None:
        """Check if a test and its suite are fully completed.
        
        Args:
            base_test_name: Base name of the test (without chunk suffix)
            suite_name: Name of the test suite
        """
        # Count how many chunks of this test are still pending/running
        remaining_chunks = 0
        for item in self.test_queue:
            item_base_name = item.test_name.split('_chunk_')[0] if '_chunk_' in item.test_name else item.test_name
            if item_base_name == base_test_name and item_base_name not in self.completed_tests:
                # Check if this item has been processed yet
                if item not in [r.test_name for r in self.results]:
                    remaining_chunks += 1
        
        # If no chunks remaining, mark the test as completed
        if remaining_chunks == 0:
            self.completed_tests.add(base_test_name)
            
            # Check if the whole suite is now complete
            suite_tests = set()
            for item in self.test_queue:
                if item.suite_name == suite_name:
                    item_base_name = item.test_name.split('_chunk_')[0] if '_chunk_' in item.test_name else item.test_name
                    suite_tests.add(item_base_name)
            
            # If all tests in the suite are completed, mark the suite as complete
            if suite_tests.issubset(self.completed_tests) and suite_name not in self.completed_suites:
                self.completed_suites.append(suite_name)
    
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
                status_obj = self.worker_status[worker_id]
                
                # Handle new status object format
                if hasattr(status_obj, 'test_name'):
                    test_name = status_obj.test_name
                else:
                    # Fallback for any remaining string status
                    test_name = str(status_obj)
                    if test_name.startswith("Running "):
                        test_name = test_name[8:]  # Remove "Running "
                
                # Truncate long test names
                if len(test_name) > 50:
                    test_name = test_name[:47] + "..."
                
                # Use animated spinner for active workers - use current time for animation
                current_time = time.time()
                spinner_text = self.spinner.render(time=current_time)
                table.add_row(f"{spinner_text} Worker {worker_id}: [white]{test_name}[/white]")
                any_active = True
            else:
                table.add_row(f"[dim]●[/dim] Worker {worker_id}: [dim]Idle[/dim]")
        
        if not any_active and completed < total:
            table.add_row("[dim]Starting workers...[/dim]")
        
        # Add completed test suites section
        if self.completed_suites:
            table.add_row("")  # Spacing
            table.add_row("[bold green]Completed Test Suites:[/bold green]")
            for suite_name in self.completed_suites:
                table.add_row(f"[green]  + {suite_name}[/green]")
        
        return table
    
    def run_all(self, test_suites: Dict[str, List[Tuple[str, BaseTest]]]) -> List[TestResult]:
        """Run all tests in parallel with enhanced progress tracking.
        
        Args:
            test_suites: Dictionary of suite name -> list of (test_name, test_instance) tuples
            
        Returns:
            List of test results
        """
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        # Prepare execution plan
        self.prepare_execution_plan(test_suites)
        
        # Display execution plan
        self.display_execution_plan()
        
        # Clear results and initialize status tracking
        self.results.clear()
        self.test_status.clear()
        self.start_time = time.time()
        
        # Initialize all tests as pending using base test names
        for item in self.test_queue:
            base_test_name = item.test_name.split('_chunk_')[0] if '_chunk_' in item.test_name else item.test_name
            self.test_status[base_test_name] = "pending"
        
        # Calculate total data tests for progress bar
        self.total_data_tests = sum(count for _, count in self.suite_progress.values())
        self.completed_count = 0
        
        # Create a single shared connection for all workers to avoid multiple auth popups
        with SnowflakeConnection(self.env_config) as conn:
            shared_session = conn.get_session()
            
            # Show initial state
            self.console.print("\n[bold cyan]Executing Tests[/bold cyan]")
            
            with Live(console=self.console, refresh_per_second=8, transient=False) as live:
                self.live_display = live
                
                # Start with initial display
                initial_display = self.create_compact_display(0, self.total_data_tests)
                live.update(initial_display)
                
                # Start background display updater for continuous spinner animation
                display_updater = self._start_display_updater()
                
                # Run tests in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    self.executor = executor  # Store reference for signal handler
                    
                    try:
                        # Submit all tests
                        future_to_item = {}
                        for item in self.test_queue:
                            if self.shutdown_requested:
                                break
                            future = executor.submit(self.run_test_item, item, shared_session)
                            future_to_item[future] = item
                    
                        # Process results as they complete
                        for future in concurrent.futures.as_completed(future_to_item):
                            if self.shutdown_requested:
                                break
                                
                            item = future_to_item[future]
                            
                            try:
                                # Get result (this will raise if the future was cancelled)
                                result = future.result()
                                self.results.append(result)
                                
                                # Update progress
                                self.update_suite_progress(item.suite_name, item.test_instance)
                                
                                # Update completed count based on chunk size, not full test count
                                if item.sub_test_name:
                                    # For chunks, add the actual number of sub-tests executed
                                    chunk_size = result.total_tested if hasattr(result, 'total_tested') else 1
                                else:
                                    # For full tests, add the full test count
                                    chunk_size = count_data_tests(item.test_instance)
                                
                                with self.lock:
                                    self.completed_count += chunk_size
                                
                            except concurrent.futures.CancelledError:
                                # Future was cancelled due to shutdown
                                continue
                            except Exception as e:
                                # Handle any other exceptions
                                self.console.print(f"[red]Error processing result: {e}[/red]")
                                continue
                                
                    except KeyboardInterrupt:
                        self.console.print("\n[yellow]⚠️  Interruption detected, shutting down workers...[/yellow]")
                        self.shutdown_requested = True
                        executor.shutdown(wait=False, cancel_futures=True)
                        raise
                    finally:
                        self.executor = None
                
                # Final update: Set progress to 100% completion
                with self.lock:
                    self.completed_count = self.total_data_tests
                
                # Show final display with 100% completion
                final_display = self.create_compact_display(self.total_data_tests, self.total_data_tests)
                live.update(final_display)
                
                # Clean up display updater
                self.live_display = None
        
        # Calculate overall execution time
        overall_time = time.time() - self.start_time
        
        # Show completion summary with overall timing
        self.console.print("\n[bold green]All tests completed![/bold green]")
        self.console.print(f"[dim]Overall execution time: {overall_time:.2f}s[/dim]")
        
        # Show SQL query count
        logger = get_sql_logger()
        if hasattr(logger, 'query_counter') and logger.query_counter > 0:
            self.console.print(f"[dim]SQL queries logged: {logger.query_counter} queries saved to {logger.output_dir}[/dim]")
        
        # Aggregate chunked results back into logical test results
        # Mark as parallel execution so display can handle timing differently
        aggregated_results = self._aggregate_chunked_results(self.results, parallel_mode=True, overall_time=overall_time)
        
        return aggregated_results
    
    def _aggregate_chunked_results(self, results: List[TestResult], parallel_mode: bool = False, overall_time: float = 0.0) -> List[TestResult]:
        """Aggregate chunked test results back into logical test results.
        
        Args:
            results: List of all test results including chunks
            parallel_mode: Whether this was parallel execution
            overall_time: Overall execution time for parallel mode
            
        Returns:
            List of aggregated test results (one per logical test)
        """
        from collections import defaultdict
        
        # Group results by base test name
        test_groups = defaultdict(list)
        for result in results:
            # Extract base test name - handle both _chunk_ and actual chunk patterns
            base_name = result.test_name
            
            # Check for patterns like "referential_integrity_relationships_1-5"
            if '_relationships_' in base_name or '_concepts_' in base_name or '_patterns_' in base_name:
                # Extract the base test name before the chunk suffix
                if 'referential_integrity_relationships_' in base_name:
                    base_name = 'referential_integrity'
                elif 'concept_mapping_concepts_' in base_name:
                    base_name = 'concept_mapping'
                elif 'person_patterns_patterns_' in base_name:
                    base_name = 'person_patterns'
            # Also handle _chunk_ pattern in case it's still used somewhere
            elif '_chunk_' in base_name:
                base_name = base_name.split('_chunk_')[0]
            
            test_groups[base_name].append(result)
        
        aggregated = []
        for base_name, chunks in test_groups.items():
            # Check if this test was actually chunked
            is_chunked = any('_relationships_' in c.test_name or 
                            '_concepts_' in c.test_name or 
                            '_patterns_' in c.test_name or
                            '_chunk_' in c.test_name 
                            for c in chunks)
            
            if len(chunks) == 1 and not is_chunked:
                # Not chunked, but still clear execution time for parallel mode
                result = chunks[0]
                if parallel_mode:
                    # Create a copy with execution_time set to None for parallel mode display
                    from copy import deepcopy
                    result = deepcopy(result)
                    result.execution_time = None
                aggregated.append(result)
            else:
                # Aggregate chunks into a single result
                aggregated_result = self._combine_chunk_results(base_name, chunks, parallel_mode)
                aggregated.append(aggregated_result)
        
        return aggregated
    
    def _combine_chunk_results(self, base_name: str, chunks: List[TestResult], parallel_mode: bool = False) -> TestResult:
        """Combine multiple chunk results into a single aggregated result.
        
        Args:
            base_name: Base name of the test
            chunks: List of chunk results to combine
            
        Returns:
            Combined TestResult
        """
        # Use the first chunk for base info
        first_chunk = chunks[0]
        
        # Aggregate metrics
        total_tested = sum(chunk.total_tested for chunk in chunks)
        failed_records = sum(chunk.failed_records for chunk in chunks)
        
        # For parallel mode, don't show individual test times as they're not meaningful
        # Individual chunks ran in parallel, so their individual times don't represent the "test suite time"
        if parallel_mode:
            total_execution_time = None  # Will be handled by overall time display
        else:
            # For sequential mode, sum the times as tests ran one after another
            total_execution_time = sum(chunk.execution_time or 0.0 for chunk in chunks)
        
        # Determine overall status
        if any(chunk.status == TestStatus.ERROR for chunk in chunks):
            status = TestStatus.ERROR
        elif any(chunk.status == TestStatus.FAILED for chunk in chunks):
            status = TestStatus.FAILED
        else:
            status = TestStatus.PASSED
        
        # Calculate failure rate
        failure_rate = (failed_records / total_tested * 100) if total_tested > 0 else 0.0
        
        # Combine failure details
        failure_details_list = []
        if failed_records > 0:
            failure_details_list.append(f"Failed {failed_records} out of {total_tested} {base_name} sub-tests:")
            
            for chunk in chunks:
                if chunk.failure_details and chunk.failed_records > 0:
                    # Extract individual failure lines from chunk details
                    lines = chunk.failure_details.split('\n')
                    for line in lines[1:]:  # Skip the summary line
                        if line.strip():
                            failure_details_list.append(line)
        
        failure_details = '\n'.join(failure_details_list) if failure_details_list else None
        
        # Get description from the base test name
        description_map = {
            'referential_integrity': 'Validates all 85 foreign key relationships in OLIDS database',
            'concept_mapping': 'Validates concept ID mappings from source tables through CONCEPT_MAP to CONCEPT',
            'person_patterns': 'Validates person data patterns based on business rules',
            'null_columns': 'Identifies columns that contain only NULL values',
            'empty_tables': 'Identifies tables that contain no data (zero rows)',
            'column_completeness': 'Checks completeness rates for critical columns'
        }
        
        return TestResult(
            test_name=base_name,
            test_description=description_map.get(base_name, f"Combined {base_name} test"),
            status=status,
            total_tested=total_tested,
            failed_records=failed_records,
            failure_rate=failure_rate,
            failure_details=failure_details,
            execution_time=total_execution_time,
            metadata={
                'chunk_count': len(chunks),
                'chunks_combined': True
            }
        )


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