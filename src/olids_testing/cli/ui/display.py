"""Display utilities for OLIDS testing framework CLI."""

import json
from pathlib import Path
from typing import Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from olids_testing.core.config import Config, EnvironmentConfig
from olids_testing.core.test_base import TestResult

console = Console()


def display_config(config: Config, env_config: EnvironmentConfig, environment: str, output_format: str = "table"):
    """Display configuration information.
    
    Args:
        config: Configuration manager
        env_config: Environment configuration
        environment: Environment name
        output_format: Output format (table or json)
    """
    if output_format == "json":
        config_data = {
            "environment": environment,
            "description": env_config.description,
            "databases": {
                "source": env_config.databases.source,
                "terminology": env_config.databases.terminology,
                "results": env_config.databases.results,
                "dictionary": env_config.databases.dictionary,
            },
            "schemas": {
                "masked": env_config.schemas.masked,
                "terminology": env_config.schemas.terminology,
                "tests": env_config.schemas.tests,
            },
            "connection": {
                "account": env_config.connection.account,
                "host": env_config.connection.host,
                "role": env_config.connection.role,
                "warehouse": env_config.connection.warehouse,
            },
            "execution": {
                "parallel_workers": env_config.execution.parallel_workers,
                "timeout_seconds": env_config.execution.timeout_seconds,
                "retry_attempts": env_config.execution.retry_attempts,
            }
        }
        console.print(json.dumps(config_data, indent=2))
        return
    
    # Table format
    table = Table(title=f"Configuration - {environment.upper()}")
    table.add_column("Category", style="cyan", no_wrap=True)
    table.add_column("Setting", style="magenta")
    table.add_column("Value", style="green")
    
    # Environment info
    table.add_row("Environment", "Name", environment)
    table.add_row("", "Description", env_config.description)
    table.add_row("", "", "")
    
    # Databases
    table.add_row("Databases", "Source", env_config.databases.source)
    table.add_row("", "Terminology", env_config.databases.terminology)
    table.add_row("", "Results", env_config.databases.results)
    table.add_row("", "Dictionary", env_config.databases.dictionary)
    table.add_row("", "", "")
    
    # Schemas
    table.add_row("Schemas", "Masked", env_config.schemas.masked)
    table.add_row("", "Terminology", env_config.schemas.terminology)
    table.add_row("", "Tests", env_config.schemas.tests)
    table.add_row("", "", "")
    
    # Connection
    table.add_row("Connection", "Account", env_config.connection.account)
    table.add_row("", "Host", env_config.connection.host)
    table.add_row("", "Role", env_config.connection.role)
    table.add_row("", "Warehouse", env_config.connection.warehouse)
    table.add_row("", "", "")
    
    # Execution
    table.add_row("Execution", "Parallel Workers", str(env_config.execution.parallel_workers))
    table.add_row("", "Timeout (sec)", str(env_config.execution.timeout_seconds))
    table.add_row("", "Retry Attempts", str(env_config.execution.retry_attempts))
    
    console.print(table)


def display_validation_results(config_ok: bool, connection_ok: bool, config: Config, environment: str):
    """Display validation results.
    
    Args:
        config_ok: Configuration validation result
        connection_ok: Connection validation result
        config: Configuration manager
        environment: Environment name
    """
    table = Table(title="Validation Results")
    table.add_column("Component", style="cyan", no_wrap=True)
    table.add_column("Status", style="bold")
    table.add_column("Details", style="dim")
    
    # Configuration validation
    config_status = "[green]PASS[/green]" if config_ok else "[red]FAIL[/red]"
    config_details = "All configuration files valid" if config_ok else "Configuration validation failed"
    table.add_row("Configuration", config_status, config_details)
    
    # Connection validation
    conn_status = "[green]PASS[/green]" if connection_ok else "[red]FAIL[/red]"
    conn_details = f"Successfully connected to {environment}" if connection_ok else f"Failed to connect to {environment}"
    table.add_row("Snowflake Connection", conn_status, conn_details)
    
    # Environment counts
    env_count = len(config.list_environments())
    suite_count = len(config.list_test_suites())
    test_count = len(config.list_tests())
    
    table.add_row("Environments", f"[blue]{env_count}[/blue]", f"Available: {', '.join(config.list_environments())}")
    table.add_row("Test Suites", f"[blue]{suite_count}[/blue]", "Configured test suite definitions")
    table.add_row("Individual Tests", f"[blue]{test_count}[/blue]", "Configured individual test definitions")
    
    console.print(table)
    
    overall_status = config_ok and connection_ok
    if overall_status:
        console.print("\n[green]Environment validation passed[/green]")
    else:
        console.print("\n[red]Environment validation failed[/red]")


def display_status(connection_status: Dict, test_count: int, suite_count: int, environment: str, output_format: str = "table"):
    """Display test runner status.
    
    Args:
        connection_status: Connection status information
        test_count: Number of available tests
        suite_count: Number of available test suites
        environment: Environment name
        output_format: Output format (table or json)
    """
    if output_format == "json":
        status_data = {
            "environment": environment,
            "connection": connection_status,
            "tests": {
                "individual_tests": test_count,
                "test_suites": suite_count,
            }
        }
        console.print(json.dumps(status_data, indent=2))
        return
    
    # Table format
    table = Table(title=f"Test Runner Status - {environment.upper()}")
    table.add_column("Component", style="cyan", no_wrap=True)
    table.add_column("Status", style="bold")
    table.add_column("Details", style="dim")
    
    # Connection status
    conn_ok = connection_status.get("status") == "OK"
    if conn_ok:
        table.add_row("Connection", "[green]CONNECTED[/green]", f"User: {connection_status.get('user', 'N/A')}")
        table.add_row("", "", f"Role: {connection_status.get('role', 'N/A')}")
        table.add_row("", "", f"Warehouse: {connection_status.get('warehouse', 'N/A')}")
        table.add_row("", "", f"Account: {connection_status.get('account', 'N/A')}")
    else:
        error_msg = connection_status.get("error", "Unknown error")
        table.add_row("Connection", "[red]DISCONNECTED[/red]", error_msg)
    
    # Test availability
    table.add_row("Tests", f"[blue]{test_count}[/blue]", "Individual tests available")
    table.add_row("Test Suites", f"[blue]{suite_count}[/blue]", "Test suites available")
    
    console.print(table)


def display_test_list(config: Config, output_format: str = "table"):
    """Display available tests and test suites.
    
    Args:
        config: Configuration manager
        output_format: Output format (table or json)
    """
    if output_format == "json":
        test_data = {
            "test_suites": {},
            "individual_tests": {}
        }
        
        # Test suites
        for suite_name in config.list_test_suites():
            try:
                suite_config = config.get_test_suite(suite_name)
                test_data["test_suites"][suite_name] = {
                    "description": suite_config.description,
                    "categories": suite_config.categories,
                    "tests": suite_config.tests,
                }
            except Exception as e:
                test_data["test_suites"][suite_name] = {"error": str(e)}
        
        # Individual tests
        for test_name in config.list_tests():
            try:
                test_config = config.get_test_config(test_name)
                test_data["individual_tests"][test_name] = {
                    "description": test_config.description,
                    "priority": test_config.priority,
                    "timeout": test_config.timeout,
                }
            except Exception as e:
                test_data["individual_tests"][test_name] = {"error": str(e)}
        
        console.print(json.dumps(test_data, indent=2))
        return
    
    # Table format for test suites
    suite_table = Table(title="Available Test Suites")
    suite_table.add_column("Name", style="cyan", no_wrap=True)
    suite_table.add_column("Description", style="white")
    suite_table.add_column("Type", style="magenta")
    
    for suite_name in config.list_test_suites():
        try:
            suite_config = config.get_test_suite(suite_name)
            if suite_config.categories:
                type_info = f"Categories: {', '.join(suite_config.categories)}"
            elif suite_config.tests:
                type_info = f"Tests: {len(suite_config.tests)} individual"
            else:
                type_info = "Empty suite"
            
            suite_table.add_row(suite_name, suite_config.description, type_info)
        except Exception as e:
            suite_table.add_row(suite_name, f"[red]Error: {e}[/red]", "")
    
    console.print(suite_table)
    console.print("")
    
    # Table format for individual tests
    test_table = Table(title="Available Individual Tests")
    test_table.add_column("Name", style="cyan", no_wrap=True)
    test_table.add_column("Description", style="white")
    test_table.add_column("Priority", style="bold")
    test_table.add_column("Timeout", style="dim")
    
    for test_name in sorted(config.list_tests()):
        try:
            test_config = config.get_test_config(test_name)
            
            priority_color = {
                "low": "bright_black",
                "medium": "yellow",
                "high": "orange1", 
                "critical": "red"
            }.get(test_config.priority, "white")
            
            priority_text = f"[{priority_color}]{test_config.priority.upper()}[/{priority_color}]"
            timeout_text = f"{test_config.timeout}s"
            
            test_table.add_row(test_name, test_config.description, priority_text, timeout_text)
        except Exception as e:
            test_table.add_row(test_name, f"[red]Error: {e}[/red]", "", "")
    
    console.print(test_table)


def display_test_results(results: List[TestResult], output_format: str = "table", export_file: Optional[Path] = None, show_passes: bool = False):
    """Display test results.
    
    Args:
        results: List of test results
        output_format: Output format (table, json, csv)
        export_file: Optional file to export results
    """
    if output_format == "json":
        results_data = {
            "summary": _get_results_summary(results),
            "results": [result.to_dict() for result in results]
        }
        output_text = json.dumps(results_data, indent=2)
        console.print(output_text)
        
        if export_file:
            export_file.write_text(output_text)
            console.print(f"[dim]Results exported to {export_file}[/dim]")
        
        return
    
    if output_format == "csv":
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=[
            'test_name', 'status', 'total_tested', 'failed_records', 
            'failure_rate', 'success_rate', 'execution_time', 'failure_details'
        ])
        writer.writeheader()
        
        for result in results:
            row = result.to_dict()
            # Remove complex fields for CSV
            row.pop('error_message', None)
            row.pop('started_at', None)
            row.pop('completed_at', None)
            row.pop('test_description', None)
            writer.writerow(row)
        
        csv_content = output.getvalue()
        console.print(csv_content)
        
        if export_file:
            export_file.write_text(csv_content)
            console.print(f"[dim]Results exported to {export_file}[/dim]")
        
        return
    
    # Table format
    _display_results_summary(results)
    _display_results_table(results)
    _display_error_details(results, show_passes=show_passes)
    
    if export_file:
        # For table format, export as JSON
        results_data = {
            "summary": _get_results_summary(results),
            "results": [result.to_dict() for result in results]
        }
        export_file.write_text(json.dumps(results_data, indent=2))
        console.print(f"[dim]Results exported to {export_file}[/dim]")


def _get_results_summary(results: List[TestResult]) -> Dict:
    """Get results summary statistics."""
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if r.failed)
    
    return {
        "total_tests": total,
        "passed": passed,
        "failed": failed,
        "success_rate": (passed / total * 100) if total > 0 else 0,
    }


def _display_results_summary(results: List[TestResult]):
    """Display results summary."""
    summary = _get_results_summary(results)
    
    # Calculate total sub-checks if available
    total_subchecks = sum(r.total_tested for r in results if r.total_tested > 0)
    failed_subchecks = sum(r.failed_records for r in results if r.failed_records > 0)
    
    summary_text = Text()
    summary_text.append("Test Results Summary\n", style="bold blue")
    summary_text.append(f"Total Tests: {summary['total_tests']}\n")
    
    # If we have sub-checks, show them too
    if total_subchecks > 0:
        summary_text.append(f"Total Checks: {total_subchecks} ", style="dim")
        summary_text.append(f"({failed_subchecks} failed)\n", style="dim red")
    
    summary_text.append(f"Passed: {summary['passed']}\n", style="green")
    summary_text.append(f"Failed: {summary['failed']}\n", style="red")
    summary_text.append(f"Success Rate: {summary['success_rate']:.1f}%\n", 
                       style="green" if summary['success_rate'] >= 90 else "yellow" if summary['success_rate'] >= 70 else "red")
    
    panel = Panel(summary_text, border_style="blue")
    console.print(panel)
    console.print("")


def _display_results_table(results: List[TestResult]):
    """Display results in table format."""
    table = Table(title="Detailed Test Results")
    table.add_column("Test Name", style="cyan", no_wrap=True)
    table.add_column("Status", style="bold")
    table.add_column("Total", justify="right", style="blue")
    table.add_column("Failed", justify="right", style="red")
    table.add_column("Success %", justify="right", style="green")
    table.add_column("Time (s)", justify="right", style="dim")
    
    for result in results:
        status_text = {
            "passed": "[green]PASS[/green]",
            "failed": "[red]FAIL[/red]",
            "error": "[red]ERROR[/red]",
            "skipped": "[yellow]SKIP[/yellow]",
        }.get(result.status.value, result.status.value.upper())
        
        table.add_row(
            result.test_name,
            status_text,
            str(result.total_tested),
            str(result.failed_records),
            f"{result.success_rate:.1f}%",
            f"{result.execution_time:.2f}"
        )
    
    console.print(table)


def _get_tested_label(test_name: str) -> str:
    """Get appropriate label for 'tested' count based on test type."""
    if 'completeness' in test_name:
        return "Checks Completed"
    elif 'mapping' in test_name:
        return "Mappings Tested"
    elif 'referential' in test_name:
        return "Relationships Tested"
    elif 'pattern' in test_name:
        return "Patterns Tested"
    else:
        return "Records Tested"


def _display_error_details(results: List[TestResult], show_passes: bool = False):
    """Display error details for failed tests and optionally passed tests."""
    # Get failed/error results
    error_results = [r for r in results if r.status.value in ['error', 'failed'] and (r.error_message or r.failure_details)]
    
    # Get passed results if requested
    passed_results = []
    if show_passes:
        passed_results = [r for r in results if r.status.value == 'passed']
    
    if not error_results and not passed_results:
        return
    
    # Display failures first
    if error_results:
        # Use appropriate title based on failure type
        has_errors = any(r.status.value == 'error' for r in error_results)
        has_failures = any(r.status.value == 'failed' for r in error_results)
        
        if has_errors and has_failures:
            console.print("\n[bold red]Error and Failure Details:[/bold red]")
        elif has_errors:
            console.print("\n[bold red]Error Details:[/bold red]")
        else:
            console.print("\n[bold red]Failure Details:[/bold red]")
            
        for result in error_results:
            console.print(f"\n[cyan]• {result.test_name}[/cyan]:")
            
            if result.error_message:
                console.print(f"  [red]Error:[/red] {result.error_message}")
                
            if result.failure_details:
                console.print(f"  [yellow]Details:[/yellow]")
                # Split details by lines and indent
                for line in result.failure_details.split('\n'):
                    if line.strip():
                        console.print(f"    {line}")
    
    # Display passed test details if requested
    if passed_results:
        console.print("\n[bold green]Passed Test Details:[/bold green]")
        
        for result in passed_results:
            console.print(f"\n[cyan]• {result.test_name}[/cyan]:")
            console.print(f"  [green]Status:[/green] PASSED")
            
            if result.total_tested > 0:
                tested_label = _get_tested_label(result.test_name)
                console.print(f"  [blue]{tested_label}:[/blue] {result.total_tested:,}")
                
            if hasattr(result, 'success_rate') and result.success_rate is not None:
                console.print(f"  [green]Success Rate:[/green] {result.success_rate:.1f}%")
                
            if result.execution_time is not None:
                console.print(f"  [dim]Execution Time:[/dim] {result.execution_time:.2f}s")
                
            # Show any success details from metadata if available
            if result.metadata and isinstance(result.metadata, dict):
                if 'pattern_tests_executed' in result.metadata:
                    pattern_executed = result.metadata['pattern_tests_executed']
                    console.print(f"  [blue]Pattern Tests:[/blue] {pattern_executed} sub-tests executed")
                    
                if 'detailed_results' in result.metadata:
                    detailed = result.metadata['detailed_results']
                    if detailed:
                        passed_subtests = [r for r in detailed if r.get('passed', False)]
                        if passed_subtests:
                            console.print(f"  [green]Sub-test Results:[/green] All {len(passed_subtests)} sub-tests passed")
            
            # Add test description for context
            if result.test_description and result.test_description != result.test_name:
                console.print(f"  [dim]Description:[/dim] {result.test_description}")
    
    # Show individual passed checks within failed tests if requested
    if show_passes:
        console.print("\n[bold green]Individual Passed Checks:[/bold green]")
        
        for result in results:
            # Show passed sub-checks from metadata
            if result.metadata and isinstance(result.metadata, dict):
                
                # Handle person pattern tests
                if 'detailed_results' in result.metadata:
                    detailed = result.metadata['detailed_results']
                    if detailed:
                        passed_subtests = [r for r in detailed if r.get('passed', False)]
                        if passed_subtests:
                            console.print(f"\n[cyan]+ {result.test_name} - Passed Sub-tests:[/cyan]")
                            for subtest in passed_subtests:
                                test_name = subtest.get('test_name', 'Unknown')
                                description = subtest.get('test_description', '')
                                total_tested = subtest.get('total_tested', 0)
                                console.print(f"  [green]+[/green] {test_name}: {description}")
                                if total_tested > 0:
                                    console.print(f"    [dim]({total_tested:,} records tested)[/dim]")
                
                # Handle empty tables test
                elif 'non_empty_tables' in result.metadata:
                    non_empty_tables = result.metadata['non_empty_tables']
                    if non_empty_tables:
                        console.print(f"\n[cyan]+ {result.test_name} - Non-Empty Tables:[/cyan]")
                        for table_info in non_empty_tables:
                            schema_name = table_info.get('schema', 'Unknown')
                            table_name = table_info.get('table', 'Unknown')
                            row_count = table_info.get('row_count', 0)
                            console.print(f"  [green]+[/green] {schema_name}.{table_name}: {row_count:,} rows")
                
                # Handle other tests with individual check results
                elif 'passed_checks' in result.metadata:
                    passed_checks = result.metadata['passed_checks']
                    if passed_checks:
                        console.print(f"\n[cyan]+ {result.test_name} - Passed Checks:[/cyan]")
                        for check in passed_checks:
                            check_name = check.get('name', 'Unknown')
                            check_desc = check.get('description', '')
                            console.print(f"  [green]+[/green] {check_name}: {check_desc}")