"""Test execution commands for OLIDS testing framework."""

import json
import sys
from pathlib import Path
from typing import List, Optional

import click
from rich.console import Console

console = Console()


@click.group(name="run")
def run_group():
    """Run tests and test suites."""
    pass


@run_group.command()
@click.argument("test_name")
@click.option(
    "--output", "-o",
    type=click.Choice(["table", "json", "csv"]),
    default="table",
    help="Output format"
)
@click.option(
    "--export",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Export results to file"
)
@click.option(
    "--timeout",
    type=int,
    help="Test timeout in seconds"
)
@click.option(
    "--show-passes",
    is_flag=True,
    help="Show details for passed tests in addition to failures"
)
@click.pass_context
def test(ctx: click.Context, test_name: str, output: str, export: Optional[Path], timeout: Optional[int], show_passes: bool):
    """Run a single test."""
    from olids_testing.core.runner import TestRunner
    from olids_testing.cli.ui.display import display_test_results
    
    config = ctx.obj["config"]
    environment = ctx.obj["environment"]
    verbose = ctx.obj["verbose"]
    
    try:
        # Initialize test runner
        runner = TestRunner(config, environment)
        
        # Run the test (runner handles its own progress indication)
        result = runner.run_test(test_name, show_progress=True)
        
        display_test_results([result], output, export, show_passes=show_passes)
        
    except Exception as e:
        if verbose:
            console.print_exception()
        else:
            console.print(f"[red]Error running test: {e}[/red]")
        sys.exit(1)


@run_group.command()
@click.argument("suite_name")
@click.option(
    "--parallel", "-p",
    is_flag=True,
    help="Run tests in parallel"
)
@click.option(
    "--output", "-o",
    type=click.Choice(["table", "json", "csv"]),
    default="table",
    help="Output format"
)
@click.option(
    "--export",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Export results to file"
)
@click.option(
    "--fail-fast",
    is_flag=True,
    help="Stop on first test failure"
)
@click.option(
    "--show-passes",
    is_flag=True,
    help="Show details for passed tests in addition to failures"
)
@click.pass_context
def suite(ctx: click.Context, suite_name: str, parallel: bool, output: str, export: Optional[Path], fail_fast: bool, show_passes: bool):
    """Run a test suite."""
    from olids_testing.core.runner import TestRunner
    from olids_testing.cli.ui.display import display_test_results
    
    config = ctx.obj["config"]
    environment = ctx.obj["environment"]
    verbose = ctx.obj["verbose"]
    
    try:
        # Validate suite exists
        try:
            suite_config = config.get_test_suite(suite_name)
        except KeyError as e:
            console.print(f"[red]Error: {e}[/red]")
            available = config.list_test_suites()
            console.print(f"Available test suites: {', '.join(available)}")
            sys.exit(1)
        
        console.print(f"[blue]Running test suite: {suite_name}[/blue]")
        console.print(f"Description: {suite_config.description}")
        
        if parallel:
            console.print("[yellow]Parallel execution enabled[/yellow]")
        
        # Initialize test runner
        runner = TestRunner(config, environment)
        
        # Run the test suite
        results = runner.run_test_suite(suite_name, parallel=parallel, show_progress=True)
        
        display_test_results(results, output, export, show_passes=show_passes)
        
        # Handle fail-fast
        # if fail_fast:
        #     failed_results = [r for r in results if r.failed]
        #     if failed_results:
        #         console.print(f"[red]Stopping due to test failure: {failed_results[0].test_name}[/red]")
        #         sys.exit(1)
        
    except Exception as e:
        if verbose:
            console.print_exception()
        else:
            console.print(f"[red]Error running test suite: {e}[/red]")
        sys.exit(1)


@run_group.command()
@click.option(
    "--parallel", "-p",
    is_flag=True,
    help="Run tests in parallel"
)
@click.option(
    "--output", "-o",
    type=click.Choice(["table", "json", "csv"]),
    default="table",
    help="Output format"
)
@click.option(
    "--export",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Export results to file"
)
@click.option(
    "--category",
    type=click.Choice(["referential_integrity", "concept_mapping", "business_rules", "data_quality"]),
    help="Run only tests in specific category"
)
@click.option(
    "--fail-fast",
    is_flag=True,
    help="Stop on first test failure"
)
@click.option(
    "--show-passes",
    is_flag=True,
    help="Show details for passed tests in addition to failures"
)
@click.pass_context
def all(ctx: click.Context, parallel: bool, output: str, export: Optional[Path], category: Optional[str], fail_fast: bool, show_passes: bool):
    """Run all tests."""
    from olids_testing.core.runner import TestRunner
    from olids_testing.cli.ui.display import display_test_results
    
    config = ctx.obj["config"]
    environment = ctx.obj["environment"]
    verbose = ctx.obj["verbose"]
    
    try:
        if category:
            console.print(f"Category filter: {category}")
        
        if parallel:
            console.print("[yellow]Parallel execution enabled[/yellow]")
        
        # Initialize test runner
        runner = TestRunner(config, environment)
        
        # Get test names (optionally filtered by category)
        if category:
            from olids_testing.tests import TEST_CATEGORIES
            test_names = TEST_CATEGORIES.get(category, [])
            if not test_names:
                console.print(f"[red]No tests found for category: {category}[/red]")
                console.print(f"Available categories: {', '.join(TEST_CATEGORIES.keys())}")
                sys.exit(1)
        else:
            # For "run all", only include main tests (not individual relationship tests)
            test_names = runner.list_tests(main_only=True)
        
        if not test_names:
            console.print("[yellow]No tests available to run[/yellow]")
            return
        
        # Run all tests
        if category:
            suite_name = f"all {category} tests"
        else:
            suite_name = "all tests"
            
        results = runner.run_tests(test_names, parallel=parallel, show_progress=True, suite_name=suite_name)
        
        display_test_results(results, output, export, show_passes=show_passes)
        
        # Handle fail-fast
        # if fail_fast:
        #     failed_results = [r for r in results if r.failed]
        #     if failed_results:
        #         console.print(f"[red]Stopping due to test failure: {failed_results[0].test_name}[/red]")
        #         sys.exit(1)
        
    except Exception as e:
        if verbose:
            console.print_exception()
        else:
            console.print(f"[red]Error running all tests: {e}[/red]")
        sys.exit(1)


@run_group.command()
@click.option(
    "--output", "-o",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format"
)
@click.pass_context
def status(ctx: click.Context, output: str):
    """Show test runner status and environment health."""
    from olids_testing.core.runner import TestRunner
    from olids_testing.core.connection import SnowflakeConnection
    from olids_testing.cli.ui.display import display_status
    
    config = ctx.obj["config"]
    environment = ctx.obj["environment"]
    env_config = ctx.obj["env_config"]
    verbose = ctx.obj["verbose"]
    
    try:
        with console.status("[bold blue]Checking environment status..."):
            # Test Snowflake connection
            with SnowflakeConnection(env_config) as conn:
                connection_status = conn.test_connection()
            
            # Test runner status
            runner = TestRunner(config, environment)
            test_count = len(runner.list_tests())
            suite_count = len(runner.list_test_suites())
        
        display_status(connection_status, test_count, suite_count, environment, output)
        
    except Exception as e:
        if verbose:
            console.print_exception()
        else:
            console.print(f"[red]Error checking status: {e}[/red]")
        sys.exit(1)