"""Test execution commands for OLIDS testing framework."""

import json
import sys
from pathlib import Path
from typing import List, Optional

import click
from rich.console import Console

console = Console()


@click.command(name="run")
@click.argument("test_names", nargs=-1)
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
    "--show-passes",
    is_flag=True,
    help="Show details for passed tests in addition to failures"
)
@click.option(
    "--hide-details",
    is_flag=True,
    help="Hide the test details section (only show summary and results table)"
)
@click.pass_context
def run_command(ctx: click.Context, test_names: tuple, parallel: bool, output: str, export: Optional[Path], show_passes: bool, hide_details: bool):
    """Run tests. 
    
    Usage:
      olids-test run all                           # Run all tests
      olids-test run null_columns                  # Run single test  
      olids-test run null_columns empty_tables     # Run multiple tests
    """
    from olids_testing.core.runner import TestRunner
    from olids_testing.cli.ui.display import display_test_results
    
    config = ctx.obj["config"]
    environment = ctx.obj["environment"]
    verbose = ctx.obj["verbose"]
    
    # If no test names provided, show usage
    if not test_names:
        console.print("[yellow]Usage: olids-test run [TEST_NAMES...][/yellow]")
        console.print("\nAvailable tests:")
        console.print("  [cyan]all[/cyan]                    Run all tests")
        console.print("  [cyan]null_columns[/cyan]          Run null columns test")
        console.print("  [cyan]empty_tables[/cyan]          Run empty tables test") 
        console.print("  [cyan]column_completeness[/cyan]   Run column completeness test")
        console.print("  [cyan]referential_integrity[/cyan] Run referential integrity test")
        console.print("  [cyan]person_patterns[/cyan]       Run person patterns test")
        console.print("  [cyan]concept_mapping[/cyan]       Run concept mapping test")
        console.print("\n[dim]Examples:[/dim]")
        console.print("  [dim]olids-test run all[/dim]")
        console.print("  [dim]olids-test run null_columns[/dim]")
        console.print("  [dim]olids-test run null_columns empty_tables --parallel[/dim]")
        return
    
    try:
        # Initialize test runner
        runner = TestRunner(config, environment)
        
        # Handle special case: "all"
        if len(test_names) == 1 and test_names[0] == "all":
            if parallel:
                console.print("[yellow]Parallel execution enabled[/yellow]")
            results = runner.run_all_tests(parallel=parallel, show_progress=True)
        elif len(test_names) == 1:
            # Single test
            result = runner.run_test(test_names[0], show_progress=True)
            results = [result]
        else:
            # Multiple tests
            if parallel:
                console.print("[yellow]Parallel execution enabled[/yellow]")
            results = runner.run_tests(list(test_names), parallel=parallel, show_progress=True, suite_name="selected tests")
        
        display_test_results(results, output, export, show_passes=show_passes, hide_details=hide_details)
        
    except Exception as e:
        if verbose:
            console.print_exception()
        else:
            console.print(f"[red]Error running tests: {e}[/red]")
        sys.exit(1)


