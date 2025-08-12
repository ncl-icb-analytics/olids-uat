"""Configuration management commands for OLIDS testing framework."""

import sys
from pathlib import Path

import click
from rich.console import Console

console = Console()


@click.group(name="config")
def config_group():
    """Manage configuration settings."""
    pass


@config_group.command()
@click.option(
    "--output", "-o",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format"
)
@click.pass_context
def show(ctx: click.Context, output: str):
    """Show current configuration."""
    from olids_testing.cli.ui.display import display_config
    
    config = ctx.obj["config"]
    environment = ctx.obj["environment"]
    env_config = ctx.obj["env_config"]
    
    display_config(config, env_config, environment, output)


@config_group.command()
@click.pass_context
def validate(ctx: click.Context):
    """Validate configuration files."""
    from olids_testing.cli.ui.display import display_validation_results
    from olids_testing.core.runner import TestRunner
    
    config = ctx.obj["config"]
    environment = ctx.obj["environment"]
    verbose = ctx.obj["verbose"]
    
    try:
        with console.status("[bold blue]Validating configuration..."):
            # Validate configuration files
            config_ok = config.validate()
            
            # Test Snowflake connection
            runner = TestRunner(config, environment)
            connection_ok = runner.validate_environment()
        
        display_validation_results(config_ok, connection_ok, config, environment)
        
        if not (config_ok and connection_ok):
            sys.exit(1)
            
    except Exception as e:
        if verbose:
            console.print_exception()
        else:
            console.print(f"[red]Validation error: {e}[/red]")
        sys.exit(1)


@config_group.command()
@click.pass_context
def environments(ctx: click.Context):
    """List available environments."""
    config = ctx.obj["config"]
    current_env = ctx.obj["environment"]
    
    environments = config.list_environments()
    
    console.print("[bold blue]Available Environments:[/bold blue]")
    for env in environments:
        if env == current_env:
            console.print(f"  [green]• {env} (current)[/green]")
        else:
            console.print(f"  • {env}")
    
    console.print(f"\nCurrent environment: [green]{current_env}[/green]")
    console.print("Use --environment/-e to switch environments")


@config_group.command()
@click.pass_context  
def test_suites(ctx: click.Context):
    """List available test suites."""
    config = ctx.obj["config"]
    
    suites = config.list_test_suites()
    
    console.print("[bold blue]Available Test Suites:[/bold blue]")
    for suite_name in suites:
        try:
            suite_config = config.get_test_suite(suite_name)
            console.print(f"  [cyan]• {suite_name}[/cyan] - {suite_config.description}")
            
            if suite_config.categories:
                console.print(f"    Categories: {', '.join(suite_config.categories)}")
            elif suite_config.tests:
                console.print(f"    Tests: {len(suite_config.tests)} individual tests")
        except Exception as e:
            console.print(f"  [red]• {suite_name} (error: {e})[/red]")


@config_group.command()
@click.pass_context
def tests(ctx: click.Context):
    """List available individual tests."""
    config = ctx.obj["config"]
    
    test_names = config.list_tests()
    
    console.print(f"[bold blue]Available Individual Tests ({len(test_names)}):[/bold blue]")
    
    for test_name in sorted(test_names):
        try:
            test_config = config.get_test_config(test_name)
            priority_color = {
                "low": "bright_black",
                "medium": "yellow", 
                "high": "orange1",
                "critical": "red"
            }.get(test_config.priority, "white")
            
            console.print(f"  [cyan]• {test_name}[/cyan] [{priority_color}]({test_config.priority})[/{priority_color}]")
            console.print(f"    {test_config.description}")
            console.print(f"    Timeout: {test_config.timeout}s")
        except Exception as e:
            console.print(f"  [red]• {test_name} (error: {e})[/red]")


@config_group.command()
@click.argument("config_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--environment", "-e",
    help="Environment to test configuration against"
)
@click.pass_context
def test_config(ctx: click.Context, config_path: Path, environment: str):
    """Test a configuration file."""
    from olids_testing.core.config import Config
    
    verbose = ctx.obj["verbose"]
    
    try:
        console.print(f"[blue]Testing configuration: {config_path}[/blue]")
        
        # Load configuration from specified path
        config_dir = config_path.parent if config_path.is_file() else config_path
        test_config = Config(config_dir)
        
        # Validate configuration
        config_ok = test_config.validate()
        
        if environment:
            try:
                env_config = test_config.get_environment(environment)
                console.print(f"[green]✓ Environment '{environment}' loaded successfully[/green]")
            except KeyError as e:
                console.print(f"[red]✗ Environment error: {e}[/red]")
                config_ok = False
        
        if config_ok:
            console.print("[green]✓ Configuration is valid[/green]")
        else:
            console.print("[red]✗ Configuration validation failed[/red]")
            sys.exit(1)
            
    except Exception as e:
        if verbose:
            console.print_exception()
        else:
            console.print(f"[red]Configuration test failed: {e}[/red]")
        sys.exit(1)