"""Main CLI entry point for OLIDS testing framework."""

import os
import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

# Add src to path for development
src_path = Path(__file__).parent.parent.parent
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from olids_testing.core.config import Config
from olids_testing.core.runner import TestRunner
from .commands.config_cmd import config_group
from .commands.run_cmd import run_command
from .commands.deploy_cmd import deploy_group


console = Console()


@click.group(name="olids-test")
@click.version_option(version="0.1.0", message="OLIDS UAT Testing Framework v%(version)s")
@click.option(
    "--config-dir", 
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help="Configuration directory path",
    envvar="OLIDS_CONFIG_DIR"
)
@click.option(
    "--environment", "-e",
    default="uat",
    help="Environment to use (dev, uat, prod)",
    envvar="OLIDS_ENVIRONMENT"
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Enable verbose output"
)
@click.pass_context
def cli(ctx: click.Context, config_dir: Optional[Path], environment: str, verbose: bool):
    """
    OLIDS UAT Testing Framework
    
    Comprehensive data validation for healthcare data warehouse built with
    Snowflake Snow CLI integration and rich terminal interface.
    """
    # Ensure context object exists
    ctx.ensure_object(dict)
    
    try:
        # Initialize configuration
        config = Config(config_dir)
        ctx.obj["config"] = config
        ctx.obj["environment"] = environment
        ctx.obj["verbose"] = verbose
        
        # Validate configuration
        if not config.validate():
            console.print("[red]Configuration validation failed![/red]")
            sys.exit(1)
            
        # Validate environment exists
        try:
            env_config = config.get_environment(environment)
            ctx.obj["env_config"] = env_config
        except KeyError as e:
            console.print(f"[red]Error: {e}[/red]")
            available = config.list_environments()
            console.print(f"Available environments: {', '.join(available)}")
            sys.exit(1)
            
    except Exception as e:
        if verbose:
            console.print_exception()
        else:
            console.print(f"[red]Configuration error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.pass_context
def info(ctx: click.Context):
    """Show framework and environment information."""
    config = ctx.obj["config"]
    environment = ctx.obj["environment"]
    env_config = ctx.obj["env_config"]
    
    # Framework info
    info_text = Text()
    info_text.append("OLIDS UAT Testing Framework\n", style="bold blue")
    info_text.append(f"Version: 0.1.0\n")
    info_text.append(f"Environment: {environment}\n", style="bold green")
    info_text.append(f"Description: {env_config.description}\n")
    info_text.append("\n")
    
    # Configuration details
    info_text.append("Configuration:\n", style="bold yellow")
    info_text.append(f"  Source Database: {env_config.databases.source}\n")
    info_text.append(f"  Results Database: {env_config.databases.results}\n")
    info_text.append(f"  Snowflake Account: {env_config.connection.account}\n")
    info_text.append(f"  Warehouse: {env_config.connection.warehouse}\n")
    info_text.append(f"  Role: {env_config.connection.role}\n")
    info_text.append("\n")
    
    # Available resources
    info_text.append("Available Resources:\n", style="bold cyan")
    info_text.append(f"  Environments: {', '.join(config.list_environments())}\n")
    info_text.append(f"  Test Suites: {', '.join(config.list_test_suites())}\n")
    info_text.append(f"  Individual Tests: {len(config.list_tests())}\n")
    
    panel = Panel(info_text, title="Framework Information", border_style="blue")
    console.print(panel)


@cli.command(name="list")
@click.option(
    "--output", "-o",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format"
)
@click.pass_context
def list_tests(ctx: click.Context, output: str):
    """List available tests and test suites."""
    from .ui.display import display_test_list
    
    config = ctx.obj["config"]
    display_test_list(config, output)


@cli.command()
@click.pass_context
def validate(ctx: click.Context):
    """Validate configuration and test environment."""
    from .ui.display import display_validation_results
    
    config = ctx.obj["config"]
    environment = ctx.obj["environment"]
    
    with console.status("[bold blue]Validating environment..."):
        # Test runner will validate Snowflake connection
        runner = TestRunner(config, environment)
        connection_ok = runner.validate_environment()
        
        config_ok = config.validate()
    
    display_validation_results(config_ok, connection_ok, config, environment)


@cli.command()
@click.argument("environment", type=click.Choice(["dev", "uat", "prod"]))
@click.pass_context
def switch(ctx: click.Context, environment: str):
    """Switch to a different environment (dev, uat, prod)."""
    config = ctx.obj["config"]
    
    try:
        # Validate the environment exists
        env_config = config.get_environment(environment)
        
        # Update context for this session
        ctx.obj["environment"] = environment
        ctx.obj["env_config"] = env_config
        
        console.print(f"[green]Switched to environment: {environment}[/green]")
        console.print(f"[dim]Database: {env_config.databases.source}[/dim]")
        console.print(f"[dim]Description: {env_config.description}[/dim]")
        
    except KeyError as e:
        console.print(f"[red]Error: {e}[/red]")
        available = config.list_environments()
        console.print(f"Available environments: {', '.join(available)}")


@cli.command()
@click.pass_context
def quickstart(ctx: click.Context):
    """Show getting started guide and common commands."""
    from rich import box
    
    # Create quickstart guide
    guide_text = Text()
    guide_text.append("OLIDS Testing Framework - Quick Start Guide\n\n", style="bold cyan")
    
    guide_text.append("COMMON WORKFLOWS\n\n", style="bold yellow")
    
    guide_text.append("1. Check your setup:\n", style="bold")
    guide_text.append("   olids-test validate\n\n", style="bright_black")
    
    guide_text.append("2. View available tests:\n", style="bold")
    guide_text.append("   olids-test list\n\n", style="bright_black")
    
    guide_text.append("3. Switch environment:\n", style="bold")
    guide_text.append("   olids-test switch dev\n\n", style="bright_black")
    
    guide_text.append("4. Run all tests:\n", style="bold")
    guide_text.append("   olids-test run all\n\n", style="bright_black")
    
    guide_text.append("5. Run specific tests:\n", style="bold")
    guide_text.append("   olids-test run null_columns empty_tables\n\n", style="bright_black")
    
    guide_text.append("6. Run with detailed output:\n", style="bold")
    guide_text.append("   olids-test run all --show-passes\n\n", style="bright_black")
    
    guide_text.append("7. Export results:\n", style="bold")
    guide_text.append("   olids-test run all --output json --export results.json\n\n", style="bright_black")
    
    guide_text.append("AVAILABLE TEST SUITES\n\n", style="bold yellow")
    guide_text.append("• referential_integrity - All 85 foreign key relationship validations\n", style="white")
    guide_text.append("• concept_mapping       - Terminology mapping validation\n", style="white")
    guide_text.append("• person_patterns       - Business rule validation\n", style="white")
    guide_text.append("• null_columns          - NULL column detection\n", style="white")
    guide_text.append("• empty_tables          - Empty table detection\n", style="white")
    guide_text.append("• column_completeness   - Column completeness validation\n\n", style="white")
    
    guide_text.append("EXPLORE MORE\n\n", style="bold yellow")
    guide_text.append("• View configuration:    olids-test config show\n", style="white")
    guide_text.append("• Show framework info:   olids-test info\n", style="white")
    guide_text.append("• Get help on any command: olids-test COMMAND --help\n", style="white")
    
    panel = Panel(guide_text, title="Getting Started", border_style="green", box=box.ROUNDED, width=80, expand=False)
    console.print(panel)


# Add command groups
cli.add_command(config_group)
cli.add_command(run_command)
cli.add_command(deploy_group)


if __name__ == "__main__":
    cli()