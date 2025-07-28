"""
Command-line interface for Better-DBT-Metrics
"""

import click
import sys
from pathlib import Path
from typing import Optional

from .core.compiler import BetterDBTCompiler, CompilerConfig
from .validation.validator import MetricsValidator


@click.group()
@click.version_option()
def cli():
    """Better-DBT-Metrics: A metrics-first approach to dbt semantic models"""
    pass


@cli.command()
@click.option('-i', '--input-dir', default='metrics/', help='Input directory containing metrics files')
@click.option('-o', '--output-dir', default='models/semantic/', help='Output directory for compiled files')
@click.option('-t', '--template-dirs', multiple=True, default=['templates/'], help='Template directories')
@click.option('--split-files/--single-file', default=True, help='Split output into multiple files')
@click.option('-v', '--verbose', is_flag=True, help='Verbose output')
def compile(input_dir: str, output_dir: str, template_dirs: tuple, split_files: bool, verbose: bool):
    """Compile metrics files to dbt semantic models"""
    try:
        config = CompilerConfig(
            input_dir=input_dir,
            output_dir=output_dir,
            template_dirs=list(template_dirs),
            split_files=split_files
        )
        
        compiler = BetterDBTCompiler(config)
        
        if verbose:
            click.echo(f"Compiling metrics from {input_dir} to {output_dir}...")
            
        result = compiler.compile_directory()
        
        # Display results
        click.echo(f"✓ Compiled {result['metrics_compiled']} metrics")
        click.echo(f"✓ Generated {result['models_generated']} semantic models")
        click.echo(f"✓ Processed {result['files_processed']} files")
        
        if result['errors']:
            click.echo(f"\n⚠️  {len(result['errors'])} errors encountered:")
            for error in result['errors']:
                click.echo(f"  - {error['file']}: {error['error']}")
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Compilation failed: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option('-i', '--input-dir', default='metrics/', help='Directory containing metrics files')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed validation output')
@click.option('--fail-on-warning', is_flag=True, help='Exit with error code on warnings')
def validate(input_dir: str, verbose: bool, fail_on_warning: bool):
    """Validate metrics files"""
    try:
        validator = MetricsValidator()
        
        click.echo(f"Validating metrics in {input_dir}...")
        
        result = validator.validate_directory(Path(input_dir))
        
        # Display results
        if verbose or not result.is_valid or result.warnings:
            click.echo(str(result))
        else:
            click.echo("✓ All validations passed")
            
        # Exit codes
        if not result.is_valid:
            sys.exit(1)
        elif result.warnings and fail_on_warning:
            sys.exit(2)
            
    except Exception as e:
        click.echo(f"❌ Validation failed: {str(e)}", err=True)
        sys.exit(1)


@cli.command('list-templates')
@click.option('-t', '--template-dirs', multiple=True, default=['templates/'], help='Template directories')
def list_templates(template_dirs: tuple):
    """List available metric templates"""
    try:
        from .features.templates import TemplateLibrary
        
        library = TemplateLibrary(list(template_dirs))
        templates = library.list_templates()
        
        if not templates:
            click.echo("No templates found")
            return
            
        click.echo("Available metric templates:")
        for name, template in templates.items():
            click.echo(f"\n  {name}:")
            
            # Show parameters
            if 'parameters' in template:
                click.echo("    Parameters:")
                for param in template['parameters']:
                    required = " (required)" if param.get('required') else ""
                    default = f" = {param['default']}" if 'default' in param else ""
                    click.echo(f"      - {param['name']}{required}{default}")
                    
            # Show description if available
            if 'description' in template:
                click.echo(f"    Description: {template['description']}")
                
    except Exception as e:
        click.echo(f"❌ Failed to list templates: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option('-i', '--input-dir', default='metrics/', help='Input directory')
@click.option('-o', '--output-dir', default='docs/', help='Output directory for documentation')
@click.option('--format', type=click.Choice(['markdown', 'html']), default='markdown', help='Documentation format')
def docs(input_dir: str, output_dir: str, format: str):
    """Generate documentation for metrics"""
    click.echo("Documentation generation not yet implemented")
    # TODO: Implement documentation generation


@cli.command()
def init():
    """Initialize a new Better-DBT-Metrics project"""
    click.echo("Creating project structure...")
    
    # Create directories
    dirs = [
        'metrics',
        'metrics/finance',
        'metrics/product',
        'templates',
        'templates/dimensions',
        'templates/metrics',
        'models/semantic'
    ]
    
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        
    # Create example files
    example_metric = '''# Example metric definition
version: 2

metrics:
  - name: total_revenue
    description: "Total revenue from completed orders"
    type: simple
    source: fct_orders
    measure:
      type: sum
      column: order_total
    dimensions:
      - name: order_date
        type: time
        grain: day
      - name: customer_segment
        type: categorical
'''
    
    example_dimension = '''# Reusable time dimensions
version: 2

dimension_groups:
  daily:
    dimensions:
      - name: date_day
        type: time
        grain: day
      - name: date_week
        type: time
        grain: week
      - name: date_month
        type: time
        grain: month
'''
    
    # Write example files
    (Path('metrics') / 'example_revenue.yml').write_text(example_metric)
    (Path('templates/dimensions') / 'temporal.yml').write_text(example_dimension)
    
    # Create .gitignore
    gitignore = '''# Compiled dbt models
models/semantic/

# Python
__pycache__/
*.pyc
.venv/
venv/

# IDE
.vscode/
.idea/
'''
    Path('.gitignore').write_text(gitignore)
    
    click.echo("✓ Project structure created")
    click.echo("\nNext steps:")
    click.echo("1. Define your metrics in the metrics/ directory")
    click.echo("2. Run 'better-dbt-metrics compile' to generate dbt models")
    click.echo("3. Run 'better-dbt-metrics validate' to check for errors")


def main():
    """Main entry point"""
    cli()


if __name__ == '__main__':
    main()