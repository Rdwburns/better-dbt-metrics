"""
Command-line interface for Better-DBT-Metrics
"""

import click
import yaml
import json
import sys
from pathlib import Path
from typing import List, Optional

from core.compiler import BetterDBTCompiler, CompilerConfig


@click.group()
@click.version_option(version='2.0.0')
def cli():
    """Better-DBT-Metrics: GitHub Actions-first metrics compilation"""
    pass


@cli.command()
@click.option('--input-dir', '-i', default='metrics/', help='Input directory containing metrics YAML')
@click.option('--output-dir', '-o', default='models/semantic/', help='Output directory for dbt models')
@click.option('--template-dir', '-t', multiple=True, help='Template directories (can specify multiple)')
@click.option('--dimension-group-dir', '-d', multiple=True, help='Dimension group directories')
@click.option('--environment', '-e', default='dev', help='Environment to compile for')
@click.option('--config', '-c', help='Configuration file')
@click.option('--split-files/--single-file', default=True, help='Split output into multiple files')
@click.option('--auto-variants/--no-auto-variants', default=True, help='Generate auto variants')
@click.option('--validate/--no-validate', default=True, help='Validate metrics')
@click.option('--json-output', is_flag=True, help='Output JSON report')
def compile(input_dir, output_dir, template_dir, dimension_group_dir, 
           environment, config, split_files, auto_variants, validate, json_output):
    """Compile better-dbt-metrics YAML to dbt semantic models"""
    
    # Load config file if provided
    config_data = {}
    if config:
        with open(config, 'r') as f:
            config_data = yaml.safe_load(f)
    
    # Override with CLI options
    template_dirs = list(template_dir) or config_data.get('template_dirs', ['templates/'])
    dimension_dirs = list(dimension_group_dir) or config_data.get('dimension_group_dirs', ['templates/dimensions/'])
    
    # Create compiler config
    compiler_config = CompilerConfig(
        input_dir=input_dir,
        output_dir=output_dir,
        template_dirs=template_dirs,
        dimension_group_dirs=dimension_dirs,
        environment=environment,
        split_files=split_files,
        auto_variants=auto_variants,
        validate=validate
    )
    
    # Run compilation
    compiler = BetterDBTCompiler(compiler_config)
    
    try:
        results = compiler.compile_directory()
        
        if json_output:
            # Output JSON report
            print(json.dumps(results, indent=2))
        else:
            # Human-readable output
            click.echo(f"✅ Compilation completed successfully!")
            click.echo(f"   Files processed: {results['files_processed']}")
            click.echo(f"   Metrics compiled: {results['metrics_compiled']}")
            click.echo(f"   Models generated: {results['models_generated']}")
            
            if results['errors']:
                click.echo("\n⚠️  Errors encountered:")
                for error in results['errors']:
                    click.echo(f"   - {error['file']}: {error['error']}")
                sys.exit(1)
                
    except Exception as e:
        if json_output:
            print(json.dumps({
                'error': str(e),
                'success': False
            }))
        else:
            click.echo(f"❌ Compilation failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--input-dir', '-i', default='metrics/', help='Metrics directory')
@click.option('--fix', is_flag=True, help='Fix issues automatically')
def validate(input_dir, fix):
    """Validate better-dbt-metrics YAML files"""
    from validation.validator import MetricsValidator
    
    validator = MetricsValidator()
    result = validator.validate_directory(input_dir, fix=fix)
    
    # Display validation results
    if result.is_valid and not result.warnings:
        click.echo("✅ All metrics are valid!")
    else:
        if result.errors:
            click.echo(f"\n❌ Found {len(result.errors)} error(s):")
            for error in result.errors:
                click.echo(f"  {error}")
                
        if result.warnings:
            click.echo(f"\n⚠️  Found {len(result.warnings)} warning(s):")
            for warning in result.warnings:
                click.echo(f"  {warning}")
                
        if result.errors:
            sys.exit(1)


# Note: generate-tests and generate-docs commands removed until implementation is ready
# These features are planned for a future release


@cli.command()
def list_templates():
    """List available metric templates"""
    from features.templates import TemplateLibrary
    
    library = TemplateLibrary(['templates/'])
    templates = library.list_templates()
    
    click.echo("📋 Available metric templates:")
    for template in templates:
        info = library.get_template_info(template)
        click.echo(f"\n   {template}")
        click.echo(f"   {info['description']}")
        if info['parameters']:
            click.echo("   Parameters:")
            for param in info['parameters']:
                required = " (required)" if param['required'] else ""
                click.echo(f"     - {param['name']}: {param['type']}{required}")


@cli.command()
@click.option('--dir', '-d', default='templates/dimensions/', help='Directory containing dimension groups')
def list_dimensions(dir):
    """List available dimension groups"""
    from features.dimension_groups import DimensionGroupManager
    from pathlib import Path
    import yaml
    
    click.echo("📊 Available dimension groups:")
    
    dim_path = Path(dir)
    if not dim_path.exists():
        click.echo(f"   No dimension groups found in {dir}")
        return
        
    # Load dimension group files
    for yaml_file in dim_path.glob("*.yml"):
        try:
            with open(yaml_file, 'r') as f:
                data = yaml.safe_load(f)
                
            if 'dimension_groups' in data:
                click.echo(f"\n   From {yaml_file.name}:")
                for name, group in data['dimension_groups'].items():
                    dim_count = len(group.get('dimensions', []))
                    extends = f" (extends: {group['extends']})" if 'extends' in group else ""
                    click.echo(f"     - {name}: {dim_count} dimensions{extends}")
        except Exception as e:
            click.echo(f"   Error loading {yaml_file}: {e}")


if __name__ == '__main__':
    cli()