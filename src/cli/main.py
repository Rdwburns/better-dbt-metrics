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
from core.error_handler import (
    ErrorCollector, CompilationReport, CompilationError, 
    ErrorCategory, ErrorSeverity
)
from core.pre_validator import PreCompilationValidator
from features.catalog_generator import MetricCatalogGenerator, CatalogConfig
from features.smart_suggestions import (
    SmartSuggestions, TableInfo, ColumnInfo, MetricConfidence
)


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
@click.option('--debug', is_flag=True, help='Enable debug output (internal diagnostics)')
@click.option('--verbose', '-v', is_flag=True, help='Show step-by-step progress and detailed errors')
@click.option('--pre-validate/--no-pre-validate', default=True, help='Run pre-compilation checks')
@click.option('--report-format', type=click.Choice(['terminal', 'json', 'junit']), default='terminal', help='Error report format')
def compile(input_dir, output_dir, template_dir, dimension_group_dir, 
           environment, config, split_files, auto_variants, validate, json_output, 
           debug, verbose, pre_validate, report_format):
    """Compile better-dbt-metrics YAML to dbt semantic models"""
    
    # Load config file if provided
    config_data = {}
    if config:
        with open(config, 'r') as f:
            config_data = yaml.safe_load(f)
    
    # Override with CLI options
    template_dirs = list(template_dir) or config_data.get('template_dirs', ['templates/'])
    dimension_dirs = list(dimension_group_dir) or config_data.get('dimension_group_dirs', ['templates/dimensions/'])
    
    # Verbose mode implies debug mode for full visibility
    effective_debug = debug or verbose
    
    # Create compiler config
    compiler_config = CompilerConfig(
        input_dir=input_dir,
        output_dir=output_dir,
        template_dirs=template_dirs,
        dimension_group_dirs=dimension_dirs,
        environment=environment,
        split_files=split_files,
        auto_variants=auto_variants,
        validate=validate,
        debug=effective_debug
    )
    
    # Initialize error collector
    error_collector = ErrorCollector()
    
    # Run pre-validation if enabled
    if pre_validate:
        if verbose:
            click.echo("🔍 Running pre-compilation validation...")
            
        pre_validator = PreCompilationValidator(debug=effective_debug)
        is_valid, validation_errors = pre_validator.validate_directory(input_dir)
        
        # Merge validation errors
        for error in validation_errors.errors:
            error_collector.add_error(error)
        for warning in validation_errors.warnings:
            error_collector.add_error(warning)
            
        if not is_valid and validate:
            # Show validation report and exit
            report = CompilationReport(error_collector, {'files_processed': 0})
            
            if report_format == 'json':
                print(report.generate_json_report())
            elif report_format == 'junit':
                print(report.to_junit_xml())
            else:
                click.echo(report.generate_terminal_report(verbose=verbose))
            sys.exit(1)
    
    # Run compilation
    compiler = BetterDBTCompiler(compiler_config)
    
    try:
        if verbose:
            click.echo("🔨 Compiling metrics...")
            
        results = compiler.compile_directory()
        
        # Add compilation errors to collector
        for error_dict in results.get('errors', []):
            error_collector.add_error(
                CompilationError(
                    message=error_dict['error'],
                    category=ErrorCategory.METRIC_DEFINITION,
                    severity=ErrorSeverity.ERROR,
                    file_path=Path(error_dict['file'])
                )
            )
        
        # Generate comprehensive report
        report = CompilationReport(error_collector, results)
        
        if json_output or report_format == 'json':
            print(report.generate_json_report())
        elif report_format == 'junit':
            print(error_collector.to_junit_xml())
        else:
            # Terminal output
            if verbose or error_collector.has_errors() or error_collector.has_warnings():
                click.echo(report.generate_terminal_report(verbose=verbose))
            else:
                # Simple success message
                click.echo(f"✅ Compilation completed successfully!")
                click.echo(f"   Files processed: {results['files_processed']}")
                click.echo(f"   Metrics compiled: {results['metrics_compiled']}")
                click.echo(f"   Models generated: {results['models_generated']}")
            
        # Exit with error if compilation failed
        if error_collector.has_errors():
            sys.exit(1)
                
    except Exception as e:
        # Add exception as error
        error_collector.add_error(
            CompilationError(
                message=f"Unexpected compilation error: {str(e)}",
                category=ErrorCategory.CONFIGURATION,
                severity=ErrorSeverity.ERROR,
                suggestion="Check your configuration and file paths"
            )
        )
        
        report = CompilationReport(error_collector, {'files_processed': 0})
        
        if json_output or report_format == 'json':
            print(report.generate_json_report())
        else:
            click.echo(report.generate_terminal_report(verbose=effective_debug))
            
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
@click.option('--input-dir', '-i', default='metrics/', help='Input directory containing metrics YAML')
@click.option('--output-dir', '-o', default='docs/metrics', help='Output directory for catalog')
@click.option('--format', '-f', type=click.Choice(['detailed', 'compact', 'custom']), default='detailed', help='Documentation format')
@click.option('--custom-template', help='Path to custom template file')
@click.option('--group-by-domain/--flat', default=True, help='Group metrics by domain')
@click.option('--include-search/--no-search', default=True, help='Include search functionality')
@click.option('--include-lineage/--no-lineage', default=True, help='Include data lineage')
@click.option('--include-sql/--no-sql', default=True, help='Include SQL examples')
@click.option('--debug', is_flag=True, help='Enable debug output')
def catalog(input_dir, output_dir, format, custom_template, group_by_domain, 
           include_search, include_lineage, include_sql, debug):
    """Generate metric catalog documentation"""
    
    # First compile the metrics
    compiler_config = CompilerConfig(
        input_dir=input_dir,
        output_dir='models/semantic/',  # Temporary output
        debug=debug
    )
    
    compiler = BetterDBTCompiler(compiler_config)
    
    try:
        # Compile metrics
        click.echo("📊 Compiling metrics...")
        results = compiler.compile_directory()
        
        if results['errors']:
            click.echo("❌ Compilation errors encountered:")
            for error in results['errors']:
                click.echo(f"   - {error['file']}: {error['error']}")
            sys.exit(1)
            
        # Generate catalog
        click.echo("📝 Generating catalog...")
        
        catalog_config = CatalogConfig(
            output_dir=output_dir,
            template_style=format,
            custom_template_path=custom_template,
            group_by_domain=group_by_domain,
            include_search_index=include_search,
            include_lineage=include_lineage,
            include_sql_examples=include_sql,
            include_technical_details=True,
            include_dependencies=True,
            include_glossary=True
        )
        
        generator = MetricCatalogGenerator(catalog_config, compiler)
        
        # Generate catalog files
        output_files = generator.generate_catalog(compiler.compiled_metrics)
        
        # Write files
        generator.write_catalog(output_files)
        
        click.echo(f"✅ Catalog generated successfully!")
        click.echo(f"   Output directory: {output_dir}")
        click.echo(f"   Files generated: {len(output_files)}")
        click.echo(f"   Total metrics documented: {len(compiler.compiled_metrics)}")
        
        if include_search:
            click.echo(f"\n🔍 Search interface available at: {output_dir}/search.html")
            
    except Exception as e:
        click.echo(f"❌ Catalog generation failed: {e}", err=True)
        if debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


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


@cli.command()
@click.option('--source', '-s', multiple=True, required=True, help='Source table(s) to analyze')
@click.option('--schema-file', '-f', help='Schema definition file (YAML or JSON)')
@click.option('--connection', '-c', help='Database connection string')
@click.option('--output', '-o', type=click.Choice(['yaml', 'json', 'text']), default='yaml', help='Output format')
@click.option('--confidence', type=click.Choice(['all', 'high', 'medium', 'low']), default='all', help='Minimum confidence level')
@click.option('--max-suggestions', '-m', type=int, default=50, help='Maximum suggestions per table')
@click.option('--output-file', help='Write output to file instead of stdout')
@click.option('--profile/--no-profile', default=False, help='Profile data for better suggestions')
@click.option('--debug', is_flag=True, help='Enable debug output')
def suggest(source, schema_file, connection, output, confidence, max_suggestions, 
           output_file, profile, debug):
    """Analyze database tables and suggest metrics"""
    
    # Initialize suggestion engine
    suggestion_engine = SmartSuggestions()
    all_suggestions = []
    
    # Load schema information
    if schema_file:
        # Load from file
        click.echo(f"📊 Loading schema from {schema_file}...")
        with open(schema_file, 'r') as f:
            if schema_file.endswith('.json'):
                schema_data = json.load(f)
            else:
                schema_data = yaml.safe_load(f)
                
        # Process each table in schema file
        for table_name, table_def in schema_data.get('tables', {}).items():
            if source and table_name not in source:
                continue
                
            # Create TableInfo
            columns = []
            for col_def in table_def.get('columns', []):
                column = ColumnInfo(
                    name=col_def['name'],
                    data_type=col_def.get('type', 'unknown'),
                    nullable=col_def.get('nullable', True),
                    is_primary_key=col_def.get('primary_key', False),
                    is_foreign_key=col_def.get('foreign_key', False),
                    foreign_table=col_def.get('references')
                )
                columns.append(column)
                
            table_info = TableInfo(
                name=table_name,
                schema=table_def.get('schema', 'public'),
                columns=columns,
                row_count=table_def.get('row_count')
            )
            
            # Analyze table
            click.echo(f"\n🔍 Analyzing table: {table_name}")
            suggestions = suggestion_engine.analyze_table(table_info)
            
            # Filter by confidence if needed
            if confidence != 'all':
                min_conf = MetricConfidence[confidence.upper()]
                suggestions = [s for s in suggestions if s.confidence.value >= min_conf.value]
                
            # Limit suggestions
            suggestions = suggestions[:max_suggestions]
            
            click.echo(f"   Found {len(suggestions)} metric suggestions")
            all_suggestions.extend(suggestions)
            
    elif connection:
        # Connect to database and analyze
        click.echo("❌ Database connection analysis not yet implemented")
        click.echo("   Please provide a schema file with --schema-file")
        sys.exit(1)
    else:
        # Create example schema
        click.echo("📝 No schema provided, using example e-commerce schema...")
        
        # Example e-commerce fact table
        example_table = TableInfo(
            name='fct_orders',
            schema='public',
            columns=[
                ColumnInfo('order_id', 'bigint', False, True, False),
                ColumnInfo('customer_id', 'bigint', False, False, True, 'dim_customers'),
                ColumnInfo('order_date', 'timestamp', False),
                ColumnInfo('order_status', 'varchar', True),
                ColumnInfo('order_total', 'decimal(10,2)', True),
                ColumnInfo('shipping_amount', 'decimal(10,2)', True),
                ColumnInfo('tax_amount', 'decimal(10,2)', True),
                ColumnInfo('discount_amount', 'decimal(10,2)', True),
                ColumnInfo('payment_method', 'varchar', True),
                ColumnInfo('is_first_order', 'boolean', True),
                ColumnInfo('created_at', 'timestamp', False),
                ColumnInfo('updated_at', 'timestamp', False),
            ]
        )
        
        suggestions = suggestion_engine.analyze_table(example_table)
        suggestions = suggestions[:max_suggestions]
        all_suggestions.extend(suggestions)
        
    # Format and output results
    if all_suggestions:
        formatted = suggestion_engine.format_suggestions(all_suggestions, output)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(formatted)
            click.echo(f"\n✅ Suggestions written to {output_file}")
        else:
            click.echo(f"\n{'='*60}")
            click.echo(formatted)
            
        # Summary
        click.echo(f"\n📊 Summary:")
        click.echo(f"   Total suggestions: {len(all_suggestions)}")
        high = len([s for s in all_suggestions if s.confidence == MetricConfidence.HIGH])
        medium = len([s for s in all_suggestions if s.confidence == MetricConfidence.MEDIUM])
        low = len([s for s in all_suggestions if s.confidence == MetricConfidence.LOW])
        click.echo(f"   High confidence: {high}")
        click.echo(f"   Medium confidence: {medium}")
        click.echo(f"   Low confidence: {low}")
    else:
        click.echo("\n❌ No metric suggestions generated")


if __name__ == '__main__':
    cli()