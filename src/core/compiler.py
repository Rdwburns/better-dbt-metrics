"""
Core Compiler for Better-DBT-Metrics
Compiles better-dbt-metrics YAML to dbt semantic models
"""

import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from copy import deepcopy

from core.parser import BetterDBTParser
from features.templates import TemplateLibrary
from features.dimension_groups import DimensionGroupManager
from core.config_loader import ConfigLoader, BDMConfig


@dataclass
class CompilerConfig:
    """Configuration for the compiler"""
    input_dir: str = "metrics/"
    output_dir: str = "models/semantic/"
    template_dirs: List[str] = field(default_factory=lambda: ["templates/"])
    dimension_group_dirs: List[str] = field(default_factory=lambda: ["templates/dimensions/"])
    validate: bool = True
    split_files: bool = True
    environment: str = "dev"
    auto_variants: bool = True
    generate_tests: bool = True
    debug: bool = False
    

class BetterDBTCompiler:
    """
    Main compiler that orchestrates the compilation process
    """
    
    def __init__(self, config: CompilerConfig):
        self.config = config
        
        # Load BDM configuration
        config_loader = ConfigLoader()
        self.bdm_config = config_loader.load_config(base_dir=config.input_dir)
        
        # Apply BDM config to compiler config if not overridden
        if not config.output_dir or config.output_dir == "models/semantic/":
            config.output_dir = self.bdm_config.output_dir
        if not config.template_dirs or config.template_dirs == ["templates/"]:
            config.template_dirs = [self.bdm_config.template_dir]
            
        self.parser = BetterDBTParser(
            base_dir=".", 
            debug=config.debug,
            import_mappings=self.bdm_config.import_mappings,
            search_paths=self.bdm_config.search_paths
        )
        self.templates = TemplateLibrary(config.template_dirs)
        self.dimension_groups = DimensionGroupManager()
        
        # Track compilation state
        self.compiled_metrics: List[Dict[str, Any]] = []
        self.semantic_models: List[Dict[str, Any]] = []
        self.metrics_by_source: Dict[str, List[Dict]] = {}
        self.entities: Dict[str, Dict[str, Any]] = {}  # Store entity definitions
        self.entity_sets: Dict[str, Dict[str, Any]] = {}  # Store entity set definitions
        self.current_file: Optional[Path] = None  # Track current file for domain detection
        self.time_spines: Dict[str, Dict[str, Any]] = {}  # Store time spine configurations
        self.join_paths: List[Dict[str, Any]] = []  # Store join path definitions
        self.join_path_aliases: Dict[str, Dict[str, Any]] = {}  # Store join path aliases
        self.offset_patterns: Dict[str, List[Dict[str, Any]]] = {}  # Store offset window patterns
        
    def compile_directory(self, input_dir: Optional[str] = None) -> Dict[str, Any]:
        """Compile all metrics files in a directory"""
        input_path = Path(input_dir or self.config.input_dir)
        
        if not input_path.exists():
            raise ValueError(f"Input directory not found: {input_path}")
        
        # Run validation first if enabled
        if self.config.validate:
            if self.config.debug:
                print("[DEBUG] Running pre-compilation validation...")
            
            from validation.validator import MetricsValidator
            validator = MetricsValidator(".")
            validation_result = validator.validate_directory(str(input_path))
            
            if validation_result.has_errors():
                print("\n❌ Validation failed - metrics have errors that must be fixed:")
                validation_result.print_summary()
                raise ValueError("Cannot compile metrics with validation errors. Please fix the issues above.")
            elif validation_result.warnings:
                print("\n⚠️  Validation warnings (compilation will continue):")
                validation_result.print_summary()
            else:
                print("✅ Validation passed - all metric references are valid")
            
        # Load dimension groups first
        self._load_dimension_groups()
        
        # Find and compile all metrics files
        results = {
            'files_processed': 0,
            'metrics_compiled': 0,
            'models_generated': 0,
            'errors': [],
            'validation_errors': 0,
            'skipped_metrics': []
        }
        
        for yaml_file in input_path.rglob("*.yml"):
            # Skip non-metrics files
            if yaml_file.name.startswith('_'):
                continue
            
            # Skip configuration files
            if yaml_file.name in ['bdm_config.yml', 'config.yml', 'dbt_project.yml']:
                if self.config.debug:
                    print(f"[DEBUG] Skipping configuration file: {yaml_file}")
                continue
                
            results['files_processed'] += 1
            try:
                self.compile_file(yaml_file)
            except Exception as e:
                if self.config.debug:
                    print(f"\n[DEBUG] Error compiling {yaml_file}:")
                    print(f"[DEBUG] Error type: {type(e).__name__}")
                    print(f"[DEBUG] Error message: {str(e)}")
                    if "'list' object has no attribute 'get'" in str(e):
                        import traceback
                        print(f"[DEBUG] Full traceback:")
                        traceback.print_exc()
                results['errors'].append({
                    'file': str(yaml_file),
                    'error': str(e)
                })
                
        # Generate output
        output_data = self._generate_output()
        results['metrics_compiled'] = len(output_data.get('metrics', []))
        results['models_generated'] = len(output_data.get('semantic_models', []))
        
        # Write output files
        if self.config.split_files:
            self._write_split_output(output_data)
        else:
            self._write_single_output(output_data)
            
        return results
        
    def compile_file(self, file_path: Path) -> Dict[str, Any]:
        """Compile a single metrics file"""
        if self.config.debug:
            print(f"\n[DEBUG] === Compiling file: {file_path} ===")
        
        # Track current file for domain detection
        self.current_file = file_path
        
        # Parse file with imports and references
        parsed_data = self.parser.parse_file(str(file_path))
        
        if self.config.debug and 'metrics' in parsed_data:
            print(f"[DEBUG] Raw parsed metrics: {parsed_data['metrics']}")
        
        # Validate version
        if parsed_data.get('version') not in [1, 2]:
            raise ValueError(f"Unsupported version: {parsed_data.get('version')}")
            
        # Register dimension groups from imported files first
        self._register_imported_dimension_groups()
        
        # Register templates from imported files
        self._register_imported_templates()
        
        # Register templates from current file
        if 'metric_templates' in parsed_data:
            for name, template_def in parsed_data['metric_templates'].items():
                self.templates.engine.register_template(name, template_def)
            
        # Register dimension groups from this file
        if 'dimension_groups' in parsed_data:
            for name, group_def in parsed_data['dimension_groups'].items():
                # Resolve any references in the dimension group
                resolved_group = self._resolve_references_in_group(group_def)
                self.dimension_groups.register_group(name, resolved_group)
                
        # Register entities if defined
        if 'entities' in parsed_data:
            for entity in parsed_data['entities']:
                self.entities[entity['name']] = entity
                
        # Register entity sets if defined
        if 'entity_sets' in parsed_data:
            for entity_set in parsed_data['entity_sets']:
                self.entity_sets[entity_set['name']] = entity_set
                
        # Register time spines if defined
        if 'time_spine' in parsed_data:
            for name, spine_config in parsed_data['time_spine'].items():
                self.time_spines[name] = spine_config
                
        # Register join paths if defined
        if 'join_paths' in parsed_data:
            self.join_paths.extend(parsed_data['join_paths'])
            
        # Register join path aliases if defined
        if 'join_path_aliases' in parsed_data:
            for name, alias_def in parsed_data['join_path_aliases'].items():
                self.join_path_aliases[name] = alias_def
                
        # Register offset patterns if defined
        if 'offset_window_config' in parsed_data:
            config = parsed_data['offset_window_config']
            if 'offset_patterns' in config:
                for name, pattern_def in config['offset_patterns'].items():
                    self.offset_patterns[name] = pattern_def
                    
        # Process metrics
        metrics = parsed_data.get('metrics', [])
        
        if self.config.debug:
            print(f"[DEBUG] Found {len(metrics)} metrics in file")
            print(f"[DEBUG] Metrics type: {type(metrics)}")
            if metrics and isinstance(metrics, list):
                print(f"[DEBUG] First metric type: {type(metrics[0])}")
                if isinstance(metrics[0], dict):
                    print(f"[DEBUG] First metric keys: {list(metrics[0].keys())}")
        
        for metric in metrics:
            try:
                if self.config.debug:
                    print(f"\n[DEBUG] Compiling metric: {metric.get('name', 'unknown')}")
                    print(f"[DEBUG] Metric type: {type(metric)}")
                    if isinstance(metric, dict):
                        print(f"[DEBUG] Metric keys: {list(metric.keys())}")
                        if 'dimensions' in metric:
                            print(f"[DEBUG] Dimensions type: {type(metric['dimensions'])}")
                            print(f"[DEBUG] Dimensions value: {metric['dimensions']}")
                
                # Validate individual metric before compilation
                if self.config.validate and not self._validate_metric_models(metric):
                    metric_name = metric.get('name', 'unknown')
                    print(f"⚠️  Skipping metric '{metric_name}' due to invalid model references")
                    # For individual file compilation, we'll just skip the metric
                    continue
                
                compiled_metric = self._compile_metric(metric)
                self.compiled_metrics.append(compiled_metric)
            except (AttributeError, TypeError) as e:
                metric_name = metric.get('name', 'unknown')
                if self.config.debug:
                    import traceback
                    print(f"\n[DEBUG] Error compiling metric '{metric_name}'")
                    print(f"[DEBUG] Error type: {type(e).__name__}")
                    print(f"[DEBUG] Error message: {str(e)}")
                    print(f"\n[DEBUG] Full traceback:")
                    traceback.print_exc()
                    
                if "'list' object has no attribute 'get'" in str(e):
                    raise AttributeError(f"Error compiling metric '{metric_name}': {e}. Check that dimensions are properly formatted.")
                elif "string indices must be integers" in str(e):
                    raise TypeError(f"Error compiling metric '{metric_name}': {e}. Check metric structure and dimension references.")
                raise
            
            # Group by source for semantic model generation
            source = compiled_metric.get('source')
            
            # Validate that metric has a source
            if not source:
                metric_type = compiled_metric.get('type', 'unknown')
                metric_name = compiled_metric.get('name', 'unknown')
                
                # Provide specific guidance based on metric type
                if metric_type == 'ratio':
                    # For ratio metrics, check if numerator/denominator have sources
                    num_source = compiled_metric.get('numerator', {}).get('source')
                    den_source = compiled_metric.get('denominator', {}).get('source')
                    
                    if num_source and den_source:
                        # If both have sources, use a composite source identifier
                        # This allows ratio metrics with different sources to work
                        if num_source == den_source:
                            source = num_source
                        else:
                            # For different sources, we'll handle them separately in semantic model generation
                            source = f"ratio_{metric_name}"
                    else:
                        raise ValueError(
                            f"Ratio metric '{metric_name}' is missing source information. "
                            f"Please add either:\n"
                            f"  1. A top-level 'source' field to the metric, OR\n"
                            f"  2. Both 'numerator.source' and 'denominator.source' fields\n"
                            f"Current state: numerator.source={num_source}, denominator.source={den_source}"
                        )
                elif metric_type == 'derived':
                    # Derived metrics don't need a source
                    source = 'derived'
                elif metric_type == 'conversion':
                    # Conversion metrics may have sources in base_measure and conversion_measure
                    base_source = compiled_metric.get('base_measure', {}).get('source')
                    conv_source = compiled_metric.get('conversion_measure', {}).get('source')
                    
                    if base_source and conv_source:
                        if base_source == conv_source:
                            source = base_source
                        else:
                            # For different sources, use a composite identifier
                            source = f"conversion_{metric_name}"
                    else:
                        raise ValueError(
                            f"Conversion metric '{metric_name}' is missing source information. "
                            f"Both 'base_measure.source' and 'conversion_measure.source' are required.\n"
                            f"Current state: base_measure.source={base_source}, conversion_measure.source={conv_source}"
                        )
                else:
                    raise ValueError(
                        f"Metric '{metric_name}' of type '{metric_type}' is missing required 'source' field"
                    )
            
            if source == 'unknown':
                raise ValueError(
                    f"Metric '{compiled_metric.get('name', 'unknown')}' has invalid source 'unknown'. "
                    f"Please specify a valid source table."
                )
                
            if source not in self.metrics_by_source:
                self.metrics_by_source[source] = []
            self.metrics_by_source[source].append(compiled_metric)
            
            # Generate auto-variants after the main metric is added
            if self.config.auto_variants and self.bdm_config.expand_auto_variants and 'auto_variants' in compiled_metric:
                self._generate_auto_variants(compiled_metric)
            
        return parsed_data
        
    def _load_dimension_groups(self):
        """Load all dimension groups from configured directories"""
        for dim_dir in self.config.dimension_group_dirs:
            dim_path = Path(dim_dir)
            if not dim_path.exists():
                continue
                
            for yaml_file in dim_path.glob("*.yml"):
                with open(yaml_file, 'r') as f:
                    data = yaml.safe_load(f)
                    
                if 'dimension_groups' in data:
                    for name, group_def in data['dimension_groups'].items():
                        self.dimension_groups.register_group(name, group_def)
                        
    def _compile_metric(self, metric_def: Dict[str, Any]) -> Dict[str, Any]:
        """Compile a single metric definition"""
        if self.config.debug:
            print(f"\n[DEBUG] _compile_metric called with: {metric_def.get('name', 'unknown')}")
            
        # Handle template expansion
        if 'template' in metric_def or 'extends' in metric_def or '$use' in metric_def:
            if self.config.debug:
                print(f"[DEBUG] Expanding template for metric: {metric_def.get('name')}")
                print(f"[DEBUG] Template/extends/$use: {metric_def.get('template', metric_def.get('extends', metric_def.get('$use')))}")
            metric_def = self._expand_metric_template(metric_def)
            if self.config.debug:
                print(f"[DEBUG] After template expansion, dimensions: {metric_def.get('dimensions', 'none')}")
            
        # Expand dimension groups first
        if 'dimension_groups' in metric_def:
            dimensions = []
            for group_name in metric_def['dimension_groups']:
                try:
                    group_dims = self.dimension_groups.get_dimensions_for_group(group_name)
                    # Ensure group_dims is a list
                    if isinstance(group_dims, list):
                        dimensions.extend(group_dims)
                    else:
                        # Convert single dimension to list
                        dimensions.append(group_dims)
                except ValueError:
                    # Group not found, skip it
                    pass
                except Exception as e:
                    # Log error but continue
                    print(f"Warning: Error expanding dimension group '{group_name}': {e}")
                    pass
            # Add any additional dimensions
            if 'dimensions' in metric_def:
                existing_dims = metric_def['dimensions']
                # Ensure it's a list before extending
                if isinstance(existing_dims, list):
                    dimensions.extend(existing_dims)
                elif existing_dims:
                    dimensions.append(existing_dims)
            metric_def['dimensions'] = dimensions
            
        # Expand dimension references
        if 'dimensions' in metric_def:
            dims = metric_def['dimensions']
            if self.config.debug:
                print(f"[DEBUG] Processing dimensions for {metric_def.get('name')}")
                print(f"[DEBUG] Dimensions before expansion: type={type(dims)}, value={dims}")
            
            # Handle unresolved $ref
            if isinstance(dims, dict) and '$ref' in dims:
                # Try to resolve it here if parser didn't
                ref_path = dims['$ref']
                if self.config.debug:
                    print(f"[DEBUG] Found unresolved $ref: {ref_path}")
                if ref_path.startswith('_base.dimension_groups.'):
                    group_name = ref_path.split('.')[-1]
                    try:
                        dims = self.dimension_groups.get_dimensions_for_group(group_name)
                        if self.config.debug:
                            print(f"[DEBUG] Resolved dimension group '{group_name}' to: {dims}")
                    except Exception as e:
                        if self.config.debug:
                            print(f"[DEBUG] Failed to resolve dimension group '{group_name}': {e}")
                        # If not found, try without _base prefix
                        dims = []
            # Also handle string format with $ref
            elif isinstance(dims, str) and dims.startswith('$ref('):
                # Extract reference from $ref(...) format
                ref_match = dims[5:-1] if dims.endswith(')') else dims[5:]
                if self.config.debug:
                    print(f"[DEBUG] Found string $ref: {ref_match}")
                    
                # Try to resolve the dimension group
                if ref_match.startswith('_base.dimension_groups.'):
                    group_name = ref_match.split('.')[-1]
                    try:
                        # Try with full alias path first
                        full_group_name = ref_match.replace('_base.dimension_groups.', 'metrics/_base/../_base/dimension_groups.yml.')
                        dims = self.dimension_groups.get_dimensions_for_group(full_group_name)
                        if self.config.debug:
                            print(f"[DEBUG] Resolved dimension group '{full_group_name}' to: {dims}")
                    except:
                        try:
                            # Try just the group name
                            dims = self.dimension_groups.get_dimensions_for_group(group_name)
                            if self.config.debug:
                                print(f"[DEBUG] Resolved dimension group '{group_name}' to: {dims}")
                        except Exception as e:
                            if self.config.debug:
                                print(f"[DEBUG] Failed to resolve dimension group '{group_name}': {e}")
                            dims = []
            
            expanded_dims = self._expand_dimensions(dims)
            if self.config.debug:
                print(f"[DEBUG] Dimensions after expansion: {expanded_dims}")
            metric_def['dimensions'] = expanded_dims
            
        # Add default fields
        compiled = {
            'name': metric_def['name'],
            'description': metric_def.get('description', ''),
            'type': metric_def.get('type', 'simple'),
            'label': metric_def.get('label', metric_def['name'].replace('_', ' ').title())
        }
        
        # Apply validation rules from config
        if self.bdm_config.require_descriptions and not compiled['description']:
            if self.config.debug:
                print(f"[DEBUG] Warning: Metric '{compiled['name']}' missing description")
                
        if self.bdm_config.require_labels and compiled['label'] == compiled['name'].replace('_', ' ').title():
            if self.config.debug:
                print(f"[DEBUG] Warning: Metric '{compiled['name']}' using auto-generated label")
        
        # Process metric_time dimensions
        if 'dimensions' in metric_def:
            processed_dims = self._process_metric_time_dimensions(metric_def['dimensions'])
            metric_def['dimensions'] = processed_dims
            
        # Also process metric_time in numerator/denominator for ratio metrics
        if metric_def.get('type') == 'ratio':
            # Validate ratio metric has proper structure
            if 'numerator' not in metric_def or 'denominator' not in metric_def:
                raise ValueError(
                    f"Ratio metric '{metric_def.get('name')}' must have both 'numerator' and 'denominator' fields"
                )
            
            # Ensure numerator is a dict
            if 'numerator' in metric_def:
                if not isinstance(metric_def['numerator'], dict):
                    if self.config.debug:
                        print(f"[DEBUG] Converting numerator to dict format for {metric_def.get('name')}")
                        print(f"[DEBUG] Original numerator: {metric_def['numerator']}")
                    # Convert simple format to dict
                    metric_def['numerator'] = {'value': metric_def['numerator']}
                    
                if 'dimensions' in metric_def['numerator']:
                    metric_def['numerator']['dimensions'] = self._process_metric_time_dimensions(
                        metric_def['numerator']['dimensions']
                    )
                    
            # Ensure denominator is a dict
            if 'denominator' in metric_def:
                if not isinstance(metric_def['denominator'], dict):
                    if self.config.debug:
                        print(f"[DEBUG] Converting denominator to dict format for {metric_def.get('name')}")
                        print(f"[DEBUG] Original denominator: {metric_def['denominator']}")
                    # Convert simple format to dict
                    metric_def['denominator'] = {'value': metric_def['denominator']}
                    
                if 'dimensions' in metric_def['denominator']:
                    metric_def['denominator']['dimensions'] = self._process_metric_time_dimensions(
                        metric_def['denominator']['dimensions']
                    )
            
            # Validate that either metric has a source, or both numerator and denominator have sources
            if 'source' not in metric_def:
                num_source = metric_def.get('numerator', {}).get('source')
                den_source = metric_def.get('denominator', {}).get('source')
                
                if not num_source or not den_source:
                    raise ValueError(
                        f"Ratio metric '{metric_def.get('name')}' must have either:\n"
                        f"  1. A top-level 'source' field, OR\n"
                        f"  2. Both 'numerator.source' and 'denominator.source' fields\n"
                        f"Current state: numerator.source={num_source}, denominator.source={den_source}"
                    )
                
                # If both have sources and they're the same, use that as the metric source
                if num_source == den_source:
                    metric_def['source'] = num_source
                    if self.config.debug:
                        print(f"[DEBUG] Auto-setting source '{num_source}' for ratio metric '{metric_def.get('name')}' from matching numerator/denominator sources")
                else:
                    # For different sources, we'll use a composite identifier
                    # This will be handled specially in semantic model generation
                    metric_def['source'] = f"ratio_{metric_def.get('name')}"
                    if self.config.debug:
                        print(f"[DEBUG] Setting composite source 'ratio_{metric_def.get('name')}' for ratio metric with different numerator/denominator sources")
        
        # Handle conversion metrics similarly
        if metric_def.get('type') == 'conversion':
            # Validate conversion metric has proper structure
            if 'base_measure' not in metric_def or 'conversion_measure' not in metric_def:
                raise ValueError(
                    f"Conversion metric '{metric_def.get('name')}' must have both 'base_measure' and 'conversion_measure' fields"
                )
            
            # Check if source is already set at metric level
            if 'source' not in metric_def:
                base_source = metric_def.get('base_measure', {}).get('source')
                conv_source = metric_def.get('conversion_measure', {}).get('source')
                
                if not base_source or not conv_source:
                    raise ValueError(
                        f"Conversion metric '{metric_def.get('name')}' must have sources in both "
                        f"'base_measure.source' and 'conversion_measure.source' fields\n"
                        f"Current state: base_measure.source={base_source}, conversion_measure.source={conv_source}"
                    )
                
                # If both have sources and they're the same, use that as the metric source
                if base_source == conv_source:
                    metric_def['source'] = base_source
                    if self.config.debug:
                        print(f"[DEBUG] Auto-setting source '{base_source}' for conversion metric '{metric_def.get('name')}' from matching base/conversion sources")
                else:
                    # For different sources, we'll use a composite identifier
                    metric_def['source'] = f"conversion_{metric_def.get('name')}"
                    if self.config.debug:
                        print(f"[DEBUG] Setting composite source 'conversion_{metric_def.get('name')}' for conversion metric with different base/conversion sources")
        
        # Only add source if it exists (not all metric types have source)
        if 'source' in metric_def:
            compiled['source'] = metric_def['source']
        
        # Preserve source_ref metadata if it exists
        if 'source_ref' in metric_def:
            compiled['source_ref'] = metric_def['source_ref']
        
        # Copy over other fields
        for key in ['measure', 'numerator', 'denominator', 'formula', 'expression', 
                   'filter', 'meta', 'config', 'validation', 'auto_variants', 
                   'window', 'grain_to_date', 'base_measure', 'conversion_measure', 
                   'entity', 'dimensions', 'fill_nulls_with', 'time_spine', 
                   'offsets', 'window_type', 'offset_pattern']:
            if key in metric_def:
                compiled[key] = deepcopy(metric_def[key])
                
        return compiled
        
    def _expand_metric_template(self, metric_def: Dict[str, Any]) -> Dict[str, Any]:
        """Expand metric that uses template or extends"""
        # Handle 'template' field
        if 'template' in metric_def:
            template_ref = metric_def['template']
            params = metric_def.get('parameters', metric_def.get('params', {}))
            
            if self.config.debug:
                print(f"[DEBUG] Expanding template: {template_ref}")
                print(f"[DEBUG] Parameters: {params}")
            
            # Extract template name from reference like "templates.revenue_metric"
            if '.' in template_ref:
                parts = template_ref.split('.')
                template_name = parts[-1]
            else:
                template_name = template_ref
            
            try:
                expanded = self.templates.expand(template_name, params)
                if self.config.debug:
                    print(f"[DEBUG] Template expanded successfully")
                    print(f"[DEBUG] Expanded keys: {list(expanded.keys())}")
                    print(f"[DEBUG] Expanded content: {expanded}")
            except Exception as e:
                if self.config.debug:
                    print(f"[DEBUG] Template expansion failed: {e}")
                # Template might not be found, return as-is
                return metric_def
            
            # Merge with metric definition (metric fields override template)
            for key, value in metric_def.items():
                if key not in ['template', 'parameters', 'params']:
                    expanded[key] = value
                    
            return expanded
            
        # Handle $use (similar to template but with dot notation)
        if '$use' in metric_def:
            template_ref = metric_def['$use']
            params = metric_def.copy()
            params.pop('$use')  # Remove $use from params
            
            if self.config.debug:
                print(f"[DEBUG] Expanding $use: {template_ref}")
                print(f"[DEBUG] Parameters: {list(params.keys())}")
            
            # Extract template name from reference like "templates.margin_metric"
            if '.' in template_ref:
                parts = template_ref.split('.')
                template_name = parts[-1]
            else:
                template_name = template_ref
            
            if self.config.debug:
                print(f"[DEBUG] Template name: {template_name}")
            
            # Expand template
            try:
                expanded = self.templates.expand(template_name, params)
                if self.config.debug:
                    print(f"[DEBUG] Template expanded successfully")
                    print(f"[DEBUG] Expanded keys: {list(expanded.keys())}")
                    print(f"[DEBUG] Expanded content: {expanded}")
            except Exception as e:
                if self.config.debug:
                    print(f"[DEBUG] Template expansion failed: {e}")
                # Template might not be found, return as-is
                return metric_def
            
            # Merge with metric definition (metric fields override template)
            for key, value in metric_def.items():
                if key != '$use':
                    expanded[key] = value
                    
            if self.config.debug:
                print(f"[DEBUG] Final expanded metric: {expanded.get('name')}")
                if 'numerator' in expanded:
                    print(f"[DEBUG] Numerator type after expansion: {type(expanded['numerator'])}")
                if 'denominator' in expanded:
                    print(f"[DEBUG] Denominator type after expansion: {type(expanded['denominator'])}")
                    
            return expanded
        
        elif 'template' in metric_def:
            template_name = metric_def['template']
            params = metric_def.get('params', metric_def.get('parameters', {}))
            
            # Expand template
            try:
                expanded = self.templates.expand(template_name, params)
            except Exception:
                # Template might not be found, return as-is
                return metric_def
            
            # Merge with metric definition (metric fields override template)
            for key, value in metric_def.items():
                if key not in ['template', 'params', 'parameters']:
                    expanded[key] = value
                    
            return expanded
            
        # Handle extends (already processed by parser)
        return metric_def
        
    def _expand_dimensions(self, dimensions: List[Any]) -> List[Dict[str, Any]]:
        """Expand dimension references including groups"""
        expanded = []
        
        # Ensure dimensions is a list
        if not isinstance(dimensions, list):
            if dimensions:
                dimensions = [dimensions]
            else:
                return []
        
        for dim in dimensions:
            if isinstance(dim, str):
                # Check if it's a reference
                if dim.startswith('$ref(') and dim.endswith(')'):
                    # This is an unresolved reference, skip it
                    if self.config.debug:
                        print(f"[DEBUG] Skipping unresolved string reference: {dim}")
                    continue
                else:
                    # Simple dimension name
                    expanded.append({'name': dim})
                
            elif isinstance(dim, dict):
                if '$ref' in dim or '$use' in dim:
                    # Dimension group reference
                    try:
                        group_dims = self.dimension_groups.expand_dimension_reference(dim)
                        if isinstance(group_dims, list):
                            expanded.extend(group_dims)
                        else:
                            expanded.append(group_dims)
                    except Exception as e:
                        print(f"Warning: Error expanding dimension reference {dim}: {e}")
                        # Keep the reference as-is
                        expanded.append(dim)
                else:
                    # Regular dimension
                    expanded.append(dim)
                    
            elif isinstance(dim, list):
                # List of dimensions
                for d in dim:
                    expanded.extend(self._expand_dimensions([d]))
                    
        return expanded
        
    def _process_metric_time_dimensions(self, dimensions: List[Any]) -> List[Any]:
        """Process metric_time dimensions and expand them if needed"""
        processed = []
        
        # Ensure dimensions is a list
        if not isinstance(dimensions, list):
            if dimensions:
                dimensions = [dimensions]
            else:
                return []
        
        for dim in dimensions:
            if isinstance(dim, dict) and dim.get('name') == 'metric_time':
                # Special handling for metric_time dimension
                metric_time_dim = deepcopy(dim)
                
                # Ensure it has proper type
                if 'type' not in metric_time_dim:
                    metric_time_dim['type'] = 'time'
                    
                # Set default grain if not specified
                if 'grain' not in metric_time_dim:
                    metric_time_dim['grain'] = 'day'
                    
                # Mark as metric_time for special handling in semantic model
                metric_time_dim['is_metric_time'] = True
                
                processed.append(metric_time_dim)
                
                # If config says to auto-create other grains, add them
                if hasattr(self, 'config') and 'metric_time' in getattr(self.config, 'config', {}):
                    mt_config = self.config.config['metric_time']
                    if mt_config.get('auto_create') and 'grains_to_create' in mt_config:
                        base_expr = metric_time_dim.get('expr', 'date_column')
                        for grain in mt_config['grains_to_create']:
                            if grain != metric_time_dim['grain']:
                                grain_dim = {
                                    'name': f'metric_time_{grain}',
                                    'type': 'time',
                                    'grain': grain,
                                    'expr': f"DATE_TRUNC('{grain}', {base_expr})",
                                    'is_metric_time_grain': True
                                }
                                processed.append(grain_dim)
            else:
                processed.append(dim)
                
        return processed
        
    def _register_imported_dimension_groups(self):
        """Register dimension groups from imported files"""
        for alias, imported_data in self.parser.imports_cache.items():
            if self.config.debug:
                print(f"\n[DEBUG] Checking import '{alias}' for dimension groups")
                print(f"[DEBUG] Import keys: {list(imported_data.keys())}")
                
            if 'dimension_groups' in imported_data:
                dimension_groups = imported_data['dimension_groups']
                
                if self.config.debug:
                    print(f"[DEBUG] Found dimension_groups in '{alias}'")
                    print(f"[DEBUG] Type: {type(dimension_groups)}")
                    if isinstance(dimension_groups, dict):
                        print(f"[DEBUG] Keys: {list(dimension_groups.keys())}")
                
                # Handle both dict and list formats
                if isinstance(dimension_groups, dict):
                    # Standard format: {name: definition}
                    for name, group_def in dimension_groups.items():
                        # Handle case where group_def is just a list of dimensions
                        if isinstance(group_def, list):
                            # Convert list format to dict format
                            group_def = {
                                'name': name,
                                'dimensions': group_def,
                                'description': f'Dimension group: {name}'
                            }
                            if self.config.debug:
                                print(f"[DEBUG] Converted list format for dimension group '{name}'")
                        elif not isinstance(group_def, dict):
                            if self.config.debug:
                                print(f"[DEBUG] Skipping invalid dimension group '{name}' - not a dict or list")
                                print(f"[DEBUG] Type: {type(group_def)}")
                                print(f"[DEBUG] Value: {group_def}")
                            continue
                            
                        # Make a copy and adjust extends references if needed
                        adjusted_group = deepcopy(group_def)
                        if 'extends' in adjusted_group:
                            # Adjust extends to include alias prefix
                            adjusted_extends = []
                            for extend_ref in adjusted_group['extends']:
                                if '.' not in extend_ref:
                                    # It's a local reference within the same import
                                    adjusted_extends.append(f"{alias}.{extend_ref}")
                                else:
                                    adjusted_extends.append(extend_ref)
                            adjusted_group['extends'] = adjusted_extends
                        
                        # Register with alias prefix
                        full_name = f"{alias}.{name}"
                        if self.config.debug:
                            print(f"[DEBUG] Registering dimension group: {full_name}")
                            if 'dimensions' in adjusted_group:
                                print(f"[DEBUG] Number of dimensions: {len(adjusted_group['dimensions'])}")
                        self.dimension_groups.register_group(full_name, adjusted_group)
                        
                elif isinstance(dimension_groups, list):
                    # List format: [{name: ..., dimensions: ...}, ...]
                    for idx, group_def in enumerate(dimension_groups):
                        if not isinstance(group_def, dict):
                            if self.config.debug:
                                print(f"[DEBUG] Skipping invalid dimension group at index {idx} - not a dict")
                            continue
                            
                        # Extract name from the definition
                        name = group_def.get('name', f'group_{idx}')
                        
                        # Make a copy and adjust extends references if needed
                        adjusted_group = deepcopy(group_def)
                        if 'extends' in adjusted_group:
                            # Adjust extends to include alias prefix
                            adjusted_extends = []
                            for extend_ref in adjusted_group['extends']:
                                if '.' not in extend_ref:
                                    # It's a local reference within the same import
                                    adjusted_extends.append(f"{alias}.{extend_ref}")
                                else:
                                    adjusted_extends.append(extend_ref)
                            adjusted_group['extends'] = adjusted_extends
                        
                        # Register with alias prefix
                        full_name = f"{alias}.{name}"
                        if self.config.debug:
                            print(f"[DEBUG] Registering dimension group: {full_name}")
                            if 'dimensions' in adjusted_group:
                                print(f"[DEBUG] Number of dimensions: {len(adjusted_group['dimensions'])}")
                        self.dimension_groups.register_group(full_name, adjusted_group)
                    
    def _register_imported_templates(self):
        """Register templates from imported files"""
        for alias, imported_data in self.parser.imports_cache.items():
            if 'metric_templates' in imported_data:
                for name, template_def in imported_data['metric_templates'].items():
                    # Register with alias prefix
                    full_name = f"{alias}.{name}"
                    self.templates.engine.register_template(full_name, template_def)
                    
    def _resolve_references_in_group(self, group_def: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve any $ref references in a dimension group definition"""
        resolved = deepcopy(group_def)
        
        if 'dimensions' in resolved:
            expanded_dims = []
            for dim in resolved['dimensions']:
                if isinstance(dim, dict) and '$ref' in dim:
                    # Resolve the reference
                    ref_path = dim['$ref']
                    resolved_dims = self._resolve_dimension_reference(ref_path)
                    expanded_dims.extend(resolved_dims)
                else:
                    expanded_dims.append(dim)
            resolved['dimensions'] = expanded_dims
            
        return resolved
        
    def _resolve_dimension_reference(self, ref_path: str) -> List[Dict[str, Any]]:
        """Resolve a dimension reference like 'time.daily' or 'dims.customer_standard.customer_segment'"""
        try:
            # Try to resolve as a dimension group
            group_dims = self.dimension_groups.get_dimensions_for_group(ref_path)
            return group_dims
        except ValueError:
            # Not a direct group reference, might be a reference to imported content
            parts = ref_path.split('.')
            if len(parts) >= 2 and parts[0] in self.parser.imports_cache:
                # It's an import reference
                imported_data = self.parser.imports_cache[parts[0]]
                
                # Check if it's a dimension group reference
                if len(parts) == 2 and 'dimension_groups' in imported_data and parts[1] in imported_data['dimension_groups']:
                    group_def = imported_data['dimension_groups'][parts[1]]
                    # Return the dimensions from the group
                    if 'dimensions' in group_def:
                        return group_def['dimensions']
                
                # Check if it's a specific dimension within a group (e.g., dims.customer_standard.customer_segment)
                elif len(parts) == 3 and 'dimension_groups' in imported_data and parts[1] in imported_data['dimension_groups']:
                    group_def = imported_data['dimension_groups'][parts[1]]
                    if 'dimensions' in group_def:
                        # Find the specific dimension
                        for dim in group_def['dimensions']:
                            dim_name = dim.get('name') if isinstance(dim, dict) else dim
                            if dim_name == parts[2]:
                                return [dim]
            
            # If we can't resolve it, return it as-is
            return [{'$ref': ref_path}]
        
    def _generate_auto_variants(self, metric: Dict[str, Any]):
        """Generate automatic metric variants"""
        auto_config = metric.get('auto_variants', {})
        base_name = metric['name']
        
        # Check if metric belongs to a domain with specific settings
        domain = None
        if 'meta' in metric and 'domain' in metric['meta']:
            domain = metric['meta']['domain']
        elif '/' in str(self.current_file):
            # Try to extract domain from file path
            parts = str(self.current_file).split('/')
            if 'metrics' in parts:
                idx = parts.index('metrics')
                if idx + 1 < len(parts):
                    domain = parts[idx + 1]
        
        # Apply domain-specific auto-variants if configured
        if domain and domain in self.bdm_config.domain_settings:
            domain_config = self.bdm_config.domain_settings[domain]
            if 'auto_variants' in domain_config:
                # Merge domain auto-variants with metric auto-variants
                for variant_type, variant_config in domain_config['auto_variants'].items():
                    if variant_type not in auto_config:
                        auto_config[variant_type] = variant_config
        
        # Time comparison variants
        if 'time_comparison' in auto_config:
            for period in auto_config['time_comparison']:
                variant = deepcopy(metric)
                variant['name'] = f"{base_name}_{period}"
                variant['description'] = f"{metric['description']} - {period.upper()} comparison"
                variant['type'] = 'time_comparison'
                variant['comparison'] = {
                    'period': period,
                    'base_metric': base_name
                }
                self.compiled_metrics.append(variant)
                # Also add to metrics_by_source
                source = variant.get('source')
                if not source:
                    # For auto-variants, inherit source from parent metric
                    source = metric.get('source', 'derived')
                    variant['source'] = source
                if source not in self.metrics_by_source:
                    self.metrics_by_source[source] = []
                self.metrics_by_source[source].append(variant)
                
        # By-dimension variants
        if 'by_dimension' in auto_config:
            for dim in auto_config['by_dimension']:
                variant = deepcopy(metric)
                variant['name'] = f"{base_name}_by_{dim}"
                variant['description'] = f"{metric['description']} by {dim}"
                # Add the dimension if not already present
                if 'dimensions' not in variant:
                    variant['dimensions'] = []
                if not any((d.get('name') if isinstance(d, dict) else d) == dim for d in variant['dimensions']):
                    variant['dimensions'].append({'name': dim})
                self.compiled_metrics.append(variant)
                # Also add to metrics_by_source
                source = variant.get('source')
                if not source:
                    # For auto-variants, inherit source from parent metric
                    source = metric.get('source', 'derived')
                    variant['source'] = source
                if source not in self.metrics_by_source:
                    self.metrics_by_source[source] = []
                self.metrics_by_source[source].append(variant)
                
        # Custom variants with multiple dimensions or filters
        # Handle all other auto_variant types as custom variants
        for variant_type, variant_configs in auto_config.items():
            if variant_type in ['time_comparison', 'by_dimension']:
                continue  # Already handled above
                
            # Check if it's a list of variant configurations
            if isinstance(variant_configs, list):
                for idx, variant_config in enumerate(variant_configs):
                    if not isinstance(variant_config, dict):
                        continue
                        
                    variant = deepcopy(metric)
                    
                    # Generate variant name
                    if 'name_suffix' in variant_config:
                        variant['name'] = f"{base_name}{variant_config['name_suffix']}"
                    elif 'label_suffix' in variant_config:
                        variant['name'] = f"{base_name}{variant_config['label_suffix']}"
                    else:
                        # Auto-generate suffix from variant type and index
                        variant['name'] = f"{base_name}_{variant_type}_{idx}"
                    
                    # Update description
                    if 'description_suffix' in variant_config:
                        variant['description'] = f"{metric['description']} {variant_config['description_suffix']}"
                    else:
                        variant['description'] = f"{metric['description']} - {variant_type} variant"
                    
                    # Add dimensions if specified
                    if 'dimensions' in variant_config:
                        if 'dimensions' not in variant:
                            variant['dimensions'] = []
                        
                        # Handle both list and dict formats
                        new_dims = variant_config['dimensions']
                        if isinstance(new_dims, list):
                            for dim in new_dims:
                                # Check if dimension already exists
                                if isinstance(dim, str):
                                    dim_name = dim
                                else:
                                    dim_name = dim.get('name', '')
                                    
                                if not any((d.get('name') if isinstance(d, dict) else d) == dim_name for d in variant['dimensions']):
                                    variant['dimensions'].append(dim)
                    
                    # Add filters if specified
                    if 'filter' in variant_config:
                        # Combine with existing filter if present
                        if 'filter' in variant:
                            variant['filter'] = f"({variant['filter']}) AND ({variant_config['filter']})"
                        else:
                            variant['filter'] = variant_config['filter']
                    
                    # Handle filter as key-value pairs (e.g., shop_code: shopify)
                    filter_parts = []
                    for key, value in variant_config.items():
                        if key not in ['name_suffix', 'label_suffix', 'description_suffix', 
                                      'dimensions', 'filter', 'name', 'description']:
                            # These are filter conditions
                            if isinstance(value, str):
                                filter_parts.append(f"{key} = '{value}'")
                            else:
                                filter_parts.append(f"{key} = {value}")
                    
                    if filter_parts:
                        filter_str = " AND ".join(filter_parts)
                        if 'filter' in variant:
                            variant['filter'] = f"({variant['filter']}) AND ({filter_str})"
                        else:
                            variant['filter'] = filter_str
                    
                    # Add any other custom fields
                    for key, value in variant_config.items():
                        if key not in ['name_suffix', 'label_suffix', 'description_suffix', 
                                      'dimensions', 'filter'] and key not in variant:
                            variant[key] = value
                    
                    self.compiled_metrics.append(variant)
                    # Also add to metrics_by_source
                    source = variant.get('source')
                    if not source:
                        # For auto-variants, inherit source from parent metric
                        source = metric.get('source', 'derived')
                        variant['source'] = source
                    if source not in self.metrics_by_source:
                        self.metrics_by_source[source] = []
                    self.metrics_by_source[source].append(variant)
                
    def _generate_output(self) -> Dict[str, Any]:
        """Generate the final dbt output"""
        try:
            # Generate semantic models from metrics grouped by source
            self._generate_semantic_models()
            
            # Generate metric definitions
            dbt_metrics = []
            for metric in self.compiled_metrics:
                try:
                    dbt_metric = self._to_dbt_metric(metric)
                    dbt_metrics.append(dbt_metric)
                except Exception as e:
                    if self.config.debug:
                        print(f"\n[DEBUG] Error converting metric to dbt format: {metric.get('name', 'unknown')}")
                        print(f"[DEBUG] Error: {e}")
                    raise
            
            return {
                'version': 2,
                'semantic_models': self.semantic_models,
                'metrics': dbt_metrics
            }
        except Exception as e:
            if self.config.debug:
                print(f"\n[DEBUG] Error in _generate_output:")
                print(f"[DEBUG] Error type: {type(e).__name__}")
                print(f"[DEBUG] Error: {e}")
                import traceback
                traceback.print_exc()
            raise
        
    def _generate_semantic_models(self):
        """Generate dbt semantic models from compiled metrics"""
        # Check if any semantic models were explicitly defined
        if 'semantic_models' in self.parser.current_data:
            for sm_def in self.parser.current_data['semantic_models']:
                self._process_semantic_model_definition(sm_def)
        
        # Generate semantic models from metrics grouped by source
        for source, metrics in self.metrics_by_source.items():
            # Skip if a semantic model was already explicitly defined for this source
            if any(sm['name'] == f"sem_{source}" for sm in self.semantic_models):
                continue
                
            # Collect all dimensions and measures
            all_dimensions = []
            all_measures = []
            dimension_names = set()
            measure_names = set()
            
            for metric in metrics:
                # Add dimensions
                dimensions = metric.get('dimensions', [])
                if not isinstance(dimensions, list):
                    continue
                    
                for dim in dimensions:
                    # Skip unresolved references
                    if isinstance(dim, dict) and '$ref' in dim:
                        continue
                    dim_name = dim.get('name') if isinstance(dim, dict) else dim
                    if dim_name and dim_name not in dimension_names:
                        dimension_names.add(dim_name)
                        all_dimensions.append(self._to_dbt_dimension(dim))
                        
                # Add measures
                if 'measure' in metric:
                    measure_name = f"{metric['name']}_measure"
                    if measure_name not in measure_names:
                        measure_names.add(measure_name)
                        all_measures.append(self._to_dbt_measure(metric['measure'], measure_name))
                        
                # Add measures for ratio metrics
                if metric['type'] == 'ratio' and 'numerator' in metric and 'denominator' in metric:
                    try:
                        # Numerator measure
                        num_measure_name = f"{metric['name']}_numerator"
                        if num_measure_name not in measure_names:
                            measure_names.add(num_measure_name)
                            # Check if numerator is a dict with measure key
                            if isinstance(metric['numerator'], dict) and 'measure' in metric['numerator']:
                                all_measures.append(self._to_dbt_measure(metric['numerator']['measure'], num_measure_name))
                            else:
                                # Handle simple format or missing measure
                                if self.config.debug:
                                    print(f"[DEBUG] Skipping numerator measure for {metric['name']} - invalid format")
                                    print(f"[DEBUG] Numerator type: {type(metric['numerator'])}")
                                    print(f"[DEBUG] Numerator value: {metric['numerator']}")
                        
                        # Denominator measure
                        den_measure_name = f"{metric['name']}_denominator"
                        if den_measure_name not in measure_names:
                            measure_names.add(den_measure_name)
                            # Check if denominator is a dict with measure key
                            if isinstance(metric['denominator'], dict) and 'measure' in metric['denominator']:
                                all_measures.append(self._to_dbt_measure(metric['denominator']['measure'], den_measure_name))
                            else:
                                # Handle simple format or missing measure
                                if self.config.debug:
                                    print(f"[DEBUG] Skipping denominator measure for {metric['name']} - invalid format")
                                    print(f"[DEBUG] Denominator type: {type(metric['denominator'])}")
                                    print(f"[DEBUG] Denominator value: {metric['denominator']}")
                    except Exception as e:
                        if self.config.debug:
                            print(f"[DEBUG] Error processing ratio measures for {metric['name']}: {e}")
                            import traceback
                            traceback.print_exc()
                        
            # Create semantic model
            # Handle special cases where source is a composite identifier
            if source.startswith('ratio_') or source.startswith('conversion_'):
                # For ratio/conversion metrics with different sources, we need to handle differently
                # Check if this is really a composite source by looking at the metrics
                is_composite = False
                actual_sources = set()
                
                for metric in metrics:
                    if metric.get('type') == 'ratio':
                        num_source = metric.get('numerator', {}).get('source')
                        den_source = metric.get('denominator', {}).get('source')
                        if num_source and den_source and num_source != den_source:
                            is_composite = True
                            actual_sources.add(num_source)
                            actual_sources.add(den_source)
                    elif metric.get('type') == 'conversion':
                        base_source = metric.get('base_measure', {}).get('source')
                        conv_source = metric.get('conversion_measure', {}).get('source')
                        if base_source and conv_source and base_source != conv_source:
                            is_composite = True
                            actual_sources.add(base_source)
                            actual_sources.add(conv_source)
                
                if is_composite and actual_sources:
                    # For composite sources, we should not create a semantic model
                    # Instead, the measures should be added to their respective source semantic models
                    # Skip creating this semantic model
                    if self.config.debug:
                        print(f"[DEBUG] Skipping composite semantic model for {source}")
                        print(f"[DEBUG] Actual sources: {actual_sources}")
                    
                    # Distribute measures to their actual source semantic models
                    for metric in metrics:
                        if metric.get('type') == 'ratio':
                            # Add numerator measures to numerator source
                            num_source = metric.get('numerator', {}).get('source')
                            if num_source:
                                if num_source not in self.metrics_by_source:
                                    self.metrics_by_source[num_source] = []
                                # Create a simplified metric for the numerator
                                num_metric = {
                                    'name': f"{metric['name']}_num_only",
                                    'type': 'simple',
                                    'source': num_source,
                                    'measure': metric['numerator'].get('measure', {}),
                                    'dimensions': metric.get('dimensions', [])
                                }
                                self.metrics_by_source[num_source].append(num_metric)
                            
                            # Add denominator measures to denominator source
                            den_source = metric.get('denominator', {}).get('source')
                            if den_source:
                                if den_source not in self.metrics_by_source:
                                    self.metrics_by_source[den_source] = []
                                # Create a simplified metric for the denominator
                                den_metric = {
                                    'name': f"{metric['name']}_den_only",
                                    'type': 'simple',
                                    'source': den_source,
                                    'measure': metric['denominator'].get('measure', {}),
                                    'dimensions': metric.get('dimensions', [])
                                }
                                self.metrics_by_source[den_source].append(den_metric)
                    
                    # Skip creating the composite semantic model
                    continue
                else:
                    # Not actually a composite, use the source as-is
                    model_ref = f"ref('{source}')"
            else:
                # Normal source
                model_ref = f"ref('{source}')"
            
            semantic_model = {
                'name': f"sem_{source}",
                'model': model_ref,
                'description': f"Semantic model for {source}",
                'dimensions': all_dimensions,
                'measures': all_measures,
                'entities': self._extract_entities(source, metrics)
            }
            
            # Add time spine configurations if any metrics use them
            time_spine_configs = self._extract_time_spine_configs(metrics)
            if time_spine_configs:
                semantic_model['time_spine_table_configurations'] = time_spine_configs
            
            # Add join configurations if any are relevant
            joins = self._extract_relevant_joins(source, metrics)
            if joins:
                semantic_model['joins'] = joins
            
            self.semantic_models.append(semantic_model)
            
    def _process_semantic_model_definition(self, sm_def: Dict[str, Any]):
        """Process an explicitly defined semantic model"""
        semantic_model = {
            'name': sm_def['name'],
            'model': f"ref('{sm_def.get('source', sm_def['name'])}')",
            'description': sm_def.get('description', f"Semantic model for {sm_def['name']}"),
        }
        
        # Handle entity sets
        if 'entity_set' in sm_def and sm_def['entity_set'] in self.entity_sets:
            entity_set = self.entity_sets[sm_def['entity_set']]
            # Build entities from entity set
            entities = []
            primary = entity_set['primary_entity']
            if primary in self.entities:
                primary_def = self.entities[primary]
                entities.append({
                    'name': primary,
                    'type': 'primary',
                    'expr': primary_def.get('column', primary)
                })
                
                # Add included entities
                for include in entity_set.get('includes', []):
                    entity_name = include['entity']
                    if entity_name in self.entities:
                        entity_def = self.entities[entity_name]
                        # Find the foreign key relationship
                        for rel in entity_def.get('relationships', []):
                            if rel['to_entity'] == primary or (include.get('through') and rel['to_entity'] == include['through']):
                                entities.append({
                                    'name': rel.get('foreign_key', f"{rel['to_entity']}_id"),
                                    'type': 'foreign',
                                    'expr': rel.get('foreign_key', f"{rel['to_entity']}_id")
                                })
                                break
            semantic_model['entities'] = entities
        
        # Handle explicit entities
        elif 'entities' in sm_def:
            entities = []
            for entity in sm_def['entities']:
                entity_dict = {
                    'name': entity['name'],
                    'type': entity.get('type', 'primary'),
                    'expr': entity.get('expr', entity['name'])
                }
                # Add relationship info if present
                if 'relationship' in entity:
                    rel = entity['relationship']
                    # Store relationship info in metadata (dbt may use this for join paths)
                    if 'meta' not in entity_dict:
                        entity_dict['meta'] = {}
                    entity_dict['meta']['relationship'] = {
                        'to_entity': rel['to_entity'],
                        'type': rel['type']
                    }
                entities.append(entity_dict)
            semantic_model['entities'] = entities
        
        # Copy other fields
        for field in ['dimensions', 'measures', 'meta', 'config', 'time_spine_table_configurations', 'primary_time_dimension', 'joins']:
            if field in sm_def:
                semantic_model[field] = sm_def[field]
        
        self.semantic_models.append(semantic_model)
            
    def _to_dbt_dimension(self, dim: Dict[str, Any]) -> Dict[str, Any]:
        """Convert dimension to dbt format"""
        # Handle metric_time specially
        if dim.get('name') == 'metric_time' or dim.get('is_metric_time'):
            dbt_dim = {
                'name': 'metric_time',
                'type': 'time',
                'type_params': {
                    'time_granularity': dim.get('grain', 'day')
                }
            }
            # Add expression if provided
            if 'expr' in dim:
                dbt_dim['expr'] = dim['expr']
            # Add label
            dbt_dim['label'] = 'Metric Time'
            
            # Mark as primary time dimension if it's the base metric_time
            if dim.get('is_metric_time') and not dim.get('is_metric_time_grain'):
                dbt_dim['is_primary_time'] = True
                
        elif dim.get('is_metric_time_grain'):
            # Handle auto-generated metric_time grains
            dbt_dim = {
                'name': dim['name'],
                'type': 'time',
                'type_params': {
                    'time_granularity': dim['grain']
                },
                'expr': dim['expr'],
                'label': f"Metric Time ({dim['grain'].title()})"
            }
        else:
            # Regular dimension handling
            dbt_dim = {
                'name': dim['name'],
                'type': dim.get('type', 'categorical')
            }
            
            if dim.get('type') == 'time':
                dbt_dim['type_params'] = {
                    'time_granularity': dim.get('grain', 'day')
                }
                
            if 'expr' in dim:
                dbt_dim['expr'] = dim['expr']
            elif 'source' in dim:
                dbt_dim['expr'] = dim['source']
                
            if 'label' in dim:
                dbt_dim['label'] = dim['label']
            
        return dbt_dim
        
    def _extract_entities(self, source: str, metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract entities from metrics for a semantic model"""
        entities = []
        entity_names = set()
        
        # Common entity patterns
        default_entities = {
            'user': ['user_id', 'customer_id', 'visitor_id'],
            'order': ['order_id', 'transaction_id'],
            'product': ['product_id', 'sku', 'item_id'],
            'session': ['session_id', 'visit_id']
        }
        
        # First, check if there are pre-defined entities
        primary_entities = []
        for metric in metrics:
            if 'entity' in metric:
                entity_name = metric['entity']
                if entity_name in self.entities:
                    # Use the pre-defined entity
                    entity_def = self.entities[entity_name]
                    if entity_name not in entity_names:
                        entity_names.add(entity_name)
                        entity_dict = {
                            'name': entity_name,
                            'type': entity_def.get('type', 'primary'),
                            'expr': entity_def.get('column', entity_name)
                        }
                        entities.append(entity_dict)
                        primary_entities.append(entity_name)
                        
                        # Add related entities through relationships
                        if 'relationships' in entity_def:
                            for rel in entity_def['relationships']:
                                related_entity = rel['to_entity']
                                if related_entity in self.entities and related_entity not in entity_names:
                                    entity_names.add(related_entity)
                                    related_def = self.entities[related_entity]
                                    entities.append({
                                        'name': rel.get('foreign_key', f"{related_entity}_id"),
                                        'type': 'foreign',
                                        'expr': rel.get('foreign_key', f"{related_entity}_id")
                                    })
                else:
                    # Entity not pre-defined, use default behavior
                    if entity_name not in entity_names:
                        entity_names.add(entity_name)
                        entities.append({
                            'name': entity_name,
                            'type': 'primary',
                            'expr': entity_name
                        })
            
            # Also check conversion metrics
            if metric['type'] == 'conversion' and 'entity' in metric.get('type_params', {}):
                entity_name = metric['type_params']['entity']
                if entity_name not in entity_names:
                    entity_names.add(entity_name)
                    if entity_name in self.entities:
                        entity_def = self.entities[entity_name]
                        entities.append({
                            'name': entity_name,
                            'type': entity_def.get('type', 'primary'),
                            'expr': entity_def.get('column', entity_name)
                        })
                    else:
                        entities.append({
                            'name': entity_name,
                            'type': 'primary',
                            'expr': entity_name
                        })
        
        # If no explicit entities, try to infer from common patterns
        if not entities:
            for entity_type, id_columns in default_entities.items():
                for id_col in id_columns:
                    # Check if any dimension uses this column
                    for metric in metrics:
                        dimensions = metric.get('dimensions', [])
                        if not isinstance(dimensions, list):
                            continue
                        for dim in dimensions:
                            if isinstance(dim, dict) and (dim.get('name') == id_col or dim.get('expr', '').lower() == id_col.lower()):
                                if id_col not in entity_names:
                                    entity_names.add(id_col)
                                    entities.append({
                                        'name': id_col,
                                        'type': 'primary',
                                        'expr': id_col
                                    })
                                break
        
        # Default to a generic entity if none found
        if not entities:
            entities.append({
                'name': 'id',
                'type': 'primary', 
                'expr': f"{source}_id"  # Assume table has an id column
            })
            
        return entities
        
    def _extract_time_spine_configs(self, metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract time spine configurations from metrics"""
        time_spine_configs = []
        seen_spines = set()
        
        for metric in metrics:
            if 'time_spine' in metric:
                spine_ref = metric['time_spine']
                
                # Handle inline time spine definition
                if isinstance(spine_ref, dict):
                    config = {
                        'location': spine_ref['model'],
                        'column_name': spine_ref.get('columns', {}).get('date_day', 'date_day'),
                        'grain': 'day'  # Default grain
                    }
                    if 'meta' in spine_ref:
                        config['meta'] = spine_ref['meta']
                    time_spine_configs.append(config)
                    
                # Handle reference to pre-defined time spine
                elif spine_ref in self.time_spines:
                    if spine_ref not in seen_spines:
                        seen_spines.add(spine_ref)
                        spine_def = self.time_spines[spine_ref]
                        
                        # Create configuration for each grain in the spine
                        for grain, column in spine_def.get('columns', {}).items():
                            # Extract grain from column name (e.g., date_day -> day)
                            grain_type = grain.split('_')[-1] if '_' in grain else grain
                            
                            config = {
                                'location': spine_def['model'],
                                'column_name': column,
                                'grain': grain_type
                            }
                            if 'meta' in spine_def:
                                config['meta'] = spine_def['meta']
                            time_spine_configs.append(config)
                            
            # Also check time dimensions for implicit time spine needs
            elif 'dimensions' in metric:
                dimensions = metric.get('dimensions', [])
                if isinstance(dimensions, list):
                    for dim in dimensions:
                        if isinstance(dim, dict) and dim.get('type') == 'time':
                            # If metric has time dimensions but no explicit spine, 
                            # check if default spine exists
                            if 'default' in self.time_spines and 'default' not in seen_spines:
                                seen_spines.add('default')
                                default_spine = self.time_spines['default']
                                
                                for grain, column in default_spine.get('columns', {}).items():
                                    grain_type = grain.split('_')[-1] if '_' in grain else grain
                                    config = {
                                        'location': default_spine['model'],
                                        'column_name': column,
                                        'grain': grain_type
                                    }
                                if 'meta' in default_spine:
                                    config['meta'] = default_spine['meta']
                                time_spine_configs.append(config)
                        break
        
        return time_spine_configs
        
    def _extract_relevant_joins(self, source: str, metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract join configurations relevant to the metrics"""
        joins = []
        seen_joins = set()
        
        # Collect all dimension sources referenced in metrics
        dimension_sources = set()
        for metric in metrics:
            # Check dimensions for source references
            for dim in metric.get('dimensions', []):
                if isinstance(dim, dict) and 'source' in dim:
                    dimension_sources.add(dim['source'])
                    
            # Check if metric uses join paths
            if 'join_paths' in metric:
                # Expand join path aliases
                for path_ref in metric['join_paths']:
                    if path_ref in self.join_path_aliases:
                        alias_def = self.join_path_aliases[path_ref]
                        for path in alias_def.get('paths', []):
                            if 'to' in path:
                                dimension_sources.add(path['to'])
        
        # Find join paths that connect source to dimension sources
        for join_path in self.join_paths:
            if join_path.get('from') == source and join_path.get('to') in dimension_sources:
                join_key = f"{join_path['from']}->{join_path['to']}"
                if join_key not in seen_joins:
                    seen_joins.add(join_key)
                    
                    # Convert to dbt join format
                    dbt_join = {
                        'name': join_path['to'],
                        'type': join_path.get('join_type', 'left')
                    }
                    
                    # Build SQL ON clause from join keys
                    on_conditions = []
                    for key in join_path.get('join_keys', []):
                        on_conditions.append(
                            f"${{{{ {source} }}}}.{key['from_column']} = ${{{{ {join_path['to']} }}}}.{key['to_column']}"
                        )
                    
                    # Add any additional join conditions
                    if 'join_conditions' in join_path:
                        on_conditions.extend(join_path['join_conditions'])
                    
                    if on_conditions:
                        dbt_join['sql_on'] = ' AND '.join(on_conditions)
                    
                    joins.append(dbt_join)
                    
            # Handle multi-hop joins
            elif 'through' in join_path and join_path.get('from') == source:
                # This is a multi-hop join, need to expand it
                if 'join_path' in join_path:
                    for path_segment in join_path['join_path']:
                        segment_key = f"{path_segment.get('from', '')}->{path_segment.get('to', '')}"
                        if segment_key not in seen_joins and path_segment.get('to') in dimension_sources:
                            seen_joins.add(segment_key)
                            
                            dbt_join = {
                                'name': path_segment['to'],
                                'type': path_segment.get('join_type', 'left')
                            }
                            
                            on_conditions = []
                            for key in path_segment.get('join_keys', []):
                                on_conditions.append(
                                    f"${{{{ {path_segment.get('from', source)} }}}}.{key['from_column']} = ${{{{ {path_segment['to']} }}}}.{key['to_column']}"
                                )
                            
                            if on_conditions:
                                dbt_join['sql_on'] = ' AND '.join(on_conditions)
                            
                            joins.append(dbt_join)
        
        return joins
        
    def _to_dbt_measure(self, measure: Dict[str, Any], name: str) -> Dict[str, Any]:
        """Convert measure to dbt format"""
        # Map common measure types to dbt aggregations
        agg_type_mapping = {
            'sum': 'sum',
            'average': 'average',
            'avg': 'average',
            'count': 'count',
            'count_distinct': 'count_distinct',
            'min': 'min',
            'max': 'max',
            'median': 'median',
            'percentile': 'percentile',
            'sum_boolean': 'sum_boolean',
            'stddev': 'stddev',
            'variance': 'variance',
            'last_value': 'max',  # Map last_value to max for now
            'first_value': 'min',  # Map first_value to min for now
            'window': 'sum'  # Window functions need special handling
        }
        
        measure_type = measure.get('type', 'sum')
        
        # Handle window functions specially
        if measure_type == 'window':
            return self._handle_window_measure(measure, name)
        
        dbt_agg = agg_type_mapping.get(measure_type, measure_type)
        
        dbt_measure = {
            'name': name,
            'agg': dbt_agg,
            'expr': measure.get('column', measure.get('expr', name))
        }
        
        # Handle filters
        if 'filters' in measure:
            where_clause = ' AND '.join(measure['filters'])
            dbt_measure['agg_params'] = {'where': where_clause}
            
            # Extract metric references from filters
            metric_refs = []
            for filter_expr in measure['filters']:
                refs = self._extract_metric_refs(filter_expr)
                metric_refs.extend(refs)
            
            if metric_refs:
                # Store metric references in agg_params
                dbt_measure['agg_params']['metric_refs'] = metric_refs
            
        # Handle percentile params
        if dbt_agg == 'percentile':
            percentile_value = measure.get('percentile', measure.get('percentile_value', 0.5))
            if 'agg_params' not in dbt_measure:
                dbt_measure['agg_params'] = {}
            dbt_measure['agg_params']['percentile'] = percentile_value
            
            # Also merge any existing agg_params
            if 'agg_params' in measure:
                dbt_measure['agg_params'].update(measure['agg_params'])
            
        return dbt_measure
        
    def _handle_window_measure(self, measure: Dict[str, Any], name: str) -> Dict[str, Any]:
        """Handle window function measures"""
        window_function = measure.get('window_function', '')
        column = measure.get('column', measure.get('expr', 'value'))
        
        # Replace {{ column }} placeholder with actual column
        window_expr = window_function.replace('{{ column }}', column)
        
        # Get the aggregation to apply after window function (if any)
        post_aggregation = measure.get('aggregation', 'sum')
        
        dbt_measure = {
            'name': name,
            'agg': post_aggregation,
            'expr': window_expr
        }
        
        # Handle filters if present
        if 'filters' in measure:
            where_clause = ' AND '.join(measure['filters'])
            dbt_measure['agg_params'] = {'where': where_clause}
        
        # Add window function metadata
        if 'agg_params' not in dbt_measure:
            dbt_measure['agg_params'] = {}
        dbt_measure['agg_params']['is_window_function'] = True
        
        # Add any window-specific parameters
        if 'null_treatment' in measure:
            dbt_measure['agg_params']['null_treatment'] = measure['null_treatment']
            
        return dbt_measure
        
    def _extract_metric_refs(self, expression: str) -> List[str]:
        """Extract metric references from an expression"""
        import re
        # Look for patterns like metric('metric_name') or {{metric('metric_name')}}
        pattern = r"metric\s*\(\s*['\"]([^'\"]+)['\"]\s*\)"
        return re.findall(pattern, expression)
        
    def _to_dbt_metric(self, metric: Dict[str, Any]) -> Dict[str, Any]:
        """Convert compiled metric to dbt metric format"""
        try:
            dbt_metric = {
                'name': metric['name'],
                'description': metric['description'],
                'type': metric['type'],
                'label': metric.get('label', metric['name'])
            }
        except (KeyError, TypeError) as e:
            if self.config.debug:
                print(f"\n[DEBUG] Error in _to_dbt_metric for metric: {metric}")
                print(f"[DEBUG] Metric type: {type(metric)}")
                print(f"[DEBUG] Error: {e}")
                import traceback
                traceback.print_exc()
            raise TypeError(f"Invalid metric structure: {e}")
        
        # Add type-specific parameters
        if metric['type'] == 'simple':
            dbt_metric['type_params'] = {
                'measure': f"{metric['name']}_measure"
            }
        elif metric['type'] == 'ratio':
            # Handle ratio metrics
            if 'numerator' in metric and 'denominator' in metric:
                # Create measures for numerator and denominator
                num_measure_name = f"{metric['name']}_numerator"
                den_measure_name = f"{metric['name']}_denominator"
                
                dbt_metric['type_params'] = {
                    'numerator': {
                        'name': num_measure_name,
                        'filter': metric['numerator'].get('filter')
                    },
                    'denominator': {
                        'name': den_measure_name,
                        'filter': metric['denominator'].get('filter')
                    }
                }
        elif metric['type'] == 'derived':
            # Handle derived metrics
            if 'expression' in metric or 'formula' in metric:
                dbt_metric['type_params'] = {
                    'expr': metric.get('expression', metric.get('formula')),
                    'metrics': self._extract_metric_refs(metric.get('expression', metric.get('formula', '')))
                }
        elif metric['type'] == 'cumulative':
            # Handle cumulative metrics
            dbt_metric['type_params'] = {
                'measure': f"{metric['name']}_measure",
                'cumulative_type_params': {
                    'window': metric.get('window', 'unbounded'),
                    'grain_to_date': metric.get('grain_to_date', 'month')
                }
            }
            
            # Handle offset windows
            if 'offsets' in metric or 'offset_pattern' in metric:
                offset_configs = []
                
                # Handle offset pattern first
                if 'offset_pattern' in metric and metric['offset_pattern'] in self.offset_patterns:
                    pattern_offsets = self.offset_patterns[metric['offset_pattern']]
                    offset_configs.extend(pattern_offsets)
                
                # Handle explicit offsets (can override pattern)
                if 'offsets' in metric:
                    offset_configs.extend(metric['offsets'])
                
                # Process offset configurations
                dbt_offset_windows = []
                for offset in offset_configs:
                    offset_window = {
                        'period': offset['period'],
                        'offset': offset['offset'],
                        'alias': offset.get('alias', f"{offset['period']}_{abs(offset['offset'])}_ago")
                    }
                    
                    # Add optional fields
                    if 'calculation' in offset:
                        offset_window['calculation'] = offset['calculation']
                    if 'calculation_alias' in offset:
                        offset_window['calculation_alias'] = offset['calculation_alias']
                    if 'calculations' in offset:
                        offset_window['calculations'] = offset['calculations']
                    if 'inherit_filters' in offset:
                        offset_window['inherit_filters'] = offset['inherit_filters']
                        
                    dbt_offset_windows.append(offset_window)
                
                # Add to cumulative type params
                dbt_metric['type_params']['cumulative_type_params']['offset_windows'] = dbt_offset_windows
                
            # Handle window type
            if 'window_type' in metric:
                dbt_metric['type_params']['cumulative_type_params']['window_type'] = metric['window_type']
        elif metric['type'] == 'conversion':
            # Handle conversion metrics
            if 'base_measure' in metric and 'conversion_measure' in metric:
                dbt_metric['type_params'] = {
                    'base_measure': {
                        'name': f"{metric['name']}_base_measure",
                        'filter': metric['base_measure'].get('filter')
                    },
                    'conversion_measure': {
                        'name': f"{metric['name']}_conversion_measure", 
                        'filter': metric['conversion_measure'].get('filter')
                    },
                    'entity': metric.get('entity', 'user_id'),
                    'window': metric.get('window', '7 days')
                }
            
        # Add optional fields
        for field in ['filter', 'meta', 'config']:
            if field in metric:
                dbt_metric[field] = metric[field]
                
        # Handle fill_nulls_with
        if 'fill_nulls_with' in metric:
            # Add to config
            if 'config' not in dbt_metric:
                dbt_metric['config'] = {}
            dbt_metric['config']['fill_nulls_with'] = metric['fill_nulls_with']
            
        # Handle time_spine
        if 'time_spine' in metric:
            # Add to config
            if 'config' not in dbt_metric:
                dbt_metric['config'] = {}
            dbt_metric['config']['time_spine'] = metric['time_spine']
                
        # Extract metric references from filter if present
        if 'filter' in dbt_metric:
            filter_metric_refs = self._extract_metric_refs(dbt_metric['filter'])
            if filter_metric_refs:
                # Add metric references to metadata
                if 'meta' not in dbt_metric:
                    dbt_metric['meta'] = {}
                dbt_metric['meta']['metric_refs_in_filter'] = filter_metric_refs
                
                # Add to type_params metrics list if not already there
                if 'type_params' in dbt_metric and 'metrics' in dbt_metric['type_params']:
                    existing_refs = dbt_metric['type_params']['metrics']
                    for ref in filter_metric_refs:
                        if ref not in existing_refs:
                            existing_refs.append(ref)
                elif 'type_params' in dbt_metric:
                    dbt_metric['type_params']['metrics'] = filter_metric_refs
        
        # Add source_ref to meta if it exists
        if 'source_ref' in metric:
            if 'meta' not in dbt_metric:
                dbt_metric['meta'] = {}
            dbt_metric['meta']['source_ref'] = metric['source_ref']
        
        # Add general meta from the original metric
        if 'meta' in metric:
            if 'meta' not in dbt_metric:
                dbt_metric['meta'] = {}
            # Merge meta fields
            for key, value in metric['meta'].items():
                if key not in dbt_metric['meta']:  # Don't overwrite existing meta
                    dbt_metric['meta'][key] = value
                
        return dbt_metric
        
    def _write_split_output(self, output_data: Dict[str, Any]):
        """Write output to separate files"""
        try:
            output_path = Path(self.config.output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            written_files = []
            
            # Write semantic models
            for model in output_data['semantic_models']:
                file_path = output_path / f"{model['name']}.yml"
                try:
                    with open(file_path, 'w') as f:
                        yaml.dump({'semantic_models': [model]}, f, default_flow_style=False)
                    written_files.append(file_path)
                except IOError as e:
                    raise IOError(f"Failed to write semantic model {model['name']}: {e}")
                    
            # Write metrics
            metrics_file = output_path / "_metrics.yml"
            try:
                with open(metrics_file, 'w') as f:
                    yaml.dump({'metrics': output_data['metrics']}, f, default_flow_style=False)
                written_files.append(metrics_file)
            except IOError as e:
                raise IOError(f"Failed to write metrics file: {e}")
                
            return written_files
            
        except Exception as e:
            raise RuntimeError(f"Error writing output files: {e}")
            
    def _write_single_output(self, output_data: Dict[str, Any]):
        """Write all output to a single file"""
        try:
            output_path = Path(self.config.output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            output_file = output_path / "compiled_semantic_models.yml"
            try:
                with open(output_file, 'w') as f:
                    yaml.dump(output_data, f, default_flow_style=False, sort_keys=False)
                return [output_file]
            except IOError as e:
                raise IOError(f"Failed to write output file: {e}")
                
        except Exception as e:
            raise RuntimeError(f"Error writing output file: {e}")
    
    def _validate_metric_models(self, metric: Dict[str, Any]) -> bool:
        """
        Validate that all model references in a metric are valid.
        Returns True if valid, False if invalid.
        """
        try:
            from validation.dbt_scanner import DBTProjectScanner
            
            # Initialize scanner if not already done
            if not hasattr(self, '_model_scanner'):
                self._model_scanner = DBTProjectScanner(str(self.parser.base_dir))
            
            # Check main source reference
            source = metric.get('source')
            if source and source != 'derived':
                is_valid, _ = self._model_scanner.validate_model_reference(source)
                if not is_valid:
                    return False
            
            # Check ratio metric numerator/denominator sources
            if metric.get('type') == 'ratio':
                for component in ['numerator', 'denominator']:
                    if component in metric and isinstance(metric[component], dict):
                        comp_source = metric[component].get('source')
                        if comp_source and comp_source != 'derived':
                            is_valid, _ = self._model_scanner.validate_model_reference(comp_source)
                            if not is_valid:
                                return False
            
            # Check dimensions with source references
            for dim in metric.get('dimensions', []):
                if isinstance(dim, dict) and 'source' in dim:
                    dim_source = dim['source']
                    if dim_source and dim_source != 'derived':
                        is_valid, _ = self._model_scanner.validate_model_reference(dim_source)
                        if not is_valid:
                            return False
            
            return True
            
        except Exception as e:
            if self.config.debug:
                print(f"[DEBUG] Error validating metric models: {e}")
            # If validation fails due to error, allow compilation to continue
            return True