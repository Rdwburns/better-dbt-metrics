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
from features.auto_inference import AutoInferenceEngine, InferenceConfig, ColumnInfo
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
        
        # Initialize auto-inference engine
        inference_config = InferenceConfig()
        
        # Apply any inference settings from BDM config
        if hasattr(self.bdm_config, 'auto_inference') and self.bdm_config.auto_inference:
            inference_settings = self.bdm_config.auto_inference
            
            # Update enabled flag
            if 'enabled' in inference_settings:
                inference_config.enabled = inference_settings['enabled']
            
            # Update pattern configurations
            if 'time_dimension_patterns' in inference_settings:
                inference_config.time_dimension_patterns.update(inference_settings['time_dimension_patterns'])
                
            if 'categorical_patterns' in inference_settings:
                inference_config.categorical_patterns.update(inference_settings['categorical_patterns'])
                
            if 'numeric_measure_patterns' in inference_settings:
                inference_config.numeric_measure_patterns.update(inference_settings['numeric_measure_patterns'])
                
            if 'exclude_patterns' in inference_settings:
                inference_config.exclude_patterns.update(inference_settings['exclude_patterns'])
                
            if self.config.debug:
                print(f"[DEBUG] Applied auto-inference config from bdm_config.yml")
                print(f"[DEBUG] Auto-inference enabled: {inference_config.enabled}")
                
        self.auto_inference = AutoInferenceEngine(inference_config)
        
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
                
        # Check if there were any errors
        if results['errors'] and self.config.validate:
            # Only raise errors if validation is enabled
            first_error = results['errors'][0]
            error_msg = first_error['error']
            # If the original error message contains "Required parameter", re-raise as ValueError
            if "Required parameter" in error_msg:
                raise ValueError(error_msg)
            else:
                raise RuntimeError(f"Compilation failed: {error_msg}")
        
        # Resolve any pending semantic model references
        self._resolve_semantic_model_references()
        
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
                
        # Register semantic model templates
        if 'semantic_model_templates' in parsed_data:
            for name, template_def in parsed_data['semantic_model_templates'].items():
                self.templates.semantic_model_engine.register_template(name, template_def)
            
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
                    
        # Process explicitly defined semantic models first
        if 'semantic_models' in parsed_data:
            if self.config.debug:
                print(f"[DEBUG] Found {len(parsed_data['semantic_models'])} semantic models in file")
            
            for sm_def in parsed_data['semantic_models']:
                try:
                    if self.config.debug:
                        print(f"\n[DEBUG] Processing semantic model: {sm_def.get('name', 'unknown')}")
                    
                    self._process_semantic_model_definition(sm_def)
                except Exception as e:
                    sm_name = sm_def.get('name', 'unknown')
                    if self.config.debug:
                        import traceback
                        print(f"\n[DEBUG] Error processing semantic model '{sm_name}'")
                        print(f"[DEBUG] Error: {str(e)}")
                        traceback.print_exc()
                    raise ValueError(f"Error processing semantic model '{sm_name}': {e}")
        
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
            
            # Skip grouping for metrics that reference semantic models
            if 'semantic_model' in compiled_metric:
                # These metrics don't need auto-generated semantic models
                continue
            
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
        
        # Handle semantic model reference
        if 'semantic_model' in metric_def:
            # Metric references a semantic model instead of a source
            sm_name = metric_def['semantic_model']
            
            # Find the semantic model in our compiled semantic models
            semantic_model = None
            for sm in self.semantic_models:
                # Check both with and without sem_ prefix
                if sm['name'] == f"sem_{sm_name}" or sm['name'] == sm_name:
                    semantic_model = sm
                    break
            
            if not semantic_model:
                # Semantic model might not be compiled yet, store reference for later resolution
                compiled['semantic_model'] = sm_name
                if self.config.debug:
                    print(f"[DEBUG] Metric '{metric_def.get('name')}' references semantic model '{sm_name}' (will resolve later)")
            else:
                # Extract source from semantic model
                if 'model' in semantic_model:
                    # Extract table name from ref()
                    model_ref = semantic_model['model']
                    if model_ref.startswith("ref('") and model_ref.endswith("')"):
                        compiled['source'] = model_ref[5:-2]
                    else:
                        compiled['source'] = model_ref
                    
                    if self.config.debug:
                        print(f"[DEBUG] Resolved semantic model '{sm_name}' to source '{compiled['source']}'")
                
                # If a measure is referenced by name, resolve it
                if 'measure' in metric_def and isinstance(metric_def['measure'], str):
                    measure_name = metric_def['measure']
                    
                    for measure in semantic_model.get('measures', []):
                        if measure['name'] == measure_name:
                            # Convert semantic model measure to metric measure format
                            compiled['measure'] = {
                                'type': measure['agg'],
                                'column': measure['expr']
                            }
                            # Also store the measure reference for dbt output
                            compiled['measure_ref'] = measure_name
                            
                            # Copy agg_time_dimension if present
                            if 'agg_time_dimension' in measure and 'time_dimension' not in metric_def:
                                compiled['time_dimension'] = measure['agg_time_dimension']
                            
                            if self.config.debug:
                                print(f"[DEBUG] Resolved measure '{measure_name}' from semantic model")
                            break
                    else:
                        # Measure not found, store reference for validation later
                        compiled['measure_ref'] = measure_name
                        # Also store the semantic model name for later validation
                        compiled['semantic_model'] = sm_name
                        if self.config.debug:
                            print(f"[DEBUG] Measure '{measure_name}' not found in semantic model (will validate later)")
        elif 'source' in metric_def:
            # Traditional source-based metric
            compiled['source'] = metric_def['source']
        
        # Preserve source_ref metadata if it exists
        if 'source_ref' in metric_def:
            compiled['source_ref'] = metric_def['source_ref']
        
        # Copy over other fields
        for key in ['measure', 'numerator', 'denominator', 'formula', 'expression', 
                   'filter', 'meta', 'config', 'validation', 'auto_variants', 
                   'window', 'grain_to_date', 'base_measure', 'conversion_measure', 
                   'entity', 'dimensions', 'fill_nulls_with', 'time_spine', 
                   'offsets', 'window_type', 'offset_pattern', 'join_paths']:
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
    
    def _expand_semantic_model_template(self, sm_def: Dict[str, Any]) -> Dict[str, Any]:
        """Expand semantic model that uses template"""
        template_name = sm_def['template']
        params = sm_def.get('parameters', sm_def.get('params', {}))
        
        if self.config.debug:
            print(f"[DEBUG] Expanding semantic model template: {template_name}")
            print(f"[DEBUG] Parameters: {params}")
        
        try:
            # Check if template exists in registered templates
            # Note: We're reusing the same template system for semantic models
            expanded = self.templates.expand(template_name, params, template_type='semantic_model')
            if self.config.debug:
                print(f"[DEBUG] Semantic model template expanded successfully")
                print(f"[DEBUG] Expanded keys: {list(expanded.keys())}")
        except ValueError as e:
            # Re-raise ValueError for required parameter errors
            if "Required parameter" in str(e):
                raise e
            if self.config.debug:
                print(f"[DEBUG] Semantic model template expansion failed: {e}")
            # Other ValueErrors, return as-is
            return sm_def
        except Exception as e:
            if self.config.debug:
                print(f"[DEBUG] Semantic model template expansion failed: {e}")
            # Template might not be found, return as-is
            return sm_def
        
        # Merge with semantic model definition
        for key, value in sm_def.items():
            if key not in ['template', 'parameters', 'params']:
                # For lists, merge instead of replace
                if key in ['dimensions', 'measures', 'entities'] and isinstance(value, list):
                    if key in expanded and isinstance(expanded[key], list):
                        # Merge lists - template items first, then additional items
                        expanded[key] = expanded[key] + value
                    else:
                        expanded[key] = value
                else:
                    # For non-list fields, sm fields override template
                    expanded[key] = value
                
        return expanded
        
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
                # Check if it's a reference in string format: $ref(path)
                if dim.startswith('$ref(') and dim.endswith(')'):
                    # Extract the reference path
                    ref_path = dim[5:-1]  # Remove $ref( and )
                    if self.config.debug:
                        print(f"[DEBUG] Processing string reference: {ref_path}")
                    
                    # Convert string $ref() to dict format for processing
                    ref_dict = {'$ref': ref_path}
                    try:
                        if self.config.debug:
                            print(f"[DEBUG] Attempting to expand dimension group reference: {ref_dict}")
                            print(f"[DEBUG] Available dimension groups: {list(self.dimension_groups.groups.keys())}")
                        group_dims = self.dimension_groups.expand_dimension_reference(ref_dict)
                        if self.config.debug:
                            print(f"[DEBUG] Expanded to: {group_dims}")
                        if isinstance(group_dims, list):
                            expanded.extend(group_dims)
                        else:
                            expanded.append(group_dims)
                    except Exception as e:
                        print(f"Warning: Error expanding dimension reference {dim}: {e}")
                        if self.config.debug:
                            import traceback
                            traceback.print_exc()
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
    
    def _apply_auto_inference(self, sm_def: Dict[str, Any]) -> Dict[str, Any]:
        """Apply auto-inference to a semantic model definition"""
        auto_infer_config = sm_def.get('auto_infer', {})
        
        if not isinstance(auto_infer_config, dict):
            if self.config.debug:
                print(f"[DEBUG] Invalid auto_infer config: {auto_infer_config}")
            return sm_def
        
        # For now, we'll simulate schema inference since we don't have direct DB access
        # In a real implementation, this would query the database schema
        table_name = sm_def.get('source', sm_def.get('name', 'unknown'))
        
        if self.config.debug:
            print(f"[DEBUG] Applying auto-inference to semantic model: {sm_def.get('name')}")
            print(f"[DEBUG] Auto-infer config: {auto_infer_config}")
        
        # Create mock schema for demonstration - in real implementation, this would come from DB
        # For now, we'll infer from existing dimensions if present, or create basic patterns
        mock_columns = self._create_mock_schema_for_inference(sm_def, auto_infer_config)
        
        if mock_columns:
            # Apply auto-inference
            inferred_model = self.auto_inference.infer_semantic_model(
                table_name, 
                mock_columns, 
                auto_infer_config
            )
            
            # Merge inferred components with existing definition
            sm_def = self._merge_inferred_components(sm_def, inferred_model)
            
            if self.config.debug:
                print(f"[DEBUG] Auto-inference applied to {sm_def.get('name')}")
                if inferred_model.get('entities'):
                    print(f"[DEBUG] Inferred entities: {[e['name'] for e in inferred_model['entities']]}")
                if inferred_model.get('dimensions'):
                    print(f"[DEBUG] Inferred dimensions: {[d['name'] for d in inferred_model['dimensions']]}")
                if inferred_model.get('measures'):
                    print(f"[DEBUG] Inferred measures: {[m['name'] for m in inferred_model['measures']]}")
        
        return sm_def
    
    def _create_mock_schema_for_inference(self, sm_def: Dict[str, Any], auto_infer_config: Dict[str, Any]) -> List[ColumnInfo]:
        """Create mock schema for auto-inference demonstration"""
        # In a real implementation, this would query the database
        # For now, create some example columns based on common patterns
        
        table_name = sm_def.get('source', sm_def.get('name', 'table'))
        columns = []
        
        # Add primary key
        columns.append(ColumnInfo(
            name=f"{table_name}_id",
            data_type="bigint",
            is_primary_key=True,
            is_nullable=False
        ))
        
        # Add common dimension columns
        if auto_infer_config.get('dimensions', True):
            # Time dimensions
            time_columns = auto_infer_config.get('time_dimensions', {}).get('from_columns', [])
            if not time_columns:
                # Default time columns
                time_columns = ['created_at', 'updated_at']
            
            for col_name in time_columns:
                columns.append(ColumnInfo(
                    name=col_name,
                    data_type="timestamp",
                    is_nullable=True
                ))
            
            # Categorical dimensions
            cat_columns = ['status', 'type', 'category']
            for col_name in cat_columns:
                columns.append(ColumnInfo(
                    name=f"{table_name}_{col_name}",
                    data_type="varchar(50)",
                    is_nullable=True,
                    cardinality=10  # Low cardinality for categorical
                ))
        
        # Add measures
        measure_columns = ['amount', 'quantity', 'value']
        for col_name in measure_columns:
            columns.append(ColumnInfo(
                name=f"{table_name}_{col_name}",
                data_type="decimal(10,2)",
                is_nullable=True
            ))
        
        # Exclude specified columns
        exclude_columns = auto_infer_config.get('exclude_columns', [])
        columns = [col for col in columns if col.name not in exclude_columns]
        
        return columns
    
    def _merge_inferred_components(self, sm_def: Dict[str, Any], inferred_model: Dict[str, Any]) -> Dict[str, Any]:
        """Merge inferred components with existing semantic model definition"""
        result = deepcopy(sm_def)
        
        # Merge entities
        if 'entities' in inferred_model and inferred_model['entities']:
            existing_entities = result.get('entities', [])
            existing_entity_names = {e.get('name') for e in existing_entities if isinstance(e, dict)}
            
            for inferred_entity in inferred_model['entities']:
                if inferred_entity.get('name') not in existing_entity_names:
                    existing_entities.append(inferred_entity)
            
            result['entities'] = existing_entities
        
        # Merge dimensions
        if 'dimensions' in inferred_model and inferred_model['dimensions']:
            existing_dimensions = result.get('dimensions', [])
            existing_dim_names = {d.get('name') for d in existing_dimensions if isinstance(d, dict)}
            
            for inferred_dim in inferred_model['dimensions']:
                if inferred_dim.get('name') not in existing_dim_names:
                    existing_dimensions.append(inferred_dim)
            
            result['dimensions'] = existing_dimensions
        
        # Merge measures
        if 'measures' in inferred_model and inferred_model['measures']:
            existing_measures = result.get('measures', [])
            existing_measure_names = {m.get('name') for m in existing_measures if isinstance(m, dict)}
            
            for inferred_measure in inferred_model['measures']:
                if inferred_measure.get('name') not in existing_measure_names:
                    existing_measures.append(inferred_measure)
            
            result['measures'] = existing_measures
        
        # Remove the auto_infer config from final output
        result.pop('auto_infer', None)
        
        return result
        
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
                    
            if 'semantic_model_templates' in imported_data:
                for name, template_def in imported_data['semantic_model_templates'].items():
                    # Register with alias prefix
                    full_name = f"{alias}.{name}"
                    self.templates.semantic_model_engine.register_template(full_name, template_def)
                    # Also register without alias for simple references
                    self.templates.semantic_model_engine.register_template(name, template_def)
                    
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
                # Update label to avoid duplicates
                if 'label' in metric:
                    variant['label'] = f"{metric['label']} ({period.upper()})"
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
                # Update label to avoid duplicates
                if 'label' in metric:
                    variant['label'] = f"{metric['label']} (by {dim})"
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
                    
                    # Update label to avoid duplicates
                    if 'label' in metric:
                        if 'label_suffix' in variant_config:
                            # Clean up the suffix - remove leading underscore and convert to title case
                            suffix = variant_config['label_suffix'].lstrip('_').replace('_', ' ').upper()
                            variant['label'] = f"{metric['label']} ({suffix})"
                        elif 'name_suffix' in variant_config:
                            suffix = variant_config['name_suffix'].lstrip('_').replace('_', ' ').title()
                            variant['label'] = f"{metric['label']} ({suffix})"
                        else:
                            variant['label'] = f"{metric['label']} ({variant_type.replace('_', ' ').title()})"
                    
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
            
            # Generate metric definitions with deduplication
            dbt_metrics = []
            metric_signatures = {}  # Track unique metrics by their signature
            metric_aliases = {}     # Map duplicate metrics to their canonical names
            
            # First pass: identify unique metrics and create aliases
            for metric in self.compiled_metrics:
                signature = self._get_metric_signature(metric)
                if signature in metric_signatures:
                    # This is a duplicate - create an alias
                    canonical_name = metric_signatures[signature]
                    metric_aliases[metric['name']] = canonical_name
                    if self.config.debug:
                        print(f"[DEBUG] Metric '{metric['name']}' is a duplicate of '{canonical_name}'")
                else:
                    # This is a new unique metric
                    metric_signatures[signature] = metric['name']
            
            # Second pass: create metrics (skipping duplicates)
            for metric in self.compiled_metrics:
                try:
                    # Skip if this is a duplicate
                    if metric['name'] in metric_aliases:
                        continue
                    
                    # For ratio metrics, we need to create component metrics first
                    if metric['type'] == 'ratio' and 'numerator' in metric and 'denominator' in metric:
                        # Check if we can reuse existing metrics for components
                        num_metric_name = self._find_or_create_component_metric(
                            metric, 'numerator', dbt_metrics, metric_signatures
                        )
                        den_metric_name = self._find_or_create_component_metric(
                            metric, 'denominator', dbt_metrics, metric_signatures
                        )
                        
                        # Update the metric to use the deduplicated component names
                        metric['_num_metric_ref'] = num_metric_name
                        metric['_den_metric_ref'] = den_metric_name
                    
                    # Convert the main metric
                    dbt_metric = self._to_dbt_metric(metric)
                    dbt_metrics.append(dbt_metric)
                except Exception as e:
                    if self.config.debug:
                        print(f"\n[DEBUG] Error converting metric to dbt format: {metric.get('name', 'unknown')}")
                        print(f"[DEBUG] Error: {e}")
                    raise
            
            # Store aliases for reference
            self.metric_aliases = metric_aliases
            
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
        # Semantic models have already been added during file compilation
        # Now we need to generate semantic models from metrics that don't reference existing ones
        
        # First pass: Create semantic models for non-composite sources
        composite_sources = {}
        
        # Generate semantic models from metrics grouped by source
        # Create a copy of the items to avoid dictionary changed size during iteration
        for source, metrics in list(self.metrics_by_source.items()):
            # Skip metrics that reference semantic models
            metrics_needing_models = [m for m in metrics if 'semantic_model' not in m]
            if not metrics_needing_models:
                continue
            # Check if this is a composite source
            if source.startswith('ratio_') or source.startswith('conversion_'):
                # Save for second pass
                composite_sources[source] = metrics_needing_models
                continue
            # Skip if a semantic model was already explicitly defined for this source
            if any(sm['name'] == f"sem_{source}" for sm in self.semantic_models):
                continue
                
            # Collect all dimensions and measures
            all_dimensions = []
            all_measures = []
            dimension_names = set()
            measure_names = set()
            
            for metric in metrics_needing_models:
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
                if 'measure' in metric and isinstance(metric['measure'], dict):
                    # Only process if measure is a dict (not a reference to semantic model measure)
                    measure_name = f"{metric['name']}_measure"
                    if measure_name not in measure_names:
                        measure_names.add(measure_name)
                        # Find time dimension from metric dimensions
                        time_dim = self._find_time_dimension(metric.get('dimensions', []))
                        all_measures.append(self._to_dbt_measure(metric['measure'], measure_name, time_dim))
                        
                # Add measures for ratio metrics
                if metric['type'] == 'ratio' and 'numerator' in metric and 'denominator' in metric:
                    try:
                        # Numerator measure
                        num_measure_name = f"{metric['name']}_numerator"
                        if num_measure_name not in measure_names:
                            measure_names.add(num_measure_name)
                            # Check if numerator is a dict with measure key
                            if isinstance(metric['numerator'], dict) and 'measure' in metric['numerator']:
                                # Find time dimension from metric dimensions
                                time_dim = self._find_time_dimension(metric.get('dimensions', []))
                                all_measures.append(self._to_dbt_measure(metric['numerator']['measure'], num_measure_name, time_dim))
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
                                # Find time dimension from metric dimensions
                                time_dim = self._find_time_dimension(metric.get('dimensions', []))
                                all_measures.append(self._to_dbt_measure(metric['denominator']['measure'], den_measure_name, time_dim))
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
            semantic_model = {
                'name': f"sem_{source}",
                'model': f"ref('{source}')",
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
        
        # Second pass: Handle composite sources (ratio metrics with different sources)
        for source, metrics in composite_sources.items():
            if self.config.debug:
                print(f"[DEBUG] Processing composite source: {source}")
            
            # Check if this is really a composite source
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
            
            if is_composite:
                # For composite sources, we need to ensure semantic models exist for all referenced sources
                # and add the measures to them
                for metric in metrics:
                    if metric.get('type') == 'ratio':
                        # Process numerator
                        num_source = metric.get('numerator', {}).get('source')
                        if num_source and 'measure' in metric.get('numerator', {}):
                            # Find or create semantic model for numerator source
                            num_model = self._find_or_create_semantic_model(num_source)
                            num_measure_name = f"{metric['name']}_numerator"
                            if not any(m['name'] == num_measure_name for m in num_model.get('measures', [])):
                                # Find time dimension from metric dimensions
                                time_dim = self._find_time_dimension(metric.get('dimensions', []))
                                num_model['measures'].append(
                                    self._to_dbt_measure(metric['numerator']['measure'], num_measure_name, time_dim)
                                )
                            
                            # Also add dimensions from the metric to the semantic model
                            if 'dimensions' in metric:
                                existing_dim_names = {d.get('name', d) for d in num_model.get('dimensions', [])}
                                for dim in metric['dimensions']:
                                    dim_name = dim.get('name') if isinstance(dim, dict) else dim
                                    if dim_name and dim_name not in existing_dim_names:
                                        num_model['dimensions'].append(self._to_dbt_dimension(dim))
                        
                        # Process denominator
                        den_source = metric.get('denominator', {}).get('source')
                        if den_source and 'measure' in metric.get('denominator', {}):
                            # Find or create semantic model for denominator source
                            den_model = self._find_or_create_semantic_model(den_source)
                            den_measure_name = f"{metric['name']}_denominator"
                            if not any(m['name'] == den_measure_name for m in den_model.get('measures', [])):
                                # Find time dimension from metric dimensions
                                time_dim = self._find_time_dimension(metric.get('dimensions', []))
                                den_model['measures'].append(
                                    self._to_dbt_measure(metric['denominator']['measure'], den_measure_name, time_dim)
                                )
                            
                            # Also add dimensions from the metric to the semantic model
                            if 'dimensions' in metric:
                                existing_dim_names = {d.get('name', d) for d in den_model.get('dimensions', [])}
                                for dim in metric['dimensions']:
                                    dim_name = dim.get('name') if isinstance(dim, dict) else dim
                                    if dim_name and dim_name not in existing_dim_names:
                                        den_model['dimensions'].append(self._to_dbt_dimension(dim))
            
    def _process_semantic_model_definition(self, sm_def: Dict[str, Any]):
        """Process an explicitly defined semantic model"""
        # First check if this uses a template
        if 'template' in sm_def:
            sm_def = self._expand_semantic_model_template(sm_def)
        
        # Apply auto-inference if enabled and requested
        if 'auto_infer' in sm_def and self.auto_inference.config.enabled:
            sm_def = self._apply_auto_inference(sm_def)
        
        # Determine the model reference
        if 'model' in sm_def:
            model_ref = sm_def['model']
        elif 'source' in sm_def:
            # Support source field for backward compatibility
            source = sm_def['source']
            # Handle ref() format
            if isinstance(source, str) and source.startswith('ref('):
                model_ref = source
            else:
                model_ref = f"ref('{source}')"
        else:
            # Default to using the name as the model reference
            model_ref = f"ref('{sm_def['name']}')"
        
        semantic_model = {
            'name': f"sem_{sm_def['name']}",  # Prefix with sem_ for consistency
            'model': model_ref,
            'description': sm_def.get('description', f"Semantic model for {sm_def['name']}"),
        }
        
        # Handle entity sets
        if 'entity_set' in sm_def and sm_def['entity_set'] in self.entity_sets:
            entity_set = self.entity_sets[sm_def['entity_set']]
            # Build entities from entity set
            entities = []
            primary_entity_def = entity_set['primary_entity']
            # Get the entity name - could be a string or dict
            if isinstance(primary_entity_def, str):
                primary = primary_entity_def
            elif isinstance(primary_entity_def, dict):
                primary = primary_entity_def.get('name')
            else:
                primary = None
                
            if primary and primary in self.entities:
                primary_def = self.entities[primary]
                entities.append({
                    'name': primary,
                    'type': 'primary',
                    'expr': primary_def.get('column', primary)
                })
            elif isinstance(primary_entity_def, dict):
                # Entity is defined inline in the entity set
                entities.append(primary_entity_def)
                
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
        
        # Process dimensions - convert to dbt format
        if 'dimensions' in sm_def:
            semantic_model['dimensions'] = []
            for dim in sm_def['dimensions']:
                # Convert dimension to dbt format
                if isinstance(dim, dict):
                    semantic_model['dimensions'].append(self._to_dbt_dimension(dim))
                else:
                    # String dimension - convert to basic categorical
                    semantic_model['dimensions'].append({
                        'name': dim,
                        'type': 'categorical'
                    })
        
        # Process measures - convert to dbt format
        if 'measures' in sm_def:
            semantic_model['measures'] = []
            for measure in sm_def['measures']:
                # Find aggregation time dimension
                agg_time_dim = measure.get('agg_time_dimension')
                if not agg_time_dim and 'dimensions' in sm_def:
                    # Try to find a time dimension
                    agg_time_dim = self._find_time_dimension(sm_def['dimensions'])
                
                dbt_measure = self._to_dbt_measure(measure, measure['name'], agg_time_dim)
                semantic_model['measures'].append(dbt_measure)
        
        # Handle auto-inference if enabled
        if sm_def.get('auto_infer', {}).get('dimensions', False):
            # This would require schema introspection - mark for future implementation
            if 'meta' not in semantic_model:
                semantic_model['meta'] = {}
            semantic_model['meta']['auto_infer_dimensions'] = True
            
        # Copy other fields directly
        for field in ['meta', 'config', 'time_spine_table_configurations', 'primary_time_dimension', 'joins']:
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
                # Check if type_params already exists (from templates)
                if 'type_params' in dim:
                    dbt_dim['type_params'] = dim['type_params']
                else:
                    # Build from grain field
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
                        
                        # Add related entities through relationships FROM this entity
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
                        
                        # Also look for relationships FROM other entities TO this entity
                        for other_entity_name, other_entity_def in self.entities.items():
                            if other_entity_name != entity_name and 'relationships' in other_entity_def:
                                for rel in other_entity_def['relationships']:
                                    if rel['to_entity'] == entity_name:
                                        # Found a relationship pointing to our primary entity
                                        foreign_key = rel.get('foreign_key', f"{entity_name}_id")
                                        if foreign_key not in entity_names:
                                            entity_names.add(foreign_key)
                                            entities.append({
                                                'name': foreign_key,
                                                'type': 'foreign',
                                                'expr': foreign_key
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
        
    def _find_time_dimension(self, dimensions: List[Any]) -> Optional[str]:
        """Find the primary time dimension from a list of dimensions"""
        if not dimensions:
            return None
            
        # Look for the first time dimension
        for dim in dimensions:
            if isinstance(dim, dict):
                if dim.get('type') == 'time':
                    return dim.get('name')
                # Check if it's a common time dimension name
                dim_name = dim.get('name', '')
                if any(time_word in dim_name.lower() for time_word in ['date', 'time', 'created', 'updated']):
                    return dim_name
            elif isinstance(dim, str):
                # Check string dimension names
                if any(time_word in dim.lower() for time_word in ['date', 'time', 'created', 'updated']):
                    return dim
                    
        return None
    
    def _to_dbt_measure(self, measure: Dict[str, Any], name: str, time_dimension: Optional[str] = None) -> Dict[str, Any]:
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
        
        # Add aggregation time dimension if available
        if 'agg_time_dimension' in measure:
            agg_time_dim = measure['agg_time_dimension']
            # Validate that agg_time_dimension is a string, not a reference
            if isinstance(agg_time_dim, str) and not agg_time_dim.startswith('$ref'):
                dbt_measure['agg_time_dimension'] = agg_time_dim
            else:
                if self.config.debug:
                    print(f"[DEBUG] Warning: Invalid agg_time_dimension '{agg_time_dim}' - must be a dimension name, not a reference")
                # Try to use the first time dimension if available
                if time_dimension:
                    dbt_measure['agg_time_dimension'] = time_dimension
        elif time_dimension:
            # Use the passed time dimension if no explicit agg_time_dimension
            dbt_measure['agg_time_dimension'] = time_dimension
        
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
            # Check if metric references a semantic model
            if 'semantic_model' in metric or 'measure_ref' in metric:
                # Metric references a measure in a semantic model
                measure_ref = metric.get('measure_ref', f"{metric['name']}_measure")
                dbt_metric['type_params'] = {
                    'measure': measure_ref
                }
            else:
                # Traditional source-based metric
                dbt_metric['type_params'] = {
                    'measure': f"{metric['name']}_measure"
                }
        elif metric['type'] == 'ratio':
            # Handle ratio metrics
            if 'numerator' in metric and 'denominator' in metric:
                # Use deduplicated component metric names if available
                num_metric_name = metric.get('_num_metric_ref', f"{metric['name']}_numerator")
                den_metric_name = metric.get('_den_metric_ref', f"{metric['name']}_denominator")
                
                # Simple format if no filters
                if not metric['numerator'].get('filter') and not metric['denominator'].get('filter'):
                    dbt_metric['type_params'] = {
                        'numerator': num_metric_name,
                        'denominator': den_metric_name
                    }
                else:
                    # Complex format with filters
                    dbt_metric['type_params'] = {}
                    
                    if metric['numerator'].get('filter'):
                        dbt_metric['type_params']['numerator'] = {
                            'name': num_metric_name,
                            'filter': metric['numerator']['filter']
                        }
                    else:
                        dbt_metric['type_params']['numerator'] = num_metric_name
                    
                    if metric['denominator'].get('filter'):
                        dbt_metric['type_params']['denominator'] = {
                            'name': den_metric_name,
                            'filter': metric['denominator']['filter']
                        }
                    else:
                        dbt_metric['type_params']['denominator'] = den_metric_name
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
            
        # Add filter first (appears before config in dbt)
        if 'filter' in metric:
            dbt_metric['filter'] = metric['filter']
                
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
        
        # Add general config from the original metric
        if 'config' in metric:
            if 'config' not in dbt_metric:
                dbt_metric['config'] = {}
            # Merge config fields
            for key, value in metric['config'].items():
                if key not in dbt_metric['config']:  # Don't overwrite existing config
                    dbt_metric['config'][key] = value
        
        # Add general meta from the original metric
        if 'meta' in metric:
            if 'meta' not in dbt_metric:
                dbt_metric['meta'] = {}
            # Merge meta fields
            for key, value in metric['meta'].items():
                if key not in dbt_metric['meta']:  # Don't overwrite existing meta
                    dbt_metric['meta'][key] = value
                
        return dbt_metric
    
    def _create_component_metric(self, parent_metric: Dict[str, Any], component: str, metric_name: str) -> Optional[Dict[str, Any]]:
        """Create a simple metric for a ratio component (numerator or denominator)"""
        component_data = parent_metric.get(component, {})
        
        # Skip if no measure defined
        if 'measure' not in component_data:
            return None
        
        # Determine the source
        source = component_data.get('source', parent_metric.get('source'))
        if not source:
            return None
        
        # Create a simple metric
        # For ratio components, the measure name in the semantic model is just the metric name
        # (e.g., "contribution_margin_1_pc_numerator" not "contribution_margin_1_pc_numerator_measure")
        component_metric = {
            'name': metric_name,
            'description': f"{component.capitalize()} of {parent_metric.get('description', parent_metric['name'])}",
            'type': 'simple',
            'label': f"{parent_metric.get('label', parent_metric['name'])} ({component})",
            'type_params': {
                'measure': metric_name  # Use metric name as measure name for ratio components
            }
        }
        
        # Add filter if present
        if component_data.get('filter'):
            component_metric['filter'] = component_data['filter']
        
        # Add minimal config to hide these component metrics from end users
        component_metric['config'] = {
            'meta': {
                'hidden': True,
                'component_of': parent_metric['name']
            }
        }
        
        return component_metric
    
    def _get_metric_signature(self, metric: Dict[str, Any]) -> str:
        """Generate a unique signature for a metric based on its configuration"""
        import hashlib
        import json
        
        # Extract the key fields that define metric uniqueness
        signature_data = {
            'type': metric.get('type'),
            'source': metric.get('source'),
            'measure': metric.get('measure'),
            'filter': metric.get('filter'),
            'dimensions': metric.get('dimensions'),
            'numerator': metric.get('numerator'),
            'denominator': metric.get('denominator'),
            'expression': metric.get('expression'),
            'formula': metric.get('formula'),
            'comparison': metric.get('comparison')  # For time comparison variants
        }
        
        # Remove None values and sort for consistency
        signature_data = {k: v for k, v in signature_data.items() if v is not None}
        
        # Create a hash of the configuration
        signature_str = json.dumps(signature_data, sort_keys=True)
        return hashlib.md5(signature_str.encode()).hexdigest()
    
    def _find_or_create_component_metric(self, parent_metric: Dict[str, Any], component: str, 
                                       existing_metrics: List[Dict], metric_signatures: Dict) -> str:
        """Find an existing metric that matches the component or create a new one"""
        component_data = parent_metric.get(component, {})
        
        # Create a temporary metric to get its signature
        temp_metric = {
            'type': 'simple',
            'source': component_data.get('source', parent_metric.get('source')),
            'measure': component_data.get('measure'),
            'filter': component_data.get('filter')
        }
        
        signature = self._get_metric_signature(temp_metric)
        
        # Check if we already have a metric with this signature
        if signature in metric_signatures:
            existing_name = metric_signatures[signature]
            if self.config.debug:
                print(f"[DEBUG] Reusing existing metric '{existing_name}' for {component} of '{parent_metric['name']}'")
            return existing_name
        
        # Create a new component metric
        metric_name = f"{parent_metric['name']}_{component}"
        component_metric = self._create_component_metric(parent_metric, component, metric_name)
        
        if component_metric:
            existing_metrics.append(component_metric)
            metric_signatures[signature] = metric_name
        
        return metric_name
    
    def _resolve_semantic_model_references(self):
        """Resolve any metrics that reference semantic models"""
        for metric in self.compiled_metrics:
            # Process metrics that have either:
            # 1. A semantic_model reference without a source (need to resolve the model)
            # 2. A measure_ref that needs validation (even if source is already set)
            if 'semantic_model' in metric or 'measure_ref' in metric:
                sm_name = metric.get('semantic_model')
                
                # Find the semantic model
                semantic_model = None
                if sm_name:
                    for sm in self.semantic_models:
                        # Check both with and without sem_ prefix
                        if sm['name'] == f"sem_{sm_name}" or sm['name'] == sm_name:
                            semantic_model = sm
                            break
                
                    # If we have a semantic_model reference but no source, resolve it
                    if semantic_model and 'source' not in metric:
                        # Extract source from semantic model
                        if 'model' in semantic_model:
                            # Extract table name from ref()
                            model_ref = semantic_model['model']
                            if model_ref.startswith("ref('") and model_ref.endswith("')"):
                                metric['source'] = model_ref[5:-2]
                            else:
                                metric['source'] = model_ref
                            
                            if self.config.debug:
                                print(f"[DEBUG] Late resolution: semantic model '{sm_name}' to source '{metric['source']}' for metric '{metric['name']}'")
                    
                    # If no semantic model was found, raise error
                    if not semantic_model and sm_name:
                        raise ValueError(
                            f"Metric '{metric['name']}' references semantic model '{sm_name}' which doesn't exist"
                        )
                
                # Resolve measure reference if present
                if 'measure_ref' in metric:
                    measure_name = metric['measure_ref']
                    
                    # Need to find the semantic model if we haven't already
                    if not semantic_model and sm_name:
                        raise ValueError(
                            f"Metric '{metric['name']}' has measure reference but semantic model '{sm_name}' not found"
                        )
                    
                    if semantic_model:
                        for measure in semantic_model.get('measures', []):
                            if measure['name'] == measure_name:
                                # Convert semantic model measure to metric measure format
                                metric['measure'] = {
                                    'type': measure['agg'],
                                    'column': measure['expr']
                                }
                                # Copy agg_time_dimension if present
                                if 'agg_time_dimension' in measure and 'time_dimension' not in metric:
                                    metric['time_dimension'] = measure['agg_time_dimension']
                                
                                if self.config.debug:
                                    print(f"[DEBUG] Late resolution: measure '{measure_name}' from semantic model")
                                
                                # Remove the reference
                                del metric['measure_ref']
                                break
                        else:
                            raise ValueError(
                                f"Metric '{metric['name']}' references measure '{measure_name}' "
                                f"which doesn't exist in semantic model '{sm_name}'"
                            )
    
    def _find_or_create_semantic_model(self, source: str) -> Dict[str, Any]:
        """Find an existing semantic model or create a new one for the given source"""
        # Look for existing semantic model
        for sm in self.semantic_models:
            if sm['name'] == f"sem_{source}":
                return sm
        
        # Create a new semantic model
        semantic_model = {
            'name': f"sem_{source}",
            'model': f"ref('{source}')",
            'description': f"Semantic model for {source}",
            'dimensions': [],
            'measures': [],
            'entities': [{
                'name': 'id',
                'type': 'primary',
                'expr': f"{source}_id"
            }]
        }
        
        self.semantic_models.append(semantic_model)
        return semantic_model
        
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