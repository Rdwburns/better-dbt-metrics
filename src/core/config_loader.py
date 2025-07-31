"""
Configuration loader for better-dbt-metrics
Loads and applies settings from bdm_config.yml
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field


@dataclass
class BDMConfig:
    """Better-DBT-Metrics configuration"""
    # Paths
    metrics_dir: str = "metrics/"
    output_dir: str = "models/semantic/"
    template_dir: str = "metrics/_base/"
    
    # Import mappings
    import_mappings: Dict[str, str] = field(default_factory=dict)
    search_paths: List[str] = field(default_factory=list)
    
    # Compilation settings
    expand_auto_variants: bool = True
    inherit_dimensions: bool = True
    validate_dimensions: bool = True
    validate_sources: bool = True
    
    # Template expansion
    template_expansion_enabled: bool = True
    template_recursive: bool = True
    template_max_depth: int = 3
    
    # Auto-variants
    time_comparisons_enabled: bool = True
    time_comparison_periods: List[str] = field(default_factory=lambda: ["wow", "mom", "yoy"])
    territory_splits_enabled: bool = True
    territories: List[str] = field(default_factory=lambda: ["UK", "CE", "EE"])
    channel_splits_enabled: bool = True
    channels: List[str] = field(default_factory=lambda: ["shopify", "amazon", "tiktok_shop"])
    
    # Domain-specific settings
    domain_settings: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # Output settings
    file_pattern: str = "{domain}_{subdomain}_metrics.yml"
    add_dbt_meta: bool = True
    include_metadata: bool = True
    
    # Validation rules
    require_descriptions: bool = True
    require_labels: bool = True
    validate_dimension_refs: bool = True
    
    # Logging
    log_level: str = "INFO"
    show_sql: bool = False
    show_yaml: bool = True
    
    # Auto-inference settings
    auto_inference: Dict[str, Any] = field(default_factory=lambda: {
        'enabled': True,
        'time_dimension_patterns': {
            'suffix': ['_date', '_at', '_time', '_timestamp', '_datetime'],
            'prefix': ['date_', 'created_', 'updated_', 'modified_', 'deleted_'],
            'exact': ['date', 'time', 'timestamp', 'created', 'updated']
        },
        'categorical_patterns': {
            'suffix': ['_id', '_code', '_type', '_status', '_category', '_group', '_segment'],
            'prefix': ['type_', 'status_', 'category_'],
            'max_cardinality': 100,
            'boolean_keywords': ['is_', 'has_', 'can_', 'should_', 'will_']
        },
        'numeric_measure_patterns': {
            'suffix': ['_amount', '_value', '_price', '_cost', '_revenue', '_count', '_total', '_sum'],
            'prefix': ['amount_', 'value_', 'price_', 'cost_', 'revenue_', 'total_'],
            'exact': ['amount', 'value', 'price', 'cost', 'revenue', 'total', 'quantity', 'count']
        },
        'exclude_patterns': {
            'prefix': ['tmp_', 'temp_', 'staging_'],
            'suffix': ['_raw', '_hash', '_encrypted', '_backup'],
            'exact': ['row_number', 'rank', 'dense_rank'],
            'starts_with_underscore': True
        }
    })


class ConfigLoader:
    """Loads configuration from bdm_config.yml"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path
        self.config = BDMConfig()
        
    def load_config(self, base_dir: str = ".") -> BDMConfig:
        """Load configuration from file or use defaults"""
        # Try to find config file
        config_file = None
        if self.config_path:
            config_file = Path(self.config_path)
        else:
            # Look for bdm_config.yml in common locations
            search_locations = [
                Path(base_dir) / "bdm_config.yml",
                Path(base_dir) / "metrics" / "bdm_config.yml",
                Path(base_dir) / "config" / "bdm_config.yml",
                Path("bdm_config.yml"),
            ]
            
            for location in search_locations:
                if location.exists():
                    config_file = location
                    break
        
        if not config_file or not config_file.exists():
            # Return default config
            return self.config
            
        # Load and parse config file
        try:
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f)
                
            if not isinstance(config_data, dict):
                raise ValueError(f"Invalid config file format: {config_file}")
                
            # Apply configuration
            self._apply_config(config_data)
            
        except Exception as e:
            print(f"Warning: Failed to load config from {config_file}: {e}")
            print("Using default configuration")
            
        return self.config
        
    def _apply_config(self, config_data: Dict[str, Any]):
        """Apply configuration data to config object"""
        # Paths
        if 'paths' in config_data:
            paths = config_data['paths']
            self.config.metrics_dir = paths.get('metrics_dir', self.config.metrics_dir)
            self.config.output_dir = paths.get('output_dir', self.config.output_dir)
            self.config.template_dir = paths.get('template_dir', self.config.template_dir)
            
        # Import settings
        if 'imports' in config_data:
            imports = config_data['imports']
            self.config.import_mappings = imports.get('mappings', {})
            self.config.search_paths = imports.get('search_paths', [])
            
        # Compilation settings
        if 'compilation' in config_data:
            comp = config_data['compilation']
            self.config.expand_auto_variants = comp.get('expand_auto_variants', True)
            self.config.inherit_dimensions = comp.get('inherit_dimensions', True)
            self.config.validate_dimensions = comp.get('validate_dimensions', True)
            self.config.validate_sources = comp.get('validate_sources', True)
            
            # Template expansion
            if 'template_expansion' in comp:
                te = comp['template_expansion']
                self.config.template_expansion_enabled = te.get('enabled', True)
                self.config.template_recursive = te.get('recursive', True)
                self.config.template_max_depth = te.get('max_depth', 3)
                
        # Auto-variants
        if 'auto_variants' in config_data:
            av = config_data['auto_variants']
            
            # Time comparisons
            if 'time_comparisons' in av:
                tc = av['time_comparisons']
                self.config.time_comparisons_enabled = tc.get('enabled', True)
                self.config.time_comparison_periods = tc.get('default_periods', ["wow", "mom", "yoy"])
                
            # Territory splits
            if 'territory_splits' in av:
                ts = av['territory_splits']
                self.config.territory_splits_enabled = ts.get('enabled', True)
                self.config.territories = ts.get('territories', ["UK", "CE", "EE"])
                
            # Channel splits
            if 'channel_splits' in av:
                cs = av['channel_splits']
                self.config.channel_splits_enabled = cs.get('enabled', True)
                self.config.channels = cs.get('channels', ["shopify", "amazon", "tiktok_shop"])
                
        # Domain-specific settings
        if 'domains' in config_data:
            self.config.domain_settings = config_data['domains']
            
        # Output settings
        if 'output' in config_data:
            output = config_data['output']
            self.config.file_pattern = output.get('file_pattern', self.config.file_pattern)
            self.config.add_dbt_meta = output.get('add_dbt_meta', True)
            self.config.include_metadata = output.get('include_metadata', True)
            
        # Validation rules
        if 'validation' in config_data:
            val = config_data['validation']
            self.config.require_descriptions = val.get('require_descriptions', True)
            self.config.require_labels = val.get('require_labels', True)
            self.config.validate_dimension_refs = val.get('validate_dimension_refs', True)
            self.config.validate_sources = val.get('validate_sources', False)
            
        # Logging
        if 'logging' in config_data:
            log = config_data['logging']
            self.config.log_level = log.get('level', 'INFO')
            self.config.show_sql = log.get('show_sql', False)
            self.config.show_yaml = log.get('show_yaml', True)
            
        # Auto-inference settings
        if 'auto_inference' in config_data:
            # Deep merge the auto_inference settings
            ai_config = config_data['auto_inference']
            
            # Update enabled flag
            if 'enabled' in ai_config:
                self.config.auto_inference['enabled'] = ai_config['enabled']
            
            # Update patterns by merging with defaults
            for pattern_type in ['time_dimension_patterns', 'categorical_patterns', 
                                'numeric_measure_patterns', 'exclude_patterns']:
                if pattern_type in ai_config:
                    if pattern_type not in self.config.auto_inference:
                        self.config.auto_inference[pattern_type] = {}
                    
                    # Merge each sub-key (suffix, prefix, exact, etc.)
                    for key, value in ai_config[pattern_type].items():
                        if isinstance(value, list):
                            # For lists, replace entirely (don't merge)
                            self.config.auto_inference[pattern_type][key] = value
                        else:
                            # For non-lists (like max_cardinality), just update
                            self.config.auto_inference[pattern_type][key] = value