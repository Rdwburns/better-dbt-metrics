"""
Auto-Inference Engine for Better-DBT-Metrics
Automatically detects dimensions, entities, and measures from table schemas
"""

import re
from typing import Dict, List, Any, Optional, Set, Pattern
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ColumnInfo:
    """Information about a database column"""
    name: str
    data_type: str
    is_nullable: bool = True
    max_length: Optional[int] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    cardinality: Optional[int] = None
    sample_values: List[Any] = field(default_factory=list)


@dataclass
class InferenceConfig:
    """Configuration for auto-inference behavior"""
    enabled: bool = True
    
    # Time dimension patterns
    time_dimension_patterns: Dict[str, List[str]] = field(default_factory=lambda: {
        'suffix': ['_date', '_at', '_time', '_timestamp', '_datetime'],
        'prefix': ['date_', 'created_', 'updated_', 'modified_', 'deleted_'],
        'exact': ['date', 'time', 'timestamp', 'created', 'updated']
    })
    
    # Categorical dimension patterns
    categorical_patterns: Dict[str, Any] = field(default_factory=lambda: {
        'suffix': ['_id', '_code', '_type', '_status', '_category', '_group', '_segment'],
        'prefix': ['type_', 'status_', 'category_'],
        'max_cardinality': 100,  # Max unique values to consider categorical
        'boolean_keywords': ['is_', 'has_', 'can_', 'should_', 'will_']
    })
    
    # Numeric measure patterns
    numeric_measure_patterns: Dict[str, List[str]] = field(default_factory=lambda: {
        'suffix': ['_amount', '_value', '_price', '_cost', '_revenue', '_count', '_total', '_sum'],
        'prefix': ['amount_', 'value_', 'price_', 'cost_', 'revenue_', 'total_'],
        'exact': ['amount', 'value', 'price', 'cost', 'revenue', 'total', 'quantity', 'count']
    })
    
    # Entity patterns (primary/foreign keys)
    entity_patterns: Dict[str, List[str]] = field(default_factory=lambda: {
        'primary_exact': ['id'],  # Exact matches for primary keys
        'primary_suffix': ['_id', '_key', '_uuid'],
        'foreign_suffix': ['_id', '_key'],
        'exclude_words': ['created', 'updated', 'deleted', 'modified']  # Don't treat these as entities
    })
    
    # Exclusion patterns
    exclude_patterns: Dict[str, List[str]] = field(default_factory=lambda: {
        'prefix': ['tmp_', 'temp_', 'staging_'],  # Removed generic '_' prefix
        'suffix': ['_raw', '_hash', '_encrypted', '_backup'],
        'exact': ['row_number', 'rank', 'dense_rank'],
        'starts_with_underscore': True  # Special flag for columns starting with underscore
    })


class AutoInferenceEngine:
    """
    Engine for automatically inferring semantic model structure from table schemas
    """
    
    def __init__(self, config: Optional[InferenceConfig] = None):
        self.config = config or InferenceConfig()
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile regex patterns for better performance"""
        self.time_patterns = self._compile_pattern_dict(self.config.time_dimension_patterns)
        self.categorical_patterns = self._compile_pattern_dict(self.config.categorical_patterns)
        self.numeric_patterns = self._compile_pattern_dict(self.config.numeric_measure_patterns)
        self.entity_patterns = self._compile_pattern_dict(self.config.entity_patterns)
        self.exclude_patterns = self._compile_pattern_dict(self.config.exclude_patterns)
    
    def _compile_pattern_dict(self, pattern_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Compile string patterns to regex patterns"""
        compiled = {}
        for key, value in pattern_dict.items():
            if isinstance(value, list) and key not in ['boolean_keywords', 'exclude_words']:
                if 'exact' in key:
                    # For exact matches, use anchors to match the whole string
                    compiled[key] = [re.compile(f'^{re.escape(pattern)}$', re.IGNORECASE) for pattern in value]
                else:
                    # For other patterns (suffix, prefix), use as-is
                    compiled[key] = [re.compile(re.escape(pattern), re.IGNORECASE) for pattern in value]
            else:
                compiled[key] = value  # Keep non-list values as-is
        return compiled
    
    def should_exclude_column(self, column_name: str) -> bool:
        """Check if a column should be excluded from inference"""
        column_lower = column_name.lower()
        
        # Check if column starts with underscore (special case)
        if (self.exclude_patterns.get('starts_with_underscore', False) and 
            column_name.startswith('_')):
            return True
        
        # Check prefix exclusions
        for pattern in self.exclude_patterns.get('prefix', []):
            if pattern.search(column_lower):
                return True
        
        # Check suffix exclusions
        for pattern in self.exclude_patterns.get('suffix', []):
            if pattern.search(column_lower):
                return True
        
        # Check exact exclusions
        for pattern in self.exclude_patterns.get('exact', []):
            if pattern.pattern == re.escape(column_lower):
                return True
        
        return False
    
    def infer_time_dimension(self, column: ColumnInfo) -> Optional[Dict[str, Any]]:
        """Infer if a column should be a time dimension"""
        if self.should_exclude_column(column.name):
            return None
        
        column_lower = column.name.lower()
        data_type_lower = column.data_type.lower()
        
        # Check data type first
        time_types = ['date', 'timestamp', 'datetime', 'time']
        is_time_type = any(t in data_type_lower for t in time_types)
        
        # Check naming patterns
        matches_pattern = False
        
        # Check suffixes
        for pattern in self.time_patterns.get('suffix', []):
            if pattern.search(column_lower):
                matches_pattern = True
                break
        
        # Check prefixes
        if not matches_pattern:
            for pattern in self.time_patterns.get('prefix', []):
                if pattern.search(column_lower):
                    matches_pattern = True
                    break
        
        # Check exact matches
        if not matches_pattern:
            for pattern in self.time_patterns.get('exact', []):
                if pattern.pattern == re.escape(column_lower):
                    matches_pattern = True
                    break
        
        if is_time_type or matches_pattern:
            # Determine granularity based on data type and name
            granularity = 'day'  # Default
            
            if 'timestamp' in data_type_lower or '_at' in column_lower or 'datetime' in data_type_lower:
                granularity = 'second'
            elif 'time' in column_lower:
                granularity = 'hour'
            elif 'week' in column_lower:
                granularity = 'week'
            elif 'month' in column_lower:
                granularity = 'month'
            elif 'year' in column_lower:
                granularity = 'year'
            
            return {
                'name': column.name,
                'type': 'time',
                'type_params': {
                    'time_granularity': granularity
                },
                'expr': column.name,
                'description': f"Time dimension inferred from {column.name}",
                '_inferred': True
            }
        
        return None
    
    def infer_categorical_dimension(self, column: ColumnInfo) -> Optional[Dict[str, Any]]:
        """Infer if a column should be a categorical dimension"""
        if self.should_exclude_column(column.name):
            return None
        
        column_lower = column.name.lower()
        data_type_lower = column.data_type.lower()
        
        # Check if it's a string-like type or has low cardinality
        is_string_type = any(t in data_type_lower for t in ['varchar', 'char', 'text', 'string'])
        is_low_cardinality = (column.cardinality is not None and 
                             column.cardinality <= self.categorical_patterns.get('max_cardinality', 100))
        
        # Check boolean patterns (these are not compiled to regex)
        boolean_keywords = self.config.categorical_patterns.get('boolean_keywords', [])
        is_boolean = any(keyword in column_lower for keyword in boolean_keywords)
        
        # Check naming patterns
        matches_pattern = False
        
        # Check suffixes
        for pattern in self.categorical_patterns.get('suffix', []):
            if pattern.search(column_lower):
                matches_pattern = True
                break
        
        # Check prefixes
        if not matches_pattern:
            for pattern in self.categorical_patterns.get('prefix', []):
                if pattern.search(column_lower):
                    matches_pattern = True
                    break
        
        if (is_string_type and matches_pattern) or is_low_cardinality or is_boolean:
            return {
                'name': column.name,
                'type': 'categorical',
                'expr': column.name,
                'description': f"Categorical dimension inferred from {column.name}",
                '_inferred': True
            }
        
        return None
    
    def infer_entity(self, column: ColumnInfo) -> Optional[Dict[str, Any]]:
        """Infer if a column should be an entity (primary or foreign key)"""
        if self.should_exclude_column(column.name):
            return None
        
        column_lower = column.name.lower()
        
        # Check if it's marked as a key in schema
        if column.is_primary_key:
            return {
                'name': column.name,
                'type': 'primary',
                'expr': column.name,
                'description': f"Primary key inferred from {column.name}",
                '_inferred': True
            }
        
        if column.is_foreign_key:
            return {
                'name': column.name,
                'type': 'foreign',
                'expr': column.name,
                'description': f"Foreign key inferred from {column.name}",
                '_inferred': True
            }
        
        # Check exclude words first - only exclude if column name is primarily about these concepts
        exclude_words = self.config.entity_patterns.get('exclude_words', [])
        # Only exclude if the column name starts with or is exactly these words
        if any(column_lower.startswith(word + '_') or column_lower == word for word in exclude_words):
            return None
        
        # Check exact primary key matches first
        for pattern in self.entity_patterns.get('primary_exact', []):
            if pattern.search(column_lower):
                return {
                    'name': column.name,
                    'type': 'primary',
                    'expr': column.name,
                    'description': f"Primary key inferred from {column.name}",
                    '_inferred': True
                }
        
        # Check primary key suffix patterns
        for pattern in self.entity_patterns.get('primary_suffix', []):
            if pattern.search(column_lower):
                # For suffix patterns, we're more selective about what's primary vs foreign
                # Generally, only table-specific IDs should be primary
                # This is harder to determine without table context, so we'll be conservative
                pass  # Skip primary inference for suffix patterns for now
        
        # Check foreign key patterns
        for pattern in self.entity_patterns.get('foreign_suffix', []):
            if pattern.search(column_lower):
                # If it's not just 'id', likely foreign key
                if column_lower != 'id':
                    return {
                        'name': column.name,
                        'type': 'foreign',
                        'expr': column.name,
                        'description': f"Foreign key inferred from {column.name}",
                        '_inferred': True
                    }
        
        return None
    
    def infer_measure(self, column: ColumnInfo) -> Optional[Dict[str, Any]]:
        """Infer if a column should be a measure"""
        if self.should_exclude_column(column.name):
            return None
        
        column_lower = column.name.lower()
        data_type_lower = column.data_type.lower()
        
        # Check if it's a numeric type
        is_numeric = any(t in data_type_lower for t in ['int', 'float', 'decimal', 'numeric', 'double', 'number'])
        
        if not is_numeric:
            return None
        
        # Check naming patterns
        matches_pattern = False
        agg_type = 'sum'  # Default aggregation
        
        # Check suffixes
        for pattern in self.numeric_patterns.get('suffix', []):
            if pattern.search(column_lower):
                matches_pattern = True
                # Determine aggregation type based on name
                if 'count' in column_lower or 'quantity' in column_lower:
                    agg_type = 'sum'
                elif 'avg' in column_lower or 'average' in column_lower:
                    agg_type = 'avg'
                elif 'max' in column_lower or 'maximum' in column_lower:
                    agg_type = 'max'
                elif 'min' in column_lower or 'minimum' in column_lower:
                    agg_type = 'min'
                break
        
        # Check prefixes
        if not matches_pattern:
            for pattern in self.numeric_patterns.get('prefix', []):
                if pattern.search(column_lower):
                    matches_pattern = True
                    break
        
        # Check exact matches
        if not matches_pattern:
            for pattern in self.numeric_patterns.get('exact', []):
                if pattern.search(column_lower):
                    matches_pattern = True
                    if column_lower == 'count':
                        agg_type = 'sum'
                    break
        
        if matches_pattern:
            return {
                'name': f"{column.name}_{agg_type}",
                'agg': agg_type,
                'expr': column.name,
                'description': f"Measure inferred from {column.name}",
                '_inferred': True
            }
        
        return None
    
    def infer_semantic_model(self, table_name: str, columns: List[ColumnInfo], 
                           config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Infer a complete semantic model from table schema
        
        Args:
            table_name: Name of the table
            columns: List of column information
            config: Optional semantic model specific configuration
            
        Returns:
            Dictionary representing the inferred semantic model
        """
        if not self.config.enabled:
            return {}
        
        config = config or {}
        
        # Initialize result structure
        semantic_model = {
            'name': table_name,
            'description': f"Auto-inferred semantic model for {table_name}",
            'source': table_name,
            '_inferred': True
        }
        
        # Infer entities, dimensions, and measures
        entities = []
        dimensions = []
        measures = []
        
        for column in columns:
            # Try to infer entity
            entity = self.infer_entity(column)
            if entity:
                entities.append(entity)
                continue  # Don't also treat as dimension/measure
            
            # Try to infer time dimension
            time_dim = self.infer_time_dimension(column)
            if time_dim:
                dimensions.append(time_dim)
                continue
            
            # Try to infer categorical dimension
            cat_dim = self.infer_categorical_dimension(column)
            if cat_dim:
                dimensions.append(cat_dim)
                continue
            
            # Try to infer measure
            measure = self.infer_measure(column)
            if measure:
                measures.append(measure)
        
        # Add inferred components if any were found
        if entities:
            semantic_model['entities'] = entities
        if dimensions:
            semantic_model['dimensions'] = dimensions
        if measures:
            # Add time dimension to measures if we have any time dimensions
            time_dims = [d for d in dimensions if d.get('type') == 'time']
            if time_dims:
                primary_time_dim = time_dims[0]['name']  # Use first time dimension
                for measure in measures:
                    if 'agg_time_dimension' not in measure:
                        measure['agg_time_dimension'] = primary_time_dim
            semantic_model['measures'] = measures
        
        # Apply any manual overrides from config
        exclude_columns = config.get('exclude_columns', [])
        if exclude_columns:
            # Remove excluded columns from all inferred components
            semantic_model = self._apply_column_exclusions(semantic_model, exclude_columns)
        
        # Apply dimension overrides
        if 'time_dimensions' in config:
            time_config = config['time_dimensions']
            if 'from_columns' in time_config:
                # Add specific time dimensions
                for col_name in time_config['from_columns']:
                    column = next((c for c in columns if c.name == col_name), None)
                    if column:
                        time_dim = self.infer_time_dimension(column)
                        if time_dim and time_dim not in dimensions:
                            dimensions.append(time_dim)
        
        return semantic_model
    
    def _apply_column_exclusions(self, semantic_model: Dict[str, Any], 
                                exclude_columns: List[str]) -> Dict[str, Any]:
        """Remove excluded columns from inferred semantic model"""
        for section in ['entities', 'dimensions', 'measures']:
            if section in semantic_model:
                semantic_model[section] = [
                    item for item in semantic_model[section]
                    if item.get('name') not in exclude_columns and 
                       item.get('expr') not in exclude_columns
                ]
        return semantic_model


def create_column_info_from_dict(column_dict: Dict[str, Any]) -> ColumnInfo:
    """Helper function to create ColumnInfo from dictionary"""
    return ColumnInfo(
        name=column_dict['name'],
        data_type=column_dict.get('data_type', 'unknown'),
        is_nullable=column_dict.get('is_nullable', True),
        max_length=column_dict.get('max_length'),
        is_primary_key=column_dict.get('is_primary_key', False),
        is_foreign_key=column_dict.get('is_foreign_key', False),
        cardinality=column_dict.get('cardinality'),
        sample_values=column_dict.get('sample_values', [])
    )


def infer_from_schema_dict(table_name: str, schema: Dict[str, Any], 
                          config: Optional[InferenceConfig] = None) -> Dict[str, Any]:
    """
    Convenience function to infer semantic model from schema dictionary
    
    Args:
        table_name: Name of the table
        schema: Dictionary with 'columns' key containing column information
        config: Optional inference configuration
        
    Returns:
        Inferred semantic model dictionary
    """
    engine = AutoInferenceEngine(config)
    columns = [create_column_info_from_dict(col) for col in schema.get('columns', [])]
    return engine.infer_semantic_model(table_name, columns)