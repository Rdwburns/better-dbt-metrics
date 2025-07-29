"""
Dimension Groups Management for Better-DBT-Metrics
Enables DRY dimension definitions through reusable groups
"""

import yaml
from typing import Dict, List, Any, Optional, Set, Union
from dataclasses import dataclass, field
from pathlib import Path
from copy import deepcopy


@dataclass
class Dimension:
    """Represents a single dimension"""
    name: str
    type: str  # categorical, time, number
    description: Optional[str] = None
    grain: Optional[str] = None  # For time dimensions
    source: Optional[str] = None  # Column or expression
    expr: Optional[str] = None  # SQL expression
    label: Optional[str] = None  # Display name
    

@dataclass
class DimensionGroup:
    """Represents a group of related dimensions"""
    name: str
    description: str
    dimensions: List[Dimension]
    extends: Optional[List[str]] = None  # Other groups to inherit from
    metadata: Dict[str, Any] = field(default_factory=dict)
    

class DimensionGroupManager:
    """
    Manages dimension groups for reuse across metrics
    Features:
    - Group inheritance (extends)
    - Dimension merging
    - Auto-discovery from sources
    - Standard dimension libraries
    """
    
    def __init__(self):
        self.groups: Dict[str, DimensionGroup] = {}
        self._inheritance_resolved = False
        
    def register_group(self, name: str, group_def: Dict[str, Any]):
        """Register a dimension group"""
        # Handle list format by converting to dict
        if isinstance(group_def, list):
            group_def = {
                'name': name,
                'dimensions': group_def,
                'description': f'Dimension group: {name}'
            }
        elif not isinstance(group_def, dict):
            raise TypeError(f"Dimension group '{name}' definition must be a dictionary or list, got {type(group_def)}")
            
        dimensions = []
        
        # Parse dimensions
        for dim_def in group_def.get('dimensions', []):
            dimension = self._parse_dimension(dim_def)
            dimensions.append(dimension)
            
        # Create group
        group = DimensionGroup(
            name=name,
            description=group_def.get('description', ''),
            dimensions=dimensions,
            extends=group_def.get('extends', []),
            metadata=group_def.get('metadata', {})
        )
        
        self.groups[name] = group
        self._inheritance_resolved = False
        
    def _parse_dimension(self, dim_def: Union[str, Dict[str, Any]]) -> Dimension:
        """Parse a dimension definition"""
        if isinstance(dim_def, str):
            # Simple string dimension
            return Dimension(
                name=dim_def,
                type='categorical'  # Default type
            )
        elif isinstance(dim_def, dict):
            # Full dimension definition
            return Dimension(
                name=dim_def['name'],
                type=dim_def.get('type', 'categorical'),
                description=dim_def.get('description'),
                grain=dim_def.get('grain'),
                source=dim_def.get('source'),
                expr=dim_def.get('expr'),
                label=dim_def.get('label')
            )
        else:
            raise ValueError(f"Invalid dimension definition: {dim_def}")
            
    def resolve_inheritance(self):
        """Resolve all group inheritance (extends)"""
        if self._inheritance_resolved:
            return
            
        # Topological sort to handle dependencies
        sorted_groups = self._topological_sort()
        
        # Resolve inheritance in order
        for group_name in sorted_groups:
            group = self.groups[group_name]
            if group.extends:
                inherited_dims = []
                
                # Collect dimensions from parent groups
                for parent_name in group.extends:
                    if parent_name not in self.groups:
                        raise ValueError(
                            f"Group '{group_name}' extends unknown group '{parent_name}'"
                        )
                    parent = self.groups[parent_name]
                    inherited_dims.extend(parent.dimensions)
                    
                # Merge with own dimensions
                group.dimensions = self._merge_dimensions(inherited_dims, group.dimensions)
                
        self._inheritance_resolved = True
        
    def _topological_sort(self) -> List[str]:
        """Topologically sort groups based on inheritance"""
        visited = set()
        stack = []
        
        def visit(name: str):
            if name in visited:
                return
            visited.add(name)
            
            group = self.groups.get(name)
            if group and group.extends:
                for parent in group.extends:
                    if parent in self.groups:
                        visit(parent)
                        
            stack.append(name)
            
        # Visit all groups
        for name in self.groups:
            visit(name)
            
        return stack
        
    def _merge_dimensions(self, base_dims: List[Dimension], 
                         new_dims: List[Dimension]) -> List[Dimension]:
        """Merge dimension lists, avoiding duplicates"""
        # Use dict to track by name
        merged = {}
        
        # Add base dimensions
        for dim in base_dims:
            merged[dim.name] = deepcopy(dim)
            
        # Add/override with new dimensions
        for dim in new_dims:
            merged[dim.name] = dim
            
        return list(merged.values())
        
    def get_dimensions(self, group_name: str) -> List[Dimension]:
        """Get all dimensions from a group"""
        group = self.get_group(group_name)
        return group.dimensions
        
    def expand_dimension_reference(self, ref: Union[str, Dict, List]) -> List[Dict[str, Any]]:
        """
        Expand a dimension reference to full dimension definitions
        Handles:
        - String: "customer_segment" 
        - Group reference: {"$ref": "groups.customer"}
        - List of mixed types
        """
        if isinstance(ref, str):
            # Simple dimension name
            return [{'name': ref}]
            
        elif isinstance(ref, dict):
            if '$ref' in ref:
                # Reference to dimension group
                ref_path = ref['$ref']
                if ref_path.startswith('dimension_groups.'):
                    group_name = ref_path.split('.', 1)[1]
                else:
                    # Allow simple group name reference
                    group_name = ref_path
                
                try:
                    group = self.get_group(group_name)
                    return [self._dimension_to_dict(dim) for dim in group.dimensions]
                except ValueError:
                    # Not a dimension group, might be a specific dimension reference
                    # Return as-is for the compiler to handle
                    return [ref]
                    
            elif '$use' in ref:
                # Use dimension group
                group_name = ref['$use']
                group = self.get_group(group_name)
                return [self._dimension_to_dict(dim) for dim in group.dimensions]
                
            else:
                # Regular dimension dict
                return [ref]
                
        elif isinstance(ref, list):
            # List of dimensions
            expanded = []
            for item in ref:
                expanded.extend(self.expand_dimension_reference(item))
            return expanded
            
        else:
            raise ValueError(f"Invalid dimension reference: {ref}")
            
    def _dimension_to_dict(self, dim: Dimension) -> Dict[str, Any]:
        """Convert Dimension object to dict"""
        result = {'name': dim.name, 'type': dim.type}
        
        if dim.description:
            result['description'] = dim.description
        if dim.grain:
            result['grain'] = dim.grain
        if dim.source:
            result['source'] = dim.source
        if dim.expr:
            result['expr'] = dim.expr
        if dim.label:
            result['label'] = dim.label
            
        return result
        
    def get_dimensions_for_group(self, group_name: str) -> List[Dict[str, Any]]:
        """Get dimensions for a specific group"""
        if group_name not in self.groups:
            # Try without the full path prefix
            available_groups = list(self.groups.keys())
            for key in available_groups:
                if key.endswith(f".{group_name}"):
                    group_name = key
                    break
            else:
                raise ValueError(f"Dimension group '{group_name}' not found. Available groups: {available_groups}")
            
        # Ensure inheritance is resolved
        if not self._inheritance_resolved:
            self.resolve_inheritance()
            
        group = self.groups[group_name]
        
        # Convert Dimension objects to dicts
        dimensions = []
        for dim in group.dimensions:
            dimensions.append(self._dimension_to_dict(dim))
        
        return dimensions
        
    def get_group(self, group_name: str) -> DimensionGroup:
        """Get a dimension group by name"""
        if group_name not in self.groups:
            raise ValueError(f"Dimension group '{group_name}' not found")
            
        # Ensure inheritance is resolved
        if not self._inheritance_resolved:
            self.resolve_inheritance()
            
        return self.groups[group_name]
        
    def get_dimensions_for_metric(self, metric: Dict[str, Any]) -> List[Dimension]:
        """Get all dimensions for a metric (from groups and direct dimensions)"""
        dimensions = []
        dimension_names = set()  # Track to avoid duplicates
        
        # Add dimensions from dimension groups
        if 'dimension_groups' in metric:
            for group_name in metric['dimension_groups']:
                group_dims = self.get_dimensions_for_group(group_name)
                for dim_dict in group_dims:
                    dim = self._parse_dimension(dim_dict)
                    if dim.name not in dimension_names:
                        dimensions.append(dim)
                        dimension_names.add(dim.name)
                        
        # Add direct dimensions
        if 'dimensions' in metric:
            for dim_def in metric['dimensions']:
                dim = self._parse_dimension(dim_def)
                if dim.name not in dimension_names:
                    dimensions.append(dim)
                    dimension_names.add(dim.name)
                    
        return dimensions


class DimensionLibrary:
    """
    Pre-built dimension libraries that ship with the package
    """
    
    @staticmethod
    def temporal_dimensions() -> Dict[str, Any]:
        """Standard temporal dimensions"""
        return {
            'dimension_groups': {
                'daily': {
                    'description': 'Daily time dimensions',
                    'dimensions': [
                        {
                            'name': 'date_day',
                            'type': 'time',
                            'grain': 'day',
                            'expr': "DATE_TRUNC('day', {TIME_COLUMN})"
                        },
                        {
                            'name': 'date_week', 
                            'type': 'time',
                            'grain': 'week',
                            'expr': "DATE_TRUNC('week', {TIME_COLUMN})"
                        },
                        {
                            'name': 'date_month',
                            'type': 'time', 
                            'grain': 'month',
                            'expr': "DATE_TRUNC('month', {TIME_COLUMN})"
                        },
                        {
                            'name': 'date_quarter',
                            'type': 'time',
                            'grain': 'quarter',
                            'expr': "DATE_TRUNC('quarter', {TIME_COLUMN})"
                        },
                        {
                            'name': 'date_year',
                            'type': 'time',
                            'grain': 'year',
                            'expr': "DATE_TRUNC('year', {TIME_COLUMN})"
                        }
                    ]
                },
                
                'hourly': {
                    'description': 'Hourly time dimensions',
                    'extends': ['daily'],
                    'dimensions': [
                        {
                            'name': 'date_hour',
                            'type': 'time',
                            'grain': 'hour',
                            'expr': "DATE_TRUNC('hour', {TIME_COLUMN})"
                        },
                        {
                            'name': 'hour_of_day',
                            'type': 'number',
                            'expr': "EXTRACT(hour FROM {TIME_COLUMN})"
                        }
                    ]
                },
                
                'fiscal': {
                    'description': 'Fiscal calendar dimensions',
                    'dimensions': [
                        {
                            'name': 'fiscal_year',
                            'type': 'time',
                            'grain': 'year',
                            'expr': """
                                CASE 
                                    WHEN EXTRACT(month FROM {TIME_COLUMN}) >= 4 
                                    THEN EXTRACT(year FROM {TIME_COLUMN})
                                    ELSE EXTRACT(year FROM {TIME_COLUMN}) - 1
                                END
                            """
                        },
                        {
                            'name': 'fiscal_quarter',
                            'type': 'time',
                            'grain': 'quarter',
                            'expr': """
                                'Q' || CASE
                                    WHEN EXTRACT(month FROM {TIME_COLUMN}) BETWEEN 4 AND 6 THEN '1'
                                    WHEN EXTRACT(month FROM {TIME_COLUMN}) BETWEEN 7 AND 9 THEN '2'
                                    WHEN EXTRACT(month FROM {TIME_COLUMN}) BETWEEN 10 AND 12 THEN '3'
                                    ELSE '4'
                                END
                            """
                        }
                    ]
                }
            }
        }
        
    @staticmethod
    def geographic_dimensions() -> Dict[str, Any]:
        """Standard geographic dimensions"""
        return {
            'dimension_groups': {
                'country': {
                    'description': 'Country-level geographic dimensions',
                    'dimensions': [
                        {
                            'name': 'country',
                            'type': 'categorical',
                            'source': 'country_code'
                        },
                        {
                            'name': 'country_name',
                            'type': 'categorical',
                            'source': 'country_name'
                        }
                    ]
                },
                
                'full_geography': {
                    'description': 'Full geographic hierarchy',
                    'extends': ['country'],
                    'dimensions': [
                        {
                            'name': 'region',
                            'type': 'categorical',
                            'source': 'region_name'
                        },
                        {
                            'name': 'state',
                            'type': 'categorical',
                            'source': 'state_code'
                        },
                        {
                            'name': 'city',
                            'type': 'categorical',
                            'source': 'city_name'
                        },
                        {
                            'name': 'postal_code',
                            'type': 'categorical',
                            'source': 'postal_code'
                        }
                    ]
                }
            }
        }
        
    @staticmethod
    def customer_dimensions() -> Dict[str, Any]:
        """Standard customer dimensions"""
        return {
            'dimension_groups': {
                'customer_basic': {
                    'description': 'Basic customer attributes',
                    'dimensions': [
                        {
                            'name': 'customer_id',
                            'type': 'categorical',
                            'source': 'customer_id'
                        },
                        {
                            'name': 'customer_type',
                            'type': 'categorical',
                            'source': 'customer_type'
                        }
                    ]
                },
                
                'customer_segment': {
                    'description': 'Customer segmentation dimensions',
                    'extends': ['customer_basic'],
                    'dimensions': [
                        {
                            'name': 'customer_segment',
                            'type': 'categorical',
                            'source': 'segment'
                        },
                        {
                            'name': 'customer_tier',
                            'type': 'categorical',
                            'source': 'tier',
                            'expr': "COALESCE(tier, 'bronze')"
                        },
                        {
                            'name': 'lifetime_value_bucket',
                            'type': 'categorical',
                            'expr': """
                                CASE 
                                    WHEN lifetime_value < 100 THEN 'low'
                                    WHEN lifetime_value < 1000 THEN 'medium'
                                    WHEN lifetime_value < 10000 THEN 'high'
                                    ELSE 'vip'
                                END
                            """
                        }
                    ]
                }
            }
        }