"""Tests for dimension groups functionality"""

import pytest
from features.dimension_groups import Dimension, DimensionGroup, DimensionGroupManager


class TestDimensionGroupManager:
    """Test the DimensionGroupManager functionality"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.manager = DimensionGroupManager()
        
    def test_register_dimension_group(self):
        """Test registering a dimension group"""
        group_def = {
            'description': 'Test dimensions',
            'dimensions': [
                {
                    'name': 'test_dim1',
                    'type': 'categorical'
                },
                {
                    'name': 'test_dim2',
                    'type': 'time',
                    'grain': 'day'
                }
            ]
        }
        
        self.manager.register_group('test_group', group_def)
        
        assert 'test_group' in self.manager.groups
        group = self.manager.groups['test_group']
        assert isinstance(group, DimensionGroup)
        assert len(group.dimensions) == 2
        assert group.dimensions[0].name == 'test_dim1'
        assert group.dimensions[1].grain == 'day'
        
    def test_dimension_group_inheritance(self):
        """Test dimension group inheritance with extends"""
        # Register base group
        base_def = {
            'description': 'Base dimensions',
            'dimensions': [
                {'name': 'base_dim1', 'type': 'categorical'},
                {'name': 'base_dim2', 'type': 'categorical'}
            ]
        }
        self.manager.register_group('base', base_def)
        
        # Register extended group
        extended_def = {
            'description': 'Extended dimensions',
            'extends': ['base'],
            'dimensions': [
                {'name': 'extended_dim', 'type': 'categorical'}
            ]
        }
        self.manager.register_group('extended', extended_def)
        
        # Resolve inheritance
        self.manager.resolve_inheritance()
        
        # Check that extended group has all dimensions
        extended_group = self.manager.groups['extended']
        dim_names = [d.name for d in extended_group.dimensions]
        
        assert 'base_dim1' in dim_names
        assert 'base_dim2' in dim_names
        assert 'extended_dim' in dim_names
        assert len(extended_group.dimensions) == 3
        
    def test_expand_dimension_reference(self):
        """Test expanding dimension references"""
        # Register a group
        group_def = {
            'dimensions': [
                {'name': 'date_day', 'type': 'time', 'grain': 'day'},
                {'name': 'date_month', 'type': 'time', 'grain': 'month'}
            ]
        }
        self.manager.register_group('temporal', group_def)
        
        # Test $ref expansion
        ref = {'$ref': 'temporal'}
        result = self.manager.expand_dimension_reference(ref)
        
        assert len(result) == 2
        assert result[0]['name'] == 'date_day'
        assert result[1]['name'] == 'date_month'
        
    def test_get_dimensions_for_metric(self):
        """Test getting dimensions for a metric"""
        # Register groups
        temporal_def = {
            'dimensions': [
                {'name': 'date_day', 'type': 'time', 'grain': 'day'}
            ]
        }
        self.manager.register_group('temporal', temporal_def)
        
        customer_def = {
            'dimensions': [
                {'name': 'customer_segment', 'type': 'categorical'}
            ]
        }
        self.manager.register_group('customer', customer_def)
        
        # Create metric definition
        metric = {
            'name': 'test_metric',
            'dimension_groups': ['temporal', 'customer'],
            'dimensions': [
                {'name': 'product_category', 'type': 'categorical'}
            ]
        }
        
        dimensions = self.manager.get_dimensions_for_metric(metric)
        
        # Should have all dimensions
        dim_names = [d.name for d in dimensions]
        assert 'date_day' in dim_names
        assert 'customer_segment' in dim_names
        assert 'product_category' in dim_names
        assert len(dimensions) == 3
        
    def test_dimension_merging(self):
        """Test that duplicate dimensions are merged properly"""
        # Register overlapping groups
        group1_def = {
            'dimensions': [
                {'name': 'shared_dim', 'type': 'categorical', 'label': 'Shared Dimension'},
                {'name': 'unique_dim1', 'type': 'categorical'}
            ]
        }
        self.manager.register_group('group1', group1_def)
        
        group2_def = {
            'dimensions': [
                {'name': 'shared_dim', 'type': 'categorical'},  # Same name
                {'name': 'unique_dim2', 'type': 'categorical'}
            ]
        }
        self.manager.register_group('group2', group2_def)
        
        # Create metric using both groups
        metric = {
            'name': 'test_metric',
            'dimension_groups': ['group1', 'group2']
        }
        
        dimensions = self.manager.get_dimensions_for_metric(metric)
        
        # Should have merged shared_dim
        dim_names = [d.name for d in dimensions]
        assert dim_names.count('shared_dim') == 1
        assert 'unique_dim1' in dim_names
        assert 'unique_dim2' in dim_names
        assert len(dimensions) == 3
        
        # Check that properties from first definition are preserved
        shared_dim = next(d for d in dimensions if d.name == 'shared_dim')
        assert shared_dim.label == 'Shared Dimension'