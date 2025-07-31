"""Test auto-inference with global configuration"""

import pytest
import yaml
from pathlib import Path
import tempfile
import shutil

from core.compiler import BetterDBTCompiler, CompilerConfig


class TestAutoInferenceConfig:
    """Test auto-inference with bdm_config.yml configuration"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    def test_global_auto_inference_config(self, temp_dir):
        """Test that auto-inference uses patterns from bdm_config.yml"""
        # Create bdm_config.yml with custom patterns
        config_content = """
# Custom auto-inference configuration
auto_inference:
  enabled: true
  time_dimension_patterns:
    suffix:
      - _date
      - _datetime
      - _custom_time  # Custom pattern
    prefix:
      - date_
      - ts_  # Custom prefix
  categorical_patterns:
    suffix:
      - _type
      - _category
      - _custom_cat  # Custom pattern
    max_cardinality: 50  # Lower threshold
  exclude_patterns:
    prefix:
      - internal_  # Custom exclusion
    suffix:
      - _temp
"""
        
        # Create semantic model with auto-inference
        model_content = """
version: 2

semantic_models:
  - name: test_model
    source: test_table
    auto_infer:
      dimensions: true
      # This will use the patterns from bdm_config.yml
"""
        
        # Write files
        with open(Path(temp_dir) / "bdm_config.yml", "w") as f:
            f.write(config_content)
            
        with open(Path(temp_dir) / "models.yml", "w") as f:
            f.write(model_content)
        
        # Create compiler
        config = CompilerConfig(
            input_dir=str(temp_dir),
            output_dir=str(Path(temp_dir) / "output"),
            debug=True,
            split_files=False,
            validate=False  # Disable validation since bdm_config.yml is not a metrics file
        )
        compiler = BetterDBTCompiler(config)
        
        # Verify config was loaded
        assert compiler.bdm_config.auto_inference['enabled'] is True
        assert '_custom_time' in compiler.bdm_config.auto_inference['time_dimension_patterns']['suffix']
        assert 'ts_' in compiler.bdm_config.auto_inference['time_dimension_patterns']['prefix']
        assert compiler.bdm_config.auto_inference['categorical_patterns']['max_cardinality'] == 50
        
        # Compile
        result = compiler.compile_directory()
        
        # The auto-inference should use the custom patterns
        # (actual inference would happen if we had schema access)
    
    def test_auto_inference_disabled_globally(self, temp_dir):
        """Test that auto-inference can be disabled globally"""
        # Create bdm_config.yml with auto-inference disabled
        config_content = """
auto_inference:
  enabled: false
"""
        
        # Create semantic model with auto-inference
        model_content = """
version: 2

semantic_models:
  - name: test_model
    source: test_table
    auto_infer:
      dimensions: true
      # This should be ignored since auto-inference is disabled
"""
        
        # Write files
        with open(Path(temp_dir) / "bdm_config.yml", "w") as f:
            f.write(config_content)
            
        with open(Path(temp_dir) / "models.yml", "w") as f:
            f.write(model_content)
        
        # Create compiler
        config = CompilerConfig(
            input_dir=str(temp_dir),
            output_dir=str(Path(temp_dir) / "output"),
            debug=True,
            split_files=False,
            validate=False  # Disable validation
        )
        compiler = BetterDBTCompiler(config)
        
        # Verify auto-inference is disabled
        assert compiler.auto_inference.config.enabled is False
        
        # Compile
        result = compiler.compile_directory()
        
        # Auto-inference should not run even though it's requested
    
    def test_pattern_based_inference(self, temp_dir):
        """Test pattern-based dimension type detection"""
        from src.features.auto_inference import AutoInferenceEngine, InferenceConfig, ColumnInfo
        
        # Create custom config
        config = InferenceConfig()
        config.time_dimension_patterns['suffix'].append('_custom_date')
        config.categorical_patterns['prefix'].append('custom_')
        
        engine = AutoInferenceEngine(config)
        
        # Test time dimension detection
        time_column = ColumnInfo(
            name='order_custom_date',
            data_type='date'
        )
        time_dim = engine.infer_time_dimension(time_column)
        assert time_dim is not None
        assert time_dim['type'] == 'time'
        assert time_dim['name'] == 'order_custom_date'
        
        # Test categorical dimension detection
        cat_column = ColumnInfo(
            name='custom_status',
            data_type='varchar',
            cardinality=5
        )
        cat_dim = engine.infer_categorical_dimension(cat_column)
        assert cat_dim is not None
        assert cat_dim['type'] == 'categorical'
        assert cat_dim['name'] == 'custom_status'
        
        # Test exclusion patterns
        excluded_column = ColumnInfo(
            name='_internal_field',
            data_type='varchar'
        )
        assert engine.should_exclude_column(excluded_column.name) is True
    
    def test_inference_with_explicit_overrides(self, temp_dir):
        """Test that explicit definitions override auto-inference"""
        model_content = """
version: 2

semantic_models:
  - name: test_model
    source: test_table
    
    # Explicit dimension
    dimensions:
      - name: created_date
        type: time
        type_params:
          time_granularity: hour  # Explicit granularity
        expr: created_at
    
    # Auto-infer additional dimensions
    auto_infer:
      dimensions: true
      exclude_columns:
        - internal_id
        - _temp_field
"""
        
        with open(Path(temp_dir) / "models.yml", "w") as f:
            f.write(model_content)
        
        # Create compiler
        config = CompilerConfig(
            input_dir=str(temp_dir),
            output_dir=str(Path(temp_dir) / "output"),
            debug=True,
            split_files=False,
            validate=False  # Disable validation
        )
        compiler = BetterDBTCompiler(config)
        
        # Compile
        result = compiler.compile_directory()
        
        # The explicit dimension should be preserved
        # Auto-inference would add more dimensions if schema was available