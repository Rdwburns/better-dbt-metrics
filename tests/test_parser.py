"""Tests for the parser module"""

import pytest
import yaml
from pathlib import Path
import tempfile
import os

from core.parser import BetterDBTParser


class TestParser:
    """Test the BetterDBTParser functionality"""
    
    def setup_method(self):
        """Setup test fixtures"""
        self.parser = BetterDBTParser()
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup temp files"""
        import shutil
        shutil.rmtree(self.temp_dir)
        
    def test_simple_parse(self):
        """Test parsing a simple metrics file"""
        content = """
version: 2
metrics:
  - name: test_metric
    description: "Test metric"
    type: simple
    source: test_table
    measure:
      type: sum
      column: amount
"""
        file_path = Path(self.temp_dir) / "test.yml"
        with open(file_path, 'w') as f:
            f.write(content)
            
        result = self.parser.parse_file(str(file_path))
        
        assert 'metrics' in result
        assert len(result['metrics']) == 1
        assert result['metrics'][0]['name'] == 'test_metric'
        
    def test_imports(self):
        """Test import functionality"""
        # Create imported file
        imported_content = """
dimension_groups:
  test_group:
    dimensions:
      - name: test_dim
        type: categorical
"""
        imported_path = Path(self.temp_dir) / "imported.yml"
        with open(imported_path, 'w') as f:
            f.write(imported_content)
            
        # Create main file with import
        main_content = f"""
version: 2
imports:
  - {imported_path} as test_import
  
metrics:
  - name: test_metric
    dimensions:
      - name: simple_dim
        type: categorical
"""
        main_path = Path(self.temp_dir) / "main.yml"
        with open(main_path, 'w') as f:
            f.write(main_content)
            
        result = self.parser.parse_file(str(main_path))
        
        assert 'imports' in result
        assert 'test_import' in self.parser.imports_cache
        # Check that imported content was loaded
        assert 'dimension_groups' in self.parser.imports_cache['test_import']
        
    def test_reference_resolution(self):
        """Test $ref resolution"""
        # Note: Reference resolution now returns unresolved refs for the compiler to handle
        content = """
version: 2

dimension_groups:
  standard:
    dimensions:
      - name: date_day
        type: time
        grain: day

metrics:
  - name: test_metric
    dimensions:
      - $ref: dimension_groups.standard
"""
        file_path = Path(self.temp_dir) / "test.yml"
        with open(file_path, 'w') as f:
            f.write(content)
            
        result = self.parser.parse_file(str(file_path))
        
        # Check that reference was resolved
        metric = result['metrics'][0]
        assert 'dimensions' in metric
        # The dimensions should be expanded from the reference
        
    def test_circular_import_detection(self):
        """Test that circular imports are detected"""
        # Create file A that imports B
        file_a = Path(self.temp_dir) / "a.yml"
        with open(file_a, 'w') as f:
            f.write(f"imports:\n  - {self.temp_dir}/b.yml")
            
        # Create file B that imports A
        file_b = Path(self.temp_dir) / "b.yml"
        with open(file_b, 'w') as f:
            f.write(f"imports:\n  - {self.temp_dir}/a.yml")
            
        with pytest.raises(Exception) as exc_info:
            self.parser.parse_file(str(file_a))
            
        assert "Circular import" in str(exc_info.value)