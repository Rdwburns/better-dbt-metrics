"""
Advanced YAML Parser for Better-DBT-Metrics
Supports imports, references ($ref, $use), and template inheritance
"""

import yaml
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
import re
from copy import deepcopy


@dataclass
class Import:
    """Represents an import statement"""
    path: str
    alias: Optional[str] = None
    items: List[str] = field(default_factory=list)  # Specific items to import


@dataclass
class Reference:
    """Represents a reference ($ref or $use)"""
    ref_type: str  # 'ref' or 'use'
    path: str
    original_value: Any


class BetterDBTParser:
    """
    Advanced parser that handles:
    - Import statements with aliases
    - References ($ref, $use)
    - Template inheritance (extends)
    - Variable substitution
    """
    
    def __init__(self, base_dir: str = ".", debug: bool = False, import_mappings: Optional[Dict[str, str]] = None, search_paths: Optional[List[str]] = None):
        self.base_dir = Path(base_dir)
        self.imports_cache: Dict[str, Any] = {}
        self.current_file: Optional[Path] = None
        self.import_stack: Set[str] = set()  # Prevent circular imports
        self.current_data: Dict[str, Any] = {}  # Store current file data for access by compiler
        self.debug = debug
        self.import_mappings = import_mappings or {}  # Map import aliases to paths
        self.search_paths = search_paths or []  # Additional paths to search for imports
        
    def parse_file(self, file_path: str) -> Dict[str, Any]:
        """Parse a better-dbt-metrics YAML file with all advanced features"""
        file_path = Path(file_path)
        if not file_path.is_absolute():
            file_path = self.base_dir / file_path
            
        # Security: Validate path to prevent directory traversal attacks
        abs_path = file_path.resolve()
        base_abs = self.base_dir.resolve()
        
        # Check if path is within base directory or a parent (for ../ imports)
        try:
            abs_path.relative_to(base_abs)
        except ValueError:
            # Path is outside base - check if it's a parent directory
            try:
                base_abs.relative_to(abs_path)
            except ValueError:
                # Not a parent either - check for sensitive locations
                sensitive_paths = ['/etc', '/root', '~/.ssh', '~/.aws', '/proc', '/sys', '/private/etc']
                path_str = str(abs_path)
                for sensitive in sensitive_paths:
                    expanded = os.path.expanduser(sensitive)
                    if path_str.startswith(expanded) or path_str.startswith(sensitive):
                        raise ValueError(f"Security error: Cannot read from {sensitive}")
            
        # Check for circular imports
        abs_path_str = str(abs_path)
        if abs_path_str in self.import_stack:
            raise ValueError(f"Circular import detected: {abs_path_str}")
            
        self.import_stack.add(abs_path_str)
        self.current_file = file_path
        
        try:
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)
                
            if not isinstance(data, dict):
                raise ValueError(f"File {file_path} must contain a YAML dictionary")
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")
        except PermissionError:
            raise PermissionError(f"Permission denied reading file: {file_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {file_path}: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Error reading file {file_path}: {str(e)}")
                
        try:
            # Process imports first
            if 'imports' in data:
                self._process_imports(data['imports'], file_path.parent)
                
            # Process the rest of the document
            processed_data = self._process_references(data)
            
            # Process table references in source fields
            processed_data = self._process_table_references(processed_data)
            
            # Handle template inheritance
            if 'metrics' in processed_data:
                processed_data['metrics'] = self._process_metric_inheritance(processed_data['metrics'])
                
            # Store current data for compiler access
            self.current_data = processed_data
                
            return processed_data
            
        finally:
            self.import_stack.remove(abs_path_str)
            
    def _process_imports(self, imports: List[Any], base_dir: Path):
        """Process import statements and load imported files"""
        for import_item in imports:
            if isinstance(import_item, str):
                # Simple import: "path/to/file.yml"
                self._load_import(import_item, base_dir)
            elif isinstance(import_item, dict):
                # Import with alias: {"path/to/file.yml": "alias"}
                for path, alias in import_item.items():
                    self._load_import(path, base_dir, alias)
            else:
                raise ValueError(f"Invalid import format: {import_item}")
                
    def _load_import(self, import_path: str, base_dir: Path, alias: Optional[str] = None):
        """Load a single import"""
        # Parse import path and alias
        if ' as ' in import_path:
            path_part, alias_part = import_path.split(' as ', 1)
            import_path = path_part.strip()
            alias = alias_part.strip()
        
        if self.debug:
            print(f"[DEBUG] Attempting to import: {import_path} (from {base_dir})")
            if self.search_paths:
                print(f"[DEBUG] Configured search paths: {self.search_paths}")
        
        # Check if import path matches any configured mappings
        if import_path in self.import_mappings:
            mapped_path = self.import_mappings[import_path]
            if self.debug:
                print(f"[DEBUG] Found import mapping: {import_path} -> {mapped_path}")
            import_path = mapped_path
            
        # Resolve import paths - try multiple strategies
        full_path = None
        
        # Handle _base.templates style imports first
        if import_path.startswith('_base.') or import_path.startswith('_base/'):
            # Convert _base.templates to _base/templates
            normalized_path = import_path.replace('.', '/')
            # Try multiple base paths for _base imports
            for base in [base_dir, base_dir.parent, self.base_dir, self.base_dir / "metrics"]:
                candidate = (base / normalized_path).resolve()
                if not candidate.suffix:
                    candidate = candidate.with_suffix('.yml')
                if candidate.exists():
                    full_path = candidate
                    break
        
        if not full_path:
            # Strategy 1: Relative to current file's directory
            if import_path.startswith('.'):
                candidate = (base_dir / import_path[1:]).resolve()
            else:
                candidate = (base_dir / import_path).resolve()
                
            if not candidate.suffix:
                candidate = candidate.with_suffix('.yml')
                
            if candidate.exists():
                full_path = candidate
        
        if not full_path:
            # Strategy 2: Relative to base directory
            candidate = (self.base_dir / import_path).resolve()
            if not candidate.suffix:
                candidate = candidate.with_suffix('.yml')
                
            if candidate.exists():
                full_path = candidate
            else:
                # Strategy 3: Check configured search paths first
                search_paths = []
                for search_path in self.search_paths:
                    search_base = Path(search_path)
                    if not search_base.is_absolute():
                        search_base = self.base_dir / search_base
                    search_candidate = search_base / import_path
                    search_paths.append(search_candidate)
                    if self.debug:
                        print(f"[DEBUG] Added search path: {search_candidate}")
                
                # Strategy 4: Check common template locations
                common_paths = search_paths + [
                    self.base_dir / "templates" / import_path,
                    self.base_dir / "_base" / import_path,
                    self.base_dir / "shared" / import_path,
                    self.base_dir / "metrics" / "_base" / import_path,  # For metrics/_base structure
                ]
                
                for candidate in common_paths:
                    if not candidate.suffix:
                        candidate = candidate.with_suffix('.yml')
                    if candidate.exists():
                        full_path = candidate
                        break
        
        if not full_path:
            if self.debug:
                print(f"[DEBUG] Tried paths:")
                print(f"[DEBUG]   - {candidate}")
                for path in common_paths:
                    print(f"[DEBUG]   - {path}")
            raise FileNotFoundError(f"Import file not found: {import_path}")
            
        # Load the imported file
        imported_data = self.parse_file(str(full_path))
        
        # Store in cache with appropriate namespace
        cache_key = alias if alias else str(full_path)
        self.imports_cache[cache_key] = imported_data
        
    def _process_references(self, data: Any) -> Any:
        """Recursively process $ref and $use references"""
        if isinstance(data, dict):
            processed = {}
            for key, value in data.items():
                if key == '$ref':
                    # Reference to a specific value
                    return self._resolve_reference(value, 'ref')
                elif key == '$use':
                    # Use/merge a group of values
                    resolved = self._resolve_reference(value, 'use')
                    if isinstance(resolved, dict):
                        processed.update(resolved)
                    elif isinstance(resolved, list):
                        return resolved
                    else:
                        raise ValueError(f"$use can only reference dicts or lists, got {type(resolved)}")
                else:
                    processed[key] = self._process_references(value)
            return processed
        elif isinstance(data, list):
            return [self._process_references(item) for item in data]
        else:
            return data
            
    def _resolve_reference(self, ref_path: str, ref_type: str) -> Any:
        """Resolve a reference path like 'alias.path.to.value'"""
        parts = ref_path.split('.')
        
        # Check if it's an aliased import
        if parts[0] in self.imports_cache:
            current = self.imports_cache[parts[0]]
            parts = parts[1:]
        else:
            # Try to match import paths for _base references
            if parts[0] == '_base' and len(parts) > 1:
                # Look for imports that end with the second part
                # e.g., _base.dimension_groups -> look for imports ending with dimension_groups
                for cache_key, data in self.imports_cache.items():
                    if cache_key.endswith(parts[1]) or cache_key.endswith(f"/{parts[1]}"):
                        current = data
                        parts = parts[2:]  # Skip _base and dimension_groups
                        break
                else:
                    # Not found, return reference for compiler
                    return {'$ref': ref_path}
            else:
                # Check current document (not yet implemented fully)
                # For now, we'll let the compiler handle these references
                return {'$ref': ref_path}
            
        # Navigate the path
        for part in parts:
            if current is None:
                # Instead of failing, return the reference for later resolution
                return {'$ref': ref_path}
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                # Instead of failing, return the reference for later resolution
                return {'$ref': ref_path}
                
        return deepcopy(current)  # Return a copy to avoid mutations
    
    def _process_table_references(self, data: Any) -> Any:
        """Process table references in source fields"""
        if isinstance(data, dict):
            # Process source field with table reference
            if 'source' in data:
                source_value = data['source']
                # Handle table reference format: ref('table_name') or $table('table_name')
                if isinstance(source_value, str):
                    # Check for ref() format
                    ref_match = re.match(r"ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", source_value)
                    if ref_match:
                        table_name = ref_match.group(1)
                        data['source'] = table_name
                        data['source_ref'] = {'table': table_name, 'type': 'ref'}
                    # Check for $table() format
                    table_match = re.match(r"\$table\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", source_value)
                    if table_match:
                        table_name = table_match.group(1)
                        data['source'] = table_name
                        data['source_ref'] = {'table': table_name, 'type': 'table'}
                elif isinstance(source_value, dict):
                    # Handle dict format: {$table: 'table_name'} or {ref: 'table_name'}
                    if '$table' in source_value:
                        table_name = source_value['$table']
                        data['source'] = table_name
                        data['source_ref'] = {'table': table_name, 'type': 'table'}
                    elif 'ref' in source_value:
                        table_name = source_value['ref']
                        data['source'] = table_name
                        data['source_ref'] = {'table': table_name, 'type': 'ref'}
            
            # Recursively process nested structures
            # Create a copy of keys to avoid dictionary changed size during iteration error
            for key in list(data.keys()):
                if key != 'source':  # Don't reprocess source
                    data[key] = self._process_table_references(data[key])
            return data
        elif isinstance(data, list):
            return [self._process_table_references(item) for item in data]
        else:
            return data
        
    def _process_metric_inheritance(self, metrics: List[Dict]) -> List[Dict]:
        """Process metric inheritance (extends and template)"""
        processed_metrics = []
        
        for metric in metrics:
            if 'extends' in metric or 'template' in metric:
                processed = self._apply_inheritance(metric)
                processed_metrics.append(processed)
            else:
                processed_metrics.append(metric)
                
        return processed_metrics
        
    def _apply_inheritance(self, metric: Dict) -> Dict:
        """Apply template or extends inheritance to a metric"""
        result = {}
        
        # Handle extends (single inheritance)
        if 'extends' in metric:
            parent_ref = metric['extends']
            parent = self._resolve_reference(parent_ref, 'ref')
            if not isinstance(parent, dict):
                raise ValueError(f"Extended metric must be a dict, got {type(parent)}")
            result.update(deepcopy(parent))
            
        # Handle template with parameters
        if 'template' in metric:
            # Don't try to resolve template references - leave them for the compiler
            # Just copy the metric as-is with the template field
            result.update(deepcopy(metric))
            return result
            
        # Apply metric's own properties (override inherited)
        for key, value in metric.items():
            if key not in ['extends', 'template', 'params', 'parameters']:
                if key == 'dimensions' and 'dimensions' in result:
                    # Special handling for dimensions - merge instead of replace
                    result['dimensions'] = self._merge_dimensions(result.get('dimensions', []), value)
                else:
                    result[key] = value
                    
        return result
        
    def _merge_dimensions(self, base_dims: List[Any], new_dims: List[Any]) -> List[Any]:
        """Intelligently merge dimension lists"""
        # Convert to a standardized format for comparison
        def normalize_dim(dim):
            if isinstance(dim, str):
                return {'name': dim}
            return dim
            
        # Track dimension names to avoid duplicates
        dim_names = set()
        merged = []
        
        # Add base dimensions
        for dim in base_dims:
            norm_dim = normalize_dim(dim)
            if 'name' in norm_dim:
                dim_names.add(norm_dim['name'])
                merged.append(dim)
                
        # Add new dimensions (skip duplicates)
        for dim in new_dims:
            norm_dim = normalize_dim(dim)
            if 'name' in norm_dim and norm_dim['name'] not in dim_names:
                merged.append(dim)
                
        return merged


class ReferenceResolver:
    """Resolves $ref and $use references after parsing"""
    
    def __init__(self, parsed_data: Dict[str, Any], imports_cache: Dict[str, Any]):
        self.data = parsed_data
        self.imports = imports_cache
        
    def resolve_all(self) -> Dict[str, Any]:
        """Resolve all references in the parsed data"""
        return self._resolve_recursive(self.data)
        
    def _resolve_recursive(self, obj: Any) -> Any:
        """Recursively resolve references"""
        if isinstance(obj, dict):
            if '$ref' in obj:
                return self._get_value(obj['$ref'])
            elif '$use' in obj:
                used = self._get_value(obj['$use'])
                if isinstance(used, dict):
                    result = {}
                    for k, v in obj.items():
                        if k != '$use':
                            result[k] = self._resolve_recursive(v)
                    result.update(used)
                    return result
                return used
            else:
                return {k: self._resolve_recursive(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._resolve_recursive(item) for item in obj]
        return obj
        
    def _get_value(self, path: str) -> Any:
        """Get value from a dot-separated path"""
        parts = path.split('.')
        
        # Start from imports or root data
        if parts[0] in self.imports:
            current = self.imports[parts[0]]
            parts = parts[1:]
        else:
            current = self.data
            
        # Navigate path
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                raise ValueError(f"Cannot resolve path: {path}")
                
        return deepcopy(current)


# Convenience function
def parse_metrics_file(file_path: str, base_dir: str = ".") -> Dict[str, Any]:
    """Parse a metrics-first YAML file with all features enabled"""
    parser = BetterDBTParser(base_dir)
    return parser.parse_file(file_path)