"""
DBT project scanner for validating model and source references
"""

import json
import yaml
from pathlib import Path
from typing import Set, Dict, List, Optional, Tuple
import re


class DBTProjectScanner:
    """Scans dbt project for available models, sources, and other resources"""
    
    def __init__(self, project_dir: str = "."):
        self.project_dir = Path(project_dir).resolve()
        self._models_cache: Optional[Set[str]] = None
        self._sources_cache: Optional[Dict[str, Set[str]]] = None
        
    def get_available_models(self) -> Set[str]:
        """Get all available dbt models"""
        if self._models_cache is not None:
            return self._models_cache
            
        models = set()
        
        # Strategy 1: Read from dbt manifest.json if available (most accurate)
        manifest_models = self._get_models_from_manifest()
        if manifest_models:
            models.update(manifest_models)
        else:
            # Strategy 2: Scan models directory structure
            models.update(self._get_models_from_directory())
        
        self._models_cache = models
        return models
    
    def get_available_sources(self) -> Dict[str, Set[str]]:
        """Get all available dbt sources as {source_name: {table_names}}"""
        if self._sources_cache is not None:
            return self._sources_cache
            
        sources = {}
        
        # Strategy 1: From manifest (most accurate)
        manifest_sources = self._get_sources_from_manifest()
        if manifest_sources:
            sources.update(manifest_sources)
        else:
            # Strategy 2: Scan for schema.yml files with sources
            sources.update(self._get_sources_from_schema_files())
        
        self._sources_cache = sources
        return sources
    
    def get_model_path(self, model_name: str) -> Optional[Path]:
        """Get the file path for a specific model"""
        # Try manifest first
        manifest_path = self.project_dir / "target" / "manifest.json"
        if manifest_path.exists():
            try:
                with open(manifest_path) as f:
                    manifest = json.load(f)
                for node_id, node in manifest.get('nodes', {}).items():
                    if (node.get('resource_type') == 'model' and 
                        node.get('name') == model_name):
                        original_file_path = node.get('original_file_path')
                        if original_file_path:
                            return self.project_dir / original_file_path
            except Exception:
                pass
        
        # Fallback: search models directory
        models_dir = self.project_dir / "models"
        if models_dir.exists():
            for sql_file in models_dir.rglob("*.sql"):
                if sql_file.stem == model_name:
                    return sql_file
        
        return None
    
    def validate_model_reference(self, model_name: str) -> Tuple[bool, str]:
        """
        Validate a model reference and return (is_valid, message)
        """
        if not model_name:
            return False, "Empty model name"
            
        available_models = self.get_available_models()
        
        if model_name in available_models:
            return True, f"Model '{model_name}' found"
        
        # Try to provide helpful suggestions
        suggestions = []
        
        # Look for similar names
        similar_models = [m for m in available_models if model_name.lower() in m.lower()]
        if similar_models:
            suggestions.append(f"Similar models found: {', '.join(sorted(similar_models)[:3])}")
        
        # Check if it might be a source reference
        sources = self.get_available_sources()
        for source_name, tables in sources.items():
            if model_name in tables:
                suggestions.append(f"Found as source table: source('{source_name}', '{model_name}')")
        
        suggestion_text = ". " + "; ".join(suggestions) if suggestions else ""
        return False, f"Model '{model_name}' not found{suggestion_text}"
    
    def _get_models_from_manifest(self) -> Set[str]:
        """Extract models from dbt manifest.json"""
        manifest_path = self.project_dir / "target" / "manifest.json"
        models = set()
        
        if not manifest_path.exists():
            return models
            
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
                
            for node_id, node in manifest.get('nodes', {}).items():
                if node.get('resource_type') == 'model':
                    model_name = node.get('name')
                    if model_name:
                        models.add(model_name)
                        
        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            pass
            
        return models
    
    def _get_models_from_directory(self) -> Set[str]:
        """Extract models by scanning models directory"""
        models = set()
        models_dir = self.project_dir / "models"
        
        if not models_dir.exists():
            return models
            
        # Find all .sql files in models directory
        for sql_file in models_dir.rglob("*.sql"):
            # Skip files starting with underscore (typically not models)
            if not sql_file.name.startswith('_'):
                models.add(sql_file.stem)
                
        return models
    
    def _get_sources_from_manifest(self) -> Dict[str, Set[str]]:
        """Extract sources from dbt manifest.json"""
        manifest_path = self.project_dir / "target" / "manifest.json"
        sources = {}
        
        if not manifest_path.exists():
            return sources
            
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
                
            for source_id, source in manifest.get('sources', {}).items():
                source_name = source.get('source_name')
                table_name = source.get('name')
                
                if source_name and table_name:
                    if source_name not in sources:
                        sources[source_name] = set()
                    sources[source_name].add(table_name)
                    
        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            pass
            
        return sources
    
    def _get_sources_from_schema_files(self) -> Dict[str, Set[str]]:
        """Extract sources by scanning schema.yml files"""
        sources = {}
        
        # Look for schema files in models and other directories
        for yml_file in self.project_dir.rglob("*.yml"):
            # Skip target and dbt_packages directories
            if any(part in str(yml_file) for part in ['target/', 'dbt_packages/', '.git/']):
                continue
                
            try:
                with open(yml_file) as f:
                    data = yaml.safe_load(f)
                    
                if not isinstance(data, dict):
                    continue
                    
                for source in data.get('sources', []):
                    source_name = source.get('name')
                    if not source_name:
                        continue
                        
                    if source_name not in sources:
                        sources[source_name] = set()
                        
                    for table in source.get('tables', []):
                        table_name = table.get('name')
                        if table_name:
                            sources[source_name].add(table_name)
                            
            except (yaml.YAMLError, FileNotFoundError, PermissionError):
                continue
                
        return sources
    
    def clear_cache(self):
        """Clear internal caches - useful if project structure changes"""
        self._models_cache = None
        self._sources_cache = None
    
    def get_project_info(self) -> Dict[str, any]:
        """Get summary information about the dbt project"""
        models = self.get_available_models()
        sources = self.get_available_sources()
        
        return {
            'project_dir': str(self.project_dir),
            'models_count': len(models),
            'sources_count': len(sources),
            'source_tables_count': sum(len(tables) for tables in sources.values()),
            'models': sorted(models),
            'sources': {name: sorted(tables) for name, tables in sources.items()},
            'has_manifest': (self.project_dir / "target" / "manifest.json").exists()
        }