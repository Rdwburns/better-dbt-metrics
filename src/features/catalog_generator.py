"""
Metric Catalog Generator
Generates formatted, editable markdown documentation for metrics
"""

import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
from datetime import datetime
from dataclasses import dataclass, field
import json
from copy import deepcopy

from core.parser import BetterDBTParser
from core.compiler import BetterDBTCompiler
from features.dimension_groups import DimensionGroupManager


@dataclass
class CatalogConfig:
    """Configuration for catalog generation"""
    output_dir: str = "docs/metrics"
    include_technical_details: bool = True
    include_sql_examples: bool = True
    include_lineage: bool = True
    include_dependencies: bool = True
    group_by_domain: bool = True
    include_search_index: bool = True
    include_glossary: bool = True
    include_change_log: bool = False
    template_style: str = "detailed"  # "detailed", "compact", "custom"
    custom_template_path: Optional[str] = None
    

class MetricCatalogGenerator:
    """Generates comprehensive metric documentation"""
    
    def __init__(self, config: CatalogConfig, compiler: Optional[BetterDBTCompiler] = None):
        self.config = config
        self.compiler = compiler
        self.metrics_by_domain: Dict[str, List[Dict[str, Any]]] = {}
        self.metrics_by_name: Dict[str, Dict[str, Any]] = {}
        self.dimension_usage: Dict[str, Set[str]] = {}
        self.metric_dependencies: Dict[str, Set[str]] = {}
        self.glossary_terms: Dict[str, str] = {}
        
    def generate_catalog(self, compiled_metrics: List[Dict[str, Any]], 
                        source_files: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """Generate catalog from compiled metrics"""
        # Organize metrics
        self._organize_metrics(compiled_metrics)
        
        # Build dependency graph
        self._build_dependency_graph(compiled_metrics)
        
        # Build dimension usage
        self._build_dimension_usage(compiled_metrics)
        
        # Generate output files
        output_files = {}
        
        # Generate main index
        output_files['index.md'] = self._generate_index()
        
        # Generate domain pages
        if self.config.group_by_domain:
            for domain, metrics in self.metrics_by_domain.items():
                output_files[f"domains/{domain}.md"] = self._generate_domain_page(domain, metrics)
        
        # Generate individual metric pages
        for metric in compiled_metrics:
            metric_path = self._get_metric_path(metric)
            output_files[metric_path] = self._generate_metric_page(metric)
        
        # Generate supporting pages
        if self.config.include_search_index:
            output_files['search-index.json'] = self._generate_search_index()
            
        if self.config.include_glossary:
            output_files['glossary.md'] = self._generate_glossary()
            
        if self.config.include_lineage:
            output_files['lineage/index.md'] = self._generate_lineage_index()
            
        # Generate dimension catalog
        output_files['dimensions/index.md'] = self._generate_dimension_catalog()
        
        # Generate metric relationships
        output_files['relationships.md'] = self._generate_relationships_page()
        
        return output_files
        
    def _organize_metrics(self, metrics: List[Dict[str, Any]]):
        """Organize metrics by domain and name"""
        for metric in metrics:
            # Determine domain
            domain = self._extract_domain(metric)
            
            # Add to domain mapping
            if domain not in self.metrics_by_domain:
                self.metrics_by_domain[domain] = []
            self.metrics_by_domain[domain].append(metric)
            
            # Add to name mapping
            self.metrics_by_name[metric['name']] = metric
            
    def _extract_domain(self, metric: Dict[str, Any]) -> str:
        """Extract domain from metric"""
        # Check meta field
        if 'meta' in metric and 'domain' in metric['meta']:
            return metric['meta']['domain']
            
        # Check source
        if 'source' in metric:
            source = metric['source']
            if '_' in source:
                # Extract domain from source table name
                parts = source.split('_')
                if parts[0] in ['fct', 'dim', 'int']:
                    return parts[1] if len(parts) > 1 else 'general'
                    
        # Default domain
        return 'general'
        
    def _build_dependency_graph(self, metrics: List[Dict[str, Any]]):
        """Build metric dependency graph"""
        for metric in metrics:
            metric_name = metric['name']
            self.metric_dependencies[metric_name] = set()
            
            # Check derived metrics
            if metric.get('type') == 'derived':
                expr = metric.get('expression', metric.get('formula', ''))
                # Extract metric references
                import re
                refs = re.findall(r"metric\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", expr)
                self.metric_dependencies[metric_name].update(refs)
                
            # Check filters
            if 'filter' in metric:
                refs = re.findall(r"metric\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", metric['filter'])
                self.metric_dependencies[metric_name].update(refs)
                
    def _build_dimension_usage(self, metrics: List[Dict[str, Any]]):
        """Build dimension usage map"""
        for metric in metrics:
            dimensions = metric.get('dimensions', [])
            for dim in dimensions:
                dim_name = dim.get('name') if isinstance(dim, dict) else dim
                if dim_name:
                    if dim_name not in self.dimension_usage:
                        self.dimension_usage[dim_name] = set()
                    self.dimension_usage[dim_name].add(metric['name'])
                    
    def _get_metric_path(self, metric: Dict[str, Any]) -> str:
        """Get output path for metric documentation"""
        domain = self._extract_domain(metric)
        return f"metrics/{domain}/{metric['name']}.md"
        
    def _generate_index(self) -> str:
        """Generate main index page"""
        content = [
            "# Metric Catalog",
            "",
            f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Overview",
            "",
            f"Total Metrics: **{len(self.metrics_by_name)}**",
            f"Total Domains: **{len(self.metrics_by_domain)}**",
            f"Total Dimensions: **{len(self.dimension_usage)}**",
            "",
            "## Quick Links",
            "",
            "- [Search Metrics](search.html)",
            "- [Metric Glossary](glossary.md)",
            "- [Dimension Catalog](dimensions/index.md)",
            "- [Metric Relationships](relationships.md)",
            "- [Data Lineage](lineage/index.md)",
            "",
            "## Domains",
            ""
        ]
        
        # Add domain summary
        for domain in sorted(self.metrics_by_domain.keys()):
            metrics = self.metrics_by_domain[domain]
            content.append(f"### [{domain.title()}](domains/{domain}.md)")
            content.append(f"*{len(metrics)} metrics*")
            content.append("")
            
            # Show top metrics
            top_metrics = sorted(metrics, key=lambda m: m.get('meta', {}).get('importance', 0), reverse=True)[:5]
            if top_metrics:
                content.append("**Key Metrics:**")
                for metric in top_metrics:
                    metric_path = self._get_metric_path(metric)
                    content.append(f"- [{metric['label']}]({metric_path}) - {metric['description'][:80]}...")
                content.append("")
                
        # Add recent changes if available
        if self.config.include_change_log:
            content.extend([
                "## Recent Changes",
                "",
                "See [Change Log](changes.md) for recent updates.",
                ""
            ])
            
        return "\n".join(content)
        
    def _generate_domain_page(self, domain: str, metrics: List[Dict[str, Any]]) -> str:
        """Generate domain-specific page"""
        content = [
            f"# {domain.title()} Metrics",
            "",
            f"This domain contains **{len(metrics)}** metrics.",
            "",
            "## Metrics",
            ""
        ]
        
        # Group by type
        metrics_by_type = {}
        for metric in metrics:
            metric_type = metric.get('type', 'simple')
            if metric_type not in metrics_by_type:
                metrics_by_type[metric_type] = []
            metrics_by_type[metric_type].append(metric)
            
        # Generate sections by type
        for metric_type, type_metrics in sorted(metrics_by_type.items()):
            content.append(f"### {metric_type.title()} Metrics")
            content.append("")
            
            # Create table
            content.append("| Metric | Description | Dimensions |")
            content.append("|--------|-------------|------------|")
            
            for metric in sorted(type_metrics, key=lambda m: m['name']):
                metric_path = f"../{self._get_metric_path(metric)}"
                dims = self._format_dimension_list(metric.get('dimensions', []))
                desc = metric['description'][:60] + '...' if len(metric['description']) > 60 else metric['description']
                content.append(f"| [{metric['label']}]({metric_path}) | {desc} | {dims} |")
                
            content.append("")
            
        # Add common dimensions section
        content.extend(self._generate_common_dimensions_section(metrics))
        
        # Add dependency graph if applicable
        if self.config.include_dependencies:
            content.extend(self._generate_dependency_section(metrics))
            
        return "\n".join(content)
        
    def _generate_metric_page(self, metric: Dict[str, Any]) -> str:
        """Generate individual metric documentation page"""
        if self.config.template_style == "compact":
            return self._generate_compact_metric_page(metric)
        elif self.config.template_style == "custom" and self.config.custom_template_path:
            return self._generate_custom_metric_page(metric)
        else:
            return self._generate_detailed_metric_page(metric)
            
    def _generate_detailed_metric_page(self, metric: Dict[str, Any]) -> str:
        """Generate detailed metric documentation"""
        content = [
            f"# {metric['label']}",
            ""
        ]
        
        # Add importance/criticality badge
        if 'meta' in metric:
            importance = metric['meta'].get('importance', 'medium')
            if importance == 'critical':
                content.append("🔴 **Critical Metric** - Essential for business operations")
            elif importance == 'high':
                content.append("🟡 **High Importance** - Key business metric")
            content.append("")
            
        content.extend([
            f"**Metric Name:** `{metric['name']}`",
            f"**Type:** {metric['type']}",
            f"**Domain:** {self._extract_domain(metric)}",
        ])
        
        # Add owner and team information
        if 'meta' in metric:
            if 'owner' in metric['meta']:
                content.append(f"**Owner:** {metric['meta']['owner']}")
            if 'team' in metric['meta']:
                content.append(f"**Team:** {metric['meta']['team']}")
                
        content.extend([
            "",
            "## Description",
            "",
            metric['description'],
            "",
        ])
        
        # Add business context if available
        if 'meta' in metric and 'business_context' in metric['meta']:
            content.extend([
                "## Business Context",
                "",
                metric['meta']['business_context'],
                ""
            ])
            
        # Add sample business questions
        if 'meta' in metric and 'sample_questions' in metric['meta']:
            content.extend([
                "## Sample Business Questions",
                "",
                "This metric helps answer questions like:",
                ""
            ])
            for question in metric['meta']['sample_questions']:
                content.append(f"- {question}")
            content.append("")
            
        # Technical Details
        if self.config.include_technical_details:
            content.extend(self._generate_technical_section(metric))
            
        # Dimensions
        if 'dimensions' in metric and metric['dimensions']:
            content.extend(self._generate_dimensions_section(metric))
            
        # SQL Examples
        if self.config.include_sql_examples:
            content.extend(self._generate_sql_examples(metric))
            
        # Used By Section - Show prominently what uses this metric
        content.extend(self._generate_used_by_section(metric))
        
        # Dependencies
        if self.config.include_dependencies:
            content.extend(self._generate_metric_dependencies(metric))
            
        # Usage Examples
        content.extend(self._generate_usage_examples(metric))
        
        # Related Metrics
        content.extend(self._generate_related_metrics(metric))
        
        # Metadata
        content.extend(self._generate_metadata_section(metric))
        
        return "\n".join(content)
        
    def _generate_technical_section(self, metric: Dict[str, Any]) -> List[str]:
        """Generate technical details section"""
        content = [
            "## Technical Details",
            "",
            "### Definition",
            ""
        ]
        
        if metric['type'] == 'simple':
            measure = metric.get('measure', {})
            content.append(f"- **Aggregation:** {measure.get('type', 'sum')}")
            content.append(f"- **Column:** `{measure.get('column', 'unknown')}`")
            if 'filters' in measure:
                content.append("- **Filters:**")
                for filter_expr in measure['filters']:
                    content.append(f"  - `{filter_expr}`")
                    
        elif metric['type'] == 'ratio':
            content.append("**Numerator:**")
            num = metric.get('numerator', {})
            if 'measure' in num:
                content.append(f"- Type: {num['measure'].get('type', 'count')}")
                content.append(f"- Column: `{num['measure'].get('column', 'unknown')}`")
            content.append("")
            content.append("**Denominator:**")
            den = metric.get('denominator', {})
            if 'measure' in den:
                content.append(f"- Type: {den['measure'].get('type', 'count')}")
                content.append(f"- Column: `{den['measure'].get('column', 'unknown')}`")
                
        elif metric['type'] == 'derived':
            content.append("**Expression:**")
            content.append("```sql")
            content.append(metric.get('expression', metric.get('formula', 'unknown')))
            content.append("```")
            
        # Add source information
        if 'source' in metric:
            content.append("")
            content.append(f"**Source Table:** `{metric['source']}`")
            
            # Add source freshness if available
            if 'meta' in metric:
                if 'source_freshness' in metric['meta']:
                    content.append(f"**Data Freshness:** {metric['meta']['source_freshness']}")
                if 'update_frequency' in metric['meta']:
                    content.append(f"**Update Frequency:** {metric['meta']['update_frequency']}")
                if 'sla' in metric['meta']:
                    content.append(f"**SLA:** {metric['meta']['sla']}")
            
        content.append("")
        return content
        
    def _generate_dimensions_section(self, metric: Dict[str, Any]) -> List[str]:
        """Generate dimensions section"""
        content = [
            "## Available Dimensions",
            "",
            "This metric can be analyzed by the following dimensions:",
            ""
        ]
        
        dimensions = metric.get('dimensions', [])
        if not dimensions:
            content.append("*No dimensions configured*")
        else:
            content.append("| Dimension | Type | Description |")
            content.append("|-----------|------|-------------|")
            
            for dim in dimensions:
                if isinstance(dim, dict):
                    dim_name = dim.get('name', 'unknown')
                    dim_type = dim.get('type', 'categorical')
                    dim_desc = dim.get('description', dim.get('label', ''))
                    
                    # Link to dimension page if it exists
                    dim_link = f"[{dim_name}](../../dimensions/{dim_name}.md)"
                    content.append(f"| {dim_link} | {dim_type} | {dim_desc} |")
                else:
                    content.append(f"| {dim} | categorical | |")
                    
        content.append("")
        return content
        
    def _generate_sql_examples(self, metric: Dict[str, Any]) -> List[str]:
        """Generate SQL query examples"""
        content = [
            "## SQL Examples",
            "",
            "### Basic Query",
            "",
            "```sql",
            f"SELECT",
            f"  date_trunc('month', order_date) as month,",
            f"  {self._generate_sql_aggregation(metric)} as {metric['name']}",
            f"FROM {metric.get('source', 'source_table')}",
            f"GROUP BY 1",
            f"ORDER BY 1;",
            "```",
            ""
        ]
        
        # Add dimension example
        if metric.get('dimensions'):
            first_dim = metric['dimensions'][0]
            dim_name = first_dim.get('name') if isinstance(first_dim, dict) else first_dim
            
            content.extend([
                "### Query with Dimension",
                "",
                "```sql",
                f"SELECT",
                f"  date_trunc('month', order_date) as month,",
                f"  {dim_name},",
                f"  {self._generate_sql_aggregation(metric)} as {metric['name']}",
                f"FROM {metric.get('source', 'source_table')}",
                f"GROUP BY 1, 2",
                f"ORDER BY 1, 2;",
                "```",
                ""
            ])
            
        return content
        
    def _generate_sql_aggregation(self, metric: Dict[str, Any]) -> str:
        """Generate SQL aggregation for metric"""
        if metric['type'] == 'simple':
            measure = metric.get('measure', {})
            agg_type = measure.get('type', 'sum')
            column = measure.get('column', 'value')
            
            if agg_type == 'count':
                return f"COUNT({column})"
            elif agg_type == 'count_distinct':
                return f"COUNT(DISTINCT {column})"
            elif agg_type == 'sum':
                return f"SUM({column})"
            elif agg_type == 'average':
                return f"AVG({column})"
            elif agg_type == 'median':
                return f"MEDIAN({column})"
            elif agg_type == 'percentile':
                percentile = measure.get('percentile', 0.5)
                return f"PERCENTILE_CONT({percentile}) WITHIN GROUP (ORDER BY {column})"
            else:
                return f"{agg_type.upper()}({column})"
                
        elif metric['type'] == 'ratio':
            return "numerator / NULLIF(denominator, 0)"
            
        elif metric['type'] == 'derived':
            return metric.get('expression', 'complex_calculation')
            
        return "calculation"
        
    def _generate_used_by_section(self, metric: Dict[str, Any]) -> List[str]:
        """Generate prominent 'Used By' section"""
        content = []
        
        # Find metrics that depend on this one
        dependents = []
        for other_name, other_deps in self.metric_dependencies.items():
            if metric['name'] in other_deps:
                dependents.append(other_name)
                
        # Find dashboards/reports that use this metric (from meta)
        dashboards = []
        reports = []
        if 'meta' in metric:
            dashboards = metric['meta'].get('used_in_dashboards', [])
            reports = metric['meta'].get('used_in_reports', [])
            
        if dependents or dashboards or reports:
            content.extend([
                "## 📊 Used By",
                "",
                "This metric is used in the following places:",
                ""
            ])
            
            if dependents:
                content.append("### Downstream Metrics")
                for dep in sorted(dependents):
                    if dep in self.metrics_by_name:
                        dep_metric = self.metrics_by_name[dep]
                        dep_path = f"../{self._get_metric_path(dep_metric)}"
                        content.append(f"- [{dep_metric['label']}]({dep_path}) - {dep_metric['description'][:60]}...")
                content.append("")
                
            if dashboards:
                content.append("### Dashboards")
                for dashboard in dashboards:
                    content.append(f"- {dashboard}")
                content.append("")
                
            if reports:
                content.append("### Reports")
                for report in reports:
                    content.append(f"- {report}")
                content.append("")
                
        return content
        
    def _generate_metric_dependencies(self, metric: Dict[str, Any]) -> List[str]:
        """Generate dependencies section"""
        content = [
            "## Dependencies",
            ""
        ]
        
        deps = self.metric_dependencies.get(metric['name'], set())
        if deps:
            content.append("This metric depends on:")
            content.append("")
            for dep in sorted(deps):
                if dep in self.metrics_by_name:
                    dep_metric = self.metrics_by_name[dep]
                    dep_path = f"../{self._get_metric_path(dep_metric)}"
                    content.append(f"- [{dep_metric['label']}]({dep_path})")
                else:
                    content.append(f"- `{dep}` (external)")
                    
        # Find metrics that depend on this one
        dependents = []
        for other_name, other_deps in self.metric_dependencies.items():
            if metric['name'] in other_deps:
                dependents.append(other_name)
                
        if dependents:
            content.append("")
            content.append("Metrics that depend on this one:")
            content.append("")
            for dep in sorted(dependents):
                if dep in self.metrics_by_name:
                    dep_metric = self.metrics_by_name[dep]
                    dep_path = f"../{self._get_metric_path(dep_metric)}"
                    content.append(f"- [{dep_metric['label']}]({dep_path})")
                    
        if not deps and not dependents:
            content.append("*This metric has no dependencies*")
            
        content.append("")
        return content
        
    def _generate_usage_examples(self, metric: Dict[str, Any]) -> List[str]:
        """Generate usage examples"""
        content = [
            "## Usage Examples",
            "",
            "### dbt Metrics Query",
            "",
            "```sql",
            "SELECT * FROM {{ metrics.calculate(",
            f"    metric('{metric['name']}'),",
            "    grain='month',",
            "    dimensions=['customer_segment']",
            ") }}",
            "```",
            ""
        ]
        
        # Add BI tool examples if available
        if 'meta' in metric and 'bi_examples' in metric['meta']:
            for tool, example in metric['meta']['bi_examples'].items():
                content.extend([
                    f"### {tool} Example",
                    "",
                    example,
                    ""
                ])
                
        return content
        
    def _generate_related_metrics(self, metric: Dict[str, Any]) -> List[str]:
        """Generate related metrics section"""
        content = [
            "## Related Metrics",
            ""
        ]
        
        related = []
        
        # Find metrics in same domain
        domain = self._extract_domain(metric)
        domain_metrics = self.metrics_by_domain.get(domain, [])
        
        for other in domain_metrics:
            if other['name'] != metric['name']:
                # Check if they share dimensions
                metric_dims = {d.get('name') if isinstance(d, dict) else d 
                             for d in metric.get('dimensions', [])}
                other_dims = {d.get('name') if isinstance(d, dict) else d 
                            for d in other.get('dimensions', [])}
                
                if metric_dims & other_dims:  # Intersection
                    related.append(other)
                    
        if related:
            content.append("Other metrics that share dimensions or context:")
            content.append("")
            
            for rel in related[:5]:  # Limit to 5
                rel_path = f"../{self._get_metric_path(rel)}"
                content.append(f"- [{rel['label']}]({rel_path}) - {rel['description'][:60]}...")
        else:
            content.append("*No directly related metrics found*")
            
        content.append("")
        return content
        
    def _generate_metadata_section(self, metric: Dict[str, Any]) -> List[str]:
        """Generate metadata section"""
        content = [
            "## Metadata",
            ""
        ]
        
        if 'meta' in metric:
            content.append("| Property | Value |")
            content.append("|----------|-------|")
            
            for key, value in metric['meta'].items():
                if key not in ['business_context', 'bi_examples']:  # Skip already shown
                    content.append(f"| {key} | {value} |")
                    
            content.append("")
            
        # Add tags if available
        if 'tags' in metric:
            content.append(f"**Tags:** {', '.join(metric['tags'])}")
            content.append("")
            
        return content
        
    def _generate_compact_metric_page(self, metric: Dict[str, Any]) -> str:
        """Generate compact metric documentation"""
        content = [
            f"# {metric['label']}",
            "",
            metric['description'],
            "",
            f"**Type:** {metric['type']} | **Source:** `{metric.get('source', 'derived')}`",
            ""
        ]
        
        # Quick reference
        if metric['type'] == 'simple':
            measure = metric.get('measure', {})
            content.append(f"**Calculation:** {measure.get('type', 'sum')}({measure.get('column', 'value')})")
        elif metric['type'] == 'derived':
            content.append(f"**Formula:** `{metric.get('expression', 'see definition')}`")
            
        # Dimensions
        if metric.get('dimensions'):
            dims = self._format_dimension_list(metric['dimensions'])
            content.append(f"**Dimensions:** {dims}")
            
        content.append("")
        return "\n".join(content)
        
    def _format_dimension_list(self, dimensions: List[Any]) -> str:
        """Format dimension list for display"""
        dim_names = []
        for dim in dimensions[:5]:  # Limit display
            if isinstance(dim, dict):
                dim_names.append(dim.get('name', 'unknown'))
            else:
                dim_names.append(str(dim))
                
        result = ", ".join(dim_names)
        if len(dimensions) > 5:
            result += f" (+{len(dimensions) - 5} more)"
            
        return result
        
    def _generate_search_index(self) -> str:
        """Generate search index in JSON format"""
        search_data = []
        
        for metric in self.metrics_by_name.values():
            # Change .md to .html for the URL
            metric_url = self._get_metric_path(metric).replace('.md', '.html')
            entry = {
                "name": metric['name'],
                "label": metric['label'],
                "description": metric['description'],
                "type": metric['type'],
                "domain": self._extract_domain(metric),
                "url": metric_url,
                "tags": metric.get('tags', []),
                "dimensions": [d.get('name') if isinstance(d, dict) else d 
                             for d in metric.get('dimensions', [])]
            }
            
            # Add metadata if available
            if 'meta' in metric:
                entry['meta'] = {
                    'importance': metric['meta'].get('importance', 'medium'),
                    'owner': metric['meta'].get('owner'),
                    'team': metric['meta'].get('team')
                }
                
            search_data.append(entry)
            
        return json.dumps(search_data, indent=2)
        
    def _generate_glossary(self) -> str:
        """Generate glossary page"""
        content = [
            "# Metrics Glossary",
            "",
            "Common terms and definitions used in metrics.",
            ""
        ]
        
        # Add metric types
        content.extend([
            "## Metric Types",
            "",
            "- **Simple**: Basic aggregation metrics (sum, count, average)",
            "- **Ratio**: Metrics calculated as numerator/denominator",
            "- **Derived**: Metrics calculated from other metrics",
            "- **Cumulative**: Running totals or period-to-date metrics",
            "- **Conversion**: Funnel or conversion rate metrics",
            ""
        ])
        
        # Add common aggregations
        content.extend([
            "## Aggregation Types",
            "",
            "- **sum**: Total of all values",
            "- **count**: Number of records",
            "- **count_distinct**: Number of unique values",
            "- **average/avg**: Mean value",
            "- **median**: Middle value",
            "- **percentile**: Value at specific percentile",
            "- **min/max**: Minimum or maximum value",
            ""
        ])
        
        # Add time grains
        content.extend([
            "## Time Grains",
            "",
            "- **day**: Daily granularity",
            "- **week**: Weekly granularity (Sunday-Saturday)",
            "- **month**: Monthly granularity",
            "- **quarter**: Quarterly granularity",
            "- **year**: Yearly granularity",
            ""
        ])
        
        # Add custom terms from metrics
        if self.glossary_terms:
            content.extend([
                "## Domain-Specific Terms",
                ""
            ])
            
            for term, definition in sorted(self.glossary_terms.items()):
                content.append(f"- **{term}**: {definition}")
                
        return "\n".join(content)
        
    def _generate_dimension_catalog(self) -> str:
        """Generate dimension catalog"""
        content = [
            "# Dimension Catalog",
            "",
            f"Total Dimensions: **{len(self.dimension_usage)}**",
            "",
            "## Dimensions by Usage",
            ""
        ]
        
        # Sort dimensions by usage count
        dim_usage_sorted = sorted(
            self.dimension_usage.items(),
            key=lambda x: len(x[1]),
            reverse=True
        )
        
        content.append("| Dimension | Type | Used By | Count |")
        content.append("|-----------|------|---------|-------|")
        
        for dim_name, metric_names in dim_usage_sorted:
            # Try to determine dimension type
            dim_type = "categorical"  # default
            
            # Check if it's a time dimension
            if any(grain in dim_name.lower() for grain in ['date', 'time', 'day', 'month', 'year']):
                dim_type = "time"
            
            # Show first few metrics
            metric_links = []
            for metric_name in sorted(metric_names)[:3]:
                if metric_name in self.metrics_by_name:
                    metric = self.metrics_by_name[metric_name]
                    metric_path = f"../{self._get_metric_path(metric)}"
                    metric_links.append(f"[{metric['label']}]({metric_path})")
                    
            usage_str = ", ".join(metric_links)
            if len(metric_names) > 3:
                usage_str += f" (+{len(metric_names) - 3} more)"
                
            content.append(f"| {dim_name} | {dim_type} | {usage_str} | {len(metric_names)} |")
            
        content.append("")
        return "\n".join(content)
        
    def _generate_lineage_index(self) -> str:
        """Generate lineage index page"""
        content = [
            "# Data Lineage",
            "",
            "Metric dependency relationships and data flow.",
            "",
            "## Dependency Graph",
            ""
        ]
        
        # Create mermaid diagram
        content.append("```mermaid")
        content.append("graph TD")
        
        # Add nodes
        for metric_name in self.metrics_by_name:
            metric = self.metrics_by_name[metric_name]
            node_label = metric['label'].replace(' ', '_')
            content.append(f"    {metric_name}[\"{node_label}\"]")
            
        # Add edges
        for metric_name, deps in self.metric_dependencies.items():
            for dep in deps:
                if dep in self.metrics_by_name:
                    content.append(f"    {dep} --> {metric_name}")
                    
        content.append("```")
        content.append("")
        
        # Add table view
        content.extend([
            "## Dependency Table",
            "",
            "| Metric | Depends On | Used By |",
            "|--------|------------|---------|"
        ])
        
        for metric_name in sorted(self.metrics_by_name.keys()):
            deps = sorted(self.metric_dependencies.get(metric_name, []))
            
            # Find usage
            used_by = []
            for other_name, other_deps in self.metric_dependencies.items():
                if metric_name in other_deps:
                    used_by.append(other_name)
                    
            deps_str = ", ".join(deps) if deps else "-"
            used_str = ", ".join(sorted(used_by)) if used_by else "-"
            
            content.append(f"| {metric_name} | {deps_str} | {used_str} |")
            
        content.append("")
        return "\n".join(content)
        
    def _generate_relationships_page(self) -> str:
        """Generate metric relationships page"""
        content = [
            "# Metric Relationships",
            "",
            "How metrics relate to each other through shared dimensions and dependencies.",
            "",
            "## Metrics by Shared Dimensions",
            ""
        ]
        
        # Find metrics that share multiple dimensions
        metric_pairs = {}
        metrics_list = list(self.metrics_by_name.values())
        
        for i, metric1 in enumerate(metrics_list):
            dims1 = {d.get('name') if isinstance(d, dict) else d 
                    for d in metric1.get('dimensions', [])}
                    
            for metric2 in metrics_list[i+1:]:
                dims2 = {d.get('name') if isinstance(d, dict) else d 
                        for d in metric2.get('dimensions', [])}
                        
                shared = dims1 & dims2
                if len(shared) >= 2:  # At least 2 shared dimensions
                    pair_key = tuple(sorted([metric1['name'], metric2['name']]))
                    metric_pairs[pair_key] = shared
                    
        if metric_pairs:
            content.append("Metrics that share multiple dimensions can be analyzed together:")
            content.append("")
            
            for (m1, m2), shared_dims in sorted(metric_pairs.items()):
                metric1 = self.metrics_by_name[m1]
                metric2 = self.metrics_by_name[m2]
                
                content.append(f"- **{metric1['label']}** ↔ **{metric2['label']}**")
                content.append(f"  - Shared dimensions: {', '.join(sorted(shared_dims))}")
                
        content.append("")
        return "\n".join(content)
        
    def _generate_common_dimensions_section(self, metrics: List[Dict[str, Any]]) -> List[str]:
        """Generate common dimensions section for a group of metrics"""
        content = [
            "## Common Dimensions",
            ""
        ]
        
        # Count dimension usage
        dim_counts = {}
        for metric in metrics:
            for dim in metric.get('dimensions', []):
                dim_name = dim.get('name') if isinstance(dim, dict) else dim
                if dim_name:
                    dim_counts[dim_name] = dim_counts.get(dim_name, 0) + 1
                    
        # Show dimensions used by multiple metrics
        common_dims = [(dim, count) for dim, count in dim_counts.items() if count > 1]
        
        if common_dims:
            content.append("Dimensions used across multiple metrics:")
            content.append("")
            
            for dim, count in sorted(common_dims, key=lambda x: x[1], reverse=True):
                content.append(f"- **{dim}** (used by {count} metrics)")
                
        content.append("")
        return content
        
    def _generate_dependency_section(self, metrics: List[Dict[str, Any]]) -> List[str]:
        """Generate dependency section for a group of metrics"""
        content = [
            "## Dependencies",
            ""
        ]
        
        # Find dependencies within this group
        group_names = {m['name'] for m in metrics}
        has_deps = False
        
        for metric in metrics:
            deps = self.metric_dependencies.get(metric['name'], set())
            internal_deps = deps & group_names
            
            if internal_deps:
                has_deps = True
                content.append(f"- **{metric['label']}** depends on: {', '.join(sorted(internal_deps))}")
                
        if not has_deps:
            content.append("*No internal dependencies within this domain*")
            
        content.append("")
        return content
        
    def _generate_custom_metric_page(self, metric: Dict[str, Any]) -> str:
        """Generate metric page using custom template"""
        # Load custom template
        template_path = Path(self.config.custom_template_path)
        if not template_path.exists():
            # Fallback to detailed template
            return self._generate_detailed_metric_page(metric)
            
        with open(template_path, 'r') as f:
            template = f.read()
            
        # Simple template variable replacement
        replacements = {
            '{{name}}': metric['name'],
            '{{label}}': metric['label'],
            '{{description}}': metric['description'],
            '{{type}}': metric['type'],
            '{{domain}}': self._extract_domain(metric),
            '{{source}}': metric.get('source', 'derived'),
            '{{dimensions}}': self._format_dimension_list(metric.get('dimensions', [])),
            '{{dependencies}}': ', '.join(self.metric_dependencies.get(metric['name'], [])),
        }
        
        result = template
        for key, value in replacements.items():
            result = result.replace(key, str(value))
            
        return result
        
    def _markdown_to_html(self, markdown_content: str, file_path: str) -> str:
        """Convert markdown content to a full HTML page"""
        # Extract title from first H1
        title = "Metric Documentation"
        lines = markdown_content.split('\n')
        for line in lines:
            if line.startswith('# '):
                title = line[2:].strip()
                break
                
        # Determine navigation depth based on file path
        depth = file_path.count('/')
        nav_prefix = '../' * depth
        
        # Simple markdown to HTML conversion (basic, but functional)
        html_body = self._simple_markdown_to_html(markdown_content)
        
        # HTML template with embedded CSS
        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - Metric Catalog</title>
    <style>
        :root {{
            --primary-color: #1976D2;
            --secondary-color: #4CAF50;
            --background: #f5f5f5;
            --card-background: white;
            --text-primary: #333;
            --text-secondary: #666;
            --border-color: #e0e0e0;
        }}
        
        * {{
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: var(--text-primary);
            background: var(--background);
            margin: 0;
            padding: 0;
        }}
        
        .header {{
            background: var(--card-background);
            border-bottom: 1px solid var(--border-color);
            padding: 1rem 0;
            position: sticky;
            top: 0;
            z-index: 100;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        .nav {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 0 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        .nav-links {{
            display: flex;
            gap: 2rem;
            align-items: center;
        }}
        
        .nav-links a {{
            color: var(--text-primary);
            text-decoration: none;
            font-weight: 500;
            transition: color 0.2s;
        }}
        
        .nav-links a:hover {{
            color: var(--primary-color);
        }}
        
        .search-link {{
            background: var(--primary-color);
            color: white !important;
            padding: 0.5rem 1rem;
            border-radius: 6px;
            transition: background 0.2s;
        }}
        
        .search-link:hover {{
            background: #1565C0 !important;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 2rem auto;
            padding: 0 2rem;
        }}
        
        .content {{
            background: var(--card-background);
            border-radius: 8px;
            padding: 2rem;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        h1 {{
            color: var(--text-primary);
            border-bottom: 3px solid var(--primary-color);
            padding-bottom: 0.5rem;
            margin-bottom: 1.5rem;
        }}
        
        h2 {{
            color: var(--text-primary);
            margin-top: 2rem;
            margin-bottom: 1rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 0.5rem;
        }}
        
        h3 {{
            color: var(--text-primary);
            margin-top: 1.5rem;
            margin-bottom: 0.75rem;
        }}
        
        code {{
            background: #f4f4f4;
            padding: 0.2rem 0.4rem;
            border-radius: 3px;
            font-family: 'Monaco', 'Consolas', monospace;
            font-size: 0.9em;
        }}
        
        pre {{
            background: #2d2d2d;
            border: 1px solid #444;
            border-radius: 6px;
            padding: 1rem;
            overflow-x: auto;
            margin: 1rem 0;
            color: #f8f8f2;
        }}
        
        pre code {{
            background: transparent;
            padding: 0;
            color: #f8f8f2;
        }}
        
        /* SQL Syntax Highlighting */
        .keyword {{ color: #66d9ef; font-weight: bold; }}
        .function {{ color: #a6e22e; }}
        .string {{ color: #e6db74; }}
        .number {{ color: #ae81ff; }}
        .comment {{ color: #75715e; font-style: italic; }}
        .operator {{ color: #f92672; }}
        .identifier {{ color: #f8f8f2; }}
        .punctuation {{ color: #f8f8f2; }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 1rem 0;
        }}
        
        th, td {{
            text-align: left;
            padding: 0.75rem;
            border-bottom: 1px solid var(--border-color);
        }}
        
        th {{
            background: #f8f8f8;
            font-weight: 600;
            color: var(--text-primary);
        }}
        
        tr:hover {{
            background: #f8f8f8;
        }}
        
        a {{
            color: var(--primary-color);
            text-decoration: none;
        }}
        
        a:hover {{
            text-decoration: underline;
        }}
        
        ul, ol {{
            margin: 1rem 0;
            padding-left: 2rem;
        }}
        
        li {{
            margin: 0.5rem 0;
        }}
        
        blockquote {{
            border-left: 4px solid var(--primary-color);
            padding-left: 1rem;
            margin: 1rem 0;
            color: var(--text-secondary);
            font-style: italic;
        }}
        
        .metric-meta {{
            background: #f8f8f8;
            border-radius: 6px;
            padding: 1rem;
            margin: 1rem 0;
        }}
        
        .metric-meta strong {{
            color: var(--text-primary);
        }}
        
        .breadcrumb {{
            margin-bottom: 1rem;
            color: var(--text-secondary);
            font-size: 0.9rem;
        }}
        
        .breadcrumb a {{
            color: var(--text-secondary);
        }}
        
        .breadcrumb a:hover {{
            color: var(--primary-color);
        }}
        
        @media (max-width: 768px) {{
            .container {{
                padding: 0 1rem;
            }}
            
            .content {{
                padding: 1rem;
            }}
            
            .nav-links {{
                gap: 1rem;
                font-size: 0.9rem;
            }}
        }}
    </style>
</head>
<body>
    <header class="header">
        <nav class="nav">
            <div class="nav-links">
                <a href="{nav_prefix}index.html">📊 Catalog Home</a>
                <a href="{nav_prefix}glossary.html">📚 Glossary</a>
                <a href="{nav_prefix}dimensions/index.html">📐 Dimensions</a>
            </div>
            <div class="nav-links">
                <a href="{nav_prefix}search.html" class="search-link">🔍 Search</a>
            </div>
        </nav>
    </header>
    
    <div class="container">
        <div class="content">
            {self._generate_breadcrumb(file_path)}
            {html_body}
        </div>
    </div>
</body>
</html>"""
        
        return html_template
        
    def _simple_markdown_to_html(self, markdown: str) -> str:
        """Simple markdown to HTML conversion"""
        html = markdown
        
        # Convert code blocks
        import re
        
        # Convert fenced code blocks with syntax highlighting
        def convert_code_block(match):
            lang = match.group(1) or ''
            code = match.group(2)
            if lang.lower() in ['sql', 'lookml']:
                highlighted_code = self._highlight_sql(code)
                return f'<pre><code class="{lang}">{highlighted_code}</code></pre>'
            else:
                return f'<pre><code class="{lang}">{self._escape_html(code)}</code></pre>'
        
        html = re.sub(r'```(\w+)?\n(.*?)```', convert_code_block, html, flags=re.DOTALL)
        
        # Convert inline code
        html = re.sub(r'`([^`]+)`', r'<code>\1</code>', html)
        
        # Convert headers
        html = re.sub(r'^### (.+)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
        html = re.sub(r'^## (.+)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
        html = re.sub(r'^# (.+)$', r'<h1>\1</h1>', html, flags=re.MULTILINE)
        
        # Convert bold and italic
        html = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', html)
        html = re.sub(r'\*([^*]+)\*', r'<em>\1</em>', html)
        
        # Convert links (and change .md to .html)
        def convert_link(match):
            text = match.group(1)
            url = match.group(2)
            if url.endswith('.md') and not url.startswith('http'):
                url = url.replace('.md', '.html')
            return f'<a href="{url}">{text}</a>'
        
        html = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', convert_link, html)
        
        # Convert tables
        lines = html.split('\n')
        in_table = False
        table_html = []
        new_lines = []
        
        for line in lines:
            if '|' in line and not in_table:
                in_table = True
                table_html = ['<table>']
                # Check if this is a header row
                if lines[lines.index(line) + 1].strip().startswith('|---'):
                    headers = [cell.strip() for cell in line.split('|')[1:-1]]
                    table_html.append('<thead><tr>')
                    for header in headers:
                        table_html.append(f'<th>{header}</th>')
                    table_html.append('</tr></thead><tbody>')
                else:
                    cells = [cell.strip() for cell in line.split('|')[1:-1]]
                    table_html.append('<tbody><tr>')
                    for cell in cells:
                        table_html.append(f'<td>{cell}</td>')
                    table_html.append('</tr>')
            elif '|---' in line and in_table:
                continue  # Skip separator line
            elif '|' in line and in_table:
                cells = [cell.strip() for cell in line.split('|')[1:-1]]
                table_html.append('<tr>')
                for cell in cells:
                    table_html.append(f'<td>{cell}</td>')
                table_html.append('</tr>')
            elif in_table and '|' not in line:
                table_html.append('</tbody></table>')
                new_lines.append(''.join(table_html))
                table_html = []
                in_table = False
                new_lines.append(line)
            else:
                new_lines.append(line)
                
        if in_table:
            table_html.append('</tbody></table>')
            new_lines.append(''.join(table_html))
            
        html = '\n'.join(new_lines)
        
        # Convert lists
        html = re.sub(r'^- (.+)$', r'<li>\1</li>', html, flags=re.MULTILINE)
        html = re.sub(r'(<li>.*</li>\n)+', lambda m: '<ul>\n' + m.group(0) + '</ul>\n', html, flags=re.DOTALL)
        
        # Convert paragraphs
        paragraphs = html.split('\n\n')
        html_paragraphs = []
        for para in paragraphs:
            para = para.strip()
            if para and not para.startswith('<') and not para.startswith('#'):
                html_paragraphs.append(f'<p>{para}</p>')
            else:
                html_paragraphs.append(para)
                
        return '\n'.join(html_paragraphs)
        
    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters"""
        return (text.replace('&', '&amp;')
                   .replace('<', '&lt;')
                   .replace('>', '&gt;')
                   .replace('"', '&quot;')
                   .replace("'", '&#39;'))
                   
    def _highlight_sql(self, sql: str) -> str:
        """Apply SQL syntax highlighting"""
        # SQL keywords
        keywords = [
            'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'JOIN',
            'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON', 'AS', 'WITH', 'UNION',
            'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TABLE',
            'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE',
            'END', 'DISTINCT', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'BETWEEN',
            'LIKE', 'IS', 'NULL', 'ALL', 'ANY', 'OVER', 'PARTITION', 'BY',
            'WITHIN', 'MEDIAN', 'PERCENTILE_CONT', 'DATE_TRUNC', 'NULLIF'
        ]
        
        # SQL functions
        functions = [
            'count', 'sum', 'avg', 'min', 'max', 'date_trunc', 'coalesce',
            'cast', 'convert', 'substring', 'length', 'upper', 'lower',
            'median', 'percentile_cont', 'row_number', 'rank', 'dense_rank'
        ]
        
        # First escape HTML
        highlighted = self._escape_html(sql)
        
        # Highlight strings (single quotes)
        highlighted = re.sub(
            r"'([^']*)'",
            r'<span class="string">\'\\1\'</span>',
            highlighted
        )
        
        # Highlight numbers
        highlighted = re.sub(
            r'\b(\d+\.?\d*)\b',
            r'<span class="number">\\1</span>',
            highlighted
        )
        
        # Highlight operators
        operators = ['=', '!=', '<>', '<=', '>=', '<', '>', '+', '-', '*', '/', '%']
        for op in operators:
            highlighted = highlighted.replace(op, f'<span class="operator">{op}</span>')
        
        # Highlight keywords (case insensitive)
        for keyword in keywords:
            pattern = r'\b(' + keyword + r')\b'
            highlighted = re.sub(
                pattern,
                r'<span class="keyword">\\1</span>',
                highlighted,
                flags=re.IGNORECASE
            )
        
        # Highlight functions (case insensitive)
        for func in functions:
            pattern = r'\b(' + func + r')(?=\s*\()'
            highlighted = re.sub(
                pattern,
                r'<span class="function">\\1</span>',
                highlighted,
                flags=re.IGNORECASE
            )
        
        # Highlight comments
        highlighted = re.sub(
            r'--.*$',
            lambda m: f'<span class="comment">{m.group(0)}</span>',
            highlighted,
            flags=re.MULTILINE
        )
        
        # Highlight template variables (dbt style)
        highlighted = re.sub(
            r'{{.*?}}',
            lambda m: f'<span class="string">{m.group(0)}</span>',
            highlighted
        )
        
        return highlighted
                   
    def _generate_breadcrumb(self, file_path: str) -> str:
        """Generate breadcrumb navigation"""
        parts = file_path.replace('.md', '').split('/')
        breadcrumb_parts = ['<div class="breadcrumb">']
        
        # Build breadcrumb with proper relative paths
        current_path = ""
        for i, part in enumerate(parts[:-1]):
            if i == 0:
                breadcrumb_parts.append(f'<a href="../index.html">Home</a> / ')
            else:
                current_path += "../"
                formatted_part = part.replace('_', ' ').title()
                breadcrumb_parts.append(f'<a href="{current_path}{part}/index.html">{formatted_part}</a> / ')
                
        # Add current page (no link)
        current_page = parts[-1].replace('_', ' ').title()
        breadcrumb_parts.append(f'<span>{current_page}</span>')
        breadcrumb_parts.append('</div>')
        
        return ''.join(breadcrumb_parts)
        
    def write_catalog(self, output_files: Dict[str, str]):
        """Write catalog files to disk"""
        base_path = Path(self.config.output_dir)
        
        for file_path, content in output_files.items():
            full_path = base_path / file_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert markdown files to HTML
            if file_path.endswith('.md') and file_path != 'README.md':
                html_path = full_path.with_suffix('.html')
                html_content = self._markdown_to_html(content, file_path)
                with open(html_path, 'w') as f:
                    f.write(html_content)
                    
            # Still write the raw markdown for compatibility
            with open(full_path, 'w') as f:
                f.write(content)
                
        # Generate index.html if search is enabled
        if self.config.include_search_index:
            self._generate_search_html(base_path)
            
    def _generate_search_html(self, base_path: Path):
        """Generate search interface HTML"""
        html_content = """
<!DOCTYPE html>
<html>
<head>
    <title>Metric Catalog Search</title>
    <style>
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: #333;
            margin-bottom: 30px;
        }
        .search-container {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        .search-box { 
            width: 100%; 
            padding: 12px 20px; 
            font-size: 16px; 
            border: 2px solid #e0e0e0;
            border-radius: 6px;
            outline: none;
            transition: border-color 0.2s;
        }
        .search-box:focus {
            border-color: #4CAF50;
        }
        .filters {
            margin-top: 15px;
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
        }
        .filter-chip {
            padding: 6px 12px;
            background: #f0f0f0;
            border-radius: 20px;
            cursor: pointer;
            transition: all 0.2s;
            border: 2px solid transparent;
            font-size: 14px;
        }
        .filter-chip:hover {
            background: #e0e0e0;
        }
        .filter-chip.active {
            background: #4CAF50;
            color: white;
            border-color: #4CAF50;
        }
        .filter-group {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .filter-label {
            font-weight: 500;
            color: #666;
        }
        .results { 
            margin-top: 20px; 
        }
        .results-summary {
            color: #666;
            margin-bottom: 15px;
            font-size: 14px;
        }
        .metric { 
            background: white;
            border: 1px solid #e0e0e0; 
            padding: 20px; 
            margin: 10px 0; 
            border-radius: 8px;
            transition: box-shadow 0.2s;
        }
        .metric:hover {
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }
        .metric h3 { 
            margin: 0 0 10px 0; 
            color: #1976D2;
        }
        .metric h3 a {
            color: inherit;
            text-decoration: none;
        }
        .metric h3 a:hover {
            text-decoration: underline;
        }
        .metric-meta {
            display: flex;
            gap: 15px;
            margin-bottom: 10px;
            font-size: 14px;
        }
        .metric .type { 
            color: #666; 
            font-weight: 500;
        }
        .metric .domain { 
            background: #f0f0f0; 
            padding: 4px 10px; 
            border-radius: 14px; 
            font-size: 12px;
            font-weight: 500;
        }
        .metric .importance-critical {
            color: #d32f2f;
            font-weight: 500;
        }
        .metric .importance-high {
            color: #f57c00;
            font-weight: 500;
        }
        .metric-description {
            margin: 10px 0;
            color: #555;
        }
        .metric-dimensions {
            font-size: 13px;
            color: #777;
        }
        .no-results {
            text-align: center;
            padding: 40px;
            color: #666;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Metric Catalog Search</h1>
        
        <div class="search-container">
            <input type="text" class="search-box" placeholder="Search metrics by name, description, or dimension..." id="searchBox">
            
            <div class="filters">
                <div class="filter-group">
                    <span class="filter-label">Type:</span>
                    <span class="filter-chip" data-filter="type" data-value="all">All</span>
                    <span class="filter-chip" data-filter="type" data-value="simple">Simple</span>
                    <span class="filter-chip" data-filter="type" data-value="ratio">Ratio</span>
                    <span class="filter-chip" data-filter="type" data-value="derived">Derived</span>
                </div>
                
                <div class="filter-group">
                    <span class="filter-label">Importance:</span>
                    <span class="filter-chip" data-filter="importance" data-value="all">All</span>
                    <span class="filter-chip" data-filter="importance" data-value="critical">Critical</span>
                    <span class="filter-chip" data-filter="importance" data-value="high">High</span>
                </div>
            </div>
        </div>
        
        <div class="results-summary" id="resultsSummary"></div>
        <div class="results" id="results"></div>
    </div>
    
    <script>
        let metricsData = [];
        let filters = {
            type: 'all',
            importance: 'all'
        };
        
        // Load metrics data
        fetch('search-index.json')
            .then(response => response.json())
            .then(data => {
                metricsData = data;
                // Enhance data with importance if available
                metricsData = metricsData.map(metric => ({
                    ...metric,
                    importance: metric.meta?.importance || 'medium'
                }));
                displayResults(metricsData);
            });
        
        // Search functionality
        document.getElementById('searchBox').addEventListener('input', function(e) {
            filterAndDisplay();
        });
        
        // Filter chips functionality
        document.querySelectorAll('.filter-chip').forEach(chip => {
            chip.addEventListener('click', function() {
                const filterType = this.dataset.filter;
                const filterValue = this.dataset.value;
                
                // Update active state
                document.querySelectorAll(`[data-filter="${filterType}"]`).forEach(c => {
                    c.classList.remove('active');
                });
                this.classList.add('active');
                
                // Update filter
                filters[filterType] = filterValue;
                filterAndDisplay();
            });
        });
        
        // Initialize default active filters
        document.querySelectorAll('[data-value="all"]').forEach(chip => {
            chip.classList.add('active');
        });
        
        function filterAndDisplay() {
            const query = document.getElementById('searchBox').value.toLowerCase();
            
            let results = metricsData.filter(metric => {
                // Text search
                const matchesQuery = !query || 
                    metric.name.toLowerCase().includes(query) ||
                    metric.label.toLowerCase().includes(query) ||
                    metric.description.toLowerCase().includes(query) ||
                    metric.dimensions.some(d => d.toLowerCase().includes(query));
                
                // Type filter
                const matchesType = filters.type === 'all' || metric.type === filters.type;
                
                // Importance filter
                const matchesImportance = filters.importance === 'all' || 
                    metric.importance === filters.importance;
                
                return matchesQuery && matchesType && matchesImportance;
            });
            
            displayResults(results);
        }
        
        function displayResults(results) {
            const resultsDiv = document.getElementById('results');
            const summaryDiv = document.getElementById('resultsSummary');
            
            // Update summary
            summaryDiv.textContent = `Found ${results.length} metric${results.length !== 1 ? 's' : ''}`;
            
            if (results.length === 0) {
                resultsDiv.innerHTML = '<div class="no-results">No metrics found matching your criteria</div>';
                return;
            }
            
            resultsDiv.innerHTML = results.map(metric => {
                const importanceClass = metric.importance === 'critical' ? 'importance-critical' : 
                                      metric.importance === 'high' ? 'importance-high' : '';
                const importanceIcon = metric.importance === 'critical' ? '🔴 ' : 
                                     metric.importance === 'high' ? '🟡 ' : '';
                
                return `
                <div class="metric">
                    <h3><a href="${metric.url}">${importanceIcon}${metric.label}</a></h3>
                    <div class="metric-meta">
                        <span class="type">Type: ${metric.type}</span>
                        <span class="domain">${metric.domain}</span>
                        ${importanceClass ? `<span class="${importanceClass}">${metric.importance.toUpperCase()}</span>` : ''}
                    </div>
                    <p class="metric-description">${metric.description}</p>
                    ${metric.dimensions.length > 0 ? 
                        `<p class="metric-dimensions">📐 Dimensions: ${metric.dimensions.slice(0, 5).join(', ')}${metric.dimensions.length > 5 ? ` (+${metric.dimensions.length - 5} more)` : ''}</p>` : 
                        ''}
                </div>
            `;
            }).join('');
        }
    </script>
</body>
</html>
        """
        
        search_path = base_path / "search.html"
        with open(search_path, 'w') as f:
            f.write(html_content)