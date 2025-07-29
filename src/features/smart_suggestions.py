"""
Smart Suggestions Feature
Analyzes database schema and suggests relevant metrics based on patterns
"""

import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
from pathlib import Path


class ColumnType(Enum):
    """Common column type patterns"""
    ID = "id"
    AMOUNT = "amount"
    DATE = "date"
    STATUS = "status"
    BOOLEAN = "boolean"
    COUNT = "count"
    SCORE = "score"
    RATE = "rate"
    FOREIGN_KEY = "foreign_key"
    TEXT = "text"
    UNKNOWN = "unknown"


class MetricConfidence(Enum):
    """Confidence levels for suggestions"""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class ColumnInfo:
    """Information about a database column"""
    name: str
    data_type: str
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_table: Optional[str] = None
    sample_values: List[Any] = field(default_factory=list)
    distinct_count: Optional[int] = None
    null_percentage: Optional[float] = None
    

@dataclass
class TableInfo:
    """Information about a database table"""
    name: str
    schema: str
    columns: List[ColumnInfo]
    row_count: Optional[int] = None
    

@dataclass
class MetricSuggestion:
    """A suggested metric"""
    name: str
    label: str
    type: str  # simple, ratio, derived
    description: str
    source: str
    measure: Dict[str, Any]
    dimensions: List[str] = field(default_factory=list)
    filter: Optional[str] = None
    confidence: MetricConfidence = MetricConfidence.MEDIUM
    reason: str = ""
    pattern: Optional[str] = None
    

class SmartSuggestions:
    """Analyzes schemas and suggests metrics"""
    
    def __init__(self):
        self.pattern_rules = self._initialize_pattern_rules()
        self.business_rules = self._initialize_business_rules()
        
    def _initialize_pattern_rules(self) -> Dict[str, Dict[str, Any]]:
        """Initialize column pattern matching rules"""
        return {
            # ID patterns
            r".*_id$|^id$": {
                "type": ColumnType.ID,
                "suggestions": [
                    {
                        "measure_type": "count",
                        "metric_suffix": "count",
                        "description": "Total number of {entity}"
                    },
                    {
                        "measure_type": "count_distinct",
                        "metric_suffix": "unique_count",
                        "description": "Number of unique {entity}"
                    }
                ]
            },
            
            # Amount/Money patterns
            r".*_(amount|price|cost|revenue|payment|fee|total|subtotal)$|^(amount|price|cost|revenue)$": {
                "type": ColumnType.AMOUNT,
                "suggestions": [
                    {
                        "measure_type": "sum",
                        "metric_suffix": "total",
                        "description": "Total {column_name}"
                    },
                    {
                        "measure_type": "average",
                        "metric_suffix": "average",
                        "description": "Average {column_name}"
                    },
                    {
                        "measure_type": "max",
                        "metric_suffix": "max",
                        "description": "Maximum {column_name}"
                    }
                ]
            },
            
            # Date patterns
            r".*_(date|time|at|timestamp)$|^(date|time|created|updated|modified)$": {
                "type": ColumnType.DATE,
                "suggestions": [
                    {
                        "is_dimension": True,
                        "time_grains": ["day", "week", "month", "quarter", "year"]
                    }
                ]
            },
            
            # Status patterns
            r".*_(status|state|stage|type)$|^(status|state|stage|type)$": {
                "type": ColumnType.STATUS,
                "suggestions": [
                    {
                        "measure_type": "count",
                        "group_by_values": True,
                        "description": "Count by {column_name}"
                    }
                ]
            },
            
            # Boolean patterns
            r"^(is_|has_|was_).*|.*_(flag|active|enabled|deleted)$": {
                "type": ColumnType.BOOLEAN,
                "suggestions": [
                    {
                        "measure_type": "count",
                        "filter_true": True,
                        "metric_suffix": "count",
                        "description": "Count of {column_name}"
                    }
                ]
            },
            
            # Score/Rating patterns
            r".*_(score|rating|rank)$|^(score|rating|rank)$": {
                "type": ColumnType.SCORE,
                "suggestions": [
                    {
                        "measure_type": "average",
                        "metric_suffix": "average",
                        "description": "Average {column_name}"
                    },
                    {
                        "measure_type": "percentile",
                        "percentile": 0.5,
                        "metric_suffix": "median",
                        "description": "Median {column_name}"
                    }
                ]
            },
            
            # Rate/Percentage patterns
            r".*_(rate|percent|percentage|ratio)$": {
                "type": ColumnType.RATE,
                "suggestions": [
                    {
                        "measure_type": "average",
                        "metric_suffix": "average",
                        "description": "Average {column_name}"
                    }
                ]
            }
        }
        
    def _initialize_business_rules(self) -> List[Dict[str, Any]]:
        """Initialize business pattern rules"""
        return [
            # Revenue patterns
            {
                "name": "revenue_metrics",
                "requires_columns": ["amount|revenue|price", "date|created"],
                "suggestions": [
                    {
                        "name": "daily_revenue",
                        "type": "simple",
                        "measure_type": "sum",
                        "time_grain": "day"
                    },
                    {
                        "name": "average_order_value",
                        "type": "simple",
                        "measure_type": "average"
                    }
                ]
            },
            
            # User activity patterns
            {
                "name": "user_activity",
                "requires_columns": ["user_id|customer_id", "date|created|timestamp"],
                "suggestions": [
                    {
                        "name": "daily_active_users",
                        "type": "simple",
                        "measure_type": "count_distinct",
                        "column": "user_id",
                        "time_grain": "day"
                    },
                    {
                        "name": "monthly_active_users",
                        "type": "simple",
                        "measure_type": "count_distinct",
                        "column": "user_id",
                        "time_grain": "month"
                    }
                ]
            },
            
            # Conversion funnel patterns
            {
                "name": "conversion_funnel",
                "requires_columns": ["status|state", "id"],
                "detect_values": ["pending", "completed", "failed"],
                "suggestions": [
                    {
                        "name": "conversion_rate",
                        "type": "ratio",
                        "numerator_filter": "status = 'completed'",
                        "denominator_filter": None
                    },
                    {
                        "name": "failure_rate",
                        "type": "ratio",
                        "numerator_filter": "status = 'failed'",
                        "denominator_filter": None
                    }
                ]
            },
            
            # Churn patterns
            {
                "name": "churn_analysis",
                "requires_columns": ["customer_id|user_id", "status|active", "date"],
                "suggestions": [
                    {
                        "name": "active_customers",
                        "type": "simple",
                        "measure_type": "count_distinct",
                        "filter": "status = 'active'"
                    },
                    {
                        "name": "churned_customers",
                        "type": "simple",
                        "measure_type": "count_distinct",
                        "filter": "status = 'churned'"
                    }
                ]
            }
        ]
        
    def analyze_table(self, table_info: TableInfo) -> List[MetricSuggestion]:
        """Analyze a table and suggest metrics"""
        suggestions = []
        
        # Analyze individual columns
        for column in table_info.columns:
            column_suggestions = self._analyze_column(column, table_info)
            suggestions.extend(column_suggestions)
            
        # Analyze column relationships
        relationship_suggestions = self._analyze_relationships(table_info)
        suggestions.extend(relationship_suggestions)
        
        # Apply business rules
        business_suggestions = self._apply_business_rules(table_info)
        suggestions.extend(business_suggestions)
        
        # Remove duplicates and score confidence
        suggestions = self._deduplicate_and_score(suggestions)
        
        return sorted(suggestions, key=lambda s: (
            s.confidence.value,
            s.name
        ), reverse=True)
        
    def _analyze_column(self, column: ColumnInfo, table: TableInfo) -> List[MetricSuggestion]:
        """Analyze a single column for metric suggestions"""
        suggestions = []
        column_type = self._detect_column_type(column)
        
        # Apply pattern rules
        for pattern, rule in self.pattern_rules.items():
            if re.match(pattern, column.name.lower()):
                for suggestion_template in rule.get("suggestions", []):
                    if suggestion_template.get("is_dimension"):
                        # Skip dimension suggestions in this method
                        continue
                        
                    suggestion = self._create_suggestion_from_template(
                        column, table, suggestion_template, rule["type"]
                    )
                    if suggestion:
                        suggestions.append(suggestion)
                        
        return suggestions
        
    def _detect_column_type(self, column: ColumnInfo) -> ColumnType:
        """Detect the semantic type of a column"""
        name_lower = column.name.lower()
        
        # Check patterns
        for pattern, rule in self.pattern_rules.items():
            if re.match(pattern, name_lower):
                return rule["type"]
                
        # Check data type
        if "int" in column.data_type.lower():
            if column.is_foreign_key:
                return ColumnType.FOREIGN_KEY
            return ColumnType.COUNT
        elif "bool" in column.data_type.lower():
            return ColumnType.BOOLEAN
        elif "date" in column.data_type.lower() or "time" in column.data_type.lower():
            return ColumnType.DATE
        elif "char" in column.data_type.lower() or "text" in column.data_type.lower():
            return ColumnType.TEXT
            
        return ColumnType.UNKNOWN
        
    def _create_suggestion_from_template(self, column: ColumnInfo, table: TableInfo,
                                       template: Dict[str, Any], column_type: ColumnType) -> Optional[MetricSuggestion]:
        """Create a metric suggestion from a template"""
        measure_type = template.get("measure_type")
        if not measure_type:
            return None
            
        # Generate metric name
        entity = self._extract_entity_name(table.name)
        column_clean = self._clean_column_name(column.name)
        metric_suffix = template.get("metric_suffix", measure_type)
        
        if column_type == ColumnType.ID and column.is_primary_key:
            metric_name = f"{entity}_{metric_suffix}"
        else:
            metric_name = f"{column_clean}_{metric_suffix}"
            
        # Generate description
        description = template.get("description", f"{measure_type} of {column.name}")
        description = description.format(
            entity=entity,
            column_name=column_clean.replace("_", " ")
        )
        
        # Build measure config
        measure = {
            "type": measure_type,
            "column": column.name
        }
        
        if template.get("percentile"):
            measure["percentile"] = template["percentile"]
            
        # Build filter if needed
        filter_expr = None
        if template.get("filter_true") and column_type == ColumnType.BOOLEAN:
            filter_expr = f"{column.name} = true"
            
        # Determine confidence
        confidence = self._calculate_confidence(column, column_type, measure_type)
        
        return MetricSuggestion(
            name=metric_name,
            label=metric_name.replace("_", " ").title(),
            type="simple",
            description=description,
            source=table.name,
            measure=measure,
            filter=filter_expr,
            confidence=confidence,
            reason=f"{column_type.value} column detected",
            pattern=column_type.value
        )
        
    def _analyze_relationships(self, table: TableInfo) -> List[MetricSuggestion]:
        """Analyze relationships between columns for metric suggestions"""
        suggestions = []
        
        # Find foreign keys
        foreign_keys = [col for col in table.columns if col.is_foreign_key]
        
        # Suggest unique counts for foreign keys
        for fk in foreign_keys:
            entity = self._extract_entity_from_fk(fk.name)
            suggestion = MetricSuggestion(
                name=f"unique_{entity}s",
                label=f"Unique {entity.title()}s",
                type="simple",
                description=f"Number of unique {entity}s",
                source=table.name,
                measure={
                    "type": "count_distinct",
                    "column": fk.name
                },
                confidence=MetricConfidence.HIGH,
                reason="Foreign key relationship detected",
                pattern="relationship"
            )
            suggestions.append(suggestion)
            
        # Find date columns for time-based analysis
        date_columns = [col for col in table.columns 
                       if self._detect_column_type(col) == ColumnType.DATE]
        
        if date_columns:
            # Add suggested time dimensions
            primary_date = date_columns[0]  # Use first date column
            dimensions = [primary_date.name]
            
            # Update existing suggestions with time dimension
            for suggestion in suggestions:
                if suggestion.dimensions is None:
                    suggestion.dimensions = []
                suggestion.dimensions.extend(dimensions)
                
        return suggestions
        
    def _apply_business_rules(self, table: TableInfo) -> List[MetricSuggestion]:
        """Apply business rules to suggest complex metrics"""
        suggestions = []
        column_names = [col.name.lower() for col in table.columns]
        
        for rule in self.business_rules:
            # Check if required columns exist
            matches = True
            matched_columns = {}
            
            for required in rule.get("requires_columns", []):
                found = False
                for col_name in column_names:
                    if any(part in col_name for part in required.split("|")):
                        matched_columns[required.split("|")[0]] = col_name
                        found = True
                        break
                if not found:
                    matches = False
                    break
                    
            if not matches:
                continue
                
            # Generate suggestions from rule
            for suggestion_template in rule.get("suggestions", []):
                suggestion = self._create_business_rule_suggestion(
                    table, rule, suggestion_template, matched_columns
                )
                if suggestion:
                    suggestions.append(suggestion)
                    
        return suggestions
        
    def _create_business_rule_suggestion(self, table: TableInfo, rule: Dict[str, Any],
                                       template: Dict[str, Any], matched_columns: Dict[str, str]) -> Optional[MetricSuggestion]:
        """Create a suggestion from a business rule"""
        name = template["name"]
        metric_type = template["type"]
        
        if metric_type == "simple":
            measure = {
                "type": template["measure_type"],
                "column": matched_columns.get(template.get("column", "id"), 
                         table.columns[0].name if table.columns[0].is_primary_key else "id")
            }
            
            return MetricSuggestion(
                name=name,
                label=name.replace("_", " ").title(),
                type=metric_type,
                description=f"{name.replace('_', ' ').title()} for {self._extract_entity_name(table.name)}",
                source=table.name,
                measure=measure,
                filter=template.get("filter"),
                confidence=MetricConfidence.HIGH,
                reason=f"Business pattern '{rule['name']}' detected",
                pattern=rule["name"]
            )
            
        elif metric_type == "ratio":
            # Create ratio metric
            base_measure = {
                "type": "count",
                "column": matched_columns.get("id", "id")
            }
            
            numerator = {
                "source": table.name,
                "measure": base_measure.copy()
            }
            if template.get("numerator_filter"):
                numerator["filter"] = template["numerator_filter"]
                
            denominator = {
                "source": table.name,
                "measure": base_measure.copy()
            }
            if template.get("denominator_filter"):
                denominator["filter"] = template["denominator_filter"]
                
            return MetricSuggestion(
                name=name,
                label=name.replace("_", " ").title(),
                type="ratio",
                description=f"{name.replace('_', ' ').title()} calculation",
                source=table.name,
                measure={
                    "numerator": numerator,
                    "denominator": denominator
                },
                confidence=MetricConfidence.MEDIUM,
                reason=f"Business pattern '{rule['name']}' detected",
                pattern=rule["name"]
            )
            
        return None
        
    def _calculate_confidence(self, column: ColumnInfo, column_type: ColumnType, 
                            measure_type: str) -> MetricConfidence:
        """Calculate confidence score for a suggestion"""
        score = 0
        
        # Column type matches
        if column_type in [ColumnType.ID, ColumnType.AMOUNT, ColumnType.FOREIGN_KEY]:
            score += 3
        elif column_type in [ColumnType.DATE, ColumnType.STATUS, ColumnType.BOOLEAN]:
            score += 2
        else:
            score += 1
            
        # Primary key bonus
        if column.is_primary_key and measure_type in ["count", "count_distinct"]:
            score += 2
            
        # Foreign key bonus
        if column.is_foreign_key and measure_type == "count_distinct":
            score += 2
            
        # Data quality
        if column.null_percentage is not None and column.null_percentage < 0.1:
            score += 1
            
        # Name clarity
        if not any(word in column.name.lower() for word in ["temp", "test", "old", "backup"]):
            score += 1
            
        # Convert score to confidence
        if score >= 6:
            return MetricConfidence.HIGH
        elif score >= 4:
            return MetricConfidence.MEDIUM
        else:
            return MetricConfidence.LOW
            
    def _deduplicate_and_score(self, suggestions: List[MetricSuggestion]) -> List[MetricSuggestion]:
        """Remove duplicate suggestions and finalize scoring"""
        seen = set()
        unique_suggestions = []
        
        for suggestion in suggestions:
            key = (suggestion.name, suggestion.source, suggestion.type)
            if key not in seen:
                seen.add(key)
                unique_suggestions.append(suggestion)
                
        return unique_suggestions
        
    def _extract_entity_name(self, table_name: str) -> str:
        """Extract entity name from table name"""
        # Remove common prefixes
        name = re.sub(r"^(fct_|fact_|dim_|dimension_|stg_|staging_)", "", table_name.lower())
        
        # Remove common suffixes
        name = re.sub(r"(_fact|_dim|_dimension|_table)$", "", name)
        
        # Singularize if needed
        if name.endswith("s") and not name.endswith("ss"):
            name = name[:-1]
            
        return name
        
    def _extract_entity_from_fk(self, fk_name: str) -> str:
        """Extract entity name from foreign key"""
        # Remove _id suffix
        name = re.sub(r"_id$", "", fk_name.lower())
        return name
        
    def _clean_column_name(self, column_name: str) -> str:
        """Clean column name for metric naming"""
        # Remove common prefixes/suffixes
        name = column_name.lower()
        name = re.sub(r"^(is_|has_|was_)", "", name)
        name = re.sub(r"(_flag|_at|_date|_time)$", "", name)
        return name
        
    def format_suggestions(self, suggestions: List[MetricSuggestion], 
                         format_type: str = "yaml") -> str:
        """Format suggestions for output"""
        if format_type == "yaml":
            return self._format_yaml(suggestions)
        elif format_type == "json":
            return self._format_json(suggestions)
        else:
            return self._format_text(suggestions)
            
    def _format_yaml(self, suggestions: List[MetricSuggestion]) -> str:
        """Format suggestions as YAML"""
        output = ["suggested_metrics:"]
        
        # Group by confidence
        high = [s for s in suggestions if s.confidence == MetricConfidence.HIGH]
        medium = [s for s in suggestions if s.confidence == MetricConfidence.MEDIUM]
        low = [s for s in suggestions if s.confidence == MetricConfidence.LOW]
        
        if high:
            output.append("  # High confidence suggestions")
            for suggestion in high:
                output.extend(self._format_single_yaml(suggestion, indent=2))
                
        if medium:
            output.append("\n  # Medium confidence suggestions")
            for suggestion in medium:
                output.extend(self._format_single_yaml(suggestion, indent=2))
                
        if low:
            output.append("\n  # Low confidence suggestions (review carefully)")
            for suggestion in low:
                output.extend(self._format_single_yaml(suggestion, indent=2))
                
        # Add pattern summary
        patterns = {}
        for s in suggestions:
            if s.pattern:
                patterns[s.pattern] = patterns.get(s.pattern, 0) + 1
                
        if patterns:
            output.append("\n# Pattern summary:")
            for pattern, count in sorted(patterns.items(), key=lambda x: x[1], reverse=True):
                output.append(f"#   {pattern}: {count} metrics")
                
        return "\n".join(output)
        
    def _format_single_yaml(self, suggestion: MetricSuggestion, indent: int = 0) -> List[str]:
        """Format a single suggestion as YAML"""
        prefix = " " * indent
        lines = [
            f"{prefix}- name: {suggestion.name}",
            f"{prefix}  label: {suggestion.label}",
            f"{prefix}  type: {suggestion.type}",
            f"{prefix}  description: {suggestion.description}",
            f"{prefix}  source: {suggestion.source}",
        ]
        
        if suggestion.type == "simple":
            lines.append(f"{prefix}  measure:")
            for key, value in suggestion.measure.items():
                lines.append(f"{prefix}    {key}: {value}")
        else:
            # Handle complex types
            lines.append(f"{prefix}  # Complex metric - review configuration")
            lines.append(f"{prefix}  measure: {json.dumps(suggestion.measure, indent=2)}")
            
        if suggestion.filter:
            lines.append(f"{prefix}  filter: \"{suggestion.filter}\"")
            
        if suggestion.dimensions:
            lines.append(f"{prefix}  dimensions:")
            for dim in suggestion.dimensions:
                lines.append(f"{prefix}    - {dim}")
                
        lines.append(f"{prefix}  confidence: {suggestion.confidence.value}")
        lines.append(f"{prefix}  reason: \"{suggestion.reason}\"")
        lines.append("")
        
        return lines
        
    def _format_json(self, suggestions: List[MetricSuggestion]) -> str:
        """Format suggestions as JSON"""
        data = {
            "suggested_metrics": [
                {
                    "name": s.name,
                    "label": s.label,
                    "type": s.type,
                    "description": s.description,
                    "source": s.source,
                    "measure": s.measure,
                    "filter": s.filter,
                    "dimensions": s.dimensions,
                    "confidence": s.confidence.value,
                    "reason": s.reason,
                    "pattern": s.pattern
                }
                for s in suggestions
            ],
            "summary": {
                "total": len(suggestions),
                "by_confidence": {
                    "high": len([s for s in suggestions if s.confidence == MetricConfidence.HIGH]),
                    "medium": len([s for s in suggestions if s.confidence == MetricConfidence.MEDIUM]),
                    "low": len([s for s in suggestions if s.confidence == MetricConfidence.LOW])
                },
                "by_type": {
                    "simple": len([s for s in suggestions if s.type == "simple"]),
                    "ratio": len([s for s in suggestions if s.type == "ratio"]),
                    "derived": len([s for s in suggestions if s.type == "derived"])
                }
            }
        }
        return json.dumps(data, indent=2)
        
    def _format_text(self, suggestions: List[MetricSuggestion]) -> str:
        """Format suggestions as human-readable text"""
        lines = [
            "Metric Suggestions Analysis",
            "=" * 50,
            f"Total suggestions: {len(suggestions)}",
            ""
        ]
        
        # Group by confidence
        for confidence in [MetricConfidence.HIGH, MetricConfidence.MEDIUM, MetricConfidence.LOW]:
            conf_suggestions = [s for s in suggestions if s.confidence == confidence]
            if conf_suggestions:
                lines.append(f"\n{confidence.value.upper()} CONFIDENCE ({len(conf_suggestions)} metrics):")
                lines.append("-" * 30)
                
                for s in conf_suggestions:
                    lines.append(f"\n📊 {s.label}")
                    lines.append(f"   Name: {s.name}")
                    lines.append(f"   Type: {s.type}")
                    lines.append(f"   Description: {s.description}")
                    lines.append(f"   Source: {s.source}")
                    lines.append(f"   Reason: {s.reason}")
                    
        return "\n".join(lines)