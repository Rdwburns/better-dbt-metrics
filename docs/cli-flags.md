# CLI Flags Reference

## Compilation Output Control

### `--verbose, -v`
**Purpose**: User-friendly detailed progress and error information  
**When to use**: When you want to see what's happening during compilation  
**Output includes**:
- Step-by-step progress messages: `🔍 Running pre-compilation validation...`
- All warnings and info messages in error reports
- Detailed error context and suggestions
- Automatically enables `--debug` for complete visibility

**Example**:
```bash
better-dbt-metrics compile --verbose
```

### `--debug`
**Purpose**: Technical debugging for troubleshooting issues  
**When to use**: When compilation fails unexpectedly or you're developing/debugging the tool  
**Output includes**:
- Internal state information: `[DEBUG] Attempting to import: ../templates/time.yml`
- Stack traces for exceptions
- Detailed execution flow
- Template expansion details
- Reference resolution steps

**Example**:
```bash
better-dbt-metrics compile --debug
```

## Key Differences

| Flag | Target Audience | Use Case | Output Style |
|------|-----------------|----------|--------------|
| `--verbose` | End users | Understanding compilation progress | User-friendly with emojis and clear language |
| `--debug` | Developers | Troubleshooting technical issues | Technical details and internal state |

## Relationship

- `--verbose` **implies** `--debug` (verbose users get all available information)
- `--debug` alone provides technical details without progress messages
- Use `--verbose` for general troubleshooting
- Use `--debug` alone when you only want technical diagnostics

## Examples

### Standard compilation (minimal output)
```bash
better-dbt-metrics compile
# Output: ✅ Compilation completed successfully!
```

### With progress information
```bash
better-dbt-metrics compile --verbose
# Output: 
# 🔍 Running pre-compilation validation...
# 🔨 Compiling metrics...
# [DEBUG] Template expanded successfully
# ✅ Compilation completed successfully!
```

### Technical debugging only
```bash
better-dbt-metrics compile --debug
# Output:
# [DEBUG] Attempting to import: ../templates/time.yml
# [DEBUG] Template expansion failed: KeyError
# ✅ Compilation completed successfully!
```

### Error reporting differences

#### Standard error (no flags)
```
❌ Compilation failed with 2 errors
```

#### With --verbose
```
============================================================
📊 Better-DBT-Metrics Compilation Report
============================================================
📋 Issues: ❌ 2 error(s) | ⚠️ 3 warning(s)
[Full detailed report with suggestions]
============================================================
```

#### With --debug only  
```
❌ Compilation failed with 2 errors
[DEBUG] Error in _compile_metric: KeyError: 'measure'
[DEBUG] Full traceback: ...
```

## Best Practices

1. **Development**: Use `--verbose` to understand what's happening
2. **CI/CD**: Use `--report-format json` or `--report-format junit` for structured output
3. **Troubleshooting**: Start with `--verbose`, escalate to `--debug` if needed
4. **Bug reports**: Always include `--debug` output when reporting issues

## Other Output Flags

### `--report-format`
Controls error report format:
- `terminal` (default): Human-readable with colors
- `json`: Structured data for programmatic use
- `junit`: XML format for CI/CD systems

### `--json-output`
Legacy flag for JSON output (use `--report-format json` instead)

The distinction ensures users get appropriate information without overwhelming them with technical details unless requested.