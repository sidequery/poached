# poached

A DuckDB extension that exposes SQL parsing functionality for building IDEs, SQL editors, query analyzers, and other developer tools.

## Features

### Tokenization
- `tokenize_sql(query)` - Returns tokens with byte positions and categories (KEYWORD, IDENTIFIER, OPERATOR, NUMERIC_CONSTANT, STRING_CONSTANT, COMMENT, ERROR). Uses DuckDB's internal tokenizer for accurate syntax highlighting.

### Statement Analysis
- `parse_statements(query)` - Parse multi-statement SQL, returns statement type, errors, and parameter count
- `num_statements(query)` - Count statements in a query
- `is_valid_sql(query)` - Check if SQL is syntactically valid
- `sql_error_message(query)` - Get parse error message (NULL if valid)

### Schema Introspection
- `parse_columns(query, stmt_index)` - Get result column names and types
- `parse_column_types(query, stmt_index)` - Get detailed type info including nested types
- `parse_type_info(query, stmt_index, col_index)` - Get full type details for a single column
- `parse_parameters(query, stmt_index)` - Extract prepared statement parameters

### Query Analysis
- `parse_tables(query)` - Extract table references with schema and context
- `parse_table_names(query)` - Get table names as array
- `parse_functions(query)` - Extract function calls with type (scalar/aggregate)
- `parse_function_names(query)` - Get function names as array
- `parse_where(query)` - Extract WHERE clause conditions

### Utilities
- `sql_keywords()` - List all SQL keywords
- `is_keyword(str)` - Check if string is a keyword
- `sql_strip_comments(query)` - Remove comments from SQL
- `sql_parse_json(query)` - Get full query plan as JSON

## Example Usage

```sql
-- Syntax highlighting
SELECT * FROM tokenize_sql('SELECT * FROM users WHERE id = 1');
┌───────────────┬──────────────────┐
│ byte_position │     category     │
├───────────────┼──────────────────┤
│             0 │ KEYWORD          │
│             7 │ OPERATOR         │
│             9 │ KEYWORD          │
│            14 │ IDENTIFIER       │
│            20 │ KEYWORD          │
│            26 │ IDENTIFIER       │
│            29 │ OPERATOR         │
│            31 │ NUMERIC_CONSTANT │
└───────────────┴──────────────────┘

-- Validate SQL
SELECT is_valid_sql('SELECT * FROM');  -- false
SELECT sql_error_message('SELECT * FROM');  -- Parser Error: ...

-- Extract functions
SELECT * FROM parse_functions('SELECT COUNT(*), UPPER(name) FROM t');
┌───────────────┬───────────────┐
│ function_name │ function_type │
├───────────────┼───────────────┤
│ count_star    │ aggregate     │
│ upper         │ scalar        │
└───────────────┴───────────────┘

-- Get result schema
SELECT * FROM parse_columns('SELECT 1 AS num, ''hello'' AS str', 0);
┌───────────┬──────────┬──────────┐
│ col_index │ col_name │ col_type │
├───────────┼──────────┼──────────┤
│         0 │ num      │ INTEGER  │
│         1 │ str      │ VARCHAR  │
└───────────┴──────────┴──────────┘

-- Get full query plan as JSON
SELECT sql_parse_json('SELECT 1 + 2 AS result');
-- Returns:
-- {
--   "error": false,
--   "plans": [{
--     "type": "LOGICAL_PROJECTION",
--     "expressions": [{
--       "alias": "result",
--       "name": "+",
--       "return_type": {"id": "INTEGER"},
--       "children": [...]
--     }],
--     "children": [{"type": "LOGICAL_DUMMY_SCAN"}]
--   }]
-- }

-- Extract specific info from query plan
SELECT json_extract_string(sql_parse_json('SELECT 1 + 2 AS x'), '$.plans[0].expressions[0].alias');
-- Returns: x
```

## Building

```shell
# Clone with submodules
git clone --recurse-submodules <repo>

# Configure (sets up Python venv with test dependencies)
make configure

# Build release
make release

# Run tests
make test_release
```

## Dependencies

- C/C++ toolchain
- Python3 + venv
- Make
- CMake
- Git

## Configuration

The `Makefile` controls build settings:

- `TARGET_DUCKDB_VERSION` - DuckDB version to target (default: v1.4.2)
- `USE_UNSTABLE_C_API` - Set to 1 for latest API features (pins to exact DuckDB version)

When changing versions, run `make update_duckdb_headers` to sync the C API headers.
