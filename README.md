# poached

A DuckDB extension that exposes SQL parsing functionality for building IDEs, SQL editors, query analyzers, and other developer tools.

## Features

### Tokenization
- `tokenize_sql(query)` - Returns tokens with byte positions and categories (KEYWORD, IDENTIFIER, OPERATOR, NUMERIC_CONSTANT, STRING_CONSTANT, COMMENT, ERROR). Uses DuckDB's internal tokenizer for accurate syntax highlighting.

### Statement Analysis
- `parse_statements(query)` - Parse multi-statement SQL, returns statement type and errors
- `num_statements(query)` - Count statements in a query
- `is_valid_sql(query)` - Check if SQL is syntactically valid
- `sql_error_message(query)` - Get parse error message (NULL if valid)

### Schema Introspection
- `parse_columns(query, stmt_index)` - Get result column names from SELECT list

### Query Analysis
- `parse_tables(query)` - Extract table references with schema and context
- `parse_table_names(query)` - Get table names as array
- `parse_functions(query)` - Extract function calls
- `parse_function_names(query)` - Get function names as array
- `parse_where(query)` - Extract WHERE clause conditions

### Utilities
- `sql_keywords()` - List all SQL keywords
- `is_keyword(str)` - Check if string is a keyword
- `sql_strip_comments(query)` - Remove comments from SQL
- `sql_parse_json(query)` - Get parse info as JSON

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
│ count_star    │ scalar        │
│ upper         │ scalar        │
└───────────────┴───────────────┘

-- Get column names from SELECT
SELECT * FROM parse_columns('SELECT 1 AS num, ''hello'' AS str', 0);
┌───────────┬──────────┐
│ col_index │ col_name │
├───────────┼──────────┤
│         0 │ num      │
│         1 │ str      │
└───────────┴──────────┘

-- Get parse info as JSON
SELECT sql_parse_json('SELECT 1 + 2 AS result');
-- Returns: {"error":false,"statements":[{"type":"SELECT","query":"SELECT (1 + 2) AS result"}]}

-- Extract table names
SELECT parse_table_names('SELECT * FROM users JOIN orders ON true');
-- Returns: [users, orders]
```

## Building

```shell
# Clone with submodules
git clone --recurse-submodules <repo>

# Build release
make release

# Run tests
make test_release
```

## Dependencies

- C/C++ toolchain
- Make
- CMake
- Git

## License
MIT License. See [LICENSE](LICENSE) for details.
