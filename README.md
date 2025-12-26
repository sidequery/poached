# poached

A DuckDB extension that exposes SQL parsing functionality for building IDEs, SQL editors, query analyzers, and other developer tools.

## Features

API shape: `parse_*` returns detailed rows (table functions), while `parse_*_names` returns names-only lists (scalars).

### Tokenization
| Function | Kind | Returns | Description | Deprecated alias of |
| --- | --- | --- | --- | --- |
| `parse_tokens(query)` | table | `byte_position bigint, category varchar` | Returns tokens with byte positions and categories (KEYWORD, IDENTIFIER, OPERATOR, NUMERIC_CONSTANT, STRING_CONSTANT). Uses DuckDB's internal tokenizer for accurate syntax highlighting. Note: comments are stripped before tokenization. | - |
| `tokenize_sql(query)` | table | `byte_position bigint, category varchar` | Deprecated alias. | `parse_tokens` |

### Statement Analysis
| Function | Kind | Returns | Description | Deprecated alias of |
| --- | --- | --- | --- | --- |
| `parse_statements(query)` | table | `stmt_index bigint, stmt_type varchar, error varchar, param_count bigint` | Parse multi-statement SQL, returns statement type and errors. | - |
| `num_statements(query)` | scalar | `bigint` | Count statements in a query. | - |
| `is_valid_sql(query)` | scalar | `boolean` | Check if SQL is syntactically valid. | - |
| `sql_error_message(query)` | scalar | `varchar` (nullable) | Get parse error message (NULL if valid). | - |

### Schema Introspection
| Function | Kind | Returns | Description | Deprecated alias of |
| --- | --- | --- | --- | --- |
| `parse_columns(query, stmt_index)` | table | `col_index bigint, col_name varchar` | Get result column names from SELECT list. | - |
| `parse_column_names(query, stmt_index)` | scalar | `list(varchar)` | Get result column names as array. | - |

### Query Analysis
| Function | Kind | Returns | Description | Deprecated alias of |
| --- | --- | --- | --- | --- |
| `parse_tables(query)` | table | `schema_name varchar, table_name varchar, context varchar` | Extract table references with schema and context. | - |
| `parse_table_names(query)` | scalar | `list(varchar)` | Get table names as array. | - |
| `parse_functions(query)` | table | `function_name varchar, function_type varchar` | Extract function calls. | - |
| `parse_function_names(query)` | scalar | `list(varchar)` | Get function names as array. | - |
| `parse_where(query)` | table | `column_name varchar, operator varchar, value varchar` | Extract WHERE clause conditions. | - |

### Utilities
| Function | Kind | Returns | Description | Deprecated alias of |
| --- | --- | --- | --- | --- |
| `parse_keywords()` | table | `keyword varchar` | List all SQL keywords. | - |
| `parse_keyword_names()` | scalar | `list(varchar)` | Get keyword names as array. | - |
| `is_keyword(str)` | scalar | `boolean` | Check if string is a keyword. | - |
| `sql_strip_comments(query)` | scalar | `varchar` | Remove comments from SQL. | - |
| `parse_sql_json(query)` | scalar | `varchar` (json) | Get parse info as JSON. | - |
| `sql_keywords()` | table | `keyword varchar` | Deprecated alias. | `parse_keywords` |
| `sql_parse_json(query)` | scalar | `varchar` (json) | Deprecated alias. | `parse_sql_json` |

## Installation

```sql
INSTALL poached FROM community;
LOAD poached;
```

## Example Usage

```sql
-- Syntax highlighting
SELECT * FROM parse_tokens('SELECT * FROM users WHERE id = 1');
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

-- Get column names from SELECT
SELECT * FROM parse_columns('SELECT 1 AS num, ''hello'' AS str', 0);
┌───────────┬──────────┐
│ col_index │ col_name │
├───────────┼──────────┤
│         0 │ num      │
│         1 │ str      │
└───────────┴──────────┘

-- Get parse info as JSON
SELECT parse_sql_json('SELECT 1 + 2 AS result');
-- Returns: {"error":false,"statements":[{"type":"SELECT","query":"SELECT (1 + 2) AS result"}]}

-- Extract table names
SELECT parse_table_names('SELECT * FROM users JOIN orders ON true');
-- Returns: [users, orders]
```

## Deprecated aliases (still supported)

- `tokenize_sql` -> `parse_tokens`
- `sql_keywords` -> `parse_keywords`
- `sql_parse_json` -> `parse_sql_json`

## Building from Source

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
