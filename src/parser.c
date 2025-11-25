#include "duckdb_extension.h"
#include "parser.h"
#include "tokenizer.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

DUCKDB_EXTENSION_EXTERN

// We don't store the database globally anymore - we create fresh in-memory DBs for parsing

// Helper: Convert statement type enum to string
static const char *StatementTypeToString(duckdb_statement_type type) {
	switch (type) {
	case DUCKDB_STATEMENT_TYPE_SELECT:
		return "SELECT";
	case DUCKDB_STATEMENT_TYPE_INSERT:
		return "INSERT";
	case DUCKDB_STATEMENT_TYPE_UPDATE:
		return "UPDATE";
	case DUCKDB_STATEMENT_TYPE_EXPLAIN:
		return "EXPLAIN";
	case DUCKDB_STATEMENT_TYPE_DELETE:
		return "DELETE";
	case DUCKDB_STATEMENT_TYPE_PREPARE:
		return "PREPARE";
	case DUCKDB_STATEMENT_TYPE_CREATE:
		return "CREATE";
	case DUCKDB_STATEMENT_TYPE_EXECUTE:
		return "EXECUTE";
	case DUCKDB_STATEMENT_TYPE_ALTER:
		return "ALTER";
	case DUCKDB_STATEMENT_TYPE_TRANSACTION:
		return "TRANSACTION";
	case DUCKDB_STATEMENT_TYPE_COPY:
		return "COPY";
	case DUCKDB_STATEMENT_TYPE_ANALYZE:
		return "ANALYZE";
	case DUCKDB_STATEMENT_TYPE_VARIABLE_SET:
		return "VARIABLE_SET";
	case DUCKDB_STATEMENT_TYPE_CREATE_FUNC:
		return "CREATE_FUNC";
	case DUCKDB_STATEMENT_TYPE_DROP:
		return "DROP";
	case DUCKDB_STATEMENT_TYPE_EXPORT:
		return "EXPORT";
	case DUCKDB_STATEMENT_TYPE_PRAGMA:
		return "PRAGMA";
	case DUCKDB_STATEMENT_TYPE_VACUUM:
		return "VACUUM";
	case DUCKDB_STATEMENT_TYPE_CALL:
		return "CALL";
	case DUCKDB_STATEMENT_TYPE_SET:
		return "SET";
	case DUCKDB_STATEMENT_TYPE_LOAD:
		return "LOAD";
	case DUCKDB_STATEMENT_TYPE_RELATION:
		return "RELATION";
	case DUCKDB_STATEMENT_TYPE_EXTENSION:
		return "EXTENSION";
	case DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN:
		return "LOGICAL_PLAN";
	case DUCKDB_STATEMENT_TYPE_ATTACH:
		return "ATTACH";
	case DUCKDB_STATEMENT_TYPE_DETACH:
		return "DETACH";
	case DUCKDB_STATEMENT_TYPE_MULTI:
		return "MULTI";
	default:
		return "INVALID";
	}
}

// Helper: Convert duckdb_type enum to string
static const char *TypeToString(duckdb_type type) {
	switch (type) {
	case DUCKDB_TYPE_BOOLEAN:
		return "BOOLEAN";
	case DUCKDB_TYPE_TINYINT:
		return "TINYINT";
	case DUCKDB_TYPE_SMALLINT:
		return "SMALLINT";
	case DUCKDB_TYPE_INTEGER:
		return "INTEGER";
	case DUCKDB_TYPE_BIGINT:
		return "BIGINT";
	case DUCKDB_TYPE_UTINYINT:
		return "UTINYINT";
	case DUCKDB_TYPE_USMALLINT:
		return "USMALLINT";
	case DUCKDB_TYPE_UINTEGER:
		return "UINTEGER";
	case DUCKDB_TYPE_UBIGINT:
		return "UBIGINT";
	case DUCKDB_TYPE_FLOAT:
		return "FLOAT";
	case DUCKDB_TYPE_DOUBLE:
		return "DOUBLE";
	case DUCKDB_TYPE_TIMESTAMP:
		return "TIMESTAMP";
	case DUCKDB_TYPE_DATE:
		return "DATE";
	case DUCKDB_TYPE_TIME:
		return "TIME";
	case DUCKDB_TYPE_INTERVAL:
		return "INTERVAL";
	case DUCKDB_TYPE_HUGEINT:
		return "HUGEINT";
	case DUCKDB_TYPE_UHUGEINT:
		return "UHUGEINT";
	case DUCKDB_TYPE_VARCHAR:
		return "VARCHAR";
	case DUCKDB_TYPE_BLOB:
		return "BLOB";
	case DUCKDB_TYPE_DECIMAL:
		return "DECIMAL";
	case DUCKDB_TYPE_TIMESTAMP_S:
		return "TIMESTAMP_S";
	case DUCKDB_TYPE_TIMESTAMP_MS:
		return "TIMESTAMP_MS";
	case DUCKDB_TYPE_TIMESTAMP_NS:
		return "TIMESTAMP_NS";
	case DUCKDB_TYPE_ENUM:
		return "ENUM";
	case DUCKDB_TYPE_LIST:
		return "LIST";
	case DUCKDB_TYPE_STRUCT:
		return "STRUCT";
	case DUCKDB_TYPE_MAP:
		return "MAP";
	case DUCKDB_TYPE_ARRAY:
		return "ARRAY";
	case DUCKDB_TYPE_UUID:
		return "UUID";
	case DUCKDB_TYPE_UNION:
		return "UNION";
	case DUCKDB_TYPE_BIT:
		return "BIT";
	case DUCKDB_TYPE_TIME_TZ:
		return "TIME_TZ";
	case DUCKDB_TYPE_TIMESTAMP_TZ:
		return "TIMESTAMP_TZ";
	case DUCKDB_TYPE_ANY:
		return "ANY";
	case DUCKDB_TYPE_SQLNULL:
		return "SQLNULL";
	default:
		return "UNKNOWN";
	}
}

// Helper: Get string from duckdb_string_t
static char *GetString(duckdb_string_t *str_data, idx_t row) {
	if (duckdb_string_is_inlined(str_data[row])) {
		return strndup(str_data[row].value.inlined.inlined, str_data[row].value.inlined.length);
	} else {
		return strndup(str_data[row].value.pointer.ptr, str_data[row].value.pointer.length);
	}
}

// Helper: Check if row is valid
static bool IsRowValid(uint64_t *validity, idx_t row) {
	return !validity || duckdb_validity_row_is_valid(validity, row);
}

// Helper: Set row invalid
static void SetRowInvalid(duckdb_vector output, idx_t row) {
	duckdb_vector_ensure_validity_writable(output);
	uint64_t *out_validity = duckdb_vector_get_validity(output);
	duckdb_validity_set_row_invalid(out_validity, row);
}

// ============================================================================
// parse_statements(query) table function
// Returns one row per statement: index, type, error, param_count
// ============================================================================

typedef struct {
	char *query;
	duckdb_database db;
	duckdb_connection conn;
	duckdb_extracted_statements stmts;
	idx_t stmt_count;
	const char *error;
} ParseStatementsBindData;

typedef struct {
	idx_t current_idx;
} ParseStatementsInitData;

static void ParseStatementsBindDataDestroy(void *data) {
	ParseStatementsBindData *bind = (ParseStatementsBindData *)data;
	if (bind) {
		if (bind->stmts) {
			duckdb_destroy_extracted(&bind->stmts);
		}
		if (bind->conn) {
			duckdb_disconnect(&bind->conn);
		}
		if (bind->db) {
			duckdb_close(&bind->db);
		}
		if (bind->query) {
			free(bind->query);
		}
		free(bind);
	}
}

static void ParseStatementsInitDataDestroy(void *data) {
	if (data) {
		free(data);
	}
}

static void ParseStatementsBind(duckdb_bind_info info) {
	ParseStatementsBindData *bind = (ParseStatementsBindData *)malloc(sizeof(ParseStatementsBindData));
	if (!bind) {
		duckdb_bind_set_error(info, "Failed to allocate bind data");
		return;
	}
	memset(bind, 0, sizeof(ParseStatementsBindData));

	// Get query parameter
	duckdb_value query_val = duckdb_bind_get_parameter(info, 0);
	if (!query_val) {
		duckdb_bind_set_error(info, "Failed to get query parameter");
		free(bind);
		return;
	}
	bind->query = duckdb_get_varchar(query_val);
	duckdb_destroy_value(&query_val);

	if (!bind->query) {
		duckdb_bind_set_error(info, "Failed to get query string");
		free(bind);
		return;
	}

	// Create in-memory database for parsing
	if (duckdb_open(NULL, &bind->db) != DuckDBSuccess) {
		bind->error = "Failed to open in-memory database";
	} else if (duckdb_connect(bind->db, &bind->conn) != DuckDBSuccess) {
		bind->error = "Failed to connect to database";
	} else {
		bind->stmt_count = duckdb_extract_statements(bind->conn, bind->query, &bind->stmts);
		if (bind->stmt_count == 0 && bind->stmts) {
			bind->error = duckdb_extract_statements_error(bind->stmts);
		}
	}

	// Add result columns
	duckdb_logical_type bigint_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);

	duckdb_bind_add_result_column(info, "stmt_index", bigint_type);
	duckdb_bind_add_result_column(info, "stmt_type", varchar_type);
	duckdb_bind_add_result_column(info, "error", varchar_type);
	duckdb_bind_add_result_column(info, "param_count", bigint_type);

	duckdb_destroy_logical_type(&bigint_type);
	duckdb_destroy_logical_type(&varchar_type);

	duckdb_bind_set_bind_data(info, bind, ParseStatementsBindDataDestroy);
}

static void ParseStatementsInit(duckdb_init_info info) {
	ParseStatementsInitData *init = (ParseStatementsInitData *)malloc(sizeof(ParseStatementsInitData));
	init->current_idx = 0;
	duckdb_init_set_init_data(info, init, ParseStatementsInitDataDestroy);
}

static void ParseStatementsFunc(duckdb_function_info info, duckdb_data_chunk output) {
	ParseStatementsBindData *bind = (ParseStatementsBindData *)duckdb_function_get_bind_data(info);
	ParseStatementsInitData *init = (ParseStatementsInitData *)duckdb_function_get_init_data(info);

	duckdb_vector idx_vec = duckdb_data_chunk_get_vector(output, 0);
	duckdb_vector type_vec = duckdb_data_chunk_get_vector(output, 1);
	duckdb_vector error_vec = duckdb_data_chunk_get_vector(output, 2);
	duckdb_vector param_vec = duckdb_data_chunk_get_vector(output, 3);

	int64_t *idx_data = (int64_t *)duckdb_vector_get_data(idx_vec);
	int64_t *param_data = (int64_t *)duckdb_vector_get_data(param_vec);

	idx_t count = 0;
	idx_t max_count = duckdb_vector_size();

	// If there was a parse error, return one row with the error
	if (bind->stmt_count == 0) {
		if (init->current_idx == 0 && bind->error) {
			idx_data[0] = 0;
			duckdb_vector_assign_string_element(type_vec, 0, "INVALID");
			duckdb_vector_assign_string_element(error_vec, 0, bind->error);
			param_data[0] = 0;
			count = 1;
			init->current_idx = 1;
		}
		duckdb_data_chunk_set_size(output, count);
		return;
	}

	// Return statement info
	while (init->current_idx < bind->stmt_count && count < max_count) {
		idx_t i = init->current_idx;

		idx_data[count] = (int64_t)i;

		// Prepare statement to get type and param count
		duckdb_prepared_statement prepared = NULL;
		if (duckdb_prepare_extracted_statement(bind->conn, bind->stmts, i, &prepared) == DuckDBSuccess) {
			duckdb_statement_type type = duckdb_prepared_statement_type(prepared);
			duckdb_vector_assign_string_element(type_vec, count, StatementTypeToString(type));
			SetRowInvalid(error_vec, count);
			param_data[count] = (int64_t)duckdb_nparams(prepared);
			duckdb_destroy_prepare(&prepared);
		} else {
			const char *err = prepared ? duckdb_prepare_error(prepared) : "Unknown error";
			duckdb_vector_assign_string_element(type_vec, count, "INVALID");
			duckdb_vector_assign_string_element(error_vec, count, err ? err : "Prepare failed");
			param_data[count] = 0;
			if (prepared) {
				duckdb_destroy_prepare(&prepared);
			}
		}

		count++;
		init->current_idx++;
	}

	duckdb_data_chunk_set_size(output, count);
}

// ============================================================================
// parse_parameters(query, stmt_index) table function
// Returns one row per parameter: index, name, type
// ============================================================================

typedef struct {
	char *query;
	idx_t stmt_index;
	duckdb_database db;
	duckdb_connection conn;
	duckdb_extracted_statements stmts;
	duckdb_prepared_statement prepared;
	idx_t param_count;
	const char *error;
} ParseParamsBindData;

typedef struct {
	idx_t current_idx;
} ParseParamsInitData;

static void ParseParamsBindDataDestroy(void *data) {
	ParseParamsBindData *bind = (ParseParamsBindData *)data;
	if (bind) {
		if (bind->prepared) {
			duckdb_destroy_prepare(&bind->prepared);
		}
		if (bind->stmts) {
			duckdb_destroy_extracted(&bind->stmts);
		}
		if (bind->conn) {
			duckdb_disconnect(&bind->conn);
		}
		if (bind->db) {
			duckdb_close(&bind->db);
		}
		if (bind->query) {
			free(bind->query);
		}
		free(bind);
	}
}

static void ParseParamsBind(duckdb_bind_info info) {
	ParseParamsBindData *bind = (ParseParamsBindData *)malloc(sizeof(ParseParamsBindData));
	memset(bind, 0, sizeof(ParseParamsBindData));

	// Get parameters
	duckdb_value query_val = duckdb_bind_get_parameter(info, 0);
	duckdb_value idx_val = duckdb_bind_get_parameter(info, 1);
	bind->query = duckdb_get_varchar(query_val);
	bind->stmt_index = (idx_t)duckdb_get_int64(idx_val);
	duckdb_destroy_value(&query_val);
	duckdb_destroy_value(&idx_val);

	// Create in-memory database and parse
	if (duckdb_open(NULL, &bind->db) == DuckDBSuccess && duckdb_connect(bind->db, &bind->conn) == DuckDBSuccess) {
		idx_t count = duckdb_extract_statements(bind->conn, bind->query, &bind->stmts);
		if (count > 0 && bind->stmt_index < count) {
			if (duckdb_prepare_extracted_statement(bind->conn, bind->stmts, bind->stmt_index, &bind->prepared) == DuckDBSuccess) {
				bind->param_count = duckdb_nparams(bind->prepared);
			} else {
				bind->error = bind->prepared ? duckdb_prepare_error(bind->prepared) : "Prepare failed";
			}
		} else if (bind->stmts) {
			bind->error = duckdb_extract_statements_error(bind->stmts);
		}
	}

	// Add result columns
	duckdb_logical_type bigint_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);

	duckdb_bind_add_result_column(info, "param_index", bigint_type);
	duckdb_bind_add_result_column(info, "param_name", varchar_type);
	duckdb_bind_add_result_column(info, "param_type", varchar_type);

	duckdb_destroy_logical_type(&bigint_type);
	duckdb_destroy_logical_type(&varchar_type);

	duckdb_bind_set_bind_data(info, bind, ParseParamsBindDataDestroy);
}

static void ParseParamsInit(duckdb_init_info info) {
	ParseParamsInitData *init = (ParseParamsInitData *)malloc(sizeof(ParseParamsInitData));
	init->current_idx = 0;
	duckdb_init_set_init_data(info, init, ParseStatementsInitDataDestroy);
}

static void ParseParamsFunc(duckdb_function_info info, duckdb_data_chunk output) {
	ParseParamsBindData *bind = (ParseParamsBindData *)duckdb_function_get_bind_data(info);
	ParseParamsInitData *init = (ParseParamsInitData *)duckdb_function_get_init_data(info);

	duckdb_vector idx_vec = duckdb_data_chunk_get_vector(output, 0);
	duckdb_vector name_vec = duckdb_data_chunk_get_vector(output, 1);
	duckdb_vector type_vec = duckdb_data_chunk_get_vector(output, 2);

	int64_t *idx_data = (int64_t *)duckdb_vector_get_data(idx_vec);

	idx_t count = 0;
	idx_t max_count = duckdb_vector_size();

	if (!bind->prepared) {
		duckdb_data_chunk_set_size(output, 0);
		return;
	}

	while (init->current_idx < bind->param_count && count < max_count) {
		idx_t i = init->current_idx;
		idx_t param_idx = i + 1; // 1-based

		idx_data[count] = (int64_t)i;

		const char *name = duckdb_parameter_name(bind->prepared, param_idx);
		if (name) {
			duckdb_vector_assign_string_element(name_vec, count, name);
		} else {
			char buf[32];
			snprintf(buf, sizeof(buf), "$%lu", (unsigned long)param_idx);
			duckdb_vector_assign_string_element(name_vec, count, buf);
		}

		duckdb_type type = duckdb_param_type(bind->prepared, param_idx);
		duckdb_vector_assign_string_element(type_vec, count, TypeToString(type));

		count++;
		init->current_idx++;
	}

	duckdb_data_chunk_set_size(output, count);
}

// ============================================================================
// parse_columns(query, stmt_index) table function
// Returns result columns for SELECT-like statements
// ============================================================================

typedef struct {
	char *query;
	idx_t stmt_index;
	duckdb_database db;
	duckdb_connection conn;
	duckdb_extracted_statements stmts;
	duckdb_prepared_statement prepared;
	duckdb_result result;
	idx_t col_count;
	bool has_result;
	const char *error;
} ParseColumnsBindData;

typedef struct {
	idx_t current_idx;
} ParseColumnsInitData;

static void ParseColumnsBindDataDestroy(void *data) {
	ParseColumnsBindData *bind = (ParseColumnsBindData *)data;
	if (bind) {
		if (bind->has_result) {
			duckdb_destroy_result(&bind->result);
		}
		if (bind->prepared) {
			duckdb_destroy_prepare(&bind->prepared);
		}
		if (bind->stmts) {
			duckdb_destroy_extracted(&bind->stmts);
		}
		if (bind->conn) {
			duckdb_disconnect(&bind->conn);
		}
		if (bind->db) {
			duckdb_close(&bind->db);
		}
		if (bind->query) {
			free(bind->query);
		}
		free(bind);
	}
}

static void ParseColumnsBind(duckdb_bind_info info) {
	ParseColumnsBindData *bind = (ParseColumnsBindData *)malloc(sizeof(ParseColumnsBindData));
	memset(bind, 0, sizeof(ParseColumnsBindData));

	// Get parameters
	duckdb_value query_val = duckdb_bind_get_parameter(info, 0);
	duckdb_value idx_val = duckdb_bind_get_parameter(info, 1);
	bind->query = duckdb_get_varchar(query_val);
	bind->stmt_index = (idx_t)duckdb_get_int64(idx_val);
	duckdb_destroy_value(&query_val);
	duckdb_destroy_value(&idx_val);

	// Create in-memory database, parse, and execute to get result schema
	if (duckdb_open(NULL, &bind->db) == DuckDBSuccess && duckdb_connect(bind->db, &bind->conn) == DuckDBSuccess) {
		idx_t count = duckdb_extract_statements(bind->conn, bind->query, &bind->stmts);
		if (count > 0 && bind->stmt_index < count) {
			if (duckdb_prepare_extracted_statement(bind->conn, bind->stmts, bind->stmt_index, &bind->prepared) == DuckDBSuccess) {
				if (duckdb_execute_prepared(bind->prepared, &bind->result) == DuckDBSuccess) {
					bind->col_count = duckdb_column_count(&bind->result);
					bind->has_result = true;
				}
			}
		}
	}

	// Add result columns
	duckdb_logical_type bigint_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);

	duckdb_bind_add_result_column(info, "col_index", bigint_type);
	duckdb_bind_add_result_column(info, "col_name", varchar_type);
	duckdb_bind_add_result_column(info, "col_type", varchar_type);

	duckdb_destroy_logical_type(&bigint_type);
	duckdb_destroy_logical_type(&varchar_type);

	duckdb_bind_set_bind_data(info, bind, ParseColumnsBindDataDestroy);
}

static void ParseColumnsInit(duckdb_init_info info) {
	ParseColumnsInitData *init = (ParseColumnsInitData *)malloc(sizeof(ParseColumnsInitData));
	init->current_idx = 0;
	duckdb_init_set_init_data(info, init, ParseStatementsInitDataDestroy);
}

static void ParseColumnsFunc(duckdb_function_info info, duckdb_data_chunk output) {
	ParseColumnsBindData *bind = (ParseColumnsBindData *)duckdb_function_get_bind_data(info);
	ParseColumnsInitData *init = (ParseColumnsInitData *)duckdb_function_get_init_data(info);

	duckdb_vector idx_vec = duckdb_data_chunk_get_vector(output, 0);
	duckdb_vector name_vec = duckdb_data_chunk_get_vector(output, 1);
	duckdb_vector type_vec = duckdb_data_chunk_get_vector(output, 2);

	int64_t *idx_data = (int64_t *)duckdb_vector_get_data(idx_vec);

	idx_t count = 0;
	idx_t max_count = duckdb_vector_size();

	if (!bind->has_result) {
		duckdb_data_chunk_set_size(output, 0);
		return;
	}

	while (init->current_idx < bind->col_count && count < max_count) {
		idx_t i = init->current_idx;

		idx_data[count] = (int64_t)i;

		const char *name = duckdb_column_name(&bind->result, i);
		duckdb_vector_assign_string_element(name_vec, count, name ? name : "");

		duckdb_type type = duckdb_column_type(&bind->result, i);
		duckdb_vector_assign_string_element(type_vec, count, TypeToString(type));

		count++;
		init->current_idx++;
	}

	duckdb_data_chunk_set_size(output, count);
}

// ============================================================================
// parse_type_info(query, stmt_index, col_index) table function
// Returns detailed type info including nested types for structs/lists/maps
// ============================================================================

typedef struct {
	char *query;
	idx_t stmt_index;
	idx_t col_index;
	duckdb_database db;
	duckdb_connection conn;
	duckdb_extracted_statements stmts;
	duckdb_prepared_statement prepared;
	duckdb_result result;
	duckdb_logical_type col_type;
	bool has_type;
} ParseTypeBindData;

// Recursively serialize a logical type to a string
static void SerializeLogicalType(duckdb_logical_type type, char *buf, size_t buf_size) {
	duckdb_type base_type = duckdb_get_type_id(type);
	const char *type_name = TypeToString(base_type);

	switch (base_type) {
	case DUCKDB_TYPE_DECIMAL: {
		uint8_t width = duckdb_decimal_width(type);
		uint8_t scale = duckdb_decimal_scale(type);
		snprintf(buf, buf_size, "DECIMAL(%d,%d)", width, scale);
		break;
	}
	case DUCKDB_TYPE_LIST: {
		duckdb_logical_type child = duckdb_list_type_child_type(type);
		char child_buf[512];
		SerializeLogicalType(child, child_buf, sizeof(child_buf));
		snprintf(buf, buf_size, "%s[]", child_buf);
		duckdb_destroy_logical_type(&child);
		break;
	}
	case DUCKDB_TYPE_ARRAY: {
		duckdb_logical_type child = duckdb_array_type_child_type(type);
		idx_t size = duckdb_array_type_array_size(type);
		char child_buf[512];
		SerializeLogicalType(child, child_buf, sizeof(child_buf));
		snprintf(buf, buf_size, "%s[%lu]", child_buf, (unsigned long)size);
		duckdb_destroy_logical_type(&child);
		break;
	}
	case DUCKDB_TYPE_MAP: {
		duckdb_logical_type key = duckdb_map_type_key_type(type);
		duckdb_logical_type val = duckdb_map_type_value_type(type);
		char key_buf[256], val_buf[256];
		SerializeLogicalType(key, key_buf, sizeof(key_buf));
		SerializeLogicalType(val, val_buf, sizeof(val_buf));
		snprintf(buf, buf_size, "MAP(%s, %s)", key_buf, val_buf);
		duckdb_destroy_logical_type(&key);
		duckdb_destroy_logical_type(&val);
		break;
	}
	case DUCKDB_TYPE_STRUCT: {
		idx_t child_count = duckdb_struct_type_child_count(type);
		size_t offset = 0;
		offset += snprintf(buf + offset, buf_size - offset, "STRUCT(");
		for (idx_t i = 0; i < child_count && offset < buf_size - 10; i++) {
			if (i > 0) offset += snprintf(buf + offset, buf_size - offset, ", ");
			char *name = duckdb_struct_type_child_name(type, i);
			duckdb_logical_type child = duckdb_struct_type_child_type(type, i);
			char child_buf[256];
			SerializeLogicalType(child, child_buf, sizeof(child_buf));
			offset += snprintf(buf + offset, buf_size - offset, "%s %s", name, child_buf);
			duckdb_free(name);
			duckdb_destroy_logical_type(&child);
		}
		snprintf(buf + offset, buf_size - offset, ")");
		break;
	}
	case DUCKDB_TYPE_UNION: {
		idx_t member_count = duckdb_union_type_member_count(type);
		size_t offset = 0;
		offset += snprintf(buf + offset, buf_size - offset, "UNION(");
		for (idx_t i = 0; i < member_count && offset < buf_size - 10; i++) {
			if (i > 0) offset += snprintf(buf + offset, buf_size - offset, ", ");
			char *name = duckdb_union_type_member_name(type, i);
			duckdb_logical_type member = duckdb_union_type_member_type(type, i);
			char member_buf[256];
			SerializeLogicalType(member, member_buf, sizeof(member_buf));
			offset += snprintf(buf + offset, buf_size - offset, "%s %s", name, member_buf);
			duckdb_free(name);
			duckdb_destroy_logical_type(&member);
		}
		snprintf(buf + offset, buf_size - offset, ")");
		break;
	}
	case DUCKDB_TYPE_ENUM: {
		uint32_t dict_size = duckdb_enum_dictionary_size(type);
		size_t offset = 0;
		offset += snprintf(buf + offset, buf_size - offset, "ENUM(");
		for (uint32_t i = 0; i < dict_size && i < 10 && offset < buf_size - 20; i++) {
			if (i > 0) offset += snprintf(buf + offset, buf_size - offset, ", ");
			char *val = duckdb_enum_dictionary_value(type, i);
			offset += snprintf(buf + offset, buf_size - offset, "'%s'", val);
			duckdb_free(val);
		}
		if (dict_size > 10) {
			offset += snprintf(buf + offset, buf_size - offset, ", ... +%u more", dict_size - 10);
		}
		snprintf(buf + offset, buf_size - offset, ")");
		break;
	}
	default:
		snprintf(buf, buf_size, "%s", type_name);
		break;
	}
}

static void ParseTypeBindDataDestroy(void *data) {
	ParseTypeBindData *bind = (ParseTypeBindData *)data;
	if (bind) {
		if (bind->col_type) {
			duckdb_destroy_logical_type(&bind->col_type);
		}
		if (bind->result.internal_data) {
			duckdb_destroy_result(&bind->result);
		}
		if (bind->prepared) {
			duckdb_destroy_prepare(&bind->prepared);
		}
		if (bind->stmts) {
			duckdb_destroy_extracted(&bind->stmts);
		}
		if (bind->conn) {
			duckdb_disconnect(&bind->conn);
		}
		if (bind->db) {
			duckdb_close(&bind->db);
		}
		if (bind->query) {
			free(bind->query);
		}
		free(bind);
	}
}

static void ParseTypeBind(duckdb_bind_info info) {
	ParseTypeBindData *bind = (ParseTypeBindData *)malloc(sizeof(ParseTypeBindData));
	memset(bind, 0, sizeof(ParseTypeBindData));

	duckdb_value query_val = duckdb_bind_get_parameter(info, 0);
	duckdb_value stmt_val = duckdb_bind_get_parameter(info, 1);
	duckdb_value col_val = duckdb_bind_get_parameter(info, 2);
	bind->query = duckdb_get_varchar(query_val);
	bind->stmt_index = (idx_t)duckdb_get_int64(stmt_val);
	bind->col_index = (idx_t)duckdb_get_int64(col_val);
	duckdb_destroy_value(&query_val);
	duckdb_destroy_value(&stmt_val);
	duckdb_destroy_value(&col_val);

	if (duckdb_open(NULL, &bind->db) == DuckDBSuccess && duckdb_connect(bind->db, &bind->conn) == DuckDBSuccess) {
		idx_t count = duckdb_extract_statements(bind->conn, bind->query, &bind->stmts);
		if (count > 0 && bind->stmt_index < count) {
			if (duckdb_prepare_extracted_statement(bind->conn, bind->stmts, bind->stmt_index, &bind->prepared) == DuckDBSuccess) {
				if (duckdb_execute_prepared(bind->prepared, &bind->result) == DuckDBSuccess) {
					idx_t col_count = duckdb_column_count(&bind->result);
					if (bind->col_index < col_count) {
						bind->col_type = duckdb_column_logical_type(&bind->result, bind->col_index);
						bind->has_type = true;
					}
				}
			}
		}
	}

	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_logical_type bigint_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);

	duckdb_bind_add_result_column(info, "base_type", varchar_type);
	duckdb_bind_add_result_column(info, "full_type", varchar_type);
	duckdb_bind_add_result_column(info, "nullable", varchar_type);
	duckdb_bind_add_result_column(info, "precision", bigint_type);
	duckdb_bind_add_result_column(info, "scale", bigint_type);
	duckdb_bind_add_result_column(info, "child_count", bigint_type);

	duckdb_destroy_logical_type(&varchar_type);
	duckdb_destroy_logical_type(&bigint_type);

	duckdb_bind_set_bind_data(info, bind, ParseTypeBindDataDestroy);
}

typedef struct {
	bool done;
} ParseTypeInitData;

static void ParseTypeInit(duckdb_init_info info) {
	ParseTypeInitData *init = (ParseTypeInitData *)malloc(sizeof(ParseTypeInitData));
	init->done = false;
	duckdb_init_set_init_data(info, init, ParseStatementsInitDataDestroy);
}

static void ParseTypeFunc(duckdb_function_info info, duckdb_data_chunk output) {
	ParseTypeBindData *bind = (ParseTypeBindData *)duckdb_function_get_bind_data(info);
	ParseTypeInitData *init = (ParseTypeInitData *)duckdb_function_get_init_data(info);

	if (init->done || !bind->has_type) {
		duckdb_data_chunk_set_size(output, 0);
		return;
	}

	duckdb_vector base_vec = duckdb_data_chunk_get_vector(output, 0);
	duckdb_vector full_vec = duckdb_data_chunk_get_vector(output, 1);
	duckdb_vector null_vec = duckdb_data_chunk_get_vector(output, 2);
	duckdb_vector prec_vec = duckdb_data_chunk_get_vector(output, 3);
	duckdb_vector scale_vec = duckdb_data_chunk_get_vector(output, 4);
	duckdb_vector child_vec = duckdb_data_chunk_get_vector(output, 5);

	int64_t *prec_data = (int64_t *)duckdb_vector_get_data(prec_vec);
	int64_t *scale_data = (int64_t *)duckdb_vector_get_data(scale_vec);
	int64_t *child_data = (int64_t *)duckdb_vector_get_data(child_vec);

	duckdb_type base_type = duckdb_get_type_id(bind->col_type);
	duckdb_vector_assign_string_element(base_vec, 0, TypeToString(base_type));

	char full_type_buf[1024];
	SerializeLogicalType(bind->col_type, full_type_buf, sizeof(full_type_buf));
	duckdb_vector_assign_string_element(full_vec, 0, full_type_buf);

	duckdb_vector_assign_string_element(null_vec, 0, "YES");

	if (base_type == DUCKDB_TYPE_DECIMAL) {
		prec_data[0] = duckdb_decimal_width(bind->col_type);
		scale_data[0] = duckdb_decimal_scale(bind->col_type);
	} else {
		SetRowInvalid(prec_vec, 0);
		SetRowInvalid(scale_vec, 0);
	}

	if (base_type == DUCKDB_TYPE_STRUCT) {
		child_data[0] = (int64_t)duckdb_struct_type_child_count(bind->col_type);
	} else if (base_type == DUCKDB_TYPE_UNION) {
		child_data[0] = (int64_t)duckdb_union_type_member_count(bind->col_type);
	} else {
		SetRowInvalid(child_vec, 0);
	}

	init->done = true;
	duckdb_data_chunk_set_size(output, 1);
}

// ============================================================================
// parse_column_types(query, stmt_index) table function
// Returns full type info for all columns at once
// ============================================================================

typedef struct {
	char *query;
	idx_t stmt_index;
	duckdb_database db;
	duckdb_connection conn;
	duckdb_extracted_statements stmts;
	duckdb_prepared_statement prepared;
	duckdb_result result;
	idx_t col_count;
	bool has_result;
} ParseColumnTypesBindData;

typedef struct {
	idx_t current_idx;
} ParseColumnTypesInitData;

static void ParseColumnTypesBindDataDestroy(void *data) {
	ParseColumnTypesBindData *bind = (ParseColumnTypesBindData *)data;
	if (bind) {
		if (bind->has_result) {
			duckdb_destroy_result(&bind->result);
		}
		if (bind->prepared) {
			duckdb_destroy_prepare(&bind->prepared);
		}
		if (bind->stmts) {
			duckdb_destroy_extracted(&bind->stmts);
		}
		if (bind->conn) {
			duckdb_disconnect(&bind->conn);
		}
		if (bind->db) {
			duckdb_close(&bind->db);
		}
		if (bind->query) {
			free(bind->query);
		}
		free(bind);
	}
}

static void ParseColumnTypesBind(duckdb_bind_info info) {
	ParseColumnTypesBindData *bind = (ParseColumnTypesBindData *)malloc(sizeof(ParseColumnTypesBindData));
	memset(bind, 0, sizeof(ParseColumnTypesBindData));

	duckdb_value query_val = duckdb_bind_get_parameter(info, 0);
	duckdb_value idx_val = duckdb_bind_get_parameter(info, 1);
	bind->query = duckdb_get_varchar(query_val);
	bind->stmt_index = (idx_t)duckdb_get_int64(idx_val);
	duckdb_destroy_value(&query_val);
	duckdb_destroy_value(&idx_val);

	if (duckdb_open(NULL, &bind->db) == DuckDBSuccess && duckdb_connect(bind->db, &bind->conn) == DuckDBSuccess) {
		idx_t count = duckdb_extract_statements(bind->conn, bind->query, &bind->stmts);
		if (count > 0 && bind->stmt_index < count) {
			if (duckdb_prepare_extracted_statement(bind->conn, bind->stmts, bind->stmt_index, &bind->prepared) == DuckDBSuccess) {
				if (duckdb_execute_prepared(bind->prepared, &bind->result) == DuckDBSuccess) {
					bind->col_count = duckdb_column_count(&bind->result);
					bind->has_result = true;
				}
			}
		}
	}

	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_logical_type bigint_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);

	duckdb_bind_add_result_column(info, "col_index", bigint_type);
	duckdb_bind_add_result_column(info, "col_name", varchar_type);
	duckdb_bind_add_result_column(info, "base_type", varchar_type);
	duckdb_bind_add_result_column(info, "full_type", varchar_type);

	duckdb_destroy_logical_type(&varchar_type);
	duckdb_destroy_logical_type(&bigint_type);

	duckdb_bind_set_bind_data(info, bind, ParseColumnTypesBindDataDestroy);
}

static void ParseColumnTypesInit(duckdb_init_info info) {
	ParseColumnTypesInitData *init = (ParseColumnTypesInitData *)malloc(sizeof(ParseColumnTypesInitData));
	init->current_idx = 0;
	duckdb_init_set_init_data(info, init, ParseStatementsInitDataDestroy);
}

static void ParseColumnTypesFunc(duckdb_function_info info, duckdb_data_chunk output) {
	ParseColumnTypesBindData *bind = (ParseColumnTypesBindData *)duckdb_function_get_bind_data(info);
	ParseColumnTypesInitData *init = (ParseColumnTypesInitData *)duckdb_function_get_init_data(info);

	duckdb_vector idx_vec = duckdb_data_chunk_get_vector(output, 0);
	duckdb_vector name_vec = duckdb_data_chunk_get_vector(output, 1);
	duckdb_vector base_vec = duckdb_data_chunk_get_vector(output, 2);
	duckdb_vector full_vec = duckdb_data_chunk_get_vector(output, 3);

	int64_t *idx_data = (int64_t *)duckdb_vector_get_data(idx_vec);

	idx_t count = 0;
	idx_t max_count = duckdb_vector_size();

	if (!bind->has_result) {
		duckdb_data_chunk_set_size(output, 0);
		return;
	}

	while (init->current_idx < bind->col_count && count < max_count) {
		idx_t i = init->current_idx;

		idx_data[count] = (int64_t)i;

		const char *name = duckdb_column_name(&bind->result, i);
		duckdb_vector_assign_string_element(name_vec, count, name ? name : "");

		duckdb_logical_type col_type = duckdb_column_logical_type(&bind->result, i);
		duckdb_type base_type = duckdb_get_type_id(col_type);
		duckdb_vector_assign_string_element(base_vec, count, TypeToString(base_type));

		char full_type_buf[1024];
		SerializeLogicalType(col_type, full_type_buf, sizeof(full_type_buf));
		duckdb_vector_assign_string_element(full_vec, count, full_type_buf);

		duckdb_destroy_logical_type(&col_type);

		count++;
		init->current_idx++;
	}

	duckdb_data_chunk_set_size(output, count);
}

// ============================================================================
// sql_keywords() table function
// Returns all DuckDB keywords
// ============================================================================

static const char *SQL_KEYWORDS[] = {
	"ALL", "ALTER", "AND", "ANY", "AS", "ASC", "BETWEEN", "BY", "CASE", "CAST",
	"CHECK", "COLUMN", "CONSTRAINT", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME",
	"CURRENT_TIMESTAMP", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE",
	"END", "EXCEPT", "EXISTS", "FALSE", "FILTER", "FOLLOWING", "FOR", "FOREIGN",
	"FROM", "FULL", "GROUP", "HAVING", "IF", "IN", "INDEX", "INNER", "INSERT",
	"INTERSECT", "INTO", "IS", "JOIN", "KEY", "LEFT", "LIKE", "LIMIT", "NATURAL",
	"NOT", "NULL", "OFFSET", "ON", "OR", "ORDER", "OUTER", "OVER", "PARTITION",
	"PRECEDING", "PRIMARY", "QUALIFY", "RANGE", "RECURSIVE", "REFERENCES", "RETURNING",
	"RIGHT", "ROWS", "SELECT", "SET", "TABLE", "THEN", "TRUE", "UNBOUNDED",
	"UNION", "UNIQUE", "UPDATE", "USING", "VALUES", "WHEN", "WHERE", "WINDOW", "WITH"
};

#define NUM_SQL_KEYWORDS (sizeof(SQL_KEYWORDS) / sizeof(SQL_KEYWORDS[0]))

typedef struct {
	idx_t current_idx;
} SqlKeywordsInitData;

static void SqlKeywordsBind(duckdb_bind_info info) {
	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_bind_add_result_column(info, "keyword", varchar_type);
	duckdb_destroy_logical_type(&varchar_type);
}

static void SqlKeywordsInit(duckdb_init_info info) {
	SqlKeywordsInitData *init = (SqlKeywordsInitData *)malloc(sizeof(SqlKeywordsInitData));
	init->current_idx = 0;
	duckdb_init_set_init_data(info, init, ParseStatementsInitDataDestroy);
}

static void SqlKeywordsFunc(duckdb_function_info info, duckdb_data_chunk output) {
	SqlKeywordsInitData *init = (SqlKeywordsInitData *)duckdb_function_get_init_data(info);

	duckdb_vector kw_vec = duckdb_data_chunk_get_vector(output, 0);

	idx_t count = 0;
	idx_t max_count = duckdb_vector_size();

	while (init->current_idx < NUM_SQL_KEYWORDS && count < max_count) {
		duckdb_vector_assign_string_element(kw_vec, count, SQL_KEYWORDS[init->current_idx]);
		count++;
		init->current_idx++;
	}

	duckdb_data_chunk_set_size(output, count);
}

// ============================================================================
// is_keyword(identifier) scalar function
// Returns true if the identifier is a SQL keyword
// ============================================================================

static void IsKeywordFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	duckdb_vector id_vec = duckdb_data_chunk_get_vector(input, 0);
	bool *result_data = (bool *)duckdb_vector_get_data(output);

	for (idx_t row = 0; row < input_size; row++) {
		uint64_t *validity = duckdb_vector_get_validity(id_vec);
		if (validity && !duckdb_validity_row_is_valid(validity, row)) {
			SetRowInvalid(output, row);
			continue;
		}

		duckdb_string_t *str_data = (duckdb_string_t *)duckdb_vector_get_data(id_vec);
		char *identifier = GetString(str_data, row);

		// Convert to uppercase for comparison
		for (char *p = identifier; *p; p++) {
			if (*p >= 'a' && *p <= 'z') *p -= 32;
		}

		bool is_kw = false;
		for (size_t i = 0; i < NUM_SQL_KEYWORDS; i++) {
			if (strcmp(identifier, SQL_KEYWORDS[i]) == 0) {
				is_kw = true;
				break;
			}
		}

		result_data[row] = is_kw;
		free(identifier);
	}
}

// ============================================================================
// sql_strip_comments(query) scalar function
// Returns query with comments removed
// ============================================================================

static char *StripComments(const char *sql) {
	size_t len = strlen(sql);
	char *result = (char *)malloc(len + 1);
	size_t j = 0;
	bool in_string = false;
	char string_char = 0;
	bool in_line_comment = false;
	bool in_block_comment = false;

	for (size_t i = 0; i < len; i++) {
		if (in_line_comment) {
			if (sql[i] == '\n') {
				in_line_comment = false;
				result[j++] = sql[i];
			}
		} else if (in_block_comment) {
			if (sql[i] == '*' && i + 1 < len && sql[i + 1] == '/') {
				in_block_comment = false;
				i++; // skip /
			}
		} else if (in_string) {
			result[j++] = sql[i];
			if (sql[i] == string_char) {
				// Check for escaped quote
				if (i + 1 < len && sql[i + 1] == string_char) {
					result[j++] = sql[++i];
				} else {
					in_string = false;
				}
			}
		} else {
			if (sql[i] == '\'' || sql[i] == '"') {
				in_string = true;
				string_char = sql[i];
				result[j++] = sql[i];
			} else if (sql[i] == '-' && i + 1 < len && sql[i + 1] == '-') {
				in_line_comment = true;
				i++; // skip second -
			} else if (sql[i] == '/' && i + 1 < len && sql[i + 1] == '*') {
				in_block_comment = true;
				i++; // skip *
			} else {
				result[j++] = sql[i];
			}
		}
	}
	result[j] = '\0';
	return result;
}

static void SqlStripCommentsFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	duckdb_vector sql_vec = duckdb_data_chunk_get_vector(input, 0);

	for (idx_t row = 0; row < input_size; row++) {
		uint64_t *validity = duckdb_vector_get_validity(sql_vec);
		if (validity && !duckdb_validity_row_is_valid(validity, row)) {
			SetRowInvalid(output, row);
			continue;
		}

		duckdb_string_t *str_data = (duckdb_string_t *)duckdb_vector_get_data(sql_vec);
		char *sql = GetString(str_data, row);
		char *stripped = StripComments(sql);

		duckdb_vector_assign_string_element(output, row, stripped);

		free(stripped);
		free(sql);
	}
}

// ============================================================================
// Helper: Extract string from first row/column of a result
// ============================================================================

static char *GetResultString(duckdb_result *result) {
	// Use duckdb_fetch_chunk to get the first chunk
	duckdb_data_chunk chunk = duckdb_fetch_chunk(*result);
	if (!chunk) return NULL;

	idx_t row_count = duckdb_data_chunk_get_size(chunk);
	if (row_count == 0) {
		duckdb_destroy_data_chunk(&chunk);
		return NULL;
	}

	duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, 0);
	duckdb_string_t *str_data = (duckdb_string_t *)duckdb_vector_get_data(vec);

	char *result_str = GetString(str_data, 0);

	duckdb_destroy_data_chunk(&chunk);
	return result_str;
}

// ============================================================================
// sql_parse_json(query) scalar function
// Returns a JSON object with parsed query info (tables, errors, etc.)
// Uses json_serialize_plan internally for rich AST access
// ============================================================================

typedef struct {
	duckdb_database db;
	duckdb_connection conn;
} SqlParseJsonData;

static void SqlParseJsonDataDestroy(void *data) {
	SqlParseJsonData *d = (SqlParseJsonData *)data;
	if (d) {
		if (d->conn) duckdb_disconnect(&d->conn);
		if (d->db) duckdb_close(&d->db);
		free(d);
	}
}

static void SqlParseJsonFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	duckdb_vector sql_vec = duckdb_data_chunk_get_vector(input, 0);

	// Create a connection for json_serialize_plan
	duckdb_database db = NULL;
	duckdb_connection conn = NULL;

	if (duckdb_open(NULL, &db) != DuckDBSuccess) {
		for (idx_t row = 0; row < input_size; row++) {
			duckdb_vector_assign_string_element(output, row, "{\"error\":\"Failed to open database\"}");
		}
		return;
	}
	if (duckdb_connect(db, &conn) != DuckDBSuccess) {
		duckdb_close(&db);
		for (idx_t row = 0; row < input_size; row++) {
			duckdb_vector_assign_string_element(output, row, "{\"error\":\"Failed to connect\"}");
		}
		return;
	}

	for (idx_t row = 0; row < input_size; row++) {
		uint64_t *validity = duckdb_vector_get_validity(sql_vec);
		if (validity && !duckdb_validity_row_is_valid(validity, row)) {
			SetRowInvalid(output, row);
			continue;
		}

		duckdb_string_t *str_data = (duckdb_string_t *)duckdb_vector_get_data(sql_vec);
		char *sql = GetString(str_data, row);

		// Build query to call json_serialize_plan
		// Need to escape the SQL string for embedding
		size_t sql_len = strlen(sql);
		size_t escaped_size = sql_len * 2 + 100;
		char *escaped_query = (char *)malloc(escaped_size);
		size_t j = 0;
		j += snprintf(escaped_query + j, escaped_size - j, "SELECT json_serialize_plan('");
		for (size_t i = 0; i < sql_len && j < escaped_size - 10; i++) {
			if (sql[i] == '\'') {
				escaped_query[j++] = '\'';
				escaped_query[j++] = '\'';
			} else {
				escaped_query[j++] = sql[i];
			}
		}
		j += snprintf(escaped_query + j, escaped_size - j, "')");

		duckdb_result result;
		if (duckdb_query(conn, escaped_query, &result) == DuckDBSuccess) {
			char *plan_json = GetResultString(&result);
			if (plan_json) {
				duckdb_vector_assign_string_element(output, row, plan_json);
				free(plan_json);
			} else {
				duckdb_vector_assign_string_element(output, row, "{\"error\":\"Empty result\"}");
			}
			duckdb_destroy_result(&result);
		} else {
			// Query failed - return error info
			const char *err = duckdb_result_error(&result);
			char error_json[2048];
			snprintf(error_json, sizeof(error_json), "{\"error\":true,\"message\":\"%s\"}", err ? err : "Unknown error");
			duckdb_vector_assign_string_element(output, row, error_json);
			duckdb_destroy_result(&result);
		}

		free(escaped_query);
		free(sql);
	}

	duckdb_disconnect(&conn);
	duckdb_close(&db);
}

// ============================================================================
// parse_tables(query) table function
// Extracts table references from a query using json_serialize_plan
// ============================================================================

typedef struct {
	duckdb_database db;
	duckdb_connection conn;
	char **schemas;
	char **tables;
	char **contexts;
	idx_t table_count;
	idx_t capacity;
} ParseTablesBindData;

static void ParseTablesBindDataDestroy(void *data) {
	ParseTablesBindData *bind = (ParseTablesBindData *)data;
	if (bind) {
		for (idx_t i = 0; i < bind->table_count; i++) {
			if (bind->schemas && bind->schemas[i]) free(bind->schemas[i]);
			if (bind->tables && bind->tables[i]) free(bind->tables[i]);
			if (bind->contexts && bind->contexts[i]) free(bind->contexts[i]);
		}
		if (bind->schemas) free(bind->schemas);
		if (bind->tables) free(bind->tables);
		if (bind->contexts) free(bind->contexts);
		if (bind->conn) duckdb_disconnect(&bind->conn);
		if (bind->db) duckdb_close(&bind->db);
		free(bind);
	}
}

// Simple JSON string extraction (finds "key":"value" patterns)
static char *ExtractJsonString(const char *json, const char *key) {
	char pattern[256];
	snprintf(pattern, sizeof(pattern), "\"%s\":\"", key);
	const char *start = strstr(json, pattern);
	if (!start) {
		// Try without quotes for nested objects
		snprintf(pattern, sizeof(pattern), "\"%s\":", key);
		start = strstr(json, pattern);
		if (!start) return NULL;
		start += strlen(pattern);
		// Skip whitespace
		while (*start == ' ' || *start == '\t' || *start == '\n') start++;
		if (*start != '"') return NULL;
		start++;
	} else {
		start += strlen(pattern);
	}

	const char *end = start;
	while (*end && *end != '"') {
		if (*end == '\\' && *(end + 1)) end++; // skip escaped chars
		end++;
	}

	size_t len = end - start;
	char *result = (char *)malloc(len + 1);
	memcpy(result, start, len);
	result[len] = '\0';
	return result;
}

// Find all LOGICAL_GET nodes and extract table info
static void ExtractTablesFromJson(ParseTablesBindData *bind, const char *json, const char *context) {
	const char *pos = json;

	while ((pos = strstr(pos, "\"type\":\"LOGICAL_GET\"")) != NULL) {
		// Find the function_data for this node
		const char *fd_start = strstr(pos, "\"function_data\":");
		if (!fd_start || fd_start > pos + 2000) {
			pos++;
			continue;
		}

		// Find the table name within function_data
		const char *table_start = strstr(fd_start, "\"table\":\"");
		if (table_start && table_start < fd_start + 500) {
			table_start += 9; // skip "table":"
			const char *table_end = strchr(table_start, '"');
			if (table_end) {
				size_t name_len = table_end - table_start;
				char *table_name = (char *)malloc(name_len + 1);
				memcpy(table_name, table_start, name_len);
				table_name[name_len] = '\0';

				// Get schema
				char *schema = NULL;
				const char *schema_start = strstr(fd_start, "\"schema\":\"");
				if (schema_start && schema_start < fd_start + 500) {
					schema_start += 10;
					const char *schema_end = strchr(schema_start, '"');
					if (schema_end) {
						size_t schema_len = schema_end - schema_start;
						schema = (char *)malloc(schema_len + 1);
						memcpy(schema, schema_start, schema_len);
						schema[schema_len] = '\0';
					}
				}

				// Add to result
				if (bind->table_count >= bind->capacity) {
					bind->capacity = bind->capacity ? bind->capacity * 2 : 16;
					bind->schemas = (char **)realloc(bind->schemas, bind->capacity * sizeof(char *));
					bind->tables = (char **)realloc(bind->tables, bind->capacity * sizeof(char *));
					bind->contexts = (char **)realloc(bind->contexts, bind->capacity * sizeof(char *));
				}

				bind->schemas[bind->table_count] = schema ? schema : strdup("main");
				bind->tables[bind->table_count] = table_name;
				bind->contexts[bind->table_count] = strdup(context);
				bind->table_count++;
			}
		}
		pos++;
	}
}

static void ParseTablesBind(duckdb_bind_info info) {
	ParseTablesBindData *bind = (ParseTablesBindData *)malloc(sizeof(ParseTablesBindData));
	memset(bind, 0, sizeof(ParseTablesBindData));

	duckdb_value query_val = duckdb_bind_get_parameter(info, 0);
	char *sql = duckdb_get_varchar(query_val);
	duckdb_destroy_value(&query_val);

	if (duckdb_open(NULL, &bind->db) == DuckDBSuccess &&
	    duckdb_connect(bind->db, &bind->conn) == DuckDBSuccess) {

		// Build escaped query
		size_t sql_len = strlen(sql);
		size_t escaped_size = sql_len * 2 + 100;
		char *escaped_query = (char *)malloc(escaped_size);
		size_t j = 0;
		j += snprintf(escaped_query + j, escaped_size - j, "SELECT json_serialize_plan('");
		for (size_t i = 0; i < sql_len && j < escaped_size - 10; i++) {
			if (sql[i] == '\'') {
				escaped_query[j++] = '\'';
				escaped_query[j++] = '\'';
			} else {
				escaped_query[j++] = sql[i];
			}
		}
		j += snprintf(escaped_query + j, escaped_size - j, "')");

		duckdb_result result;
		if (duckdb_query(bind->conn, escaped_query, &result) == DuckDBSuccess) {
			char *plan_json = GetResultString(&result);
			if (plan_json) {
				ExtractTablesFromJson(bind, plan_json, "from");
				free(plan_json);
			}
			duckdb_destroy_result(&result);
		}
		free(escaped_query);
	}

	free(sql);

	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_bind_add_result_column(info, "schema", varchar_type);
	duckdb_bind_add_result_column(info, "table", varchar_type);
	duckdb_bind_add_result_column(info, "context", varchar_type);
	duckdb_destroy_logical_type(&varchar_type);

	duckdb_bind_set_bind_data(info, bind, ParseTablesBindDataDestroy);
}

typedef struct {
	idx_t current_idx;
} ParseTablesInitData;

static void ParseTablesInit(duckdb_init_info info) {
	ParseTablesInitData *init = (ParseTablesInitData *)malloc(sizeof(ParseTablesInitData));
	init->current_idx = 0;
	duckdb_init_set_init_data(info, init, ParseStatementsInitDataDestroy);
}

static void ParseTablesFunc(duckdb_function_info info, duckdb_data_chunk output) {
	ParseTablesBindData *bind = (ParseTablesBindData *)duckdb_function_get_bind_data(info);
	ParseTablesInitData *init = (ParseTablesInitData *)duckdb_function_get_init_data(info);

	duckdb_vector schema_vec = duckdb_data_chunk_get_vector(output, 0);
	duckdb_vector table_vec = duckdb_data_chunk_get_vector(output, 1);
	duckdb_vector context_vec = duckdb_data_chunk_get_vector(output, 2);

	idx_t count = 0;
	idx_t max_count = duckdb_vector_size();

	while (init->current_idx < bind->table_count && count < max_count) {
		idx_t i = init->current_idx;

		duckdb_vector_assign_string_element(schema_vec, count, bind->schemas[i]);
		duckdb_vector_assign_string_element(table_vec, count, bind->tables[i]);
		duckdb_vector_assign_string_element(context_vec, count, bind->contexts[i]);

		count++;
		init->current_idx++;
	}

	duckdb_data_chunk_set_size(output, count);
}

// ============================================================================
// is_valid_sql(query) scalar function
// Returns true if the query parses without errors
// ============================================================================

static void IsValidSqlFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	duckdb_vector sql_vec = duckdb_data_chunk_get_vector(input, 0);
	bool *result_data = (bool *)duckdb_vector_get_data(output);

	duckdb_database db = NULL;
	duckdb_connection conn = NULL;

	bool have_conn = (duckdb_open(NULL, &db) == DuckDBSuccess &&
	                  duckdb_connect(db, &conn) == DuckDBSuccess);

	for (idx_t row = 0; row < input_size; row++) {
		uint64_t *validity = duckdb_vector_get_validity(sql_vec);
		if (validity && !duckdb_validity_row_is_valid(validity, row)) {
			SetRowInvalid(output, row);
			continue;
		}

		if (!have_conn) {
			result_data[row] = false;
			continue;
		}

		duckdb_string_t *str_data = (duckdb_string_t *)duckdb_vector_get_data(sql_vec);
		char *sql = GetString(str_data, row);

		duckdb_extracted_statements stmts = NULL;
		idx_t count = duckdb_extract_statements(conn, sql, &stmts);
		result_data[row] = (count > 0);

		if (stmts) duckdb_destroy_extracted(&stmts);
		free(sql);
	}

	if (conn) duckdb_disconnect(&conn);
	if (db) duckdb_close(&db);
}

// ============================================================================
// sql_error_message(query) scalar function
// Returns the parse error message, or NULL if valid
// ============================================================================

static void SqlErrorMessageFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	duckdb_vector sql_vec = duckdb_data_chunk_get_vector(input, 0);

	duckdb_database db = NULL;
	duckdb_connection conn = NULL;

	bool have_conn = (duckdb_open(NULL, &db) == DuckDBSuccess &&
	                  duckdb_connect(db, &conn) == DuckDBSuccess);

	for (idx_t row = 0; row < input_size; row++) {
		uint64_t *validity = duckdb_vector_get_validity(sql_vec);
		if (validity && !duckdb_validity_row_is_valid(validity, row)) {
			SetRowInvalid(output, row);
			continue;
		}

		if (!have_conn) {
			duckdb_vector_assign_string_element(output, row, "Failed to create parser connection");
			continue;
		}

		duckdb_string_t *str_data = (duckdb_string_t *)duckdb_vector_get_data(sql_vec);
		char *sql = GetString(str_data, row);

		duckdb_extracted_statements stmts = NULL;
		idx_t count = duckdb_extract_statements(conn, sql, &stmts);

		if (count == 0 && stmts) {
			const char *err = duckdb_extract_statements_error(stmts);
			if (err) {
				duckdb_vector_assign_string_element(output, row, err);
			} else {
				SetRowInvalid(output, row);
			}
		} else {
			SetRowInvalid(output, row); // NULL = no error
		}

		if (stmts) duckdb_destroy_extracted(&stmts);
		free(sql);
	}

	if (conn) duckdb_disconnect(&conn);
	if (db) duckdb_close(&db);
}

// ============================================================================
// parse_table_names(query) scalar function
// Returns list of table names from a query
// ============================================================================

static void ParseTableNamesFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	duckdb_vector sql_vec = duckdb_data_chunk_get_vector(input, 0);

	duckdb_database db = NULL;
	duckdb_connection conn = NULL;

	bool have_conn = (duckdb_open(NULL, &db) == DuckDBSuccess &&
	                  duckdb_connect(db, &conn) == DuckDBSuccess);

	duckdb_vector child = duckdb_list_vector_get_child(output);
	idx_t list_offset = 0;

	for (idx_t row = 0; row < input_size; row++) {
		uint64_t *validity = duckdb_vector_get_validity(sql_vec);
		if (validity && !duckdb_validity_row_is_valid(validity, row)) {
			SetRowInvalid(output, row);
			duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(output);
			entries[row].offset = list_offset;
			entries[row].length = 0;
			continue;
		}

		duckdb_string_t *str_data = (duckdb_string_t *)duckdb_vector_get_data(sql_vec);
		char *sql = GetString(str_data, row);

		duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(output);
		entries[row].offset = list_offset;
		entries[row].length = 0;

		if (have_conn) {
			// Build escaped query for json_serialize_plan
			size_t sql_len = strlen(sql);
			size_t escaped_size = sql_len * 2 + 100;
			char *escaped_query = (char *)malloc(escaped_size);
			size_t j = 0;
			j += snprintf(escaped_query + j, escaped_size - j, "SELECT json_serialize_plan('");
			for (size_t i = 0; i < sql_len && j < escaped_size - 10; i++) {
				if (sql[i] == '\'') {
					escaped_query[j++] = '\'';
					escaped_query[j++] = '\'';
				} else {
					escaped_query[j++] = sql[i];
				}
			}
			j += snprintf(escaped_query + j, escaped_size - j, "')");

			duckdb_result result;
			if (duckdb_query(conn, escaped_query, &result) == DuckDBSuccess) {
				char *plan_json = GetResultString(&result);
				if (plan_json) {
					// Extract table names using simple pattern matching
					const char *pos = plan_json;
					idx_t table_count = 0;

					while ((pos = strstr(pos, "\"table\":\"")) != NULL) {
						pos += 9;
						const char *end = strchr(pos, '"');
						if (end) {
							size_t name_len = end - pos;
							if (name_len > 0 && name_len < 256) {
								char *name = (char *)malloc(name_len + 1);
								memcpy(name, pos, name_len);
								name[name_len] = '\0';

								// Add to list
								duckdb_list_vector_set_size(output, list_offset + table_count + 1);
								duckdb_vector_assign_string_element(child, list_offset + table_count, name);
								table_count++;
								free(name);
							}
							pos = end;
						} else {
							break;
						}
					}

					entries[row].length = table_count;
					list_offset += table_count;
					free(plan_json);
				}
				duckdb_destroy_result(&result);
			}
			free(escaped_query);
		}

		free(sql);
	}

	if (conn) duckdb_disconnect(&conn);
	if (db) duckdb_close(&db);
}

// ============================================================================
// tokenize_sql(query) table function
// Returns tokens with their byte positions and categories
// Uses DuckDB's internal Parser::Tokenize via C++ wrapper
// ============================================================================

typedef struct {
	TokenizeResult result;
} TokenizeSqlBindData;

static void TokenizeSqlBindDataDestroy(void *data) {
	TokenizeSqlBindData *bind = (TokenizeSqlBindData *)data;
	if (bind) {
		free_tokenize_result(&bind->result);
		free(bind);
	}
}

static void TokenizeSqlBind(duckdb_bind_info info) {
	TokenizeSqlBindData *bind = (TokenizeSqlBindData *)malloc(sizeof(TokenizeSqlBindData));
	memset(bind, 0, sizeof(TokenizeSqlBindData));

	duckdb_value query_val = duckdb_bind_get_parameter(info, 0);
	char *sql = duckdb_get_varchar(query_val);
	duckdb_destroy_value(&query_val);

	if (sql) {
		bind->result = tokenize_sql_impl(sql);
		free(sql);
	}

	duckdb_logical_type int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);

	duckdb_bind_add_result_column(info, "byte_position", int_type);
	duckdb_bind_add_result_column(info, "category", varchar_type);

	duckdb_destroy_logical_type(&int_type);
	duckdb_destroy_logical_type(&varchar_type);

	duckdb_bind_set_bind_data(info, bind, TokenizeSqlBindDataDestroy);
}

typedef struct {
	idx_t current_idx;
} TokenizeSqlInitData;

static void TokenizeSqlInit(duckdb_init_info info) {
	TokenizeSqlInitData *init = (TokenizeSqlInitData *)malloc(sizeof(TokenizeSqlInitData));
	init->current_idx = 0;
	duckdb_init_set_init_data(info, init, ParseStatementsInitDataDestroy);
}

static void TokenizeSqlFunc(duckdb_function_info info, duckdb_data_chunk output) {
	TokenizeSqlBindData *bind = (TokenizeSqlBindData *)duckdb_function_get_bind_data(info);
	TokenizeSqlInitData *init = (TokenizeSqlInitData *)duckdb_function_get_init_data(info);

	duckdb_vector pos_vec = duckdb_data_chunk_get_vector(output, 0);
	duckdb_vector cat_vec = duckdb_data_chunk_get_vector(output, 1);

	int32_t *pos_data = (int32_t *)duckdb_vector_get_data(pos_vec);

	idx_t count = 0;
	idx_t max_count = duckdb_vector_size();

	while (init->current_idx < bind->result.count && count < max_count) {
		idx_t i = init->current_idx;

		pos_data[count] = (int32_t)bind->result.tokens[i].start;
		duckdb_vector_assign_string_element(cat_vec, count, token_type_name(bind->result.tokens[i].type));

		count++;
		init->current_idx++;
	}

	duckdb_data_chunk_set_size(output, count);
}

// ============================================================================
// num_statements(query) scalar function
// Returns the count of statements in a query
// ============================================================================

static void NumStatementsFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	duckdb_vector sql_vec = duckdb_data_chunk_get_vector(input, 0);
	int64_t *result_data = (int64_t *)duckdb_vector_get_data(output);

	duckdb_database db = NULL;
	duckdb_connection conn = NULL;
	bool have_conn = (duckdb_open(NULL, &db) == DuckDBSuccess &&
	                  duckdb_connect(db, &conn) == DuckDBSuccess);

	for (idx_t row = 0; row < input_size; row++) {
		uint64_t *validity = duckdb_vector_get_validity(sql_vec);
		if (validity && !duckdb_validity_row_is_valid(validity, row)) {
			SetRowInvalid(output, row);
			continue;
		}

		if (!have_conn) {
			result_data[row] = 0;
			continue;
		}

		duckdb_string_t *str_data = (duckdb_string_t *)duckdb_vector_get_data(sql_vec);
		char *sql = GetString(str_data, row);

		duckdb_extracted_statements stmts = NULL;
		idx_t count = duckdb_extract_statements(conn, sql, &stmts);
		result_data[row] = (int64_t)count;

		if (stmts) duckdb_destroy_extracted(&stmts);
		free(sql);
	}

	if (conn) duckdb_disconnect(&conn);
	if (db) duckdb_close(&db);
}

// ============================================================================
// parse_function_names(query) scalar function
// Returns list of function names from a query
// ============================================================================

static void ParseFunctionNamesFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	idx_t input_size = duckdb_data_chunk_get_size(input);
	duckdb_vector sql_vec = duckdb_data_chunk_get_vector(input, 0);

	duckdb_database db = NULL;
	duckdb_connection conn = NULL;
	bool have_conn = (duckdb_open(NULL, &db) == DuckDBSuccess &&
	                  duckdb_connect(db, &conn) == DuckDBSuccess);

	duckdb_vector child = duckdb_list_vector_get_child(output);
	idx_t list_offset = 0;

	for (idx_t row = 0; row < input_size; row++) {
		uint64_t *validity = duckdb_vector_get_validity(sql_vec);
		if (validity && !duckdb_validity_row_is_valid(validity, row)) {
			SetRowInvalid(output, row);
			duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(output);
			entries[row].offset = list_offset;
			entries[row].length = 0;
			continue;
		}

		duckdb_string_t *str_data = (duckdb_string_t *)duckdb_vector_get_data(sql_vec);
		char *sql = GetString(str_data, row);

		duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(output);
		entries[row].offset = list_offset;
		entries[row].length = 0;

		if (have_conn) {
			// Build query that uses JSON functions to extract function names
			size_t sql_len = strlen(sql);
			size_t query_size = sql_len * 2 + 500;
			char *query = (char *)malloc(query_size);
			size_t j = 0;
			j += snprintf(query + j, query_size - j,
				"WITH plan AS (SELECT json_serialize_plan('");
			for (size_t i = 0; i < sql_len && j < query_size - 200; i++) {
				if (sql[i] == '\'') {
					query[j++] = '\'';
					query[j++] = '\'';
				} else {
					query[j++] = sql[i];
				}
			}
			j += snprintf(query + j, query_size - j,
				"') as j) "
				"SELECT DISTINCT json_extract_string(t.value, '$.name') as name "
				"FROM plan, json_tree(j) t "
				"WHERE json_extract_string(t.value, '$.expression_class') IN ('BOUND_AGGREGATE', 'BOUND_FUNCTION') "
				"  AND json_extract_string(t.value, '$.name') IS NOT NULL");

			duckdb_result result;
			if (duckdb_query(conn, query, &result) == DuckDBSuccess) {
				idx_t func_count = 0;
				duckdb_data_chunk chunk;
				while ((chunk = duckdb_fetch_chunk(result)) != NULL) {
					idx_t chunk_size = duckdb_data_chunk_get_size(chunk);
					if (chunk_size == 0) {
						duckdb_destroy_data_chunk(&chunk);
						break;
					}
					duckdb_vector name_vec = duckdb_data_chunk_get_vector(chunk, 0);
					duckdb_string_t *name_data = (duckdb_string_t *)duckdb_vector_get_data(name_vec);

					for (idx_t i = 0; i < chunk_size; i++) {
						char *name = GetString(name_data, i);
						if (name) {
							duckdb_list_vector_set_size(output, list_offset + func_count + 1);
							duckdb_vector_assign_string_element(child, list_offset + func_count, name);
							func_count++;
							free(name);
						}
					}
					duckdb_destroy_data_chunk(&chunk);
				}
				entries[row].length = func_count;
				list_offset += func_count;
				duckdb_destroy_result(&result);
			}
			free(query);
		}

		free(sql);
	}

	if (conn) duckdb_disconnect(&conn);
	if (db) duckdb_close(&db);
}

// ============================================================================
// parse_functions(query) table function
// Extracts function calls from a query
// ============================================================================

typedef struct {
	char **names;
	char **contexts;
	idx_t count;
	idx_t capacity;
	duckdb_database db;
	duckdb_connection conn;
} ParseFunctionsBindData;

static void ParseFunctionsBindDataDestroy(void *data) {
	ParseFunctionsBindData *bind = (ParseFunctionsBindData *)data;
	if (bind) {
		for (idx_t i = 0; i < bind->count; i++) {
			if (bind->names && bind->names[i]) free(bind->names[i]);
			if (bind->contexts && bind->contexts[i]) free(bind->contexts[i]);
		}
		if (bind->names) free(bind->names);
		if (bind->contexts) free(bind->contexts);
		if (bind->conn) duckdb_disconnect(&bind->conn);
		if (bind->db) duckdb_close(&bind->db);
		free(bind);
	}
}

static void AddFunction(ParseFunctionsBindData *bind, const char *name, const char *context) {
	if (bind->count >= bind->capacity) {
		bind->capacity = bind->capacity ? bind->capacity * 2 : 16;
		bind->names = (char **)realloc(bind->names, bind->capacity * sizeof(char *));
		bind->contexts = (char **)realloc(bind->contexts, bind->capacity * sizeof(char *));
	}
	bind->names[bind->count] = strdup(name);
	bind->contexts[bind->count] = strdup(context);
	bind->count++;
}

static void ParseFunctionsBind(duckdb_bind_info info) {
	ParseFunctionsBindData *bind = (ParseFunctionsBindData *)malloc(sizeof(ParseFunctionsBindData));
	memset(bind, 0, sizeof(ParseFunctionsBindData));

	duckdb_value query_val = duckdb_bind_get_parameter(info, 0);
	char *sql = duckdb_get_varchar(query_val);
	duckdb_destroy_value(&query_val);

	if (duckdb_open(NULL, &bind->db) == DuckDBSuccess &&
	    duckdb_connect(bind->db, &bind->conn) == DuckDBSuccess) {

		// Build query that uses JSON functions to extract function names
		size_t sql_len = strlen(sql);
		size_t query_size = sql_len * 2 + 500;
		char *query = (char *)malloc(query_size);
		size_t j = 0;
		j += snprintf(query + j, query_size - j,
			"WITH plan AS (SELECT json_serialize_plan('");
		for (size_t i = 0; i < sql_len && j < query_size - 200; i++) {
			if (sql[i] == '\'') {
				query[j++] = '\'';
				query[j++] = '\'';
			} else {
				query[j++] = sql[i];
			}
		}
		j += snprintf(query + j, query_size - j,
			"') as j) "
			"SELECT DISTINCT "
			"  json_extract_string(t.value, '$.name') as name, "
			"  CASE WHEN json_extract_string(t.value, '$.expression_class') = 'BOUND_AGGREGATE' "
			"       THEN 'aggregate' ELSE 'scalar' END as type "
			"FROM plan, json_tree(j) t "
			"WHERE json_extract_string(t.value, '$.expression_class') IN ('BOUND_AGGREGATE', 'BOUND_FUNCTION') "
			"  AND json_extract_string(t.value, '$.name') IS NOT NULL");

		duckdb_result result;
		if (duckdb_query(bind->conn, query, &result) == DuckDBSuccess) {
			duckdb_data_chunk chunk;
			while ((chunk = duckdb_fetch_chunk(result)) != NULL) {
				idx_t row_count = duckdb_data_chunk_get_size(chunk);
				if (row_count == 0) {
					duckdb_destroy_data_chunk(&chunk);
					break;
				}
				duckdb_vector name_vec = duckdb_data_chunk_get_vector(chunk, 0);
				duckdb_vector type_vec = duckdb_data_chunk_get_vector(chunk, 1);
				duckdb_string_t *name_data = (duckdb_string_t *)duckdb_vector_get_data(name_vec);
				duckdb_string_t *type_data = (duckdb_string_t *)duckdb_vector_get_data(type_vec);

				for (idx_t row = 0; row < row_count; row++) {
					char *name = GetString(name_data, row);
					char *type = GetString(type_data, row);
					if (name && type) {
						AddFunction(bind, name, type);
					}
					if (name) free(name);
					if (type) free(type);
				}
				duckdb_destroy_data_chunk(&chunk);
			}
			duckdb_destroy_result(&result);
		}
		free(query);
	}

	free(sql);

	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_bind_add_result_column(info, "function_name", varchar_type);
	duckdb_bind_add_result_column(info, "function_type", varchar_type);
	duckdb_destroy_logical_type(&varchar_type);

	duckdb_bind_set_bind_data(info, bind, ParseFunctionsBindDataDestroy);
}

typedef struct {
	idx_t current_idx;
} ParseFunctionsInitData;

static void ParseFunctionsInit(duckdb_init_info info) {
	ParseFunctionsInitData *init = (ParseFunctionsInitData *)malloc(sizeof(ParseFunctionsInitData));
	init->current_idx = 0;
	duckdb_init_set_init_data(info, init, ParseStatementsInitDataDestroy);
}

static void ParseFunctionsFunc(duckdb_function_info info, duckdb_data_chunk output) {
	ParseFunctionsBindData *bind = (ParseFunctionsBindData *)duckdb_function_get_bind_data(info);
	ParseFunctionsInitData *init = (ParseFunctionsInitData *)duckdb_function_get_init_data(info);

	duckdb_vector name_vec = duckdb_data_chunk_get_vector(output, 0);
	duckdb_vector type_vec = duckdb_data_chunk_get_vector(output, 1);

	idx_t count = 0;
	idx_t max_count = duckdb_vector_size();

	while (init->current_idx < bind->count && count < max_count) {
		idx_t i = init->current_idx;
		duckdb_vector_assign_string_element(name_vec, count, bind->names[i]);
		duckdb_vector_assign_string_element(type_vec, count, bind->contexts[i]);
		count++;
		init->current_idx++;
	}

	duckdb_data_chunk_set_size(output, count);
}

// ============================================================================
// parse_where(query) table function
// Extracts WHERE clause conditions
// ============================================================================

typedef struct {
	char **columns;
	char **operators;
	char **values;
	idx_t count;
	idx_t capacity;
	duckdb_database db;
	duckdb_connection conn;
} ParseWhereBindData;

static void ParseWhereBindDataDestroy(void *data) {
	ParseWhereBindData *bind = (ParseWhereBindData *)data;
	if (bind) {
		for (idx_t i = 0; i < bind->count; i++) {
			if (bind->columns && bind->columns[i]) free(bind->columns[i]);
			if (bind->operators && bind->operators[i]) free(bind->operators[i]);
			if (bind->values && bind->values[i]) free(bind->values[i]);
		}
		if (bind->columns) free(bind->columns);
		if (bind->operators) free(bind->operators);
		if (bind->values) free(bind->values);
		if (bind->conn) duckdb_disconnect(&bind->conn);
		if (bind->db) duckdb_close(&bind->db);
		free(bind);
	}
}

static void AddWhereCondition(ParseWhereBindData *bind, const char *col, const char *op, const char *val) {
	if (bind->count >= bind->capacity) {
		bind->capacity = bind->capacity ? bind->capacity * 2 : 16;
		bind->columns = (char **)realloc(bind->columns, bind->capacity * sizeof(char *));
		bind->operators = (char **)realloc(bind->operators, bind->capacity * sizeof(char *));
		bind->values = (char **)realloc(bind->values, bind->capacity * sizeof(char *));
	}
	bind->columns[bind->count] = strdup(col);
	bind->operators[bind->count] = strdup(op);
	bind->values[bind->count] = strdup(val);
	bind->count++;
}

static void ParseWhereBind(duckdb_bind_info info) {
	ParseWhereBindData *bind = (ParseWhereBindData *)malloc(sizeof(ParseWhereBindData));
	memset(bind, 0, sizeof(ParseWhereBindData));

	duckdb_value query_val = duckdb_bind_get_parameter(info, 0);
	char *sql = duckdb_get_varchar(query_val);
	duckdb_destroy_value(&query_val);

	if (duckdb_open(NULL, &bind->db) == DuckDBSuccess &&
	    duckdb_connect(bind->db, &bind->conn) == DuckDBSuccess) {

		// Build query that uses JSON functions to extract WHERE conditions
		size_t sql_len = strlen(sql);
		size_t query_size = sql_len * 2 + 1000;
		char *query = (char *)malloc(query_size);
		size_t j = 0;
		j += snprintf(query + j, query_size - j,
			"WITH plan AS (SELECT json_serialize_plan('");
		for (size_t i = 0; i < sql_len && j < query_size - 500; i++) {
			if (sql[i] == '\'') {
				query[j++] = '\'';
				query[j++] = '\'';
			} else {
				query[j++] = sql[i];
			}
		}
		j += snprintf(query + j, query_size - j,
			"') as j) "
			"SELECT "
			"  COALESCE(json_extract_string(t.value, '$.left.alias'), '') as col, "
			"  CASE json_extract_string(t.value, '$.type') "
			"    WHEN 'COMPARE_GREATERTHAN' THEN '>' "
			"    WHEN 'COMPARE_LESSTHAN' THEN '<' "
			"    WHEN 'COMPARE_EQUAL' THEN '=' "
			"    WHEN 'COMPARE_NOTEQUAL' THEN '!=' "
			"    WHEN 'COMPARE_GREATERTHANOREQUALTO' THEN '>=' "
			"    WHEN 'COMPARE_LESSTHANOREQUALTO' THEN '<=' "
			"    ELSE json_extract_string(t.value, '$.type') "
			"  END as op, "
			"  COALESCE("
			"    json_extract_string(t.value, '$.right.child.value.value')::VARCHAR, "
			"    json_extract(t.value, '$.right.child.value.value')::VARCHAR, "
			"    ''"
			"  ) as val "
			"FROM plan, json_tree(j) t "
			"WHERE json_extract_string(t.value, '$.expression_class') = 'BOUND_COMPARISON'");

		duckdb_result result;
		if (duckdb_query(bind->conn, query, &result) == DuckDBSuccess) {
			duckdb_data_chunk chunk;
			while ((chunk = duckdb_fetch_chunk(result)) != NULL) {
				idx_t row_count = duckdb_data_chunk_get_size(chunk);
				if (row_count == 0) {
					duckdb_destroy_data_chunk(&chunk);
					break;
				}
				duckdb_vector col_vec = duckdb_data_chunk_get_vector(chunk, 0);
				duckdb_vector op_vec = duckdb_data_chunk_get_vector(chunk, 1);
				duckdb_vector val_vec = duckdb_data_chunk_get_vector(chunk, 2);
				duckdb_string_t *col_data = (duckdb_string_t *)duckdb_vector_get_data(col_vec);
				duckdb_string_t *op_data = (duckdb_string_t *)duckdb_vector_get_data(op_vec);
				duckdb_string_t *val_data = (duckdb_string_t *)duckdb_vector_get_data(val_vec);

				for (idx_t row = 0; row < row_count; row++) {
					char *col = GetString(col_data, row);
					char *op = GetString(op_data, row);
					char *val = GetString(val_data, row);
					if (col && op && val) {
						AddWhereCondition(bind, col, op, val);
					}
					if (col) free(col);
					if (op) free(op);
					if (val) free(val);
				}
				duckdb_destroy_data_chunk(&chunk);
			}
			duckdb_destroy_result(&result);
		}
		free(query);
	}

	free(sql);

	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_bind_add_result_column(info, "column_name", varchar_type);
	duckdb_bind_add_result_column(info, "operator", varchar_type);
	duckdb_bind_add_result_column(info, "value", varchar_type);
	duckdb_destroy_logical_type(&varchar_type);

	duckdb_bind_set_bind_data(info, bind, ParseWhereBindDataDestroy);
}

typedef struct {
	idx_t current_idx;
} ParseWhereInitData;

static void ParseWhereInit(duckdb_init_info info) {
	ParseWhereInitData *init = (ParseWhereInitData *)malloc(sizeof(ParseWhereInitData));
	init->current_idx = 0;
	duckdb_init_set_init_data(info, init, ParseStatementsInitDataDestroy);
}

static void ParseWhereFunc(duckdb_function_info info, duckdb_data_chunk output) {
	ParseWhereBindData *bind = (ParseWhereBindData *)duckdb_function_get_bind_data(info);
	ParseWhereInitData *init = (ParseWhereInitData *)duckdb_function_get_init_data(info);

	duckdb_vector col_vec = duckdb_data_chunk_get_vector(output, 0);
	duckdb_vector op_vec = duckdb_data_chunk_get_vector(output, 1);
	duckdb_vector val_vec = duckdb_data_chunk_get_vector(output, 2);

	idx_t count = 0;
	idx_t max_count = duckdb_vector_size();

	while (init->current_idx < bind->count && count < max_count) {
		idx_t i = init->current_idx;
		duckdb_vector_assign_string_element(col_vec, count, bind->columns[i]);
		duckdb_vector_assign_string_element(op_vec, count, bind->operators[i]);
		duckdb_vector_assign_string_element(val_vec, count, bind->values[i]);
		count++;
		init->current_idx++;
	}

	duckdb_data_chunk_set_size(output, count);
}

// ============================================================================
// Registration
// ============================================================================

static void RegisterTableFunction(
	duckdb_connection connection,
	const char *name,
	duckdb_logical_type *param_types,
	idx_t param_count,
	duckdb_table_function_bind_t bind_func,
	duckdb_table_function_init_t init_func,
	duckdb_table_function_t func
) {
	duckdb_table_function table_func = duckdb_create_table_function();
	duckdb_table_function_set_name(table_func, name);

	for (idx_t i = 0; i < param_count; i++) {
		duckdb_table_function_add_parameter(table_func, param_types[i]);
	}

	duckdb_table_function_set_bind(table_func, bind_func);
	duckdb_table_function_set_init(table_func, init_func);
	duckdb_table_function_set_function(table_func, func);

	duckdb_register_table_function(connection, table_func);
	duckdb_destroy_table_function(&table_func);
}

static void RegisterScalarFunction(
	duckdb_connection connection,
	const char *name,
	duckdb_logical_type *param_types,
	idx_t param_count,
	duckdb_logical_type return_type,
	duckdb_scalar_function_t func
) {
	duckdb_scalar_function scalar_func = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(scalar_func, name);

	for (idx_t i = 0; i < param_count; i++) {
		duckdb_scalar_function_add_parameter(scalar_func, param_types[i]);
	}

	duckdb_scalar_function_set_return_type(scalar_func, return_type);
	duckdb_scalar_function_set_function(scalar_func, func);

	duckdb_register_scalar_function(connection, scalar_func);
	duckdb_destroy_scalar_function(&scalar_func);
}

void RegisterParserFunctions(duckdb_connection connection) {
	duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_logical_type bigint_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_logical_type bool_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);

	// ===== Table Functions =====

	// parse_statements(query) -> table(stmt_index, stmt_type, error, param_count)
	{
		duckdb_logical_type params[] = {varchar_type};
		RegisterTableFunction(connection, "parse_statements", params, 1,
			ParseStatementsBind, ParseStatementsInit, ParseStatementsFunc);
	}

	// parse_parameters(query, stmt_index) -> table(param_index, param_name, param_type)
	{
		duckdb_logical_type params[] = {varchar_type, bigint_type};
		RegisterTableFunction(connection, "parse_parameters", params, 2,
			ParseParamsBind, ParseParamsInit, ParseParamsFunc);
	}

	// parse_columns(query, stmt_index) -> table(col_index, col_name, col_type)
	{
		duckdb_logical_type params[] = {varchar_type, bigint_type};
		RegisterTableFunction(connection, "parse_columns", params, 2,
			ParseColumnsBind, ParseColumnsInit, ParseColumnsFunc);
	}

	// parse_type_info(query, stmt_index, col_index) -> table(base_type, full_type, ...)
	{
		duckdb_logical_type params[] = {varchar_type, bigint_type, bigint_type};
		RegisterTableFunction(connection, "parse_type_info", params, 3,
			ParseTypeBind, ParseTypeInit, ParseTypeFunc);
	}

	// parse_column_types(query, stmt_index) -> table(col_index, col_name, base_type, full_type)
	{
		duckdb_logical_type params[] = {varchar_type, bigint_type};
		RegisterTableFunction(connection, "parse_column_types", params, 2,
			ParseColumnTypesBind, ParseColumnTypesInit, ParseColumnTypesFunc);
	}

	// sql_keywords() -> table(keyword)
	{
		RegisterTableFunction(connection, "sql_keywords", NULL, 0,
			SqlKeywordsBind, SqlKeywordsInit, SqlKeywordsFunc);
	}

	// parse_tables(query) -> table(schema, table, context)
	{
		duckdb_logical_type params[] = {varchar_type};
		RegisterTableFunction(connection, "parse_tables", params, 1,
			ParseTablesBind, ParseTablesInit, ParseTablesFunc);
	}

	// tokenize_sql(query) -> table(byte_position, category)
	{
		duckdb_logical_type params[] = {varchar_type};
		RegisterTableFunction(connection, "tokenize_sql", params, 1,
			TokenizeSqlBind, TokenizeSqlInit, TokenizeSqlFunc);
	}

	// parse_functions(query) -> table(function_name, function_type)
	{
		duckdb_logical_type params[] = {varchar_type};
		RegisterTableFunction(connection, "parse_functions", params, 1,
			ParseFunctionsBind, ParseFunctionsInit, ParseFunctionsFunc);
	}

	// parse_where(query) -> table(column_name, operator, value)
	{
		duckdb_logical_type params[] = {varchar_type};
		RegisterTableFunction(connection, "parse_where", params, 1,
			ParseWhereBind, ParseWhereInit, ParseWhereFunc);
	}

	// ===== Scalar Functions =====

	// is_keyword(identifier) -> BOOLEAN
	{
		duckdb_logical_type params[] = {varchar_type};
		RegisterScalarFunction(connection, "is_keyword", params, 1, bool_type, IsKeywordFunction);
	}

	// sql_strip_comments(query) -> VARCHAR
	{
		duckdb_logical_type params[] = {varchar_type};
		RegisterScalarFunction(connection, "sql_strip_comments", params, 1, varchar_type, SqlStripCommentsFunction);
	}

	// sql_parse_json(query) -> VARCHAR (JSON)
	{
		duckdb_logical_type params[] = {varchar_type};
		RegisterScalarFunction(connection, "sql_parse_json", params, 1, varchar_type, SqlParseJsonFunction);
	}

	// is_valid_sql(query) -> BOOLEAN
	{
		duckdb_logical_type params[] = {varchar_type};
		RegisterScalarFunction(connection, "is_valid_sql", params, 1, bool_type, IsValidSqlFunction);
	}

	// sql_error_message(query) -> VARCHAR
	{
		duckdb_logical_type params[] = {varchar_type};
		RegisterScalarFunction(connection, "sql_error_message", params, 1, varchar_type, SqlErrorMessageFunction);
	}

	// parse_table_names(query) -> VARCHAR[]
	{
		duckdb_logical_type list_type = duckdb_create_list_type(varchar_type);
		duckdb_logical_type params[] = {varchar_type};
		RegisterScalarFunction(connection, "parse_table_names", params, 1, list_type, ParseTableNamesFunction);
		duckdb_destroy_logical_type(&list_type);
	}

	// num_statements(query) -> BIGINT
	{
		duckdb_logical_type params[] = {varchar_type};
		RegisterScalarFunction(connection, "num_statements", params, 1, bigint_type, NumStatementsFunction);
	}

	// parse_function_names(query) -> VARCHAR[]
	{
		duckdb_logical_type list_type = duckdb_create_list_type(varchar_type);
		duckdb_logical_type params[] = {varchar_type};
		RegisterScalarFunction(connection, "parse_function_names", params, 1, list_type, ParseFunctionNamesFunction);
		duckdb_destroy_logical_type(&list_type);
	}

	duckdb_destroy_logical_type(&varchar_type);
	duckdb_destroy_logical_type(&bigint_type);
	duckdb_destroy_logical_type(&bool_type);
}

