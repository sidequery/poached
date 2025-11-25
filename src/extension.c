#include "duckdb_extension.h"
#include "parser.h"

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info, struct duckdb_extension_access *access) {
	RegisterParserFunctions(connection);
	return true;
}
