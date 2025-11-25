#include "duckdb_extension.h"

#include "add_numbers.h"
#include "parser.h"

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info, struct duckdb_extension_access *access) {
	// Register demo function
	RegisterAddNumbersFunction(connection);

	// Register parser functions
	RegisterParserFunctions(connection);

	// Return true to indicate successful initialization
	return true;
}
