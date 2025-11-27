#define DUCKDB_EXTENSION_MAIN

#include "poached_extension.hpp"
#include "duckdb.hpp"

namespace duckdb {

void PoachedExtension::Load(ExtensionLoader &loader) {
	RegisterParserFunctions(loader);
}

std::string PoachedExtension::Name() {
	return "poached";
}

std::string PoachedExtension::Version() const {
#ifdef EXT_VERSION_POACHED
	return EXT_VERSION_POACHED;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(poached, loader) {
	duckdb::RegisterParserFunctions(loader);
}
}
