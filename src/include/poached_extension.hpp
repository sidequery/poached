#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class PoachedExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

// Register all parser functions with the extension loader
void RegisterParserFunctions(ExtensionLoader &loader);

} // namespace duckdb
