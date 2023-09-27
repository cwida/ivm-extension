#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "ivm_parser.hpp"

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

namespace duckdb {

class IvmExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
