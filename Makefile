PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=poached
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# DuckDB 2.0 source builds need an explicit release version for extension metadata.
OVERRIDE_GIT_DESCRIBE ?= v2.0.0

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
