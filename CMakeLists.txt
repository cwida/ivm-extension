cmake_minimum_required(VERSION 2.8.12)

project(IvmExtension)

add_extension_definitions()

include_directories(include ../../extension/server/include)

add_library(ivm_extension STATIC ivm_extension.cpp ivm_parser.cpp
        ../../extension/server/rdda_view.cpp ../../extension/server/rdda_table.cpp)

set(PARAMETERS "-warnings")
build_loadable_extension(ivm ${PARAMETERS} ivm_extension.cpp ivm_parser.cpp ../../extension/server/rdda_view.cpp ../../extension/server/rdda_table.cpp)

install(
  TARGETS ivm_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
