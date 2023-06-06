cmake_minimum_required(VERSION 2.8.12)

project(IVMExtension)

add_extension_definitions()

include_directories(include)

add_library(
  ivm_extension STATIC ivm_extension.cpp ivm_parser.cpp
                          ivm_parser_helpers.cpp)

set(PARAMETERS "-warnings")
build_loadable_extension(
  ivm
  ${PARAMETERS}
  ivm_extension.cpp ivm_parser.cpp
        ivm_parser_helpers.cpp)

install(
  TARGETS ivm_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
