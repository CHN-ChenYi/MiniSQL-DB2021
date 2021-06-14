#include "CatalogManager.hpp"

#include <exception>
#include <fstream>
#include <iostream>

CatalogManager::CatalogManager() {
  std::ifstream os(kCatalogFileName, std::ios::binary);
  if (!os) {
    std::cerr << "Couldn't find the catalog file, assuming it is the first "
                 "time of running MiniSQL."
              << std::endl;
    return;
  }
  size_t size;
  Table table;
  os.read(reinterpret_cast<char *>(&size), sizeof(size));
  while (size--) {
    table.read(os);
    tables_[table.table_name] = table;
  }
}

CatalogManager::~CatalogManager() {
  std::ofstream os(kCatalogFileName, std::ios::binary);
  const auto size = tables_.size();
  os.write(reinterpret_cast<const char *>(&size), sizeof(size));
  for (const auto &table : tables_) table.second.write(os);
}

bool CatalogManager::CreateTable(
    const string &table_name,
    const vector<tuple<string, SqlValueType, SpecialAttribute>> &attributes) {
  if (tables_.contains(table_name)) return false;
  size_t offset = 0;
  Table table;
  table.table_name = table_name;
  for (const auto &[attribute_name, type, special_attribute] : attributes) {
    table.attributes[attribute_name] =
        std::make_tuple(type, special_attribute, offset);
    switch (type) {
      case static_cast<SqlValueType>(SqlValueTypeBase::Integer):
        offset += sizeof(int);
        break;
      case static_cast<SqlValueType>(SqlValueTypeBase::Float):
        offset += sizeof(float);
        break;
      default:
        offset += type - static_cast<size_t>(SqlValueTypeBase::String);
    }
    if (special_attribute == SpecialAttribute::PrimaryKey)
      table.indexes[attribute_name] = attribute_name;
  }
  tables_[table_name] = table;
  return true;
}

Table CatalogManager::TableInfo(const string &table_name) {
  if (!tables_.contains(table_name)) {
    throw std::runtime_error("Table not found");
  }
  return tables_[table_name];
}

CatalogManager catalog_manager;
