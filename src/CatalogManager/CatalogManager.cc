#include "CatalogManager.hpp"

#include <exception>
#include <fstream>
#include <iostream>
#include "DataStructure.hpp"

CatalogManager::CatalogManager() {
  std::ifstream os(Config::kCatalogFileName, std::ios::binary);
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
  std::ofstream os(Config::kCatalogFileName, std::ios::binary);
  const auto size = tables_.size();
  os.write(reinterpret_cast<const char *>(&size), sizeof(size));
  for (const auto &table : tables_) table.second.write(os);
}

bool CatalogManager::CreateTable(
    const string &table_name,
    const vector<tuple<string, SqlValueType, SpecialAttribute>> &attributes) {
  if (tables_.contains(table_name)) return false;
  size_t index = 0, offset = 0;
  Table table;
  table.table_name = table_name;
  for (const auto &[attribute_name, type, special_attribute] : attributes) {
    table.attributes[attribute_name] =
        std::make_tuple(index++, type, special_attribute, offset);
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

bool CatalogManager::CreateIndex(const string &table_name,
                                 const string &attribute_name,
                                 const string &index_name) {
  if (!tables_.contains(table_name)) return false;
  auto &table = tables_[table_name];
  if (!table.attributes.contains(attribute_name)) return false;
  if (table.indexes.contains(attribute_name)) return false;
  if (std::get<2>(table.attributes[attribute_name]) !=
      SpecialAttribute::UniqueKey)
    return false;
  for (const auto &i : tables_) {
    for (const auto &index : i.second.indexes) {
      if (index.second == index_name) return false;
    }
  }
  table.indexes[attribute_name] = index_name;
}

bool CatalogManager::DropIndex(const string &index_name) {
  for (auto &table : tables_) {
    for (const auto &index : table.second.indexes) {
      if (index.second == index_name) {
        if (std::get<2>(table.second.attributes[index.first]) ==
            SpecialAttribute::PrimaryKey)
          return false;
        table.second.indexes.erase(index.first);
        return true;
      }
    }
  }
  return false;
}

const Table &CatalogManager::TableInfo(const string &table_name) {
  if (!tables_.contains(table_name)) {
    std::cerr << "such a table doesn't exist" << std::endl;
    throw invalid_ident("table not found");
  }
  return tables_[table_name];
}

void CatalogManager::DropTable(const string &table_name) {
  if (!tables_.contains(table_name)) {
    std::cerr << "such a table doesn't exist" << std::endl;
    throw invalid_ident("table not found");
  }
  tables_.erase(table_name);
}

CatalogManager catalog_manager;
