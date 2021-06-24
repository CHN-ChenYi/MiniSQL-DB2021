#include "API.hpp"
#include "CatalogManager.hpp"
#include "RecordManager.hpp"
#include "IndexManager.hpp"

bool CreateTable(
    const string &table_name,
    const vector<tuple<string, SqlValueType, SpecialAttribute>> &attributes) {
  if (!catalog_manager.CreateTable(table_name, attributes)) return false;
  if (!record_manager.createTable(catalog_manager.TableInfo(table_name)))
    return false;
}

void DropTable(const string &table_name) {}

bool CreateIndex(const string &table_name,
                 const string &index_name,
                 const string &column) {
  if (!index_manager.CreateIndex(catalog_manager.TableInfo(table_name), index_name, column)) return false;
  return true;
}

void DropIndex(const string &index_name) {}

void Select(const string &table_name, const vector<Condition> &conditions) {
  auto res =
      record_manager.selectAllRecord(catalog_manager.TableInfo(table_name));
  for (auto &v : res) {
    std::cout << static_cast<std::string>(v) << std::endl;
  }
}

void Insert(const string &table_name, const Tuple &tuple) {
  record_manager.insertRecord(catalog_manager.TableInfo(table_name), tuple);
}

void Delete(const string &table_name, const vector<Condition> &conditions) {}
