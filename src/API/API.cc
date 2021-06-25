#include "API.hpp"

#include "CatalogManager.hpp"
#include "IndexManager.hpp"
#include "RecordManager.hpp"

bool CreateTable(
    const string &table_name,
    const vector<tuple<string, SqlValueType, SpecialAttribute>> &attributes) {
  if (!catalog_manager.CreateTable(table_name, attributes)) return false;
  if (!record_manager.createTable(catalog_manager.TableInfo(table_name)))
    return false;
  // untested↓
  // if (!index_manager.PrimaryKeyIndex(catalog_manager.TableInfo(table_name)))
  return false;
}

void DropTable(const string &table_name) {}

bool CreateIndex(const string &table_name, const string &index_name,
                 const string &column) {
  // untested↓
  // if (!index_manager.CreateIndex(catalog_manager.TableInfo(table_name),
  // index_name, column)) return false;
  return true;
}

void DropIndex(const string &index_name) {}

void Select(const string &table_name, const vector<Condition> &conditions) {
  vector<Tuple> res;
  if (conditions.empty())
    res = record_manager.selectAllRecord(catalog_manager.TableInfo(table_name));
  else
    res = record_manager.selectRecord(catalog_manager.TableInfo(table_name),
                                conditions);
  for (auto &v : res) {
    std::cout << static_cast<std::string>(v) << std::endl;
  }
}

void Insert(const string &table_name, const Tuple &tuple) {
  Position pos =
      record_manager.insertRecord(catalog_manager.TableInfo(table_name), tuple);
  // untested↓
  // index_manager.InsertKey(catalog_manager.TableInfo(table_name), tuple,
  // pos);
}

void Delete(const string &table_name, const vector<Condition> &conditions) {
  record_manager.deleteRecord(catalog_manager.TableInfo(table_name),
                              conditions);
}
