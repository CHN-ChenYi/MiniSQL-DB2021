#include "API.hpp"

#include "CatalogManager.hpp"
#include "DataStructure.hpp"
#include "IndexManager.hpp"
#include "RecordManager.hpp"

bool CreateTable(
    const string &table_name,
    const vector<tuple<string, SqlValueType, SpecialAttribute>> &attributes) {
  if (!catalog_manager.CreateTable(table_name, attributes)) return false;
  if (!record_manager.createTable(catalog_manager.TableInfo(table_name)))
    return false;
    //Problem to be solved ↓↓↓
    //PrimaryKeyIndex won't call catalog_manager.CreateIndex
  if (!index_manager.PrimaryKeyIndex(catalog_manager.TableInfo(table_name)))
    return false;
}

bool DropTable(const string &table_name) {
  if (!record_manager.dropTable(catalog_manager.TableInfo(table_name)))
    return false;
  catalog_manager.DropTable(table_name);
  return true;
}

bool CreateIndex(const string &table_name, const string &index_name,
                 const string &column) {
  if (!catalog_manager.CreateIndex(table_name, column, index_name))
    return false;
  if (!index_manager.CreateIndex(catalog_manager.TableInfo(table_name),
    index_name, column)) return false;
  return true;
}

bool DropIndex(const string &table_name, const string &index_name) {
  if (!catalog_manager.DropIndex(index_name)) return false;
  //if (!index_manager.DropIndex(index_name)) return false;
  //return true;
}

void Select(const string &table_name, const vector<Condition> &conditions,
            bool redirect) {
  vector<Tuple> res;
  if (conditions.empty())
    res =
        record_manager.selectAllRecords(catalog_manager.TableInfo(table_name));
  else
    res = record_manager.selectRecord(catalog_manager.TableInfo(table_name),
                                      conditions);
  std::ofstream out("output.txt");
  if (redirect) {
    for (auto &v : res) {
      out << static_cast<std::string>(v) << std::endl;
    }
  } else {
    for (auto &v : res) {
      std::cout << static_cast<std::string>(v) << std::endl;
    }
  }
  std::cout << ANSI_COLOR_CYAN << res.size() << ANSI_COLOR_RESET " rows in set"
            << std::endl;
}

void Insert(const string &table_name, const Tuple &tuple) {
  Position pos =
      record_manager.insertRecord(catalog_manager.TableInfo(table_name), tuple);
  index_manager.InsertKey(catalog_manager.TableInfo(table_name), tuple, pos);
}

void Delete(const string &table_name, const vector<Condition> &conditions) {
  size_t n;
  if (conditions.empty())
    n = record_manager.deleteAllRecords(catalog_manager.TableInfo(table_name));
  else
    n = record_manager.deleteRecord(catalog_manager.TableInfo(table_name),
                                    conditions);
  std::cout << ANSI_COLOR_CYAN << n << ANSI_COLOR_RESET " rows affect"
            << std::endl;
}
