#include "API.hpp"

#include "CatalogManager.hpp"
#include "DataStructure.hpp"
#include "IndexManager.hpp"
#include "Interpreter.hpp"
#include "RecordManager.hpp"

bool CreateTable(
    const string &table_name,
    const vector<tuple<string, SqlValueType, SpecialAttribute>> &attributes) {
  catalog_manager.CreateTable(table_name, attributes);
  if (!record_manager.createTable(catalog_manager.TableInfo(table_name)))
    return false;
  // if (!index_manager.PrimaryKeyIndex(catalog_manager.TableInfo(table_name)))
  // return false;
}

bool DropTable(const string &table_name) {
  if (!record_manager.dropTable(catalog_manager.TableInfo(table_name)))
    return false;
  index_manager.DropAllIndex(catalog_manager.TableInfo(table_name));
  catalog_manager.DropTable(table_name);
  return true;
}

bool CreateIndex(const string &table_name, const string &index_name,
                 const string &column) {
  catalog_manager.CreateIndex(table_name, column, index_name);
  if (!index_manager.CreateIndex(catalog_manager.TableInfo(table_name),
                                 index_name, column))
    return false;
  return true;
}

bool DropIndex(const string &table_name, const string &index_name) {
  catalog_manager.DropIndex(table_name, index_name);
  if (!index_manager.DropIndex(index_name)) return false;
  return true;
}

vector<Tuple> Select(const string &table_name,
                     const vector<Condition> &conditions) {
  vector<Tuple> res;
  if (conditions.empty())
    res =
        record_manager.selectAllRecords(catalog_manager.TableInfo(table_name));
  else if (index_manager.checkCondition(catalog_manager.TableInfo(table_name),
                                        conditions))
    res = index_manager.SelectRecord(catalog_manager.TableInfo(table_name),
                                     conditions);
  else
    res = record_manager.selectRecord(catalog_manager.TableInfo(table_name),
                                      conditions);
  return res;
}

size_t Insert(const string &table_name, const Tuple &tuple) {
  Position pos =
      record_manager.insertRecord(catalog_manager.TableInfo(table_name), tuple);
  index_manager.InsertKey(catalog_manager.TableInfo(table_name), tuple, pos);
  return 1;
}

size_t InsertFast(const Table &table, const Tuple &tp,
                  const vector<tuple<const char *, size_t, size_t>> &unique) {
  Position pos = record_manager.insertRecordUnique(table, tp, unique);
  index_manager.InsertKey(table, tp, pos);
  return 1;
}

size_t Delete(const string &table_name, const vector<Condition> &conditions) {
  size_t n;
  if (conditions.empty())
    n = record_manager.deleteAllRecords(catalog_manager.TableInfo(table_name));
  else
    n = record_manager.deleteRecord(catalog_manager.TableInfo(table_name),
                                    conditions);
  return n;
}
