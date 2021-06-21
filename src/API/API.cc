#include "API.hpp"

#include "CatalogManager.hpp"
#include "RecordManager.hpp"

bool CreateTable(
    const string &table_name,
    const vector<tuple<string, SqlValueType, SpecialAttribute>> &attributes) {
  if (!catalog_manager.CreateTable(table_name, attributes)) return false;
  if (!record_manager.createTable(catalog_manager.TableInfo(table_name)))
    return false;
}

void DropTable(const string &table_name) {}

void CreateIndex(const string &table_name, const string &index_name,
                 const string &column) {}

void DropIndex(const string &index_name) {}

void Select(const string &table_name, const vector<Condition> &conditions) {}

void Insert(const string &table_name, const Tuple &tuple) {
  record_manager[table_name].insertRecord(tuple);
}

void Delete(const string &table_name, const vector<Condition> &conditions) {}
