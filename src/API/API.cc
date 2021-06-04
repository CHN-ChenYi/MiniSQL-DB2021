#include "API.hpp"

void CreateTable(
    const string &table_name,
    const vector<tuple<SqlValueType, SpecialAttribute>> &attributes) {}

void DropTable(const string &table_name) {}

void CreateIndex(const string &table_name, const string &index_name,
                 const string &column) {}

void DropIndex(const string &index_name) {}

void Select(const string &table_name, const vector<Condition> &conditions) {}

void Insert(const string &table_name, const Tuple &tuple) {}

void Delete(const string &table_name, const vector<Condition> &conditions) {}

Table *TableInfo(const string &table_name) {}
