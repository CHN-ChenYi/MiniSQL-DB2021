#include <map>

#include "IndexManager.hpp"
#include "RecordManager.hpp"
using std::make_tuple;

static map<tuple<string, string>, std::map<SqlValue, Position>> idx;

bool IndexManager::CreateIndex(const Table &table, const string &index_name,
                               const string &column) {
  idx[make_tuple(table.table_name, index_name)] =
      std::map<SqlValue, Position>{};
  auto &s = idx[make_tuple(table.table_name, index_name)];

  const auto index = get<0>(table.attributes.at(column));
  RecordAccessProxy rap = record_manager.getIterator(table);
  do {
    if (!rap.isCurrentSlotValid()) continue;
    s[rap.extractData().values[index]] = rap.extractPostion();
  } while (rap.next());
  return true;
}

bool DropIndex(const Table &table, const string &index_name) {
  idx.erase(make_tuple(table.table_name, index_name));
  return true;
}

bool InsertKey(const Table &table, const Tuple &tuple, Position &pos) {
  for (const auto &v : table.indexes) {
    const auto &attribute_name = v.first;
    const auto &attribute_index = get<0>(table.attributes.at(attribute_name));
    const auto &index_name = v.second;
    idx[make_tuple(table.table_name, index_name)]
       [tuple.values[attribute_index]] = pos;
  }
  return true;
}

bool RemoveKey(const Table &table, const Tuple &tuple) {
  for (const auto &v : table.indexes) {
    const auto &attribute_name = v.first;
    const auto &attribute_index = get<0>(table.attributes.at(attribute_name));
    const auto &index_name = v.second;
    idx[make_tuple(table.table_name, index_name)].erase(
        tuple.values[attribute_index]);
  }
  return true;
}

bool checkCondition(const Table &table, const vector<Condition> &condition) {
  for (const auto &c : condition) {
    if (table.indexes.contains(c.attribute) && c.op != Operator::NE) return true;
  }
  return false;
}

vector<Tuple> SelectRecord(const Table &table,
                           const vector<Condition> &conditions) {
  for (const auto &c : conditions) {
    if (!(table.indexes.contains(c.attribute) && c.op != Operator::NE)) continue;
    vector<Tuple> ret;
    switch (c.op) {
      case Operator::EQ:
        return ret;
      case Operator::GT:
        return ret;
      case Operator::GE:
        return ret;
      case Operator::LT:
        return ret;
      case Operator::LE:
        return ret;
    }
  }
}
