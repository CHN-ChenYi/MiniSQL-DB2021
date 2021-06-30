#include <map>
#include <vector>

#include "BufferManager.hpp"
#include "DataStructure.hpp"
#include "IndexManager.hpp"
#include "RecordManager.hpp"
using std::make_tuple;

static map<tuple<string, string>, std::map<SqlValue, Position>> idx;
IndexManager index_manager;

IndexManager::IndexManager() {
  const auto &is = catalog_manager.tables_.begin();
  if (is == catalog_manager.tables_.end()) {
    std::cerr << "Couldn't find the Index file, assuming it is the first "
                 "time of running MiniSQL."
              << std::endl;
    return;
  }
  while (is != catalog_manager.tables_.end()) {
    for (const auto &it : is->second.indexes)
      CreateIndex(is->second, it.second, it.first);
  }
}

IndexManager::~IndexManager() {}

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

bool IndexManager::PrimaryKeyIndex(const Table &table) {
  auto it = table.attributes.begin();
  while (it != table.attributes.end()) {
    auto &c = *it;
    if (get<2>(c.second) == SpecialAttribute::PrimaryKey) break;
    ++it;
  }
  if (it == table.attributes.end()) {
    cerr << "failed to build an index: the table doesn't have a Primary Key"
         << endl;
    return true;
  }
  CreateIndex(table, table.indexes.at(it->first), it->first);
  return true;
}

void IndexManager::DropAllIndex(const Table &table) {
  for (const auto &v : table.indexes) {
    auto &index_name = v.second;
    DropIndex(table, index_name);
  }
}

bool IndexManager::DropIndex(const Table &table, const string &index_name) {
  idx.erase(make_tuple(table.table_name, index_name));
  return true;
}

bool IndexManager::InsertKey(const Table &table, const Tuple &tuple,
                             Position &pos) {
  for (const auto &v : table.indexes) {
    const auto &attribute_name = v.first;
    const auto &attribute_index = get<0>(table.attributes.at(attribute_name));
    const auto &index_name = v.second;
    idx[make_tuple(table.table_name, index_name)]
       [tuple.values[attribute_index]] = pos;
  }
  return true;
}

bool IndexManager::RemoveKey(const Table &table, const Tuple &tuple) {
  for (const auto &v : table.indexes) {
    const auto &attribute_name = v.first;
    const auto &attribute_index = get<0>(table.attributes.at(attribute_name));
    const auto &index_name = v.second;
    idx[make_tuple(table.table_name, index_name)].erase(
        tuple.values[attribute_index]);
  }
  return true;
}

bool IndexManager::checkCondition(const Table &table,
                                  const vector<Condition> &condition) {
  for (const auto &c : condition) {
    if (table.indexes.contains(c.attribute) && c.op != Operator::NE)
      return true;
  }
  return false;
}

vector<Tuple> IndexManager::SelectRecord(const Table &table,
                                         const vector<Condition> &conditions) {
  vector<Position> ret;
  for (const auto &c : conditions) {
    if (!(table.indexes.contains(c.attribute) && c.op != Operator::NE))
      continue;
    switch (c.op) {
      case Operator::EQ: {
        auto index_name = table.indexes.at(c.attribute);
        auto s = idx[make_tuple(table.table_name, index_name)];
        if (s.contains(c.val)) ret.push_back(s[c.val]);
        break;
      }
      case Operator::GT: {
        auto index_name = table.indexes.at(c.attribute);
        auto s = idx[make_tuple(table.table_name, index_name)];
        auto it = s.upper_bound(c.val);
        while (it != s.end()) {
          ret.push_back(it->second);
          it++;
        }
        break;
      }
      case Operator::GE: {
        auto index_name = table.indexes.at(c.attribute);
        auto s = idx[make_tuple(table.table_name, index_name)];
        auto it = s.lower_bound(c.val);
        while (it != s.end()) {
          ret.push_back(it->second);
          it++;
        }
        break;
      }
      case Operator::LT: {
        auto index_name = table.indexes.at(c.attribute);
        auto s = idx[make_tuple(table.table_name, index_name)];
        for (auto &it : s) {
          if (c.val == it.first || c.val < it.first) break;
          ret.push_back(it.second);
        }
        break;
      }
      case Operator::LE: {
        auto index_name = table.indexes.at(c.attribute);
        auto s = idx[make_tuple(table.table_name, index_name)];
        for (auto &it : s) {
          if (c.val < it.first) break;
          ret.push_back(it.second);
        }
        break;
      }
      default:
        break;
    }
    break;
  }
  return record_manager.selectRecordFromPosition(table, ret, conditions);
}