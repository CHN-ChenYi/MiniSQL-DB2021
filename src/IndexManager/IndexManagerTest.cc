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
  for (const auto &c : conditions) {
    if (!(table.indexes.contains(c.attribute) && c.op != Operator::NE))
      continue;
    vector<Tuple> ret;
    switch (c.op) {
      case Operator::EQ: {
        auto index_name = table.indexes.at(c.attribute);
        auto s = idx[make_tuple(table.table_name, index_name)];
        if (s.contains(c.val)) ret.push_back(extractData(table, s[c.val]));
        return ret;
      }
      case Operator::GT: {
        auto index_name = table.indexes.at(c.attribute);
        auto s = idx[make_tuple(table.table_name, index_name)];
        auto it = s.upper_bound(c.val);
        while (it != s.end()) {
          ret.push_back(extractData(table, it->second));
          it++;
        }
        return ret;
      }
      case Operator::GE: {
        auto index_name = table.indexes.at(c.attribute);
        auto s = idx[make_tuple(table.table_name, index_name)];
        auto it = s.lower_bound(c.val);
        while (it != s.end()) {
          ret.push_back(extractData(table, it->second));
          it++;
        }
        return ret;
      }
      case Operator::LT: {
        auto index_name = table.indexes.at(c.attribute);
        auto s = idx[make_tuple(table.table_name, index_name)];
        for (auto &it : s) {
          if (c.val == it.first || c.val < it.first) break;
          ret.push_back(extractData(table, it.second));
        }
        return ret;
      }
      case Operator::LE: {
        auto index_name = table.indexes.at(c.attribute);
        auto s = idx[make_tuple(table.table_name, index_name)];
        for (auto &it : s) {
          if (c.val < it.first) break;
          ret.push_back(extractData(table, it.second));
        }
        return ret;
      }
      default:
        return ret;
    }
  }
}

const Tuple IndexManager::extractData(const Table &table, const Position &pos) {
  Block *blk = buffer_manager.Read(pos.block_id);
  char *data = blk->val_ + pos.offset;
  char *tmp = data;
  int size = table.attributes.size();
  Tuple tuple_;
  SqlValue t;
  for (auto &v : table.attributes) {
    switch (get<1>(v.second)) {
      case static_cast<SqlValueType>(SqlValueTypeBase::Integer):
        memcpy(&t.val.Integer, tmp, sizeof(t.val.Integer));
        t.type = static_cast<SqlValueType>(SqlValueTypeBase::Integer);
        tuple_.values.push_back(t);
        tmp += sizeof(t.val.Integer);
        break;
      case static_cast<SqlValueType>(SqlValueTypeBase::Float):
        memcpy(&t.val.Float, tmp, sizeof(t.val.Float));
        t.type = static_cast<SqlValueType>(SqlValueTypeBase::Float);
        tuple_.values.push_back(t);
        tmp += sizeof(t.val.Float);
        break;
      default: {
        size_t len = get<1>(v.second) -
                     static_cast<SqlValueType>(SqlValueTypeBase::String);
        memcpy(t.val.String, tmp, len);
        t.type = static_cast<SqlValueType>(len);
        tuple_.values.push_back(t);
        tmp += len;
        break;
      }
    }
  }
  return tuple_;
}
