#include "RecordManager.hpp"

#include <fstream>
#include <iostream>
#include <ostream>
#include <stdexcept>
#include <utility>

#include "BufferManager.hpp"
#include "CatalogManager.hpp"
#include "DataStructure.hpp"

using std::cerr;
using std::endl;
using std::ifstream;
using std::ostream;

RecordAccessProxy::RecordAccessProxy(vector<size_t> *block_id,
                                     const Table *table, size_t blk_idx)
    : p_block_id_(block_id),
      p_table_(table),
      record_len_(table->getAttributeSize() + 1 /* tag size is 1*/),
      blk_idx_(blk_idx) {
  if (block_id->empty()) {
    cur_blk_ = nullptr;
    data_ = nullptr;
    return;
  }
  cur_blk_ = static_cast<RecordBlock *>(
      buffer_manager.Read(block_id->data()[blk_idx]));
  cur_blk_->pin_ = true;
  data_ = cur_blk_->val_;
  tuple_ = table->makeEmptyTuple();
}

bool RecordAccessProxy::isCurrentSlotValid() {
  if (data_)
    return *data_;
  else
    return false;
}

void RecordAccessProxy::releaseCurrentBlock() {
  if (cur_blk_) cur_blk_->pin_ = false;
}

bool RecordAccessProxy::nextBlock() {
  if (blk_idx_ + 1 < p_block_id_->size()) {
    ++blk_idx_;
  } else {
    return false;
  }
  cur_blk_->pin_ = false;
  cur_blk_ = static_cast<RecordBlock *>(
      buffer_manager.Read(p_block_id_->data()[blk_idx_]));
  cur_blk_->pin_ = true;
  data_ = cur_blk_->val_;

  return true;
}

void RecordAccessProxy::newBlock() {
  size_t new_id = buffer_manager.NextId();
  if (cur_blk_) {
    cur_blk_->pin_ = false;
    ++blk_idx_;
  }
  cur_blk_ = static_cast<RecordBlock *>(buffer_manager.Read(new_id));
  cur_blk_->pin_ = true;
  cur_blk_->dirty_ = true;
  data_ = cur_blk_->val_;
  memset(data_, 0, Config::kBlockSize);

  p_block_id_->push_back(new_id);
}

bool RecordAccessProxy::next() {
  if (!cur_blk_) return false;
  data_ += record_len_;
  if (data_ - cur_blk_->val_ + record_len_ < Config::kBlockSize) return true;
  return nextBlock();
}

const Tuple &RecordAccessProxy::extractData() {
  assert(isCurrentSlotValid());
  char *tmp = data_ + 1;
  for (auto &v : tuple_.values) {
    switch (v.type) {
      case static_cast<SqlValueType>(SqlValueTypeBase::Integer):
        memcpy(&v.val.Integer, tmp, sizeof(v.val.Integer));
        tmp += sizeof(v.val.Integer);
        break;
      case static_cast<SqlValueType>(SqlValueTypeBase::Float):
        memcpy(&v.val.Float, tmp, sizeof(v.val.Float));
        tmp += sizeof(v.val.Float);
        break;
      default: {
        size_t len =
            v.type - static_cast<SqlValueType>(SqlValueTypeBase::String);
        memcpy(v.val.String, tmp, len);
        tmp += len;
        break;
      }
    }
  }
  return tuple_;
}

char *RecordAccessProxy::getRawData() { return data_ + 1; }

Position RecordAccessProxy::extractPostion() {
  Position pos;
  pos.block_id = p_block_id_->data()[blk_idx_];
  pos.offset = data_ - cur_blk_->val_ + 1 /* skip the 1-byte tag */;
  return pos;
}

void RecordAccessProxy::deleteRecord() {
  cur_blk_->dirty_ = true;
  *data_ = 0;
}

void RecordAccessProxy::modifyData(const Tuple &tuple) {
  cur_blk_->dirty_ = true;
  *data_ = 1;
  char *tmp = data_ + 1;
  for (auto &v : tuple.values) {
    switch (v.type) {
      case static_cast<SqlValueType>(SqlValueTypeBase::Integer):
        memcpy(tmp, &v.val.Integer, sizeof(v.val.Integer));
        tmp += sizeof(v.val.Integer);
        break;
      case static_cast<SqlValueType>(SqlValueTypeBase::Float):
        memcpy(tmp, &v.val.Float, sizeof(v.val.Float));
        tmp += sizeof(v.val.Float);
        break;
      default: {
        size_t len =
            v.type - static_cast<SqlValueType>(SqlValueTypeBase::String);
        memcpy(tmp, v.val.String, len);
        tmp += len;
        break;
      }
    }
  }
}

RecordManager::RecordManager() {
  ifstream is(Config::kRecordFileName, std::ios::binary);
  if (!is) {
    cerr << "Couldn't find the record file, assuming it is the first "
            "time of running MiniSQL."
         << endl;
    return;
  }
  size_t table_count;
  is.read(reinterpret_cast<char *>(&table_count), sizeof(table_count));
  for (size_t i = 0; i < table_count; ++i) {
    size_t table_name_size;
    size_t block_count;
    string table_name;
    vector<size_t> blks;

    is.read(reinterpret_cast<char *>(&table_name_size),
            sizeof(table_name_size));
    table_name.resize(table_name_size);
    is.read(table_name.data(), table_name_size);

    is.read(reinterpret_cast<char *>(&block_count), sizeof(block_count));
    for (size_t j = 0; j < block_count; ++j) {
      size_t block_id;
      is.read(reinterpret_cast<char *>(&block_id), sizeof(block_id));
      blks.push_back(block_id);
    }
    table_blocks[table_name] = blks;
  }
}

RecordManager::~RecordManager() {
  ofstream os(Config::kRecordFileName, std::ios::binary);

  size_t table_count = table_blocks.size();
  os.write(reinterpret_cast<char *>(&table_count), sizeof(table_count));

  for (const auto &kv : table_blocks) {
    auto &table_name = kv.first;
    auto table_name_size = table_name.size();
    auto &blks = kv.second;
    auto block_count = blks.size();

    os.write(reinterpret_cast<char *>(&table_name_size),
             sizeof(table_name_size));
    os.write(table_name.data(), table_name_size);

    os.write(reinterpret_cast<char *>(&block_count), sizeof(block_count));
    for (size_t j = 0; j < block_count; ++j) {
      os.write(reinterpret_cast<const char *>(&blks[j]), sizeof(blks[j]));
    }
  }
}

bool RecordManager::checkAttributeUnique(const Table &table,
                                         vector<size_t> &blks, const char *v,
                                         size_t len, size_t offset) {
  RecordAccessProxy rap(&blks, &table, 0);
  do {
    if (!rap.isCurrentSlotValid()) continue;
    auto data = rap.getRawData();
    if (memcmp(data + offset, v, len) == 0) return false;
  } while (rap.next());
  return true;
}

bool RecordManager::createTable(const Table &table) {
  if (table_blocks.contains(table.table_name)) {
    cerr << "such a table already exists" << endl;
    return false;
  }
  RecordAccessProxy rap(&table_blocks[table.table_name], &table, 0);
  table_blocks.insert({table.table_name, {}});
  table_current.emplace(table.table_name, rap);
  table_current[table.table_name].newBlock();
  return true;
}

bool RecordManager::dropTable(const Table &table) {
  if (!table_blocks.contains(table.table_name)) {
    cerr << "such a table doesn't exist" << endl;
    return false;
  }
  for (auto &id : table_blocks[table.table_name]) {
    auto p = buffer_manager.Read(id);
    memset(p->val_, 0, Config::kBlockSize);
    p->dirty_ = true;
    p->pin_ = false;
  }
  table_blocks.erase(table.table_name);
  table_current.erase(table.table_name);
  return true;
}

void RecordManager::checkConditionValid(const Table &table,
                                        const vector<Condition> &conds) {
  for (auto &cond : conds)
    if (!table.attributes.contains(cond.attribute)) {
      std::cerr << "no such an attribute `" ANSI_COLOR_RED << cond.attribute
                << ANSI_COLOR_RESET " `referenced in condition" << std::endl;
      throw invalid_ident("invalid attribute name");
    }
}

void RecordManager::checkTableName(const Table &table) {
  if (!table_blocks.contains(table.table_name)) {
    std::cerr << "such a table doesn't exist" << std::endl;
    throw invalid_ident("table not found");
  }
}

vector<tuple<Operator, SqlValue, size_t>> RecordManager::convertConditions(
    const Table &table, const vector<Condition> conds) {
  vector<tuple<Operator, SqlValue, size_t>> res;
  for (auto &cond : conds) {
    auto &[_1, _2, _3, offset] = table.attributes.find(cond.attribute)->second;
    res.push_back({cond.op, cond.val, offset});
  }
  return res;
}

bool RecordManager::checkRecordSatisfyCondition(
    const vector<tuple<Operator, SqlValue, size_t>> &conds, char *record) {
  for (auto &[op, val, offset] : conds) {
    if (!rawCompare(op, val, offset, record)) return false;
  }
  return true;
}

bool RecordManager::rawCompare(Operator op, SqlValue val, size_t offset,
                               char *record) {
  switch (val.type) {
    case static_cast<SqlValueType>(SqlValueTypeBase::Integer): {
      int record_num;
      memcpy(&record_num, record + offset, sizeof(record_num));
      switch (op) {
        case Operator::GT:
          return record_num > val.val.Integer;
          break;
        case Operator::GE:
          return record_num >= val.val.Integer;
          break;
        case Operator::LT:
          return record_num < val.val.Integer;
          break;
        case Operator::LE:
          return record_num <= val.val.Integer;
          break;
        case Operator::EQ:
          return record_num == val.val.Integer;
          break;
        case Operator::NE:
          return record_num != val.val.Integer;
          break;
        default:
          assert(0);
      }
      break;
    }
    case static_cast<SqlValueType>(SqlValueTypeBase::Float): {
      float record_num;
      memcpy(&record_num, record + offset, sizeof(record_num));
      switch (op) {
        case Operator::GT:
          return record_num > val.val.Float;
          break;
        case Operator::GE:
          return record_num >= val.val.Float;
          break;
        case Operator::LT:
          return record_num < val.val.Float;
          break;
        case Operator::LE:
          return record_num <= val.val.Float;
          break;
        case Operator::EQ:
          return record_num == val.val.Float;
          break;
        case Operator::NE:
          return record_num != val.val.Float;
          break;
        default:
          assert(0);
      }
      break;
    }
    default: {
      size_t len =
          val.type - static_cast<SqlValueType>(SqlValueTypeBase::String);
      char buf[Config::kMaxStringLength] = {0};
      memcpy(buf, record + offset, len);
      switch (op) {
        case Operator::GT:
          return strncmp(buf, val.val.String, len) > 0;
          break;
        case Operator::GE:
          return strncmp(buf, val.val.String, len) >= 0;
          break;
        case Operator::LT:
          return strncmp(buf, val.val.String, len) < 0;
          break;
        case Operator::LE:
          return strncmp(buf, val.val.String, len) <= 0;
          break;
        case Operator::EQ:
          return strncmp(buf, val.val.String, len) == 0;
          break;
        case Operator::NE:
          return strncmp(buf, val.val.String, len) != 0;
          break;
        default:
          assert(0);
      }
      break;
    }
  }
  return false;
}

Position RecordManager::insertRecord(const Table &table, const Tuple &tuple) {
  checkTableName(table);
  if (!table_current.contains(table.table_name))
    table_current[table.table_name] =
        RecordAccessProxy(&table_blocks[table.table_name], &table, 0);
  auto &access = table_current[table.table_name];
  while (access.isCurrentSlotValid()) {
    if (!access.next()) {
      access.newBlock();
    }
  }
  access.modifyData(tuple);
  return access.extractPostion();
}

Position RecordManager::insertRecordUnique(
    const Table &table, const Tuple &tp,
    const vector<tuple<const char *, size_t, size_t>> &unique) {
  if (!table_current.contains(table.table_name))
    table_current[table.table_name] =
        RecordAccessProxy(&table_blocks[table.table_name], &table, 0);
  auto &access = table_current[table.table_name];
  for (auto &u : unique) {
    auto &[p, len, offset] = u;
    if (!checkAttributeUnique(table, *access.p_block_id_, p, len, offset)) {
      cerr << "the record is not unique" << endl;
      throw invalid_value("record duplicate");
    }
  }
  while (access.isCurrentSlotValid()) {
    if (!access.next()) {
      access.newBlock();
    }
  }
  access.modifyData(tp);
  return access.extractPostion();
}

vector<Tuple> RecordManager::selectRecord(const Table &table,
                                          const vector<Condition> &conds) {
  vector<Tuple> res;
  checkTableName(table);
  checkConditionValid(table, conds);
  RecordAccessProxy rap(&table_blocks[table.table_name], &table, 0);
  auto conds_ = convertConditions(table, conds);
  do {
    if (!rap.isCurrentSlotValid()) continue;
    if (checkRecordSatisfyCondition(conds_, rap.getRawData()))
      res.push_back(rap.extractData());
  } while (rap.next());
  return res;
}

vector<Tuple> RecordManager::selectAllRecords(const Table &table) {
  vector<Tuple> res;
  checkTableName(table);
  RecordAccessProxy rap(&table_blocks[table.table_name], &table, 0);
  do {
    if (!rap.isCurrentSlotValid()) continue;
    res.push_back(rap.extractData());
  } while (rap.next());
  return res;
}

size_t RecordManager::deleteRecord(const Table &table,
                                   const vector<Condition> &conds) {
  size_t n = 0;
  checkTableName(table);
  checkConditionValid(table, conds);
  RecordAccessProxy rap(&table_blocks[table.table_name], &table, 0);
  auto conds_ = convertConditions(table, conds);
  do {
    if (!rap.isCurrentSlotValid()) continue;
    if (checkRecordSatisfyCondition(conds_, rap.getRawData())) {
      rap.deleteRecord();
      n++;
    }
  } while (rap.next());
  return n;
}

size_t RecordManager::deleteAllRecords(const Table &table) {
  size_t n = 0;
  checkTableName(table);
  RecordAccessProxy rap(&table_blocks[table.table_name], &table, 0);
  do {
    if (!rap.isCurrentSlotValid()) continue;
    rap.deleteRecord();
    n++;
  } while (rap.next());
  return n;
}

RecordAccessProxy RecordManager::getIterator(const Table &table) {
  RecordAccessProxy rap(&table_blocks[table.table_name], &table, 0);
  return rap;
}

RecordManager record_manager;
