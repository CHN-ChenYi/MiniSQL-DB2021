#pragma once

#include <cassert>
#include <climits>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <iostream>
#include <map>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "BufferManager.hpp"
#include "DataStructure.hpp"
#include "Interpreter.hpp"

using RecordBlock = Block;

struct RecordAccessProxy {
  vector<size_t>* p_block_id_;
  const Table* p_table_;
  size_t record_len_;
  size_t blk_idx_;
  RecordBlock* cur_blk_;
  char* data_;
  Tuple tuple_;

  RecordAccessProxy() = default;
  RecordAccessProxy(vector<size_t>* block_id, const Table* table,
                    size_t blk_idx);
  bool isCurrentSlotValid();
  void releaseCurrentBlock();
  bool nextBlock();
  void newBlock();
  bool next();
  const Tuple& extractData();
  static const Tuple& extractData(const char* data, Tuple& tuple);
  char* getRawData();
  Position extractPostion();
  void deleteRecord();
  void modifyData(const Tuple& tuple);
};

class RecordManager {
  unordered_map<std::string, vector<size_t>> table_blocks;
  unordered_map<std::string, RecordAccessProxy> table_current;

 private:
  void checkConditionValid(const Table& table, const vector<Condition>& conds);
  void checkTableName(const Table& table);
  vector<tuple<Operator, SqlValue, size_t>> convertConditions(
      const Table& table, const vector<Condition> conds);
  bool checkRecordSatisfyCondition(
      const vector<tuple<Operator, SqlValue, size_t>>& conds, char* record);
  bool rawCompare(Operator op, SqlValue val, size_t offset, char* record);
  bool checkAttributeUnique(const Table& table, vector<size_t>& blks,
                            const char* v, size_t len, size_t offset);

 public:
  RecordManager();
  ~RecordManager();
  bool createTable(const Table& table);
  bool dropTable(const Table& table);
  Position insertRecord(const Table& table, const Tuple& tuple);
  Position insertRecordUnique(
      const Table& table, const Tuple& tp,
      const vector<tuple<const char*, size_t, size_t>>& unique);
  vector<Tuple> selectAllRecords(const Table& table);
  vector<Tuple> selectRecord(const Table& table,
                             const vector<Condition>& conds);
  vector<Tuple> selectRecordFromPosition(const Table& table,
                                         const vector<Position>& pos,
                                         const vector<Condition>& conds);
  size_t deleteRecord(const Table& table, const vector<Condition>& conds);
  size_t deleteAllRecords(const Table& table);
  RecordAccessProxy getIterator(const Table& table);
};

extern RecordManager record_manager;
