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
#include <vector>
#include <unordered_map>

#include "BufferManager.hpp"
#include "DataStructure.hpp"
#include "Interpreter.hpp"

using RecordBlock = Block;

class RecordManager {
  inline static const string kRecordFileName = "Record.data";

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
                      size_t blk_idx)
        : p_block_id_(block_id),
          p_table_(table),
          record_len_(table->getAttributeSize() + 1 /* tag size is 1*/),
          blk_idx_(blk_idx) {
      if (block_id->empty()) {
        cur_blk_ = nullptr;
        data_ = nullptr;
        return;
      }
      cur_blk_ = static_cast<RecordBlock*>(
          buffer_manager.Read(block_id->data()[blk_idx]));
      cur_blk_->pin_ = true;
      data_ = cur_blk_->val_;
      tuple_ = table->makeEmptyTuple();
    }

    bool isCurrentSlotValid() {
      if (data_)
        return *data_;
      else
        return false;
    }

    void releaseCurrentBlock() {
      if (cur_blk_) cur_blk_->pin_ = false;
    }

    bool nextBlock() {
      if (blk_idx_ + 1 < p_block_id_->size()) {
        ++blk_idx_;
      } else {
        return false;
      }
      cur_blk_->pin_ = false;
      cur_blk_ = static_cast<RecordBlock*>(
          buffer_manager.Read(p_block_id_->data()[blk_idx_]));
      cur_blk_->pin_ = true;
      data_ = cur_blk_->val_;

      return true;
    }

    void newBlock() {
      size_t new_id = buffer_manager.NextId();
      if (cur_blk_) {
        cur_blk_->pin_ = false;
        ++blk_idx_;
      }
      cur_blk_ = static_cast<RecordBlock*>(buffer_manager.Read(new_id));
      cur_blk_->pin_ = true;
      cur_blk_->dirty_ = true;
      data_ = cur_blk_->val_;
      memset(data_, 0, Config::kBlockSize);

      p_block_id_->push_back(new_id);
    }

    bool next() {
      if (!cur_blk_) return false;
      data_ += record_len_;
      if (data_ - cur_blk_->val_ + record_len_ < Config::kBlockSize)
        return true;
      return nextBlock();
    }

    const Tuple& extractData() {
      assert(isCurrentSlotValid());
      char* tmp = data_ + 1;
      for (auto& v : tuple_.values) {
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

    char* getRawData() { return data_ + 1; }

    Position extractPostion() {
      Position pos;
      pos.block_id = p_block_id_->data()[blk_idx_];
      pos.offset = data_ - cur_blk_->val_ + 1 /* skip the 1-byte tag */;
      return pos;
    }

    void deleteRecord() {
      cur_blk_->dirty_ = true;
      *data_ = 0;
    }

    void modifyData(const Tuple& tuple) {
      cur_blk_->dirty_ = true;
      *data_ = 1;
      char* tmp = data_ + 1;
      for (auto& v : tuple.values) {
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
  };

  void checkConditionValid(const Table& table, const vector<Condition>& conds) {
    for (auto& cond : conds)
      if (!table.attributes.contains(cond.attribute)) {
        std::cerr << "no such an attribute `" ANSI_COLOR_RED << cond.attribute
                  << ANSI_COLOR_RESET "`referenced in condition" << std::endl;
        throw syntax_error("invalid attribute name");
      }
  }

  void checkTableName(const Table& table) {
    if (!table_current.contains(table.table_name) ||
        !table_blocks.contains(table.table_name)) {
      std::cerr << "such a table doesn't exist" << std::endl;
      throw syntax_error("table not found");
    }
  }

  bool checkTupleSatisfyCondition() { return true; }

  bool rawCompare(Operator op, SqlValue val, size_t offset, char* record) {
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
      default:
        switch (op) {
          default:
            assert(0);
        }
        break;
    }
  }

  void deleteRecord(const Table& table, const vector<Condition>& conds) {
    checkTableName(table);
    checkConditionValid(table, conds);
    RecordAccessProxy rap(&table_blocks[table.table_name], &table, 0);
    do {
      if (!rap.isCurrentSlotValid()) continue;

    } while (rap.next());
  }

  vector<Tuple> selectRecord(const Table& table,
                             const vector<Condition>& conds) {
    vector<Tuple> res;
    checkTableName(table);
    checkConditionValid(table, conds);
    RecordAccessProxy rap(&table_blocks[table.table_name], &table, 0);
    do {
      if (!rap.isCurrentSlotValid()) continue;

    } while (rap.next());
    return res;
  }

  unordered_map<std::string, vector<size_t>> table_blocks;
  unordered_map<std::string, RecordAccessProxy> table_current;

 public:
  RecordManager();
  ~RecordManager();
  bool createTable(const Table& table);
  bool dropTable(const Table& table);

  Position insertRecord(const Table& table, const Tuple& tuple) {
    checkTableName(table);
    auto& access = table_current[table.table_name];
    while (access.isCurrentSlotValid()) {
      if (!access.next()) {
        access.newBlock();
      }
    }
    access.modifyData(tuple);
    return access.extractPostion();
  }

  vector<Tuple> selectAllRecord(const Table& table) {
    vector<Tuple> res;
    checkTableName(table);
    RecordAccessProxy rap(&table_blocks[table.table_name], &table, 0);
    do {
      if (!rap.isCurrentSlotValid()) continue;
      res.push_back(rap.extractData());
    } while (rap.next());
    return res;
  }
};

extern RecordManager record_manager;
