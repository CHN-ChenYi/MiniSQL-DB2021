#pragma once

#include <cassert>
#include <climits>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <map>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "BufferManager.hpp"
#include "DataStructure.hpp"

struct RecordBlock : public Block {
  /* Record block layout
     +--------------------------------------+
     |           records         | metadata |
     +--------------------------------------+
  */

  struct MetaData {
    unsigned record_len;
    unsigned record_end;
    void setRecordLength(const Table &table) {
      
    }
  };

  MetaData getMetaData() {
    MetaData md;
    memcpy(&md, val_ + Config::kBlockSize - sizeof(MetaData), sizeof(MetaData));
    return md;
  }

  void setMetaData(const MetaData& md) {
    memcpy(val_ + Config::kBlockSize - sizeof(MetaData), &md, sizeof(MetaData));
  }
};

class RecordManager {
  inline static const string kRecordFileName = "Record.data";

  struct RecordIterator;
  friend bool operator==(const RecordIterator& a, const RecordIterator& b);
  friend bool operator!=(const RecordIterator& a, const RecordIterator& b);

  struct RecordAccessProxy {
    vector<size_t>* p_vec_blk_id;
    size_t blk_idx;
    RecordBlock* cur_blk;
    char* p;
    RecordBlock::MetaData cur_md;

    RecordAccessProxy(vector<size_t>* p_vec_blk_id, size_t blk_idx) {
      if (p_vec_blk_id->empty()) return;
      cur_blk = dynamic_cast<RecordBlock*>(
          buffer_manager.Read((*p_vec_blk_id)[blk_idx]));
      cur_md = cur_blk->getMetaData();
      p = cur_blk->val_;
    }

    bool nextBlock() {
      if (blk_idx + 1 < p_vec_blk_id->size()) {
        ++blk_idx;
      } else {
        return false;
      }
      cur_blk = dynamic_cast<RecordBlock*>(
          buffer_manager.Read((*p_vec_blk_id)[blk_idx]));
      cur_md = cur_blk->getMetaData();
      p = cur_blk->val_;

      if (cur_md.record_end == 0) return false;
      if (*p) return true;
      return next();
    }

    bool next() {
      p += cur_md.record_len;
      if (*p) return true;
      for (; p - cur_blk->val_ < cur_md.record_end && !*p;
           p += cur_md.record_len)
        ;
      if (p - cur_blk->val_ < cur_md.record_end) return true;
      return nextBlock();
    }

    void extractData(Tuple& tuple) {
      assert(*p);
      char* tmp = p + 1;
      for (auto& v : tuple.values) {
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
    }

    void extractPostion(Position& pos) {
      pos.block_id = p_vec_blk_id->data()[blk_idx];
      pos.offset = p - cur_blk->val_;
    }

    void deleteRecord() {
      cur_blk->dirty_ = true;
      *p = 0;
      if (p + cur_md.record_len == cur_blk->val_ + cur_md.record_end) {
        for (char* tmp = p; tmp >= cur_blk->val_ && !*tmp;
             tmp -= cur_md.record_len, cur_md.record_end -= cur_md.record_len)
          ;
      }
    }

    void modifyData(const Tuple& tuple) {
      cur_blk->dirty_ = true;
      *p = 1;
      char* tmp = p + 1;
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

  struct RecordIterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = ptrdiff_t;
    using value_type = RecordAccessProxy;
    using pointer = value_type*;
    using reference = value_type&;
    using iterator_type = RecordIterator;

    RecordIterator(bool state, vector<size_t>* p_blks, size_t blk_idx)
        : m(p_blks, blk_idx), valid(state) {}

    reference operator*() { return m; }
    pointer operator->() { return &m; }

    RecordIterator& operator++() {
      valid = m.next();
      return *this;
    }

    RecordIterator operator++(int) {
      RecordIterator tmp = *this;
      ++(*this);
      return tmp;
    }

    friend bool operator==(const RecordIterator& a, const RecordIterator& b) {
      if (a.valid != b.valid) {
        return false;
      }
      if (a.valid == false) {
        return true;
      }
      return memcmp(&a.m, &b.m, sizeof(RecordAccessProxy)) == 0;
    };

    friend bool operator!=(const RecordIterator& a, const RecordIterator& b) {
      if (a.valid != b.valid) {
        return true;
      }
      if (a.valid == false) {
        return false;
      }
      return memcmp(&a.m, &b.m, sizeof(RecordAccessProxy)) != 0;
    };

   private:
    value_type m;
    bool valid;
  };

  class TableProxy {
   public:
    using iterator_type = RecordIterator;
    iterator_type begin() { return RecordIterator(true, p_blks, 0); }
    iterator_type end() { return RecordIterator(false, p_blks, 0); }
    TableProxy(vector<size_t>* p, const Table* info)
        : p_table_info(info), p_blks(p) {}

   private:
    const Table* p_table_info;
    vector<size_t>* p_blks;
  };

  map<std::string, vector<size_t>> table_blocks;

  size_t nextAvailingBlock();

 public:
  RecordManager();
  ~RecordManager();
  bool createTable(const Table& table);
  bool dropTable(const Table& table);
  TableProxy operator[](const Table& table_name);
};

extern RecordManager record_manager;
