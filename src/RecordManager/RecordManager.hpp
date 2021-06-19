#pragma once

#include <cassert>
#include <cstring>
#include <exception>
#include <functional>
#include <map>
#include <string>
#include <string_view>

#include "BufferManager.hpp"
#include "DataStructure.hpp"

struct RecordBlock : public Block {};

class RecordManager {
  inline static const string kRecordFileName = "Record.data";

  struct RecordIterator;
  friend bool operator==(const RecordIterator& a, const RecordIterator& b);
  friend bool operator!=(const RecordIterator& a, const RecordIterator& b);

  struct RecordAccessProxy {
    vector<size_t>* p_blks;
    Position pos;
    void next();
  };

  struct RecordIterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = ptrdiff_t;
    using value_type = RecordAccessProxy;
    using pointer = value_type*;
    using reference = value_type&;
    using iterator_type = RecordIterator;

    RecordIterator(vector<size_t>* p_blks, Position pos) : m{p_blks, pos} {}

    reference operator*() { return m; }
    pointer operator->() { return &m; }

    RecordIterator& operator++() {
      m.next();
      return *this;
    }

    RecordIterator operator++(int) {
      RecordIterator tmp = *this;
      ++(*this);
      return tmp;
    }

    friend bool operator==(const RecordIterator& a, const RecordIterator& b) {
      return memcmp(&a.m, &b.m, sizeof(RecordAccessProxy)) == 0;
    };

    friend bool operator!=(const RecordIterator& a, const RecordIterator& b) {
      return memcmp(&a.m, &b.m, sizeof(RecordAccessProxy)) != 0;
    };

   private:
    value_type m;
  };

  class TableProxy {
   public:
    using iterator_type = RecordIterator;
    iterator_type begin() {
      assert(p_blks->size() > 0);
      auto first_blk = p_blks->front();
      return RecordIterator(p_blks, {first_blk, 0});
    }
    iterator_type end() {
      return RecordIterator(p_blks,
                            {static_cast<size_t>(-1), static_cast<size_t>(-1)});
    }
    TableProxy(vector<size_t>* p) : p_blks(p) {}

   private:
    vector<size_t>* p_blks;
  };

  map<std::string, vector<size_t>> table_blocks;

 public:
  RecordManager();
  ~RecordManager();
  bool createTable(const std::string& table_name);
  bool dropTable(const std::string& table_name);
  bool deleteTable(const std::string& table_name);
  TableProxy operator[](const std::string& table_name);
};

extern RecordManager record_manager;
