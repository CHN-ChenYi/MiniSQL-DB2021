#pragma once

#include "DataStructure.hpp"

struct RecordBlock : public Block {};

class RecordManager {
  struct RecordIterator {
    Position pos;
    Tuple value;

    /**
     * @brief set the iterator to the start of a specified table
     *
     * @param table_name the name of the table
     * @return false is the table is empty
     */
    bool Init(const string &table_name) {}

    /**
     * @brief move the iterator to the next record
     *
     * @return false if reach the end of the table
     */
    bool Next() {}
  };

 public:
  RecordIterator record_iterator_;
};

extern RecordManager record_manager;
