#pragma once

#include "DataStructure.hpp"

struct IndexBlock : public Block {};

class IndexManager {
 public:
  /**
   * @brief Select specified records by an index
   *
   * @param table the name of the index
   * @param conditions the specified conditions (must be based on the index key)
   * @return selected records
   */
  vector<Tuple> SelectRecord(const string &index,
                             const vector<Condition> &conditions) {}
};

extern IndexManager index_manager;

// B+ 树的 key 是 attribute 的值，value 是 Position
