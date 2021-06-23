#pragma once

#include "DataStructure.hpp"
#include "CatalogManager.hpp"
#include "BufferManager.hpp"

struct IndexBlock : public Block {};

class IndexManager {
 public:
    IndexManager();
    ~IndexManager();
  /**
   * @brief Create a new index
   *
   * @param table_name the name of the table on which we build index
   * @param index_name the name of the index itself
   * @param column the key value used by the index
   */
    bool CreateIndex(const string &table_name, const string &index_name,
                 const string &column);
  /**
   * @brief delete an index
   *
   * @param index_name the name of the index to be deleted
   */
    bool DeleteIndex(const string &index_name);
  /**
   * @brief insert a new key into an index
   *
   * @param index_name the name of the index
   * @param attributes the key value
   * @param pos the position of the data
   */
    bool InsertKey(const string &index_name,
                 tuple<string, SqlValueType, SpecialAttribute> &attributes,
                 Position &pos);
  /**
   * @brief delete a key from an index
   *
   * @param index_name the name of the index
   * @param attributes the key value
   */
    bool RemoveKey(const string &index_name,
                    tuple<string, SqlValueType, SpecialAttribute> &attributes);
  /**
   * @brief Select specified records by an index
   *
   * @param index the name of the index
   * @param conditions the specified conditions (must be based on the index key)
   * @return selected records
   */
    vector<Tuple> SelectRecord(const string &index,
                             const vector<Condition> &conditions) {}
};

extern IndexManager index_manager;

// B+ 树的 key 是 attribute 的值，value 是 Position
