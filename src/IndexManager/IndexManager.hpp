#pragma once

#include <map>
#include <unordered_map>

#include "DataStructure.hpp"
#include "CatalogManager.hpp"
#include "BufferManager.hpp"

#include <unordered_map>
using std::unordered_map;

struct IndexBlock : public Block {};

class IndexManager {
    inline static const string kIndexFileName = "Index.data";
    unordered_map<std::string, size_t> index_blocks;

 public:
  /**
   * @brief Construct a new Index Manager object. Open the file.
   */
    IndexManager();

  /**
   * @brief Destroy the Index Manager object. Write back to the file.
   */
    ~IndexManager();

  /**
   * @brief Create a new index
   *
   * @param table the table on which we build index
   * @param index_name the name of the index itself
   * @param column the key value used by the index
   */
    bool CreateIndex(const Table &table, const string &index_name,
                 const string &column);

  /**
   * @brief delete an index
   *
   * @param table the table with the index to be droped
   * @param index_name the name of the index to be deleted
   */
    bool DropIndex(const Table &table, const string &index_name);

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
                             const vector<Condition> &conditions) ;
};

extern IndexManager index_manager;

// B+ 树的 key 是 attribute 的值，value 是 Position
