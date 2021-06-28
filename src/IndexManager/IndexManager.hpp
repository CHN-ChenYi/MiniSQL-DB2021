#pragma once

//#define _indexDEBUG

#include <iostream>
#include <limits>
#include <map>
#include <unordered_map>
#include <vector>

#include "BufferManager.hpp"
#include "CatalogManager.hpp"
#include "DataStructure.hpp"

using std::cerr;
using std::cout;
using std::endl;
using std::unordered_map;

#define NEWBLOCK std::numeric_limits<size_t>::max()
#define ROOT std::numeric_limits<size_t>::max()
#define NULLBLOCK std::numeric_limits<size_t>::max()

enum struct NodeType { Root, nonLeafNode, LeafNode };

struct IndexBlock : public Block {};

struct bplusNode {
  vector<SqlValue> elem;
  vector<Position> pos;
  Position parent;
  NodeType type;
};

struct getBplus {
  size_t block_id_;
  int element_num;
  struct bplusNode Node_;
  char *data_;
  IndexBlock *cur_blk_;
  SqlValueType value_type_;
  string index_name;
  size_t root_id;

  getBplus(size_t blk_id, SqlValueType p, string n);

  void releaseBlock();

  const bplusNode &getNodeInfo();

  void newNode(NodeType t);

  void switchToBlock(size_t blk_id);

  void updateBlock();

  /**
   * @brief insert ONE value into index
   * the cur_block_ should be set to ROOT before insert()
   * */
  void insert(SqlValue val, Position pos);

  void splitLeaf();

  /**
   * the cur_Node_ should be set to newNode
   * */
  void insert_in_parent(size_t old_id, size_t new_id, SqlValue k);

  size_t findLeaf(SqlValue val);

  Position find(SqlValue val);

  Position findMin();

  /**
   * @brief delete ONE key in the index at one time
   * please set cur_blk_ to the rootNode before erase
   * */
  void erase(SqlValue val);

  void delete_entry(size_t N, SqlValue K);

  void deleteIndexRoot();
};

class IndexManager {
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
   * @brief Create a new index of primary key
   *
   * @param table the table on which we build index
   * @param index_name the name of the index itself
   * @param column the key value used by the index
   */
  bool PrimaryKeyIndex(const Table &table);

  /**
   * @brief delete an index
   *
   * @param table the table with the index to be droped
   * @param index_name the name of the index to be deleted
   */
  bool DropIndex(const string &index_name);

  /**
   * @brief delete all the indexes in a table
   *
   * @param table the table with the index to be droped
   */
  void DropAllIndex(const Table &table);

  /**
   * @brief insert a new key into an index
   *
   * @param table the table with the element to be insert
   * @param attributes the key value
   * @param pos the position of the data
   */
  bool InsertKey(const Table &table, const Tuple &tuple, Position &pos);

  /**
   * @brief delete a key from an index
   *
   * @param table the table with the element to be delete
   * @param attributes the key value
   */
  bool RemoveKey(const Table &table, const Tuple &tuple);

  /**
   * @brief check whether the index can be used in these conditions
   * */
  bool checkCondition(const Table &table, const vector<Condition> &condition);

  bool judgeCondition(string attribute, const SqlValue &val,
                      Condition &condition);

  bool judgeConditions(const Table &table, Position pos,
                       const vector<Condition> &conditions);

  Position posNext(Position pos);

  /**
   * @brief get the record data at pos
   * */
  const Tuple extractData(const Table &table, const Position &pos);

  /**
   * @brief Select specified records by an index
   *
   * @param index the name of the index
   * @param conditions the specified conditions (must be based on the index key)
   * @return selected records
   */
  vector<Tuple> SelectRecord(const Table &table,
                             const vector<Condition> &conditions);
};

extern IndexManager index_manager;

// B+ 树的 key 是 attribute 的值，value 是 Position
