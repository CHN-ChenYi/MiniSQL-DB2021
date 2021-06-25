#pragma once

#include <map>
#include <unordered_map>

#include "DataStructure.hpp"
#include "CatalogManager.hpp"
#include "BufferManager.hpp"

#include <unordered_map>
using std::unordered_map;

#define NEWBLOCK Config::kMaxBlockNum+5

struct IndexBlock : public Block {};

struct bplusNode{
    map<SqlValue,Position> elem;
    Position largest;
    Position parent;
};

class IndexManager {
    inline static const string kIndexFileName = "Index.data";
    unordered_map<std::string, size_t> index_blocks;

    struct getBplus {
        size_t block_id_;
        size_t element_num;
        struct bplusNode Node_;
        char* data_;
        IndexBlock* cur_blk_;
        SqlValueType value_type_;

        getBplus(size_t blk_id, SqlValueType p)
            : block_id_(blk_id),
              value_type_(p)
        {
            if(block_id_== NEWBLOCK){
                cur_blk_ = nullptr;
                data_ = nullptr;
                return;
            }
            cur_blk_ = static_cast<IndexBlock*>(buffer_manager.Read(block_id_));
            cur_blk_->pin_ = true;
            data_ = cur_blk_ ->val_;
        }

        void releaseBlock(){
            cur_blk_ ->pin_ = false;
        }

        const bplusNode& getNodeInfo(){
            char *tmp = data_;
            Position pos;
            SqlValue v;
            memcpy(&element_num, tmp, sizeof(element_num));
            tmp += sizeof(element_num);
            for(size_t i=0; i<element_num; i++){
                memcpy(&pos, tmp, sizeof(pos));
                tmp += sizeof(Position);
                memcpy(&v.val, tmp, sizeof(v.val));
                tmp += sizeof(v.val);
                v.type = value_type_;
                Node_.elem.insert(std::pair<SqlValue, Position>(v, pos));
            }
            memcpy(&Node_.largest, tmp, sizeof(Node_.largest));
            tmp += sizeof(Node_.largest);
            memcpy(&Node_.parent, tmp, sizeof(Node_.parent));
            return Node_;
        }

        void newNode(){
            size_t new_id = buffer_manager.NextId();
            if(cur_blk_){
                cur_blk_->pin_ = false;
            }
            cur_blk_ = static_cast<IndexBlock*>(buffer_manager.Read(new_id));
            cur_blk_->pin_ = true;
            cur_blk_->dirty_ = true;
            block_id_ = new_id;
            data_ = cur_blk_->val_;
            memset(data_, 0, Config::kBlockSize);
        }

        void insert(SqlValue val, Position pos){
            //
            return;
        }

        void erase(SqlValue val){
            //
            return;
        }

        void deleteIndex(){
            //
            return;
        }
    };

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
    bool DropIndex(const Table &table, const string &index_name);

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
bool InsertKey(const Table &table,
                 const Tuple &tuple,
                 Position &pos);

  /**
   * @brief delete a key from an index
   *
   * @param table the table with the element to be delete
   * @param attributes the key value
   */
    bool RemoveKey(const Table &table,
                   const Tuple &tuple);

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