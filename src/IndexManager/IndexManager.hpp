#pragma once

#include <map>
#include <unordered_map>
#include <vector>

#include "DataStructure.hpp"
#include "CatalogManager.hpp"
#include "BufferManager.hpp"

using std::unordered_map;

#define NEWBLOCK Config::kMaxBlockNum+5
#define ROOT Config::kMaxBlockNum+5

enum struct NodeType { Root, nonLeafNode, LeafNode };

struct IndexBlock : public Block {};

struct bplusNode{
    vector<SqlValue> elem;
    vector<Position> pos;
    Position parent;
    NodeType type;
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
            getNodeInfo();
        }

        void releaseBlock(){
            cur_blk_ ->pin_ = false;
        }

        const bplusNode& getNodeInfo(){
            Node_.elem.clear();
            Node_.pos.clear();
            char *tmp = data_;
            Position pos;
            SqlValue v;
            memcpy(&element_num, tmp, sizeof(element_num));
            tmp += sizeof(element_num);
            for(size_t i=0; i<=element_num; i++){
                memcpy(&pos, tmp, sizeof(pos));
                tmp += sizeof(pos);
                Node_.pos.push_back(pos);
            }
            for(size_t i=0; i<element_num; i++){
                memcpy(&v.val, tmp, sizeof(v.val));
                tmp += sizeof(v.val);
                v.type = value_type_;
                Node_.elem.push_back(v);
            }
            memcpy(&Node_.parent, tmp, sizeof(Node_.parent));
            tmp += sizeof(Node_.parent);
            memcpy(&Node_.type, tmp, sizeof(Node_.type));
            return Node_;
        }

        void newNode(){
            size_t new_id = buffer_manager.NextId();
            if(cur_blk_){
                releaseBlock();
            }
            cur_blk_ = static_cast<IndexBlock*>(buffer_manager.Read(new_id));
            cur_blk_->pin_ = true;
            cur_blk_->dirty_ = true;
            block_id_ = new_id;
            data_ = cur_blk_->val_;
            memset(data_, 0, Config::kBlockSize);
            *data_ = 0;
            element_num = 0;
            Node_.elem.clear();
            Node_.pos.clear();
            Node_.parent.block_id = NEWBLOCK;
        }

        void switchToBlock(size_t blk_id){
            if(cur_blk_){
                releaseBlock();
            }
            cur_blk_ = static_cast<IndexBlock*>(buffer_manager.Read(blk_id));
            cur_blk_->pin_ = true;
            block_id_ = blk_id;
            data_ = cur_blk_->val_;
            getNodeInfo();
        }

        void updateBlock(){
            cur_blk_->dirty_ = true;
            char* tmp = data_;
            memcpy(tmp, &element_num, sizeof(element_num));
            tmp += sizeof(element_num);
            for(auto& v : Node_.pos){
                memcpy(tmp, &v, sizeof(v));
                tmp += sizeof(v);
            }
            for(auto& v : Node_.elem){
                memcpy(tmp, &v.val, sizeof(v.val));
                tmp += sizeof(v.val);
            }
            memcpy(tmp, &Node_.parent, sizeof(Node_.parent));
            tmp += sizeof(Node_.parent);
            memcpy(tmp, &Node_.type, sizeof(Node_.type));
        }

        /**
         * @brief insert ONE value into index
         * the cur_block_ should be set to ROOT before insert()
         * */
        void insert(SqlValue val, Position pos){
            int maxElem = Config::kNodeCapacity - 1;
            //find the leafNode
            int i = 0;
            while(Node_.type != NodeType::LeafNode){
                size_t Next_blk;
                while(i < element_num){
                    if(Node_.elem[i].Compare(Operator::GT,val)) break;
                    i++;
                }
                Next_blk = Node_.pos[i].block_id;
                switchToBlock(Next_blk);
            }
            //reach the leafNode
            //find insert position
            for(i=0; i<element_num; i++){
                if(Node_.elem[i].Compare(Operator::GT,val)) break;
            }
            //insert into "Node_"
            auto e = Node_.elem.begin();
            Node_.elem.insert(e+i, val);
            auto p = Node_.pos.begin();
            Node_.pos.insert(p+i, pos);
            element_num++;

            //insert into real Index
            if (element_num <= maxElem)
            {
                updateBlock();
            }
            //the leafnode needs to split
            else
            {   
                splitLeaf();
            }
        }

        void splitLeaf(){
            vector<SqlValue> temp_val;
            vector<Position> temp_pos;
            Position temp_parent = Node_.parent;
            NodeType temp_type = Node_.type;
            temp_val.insert(temp_val.begin(), Node_.elem.begin(), Node_.elem.end());
            temp_pos.insert(temp_pos.begin(), Node_.pos.begin(), Node_.pos.end());

            Node_.elem.clear();
            Node_.pos.clear();
            int left_ = element_num / 2;
            int right_ = element_num - left_;
            Node_.elem.insert(Node_.elem.begin(), temp_val.begin(), temp_val.begin()+left_);
            Node_.pos.insert(Node_.pos.begin(), temp_pos.begin(), temp_pos.begin()+left_);
            size_t blk_id_ = buffer_manager.NextId();
            Position p = {blk_id_, 0};
            Node_.pos.push_back(p);
            element_num = left_;
            blk_id_ = block_id_;

            updateBlock();
            newNode();
            Node_.elem.insert(Node_.elem.begin(), temp_val.begin()+left_, temp_val.end());
            Node_.pos.insert(Node_.pos.begin(), temp_pos.begin()+left_, temp_pos.end());
            Node_.parent = temp_parent;
            Node_.type = temp_type;
            element_num = right_;
            updateBlock();

            insert_in_parent(blk_id_, block_id_, Node_.elem[0]);
        }

        /**
         * the cur_Node_ should be set to 
         * */
        void insert_in_parent(size_t old_id, size_t new_id, SqlValue k){
            Position new_node = {new_id, 0};
            Position old_node = {old_id, 0};
            size_t blk_id_ = Node_.parent.block_id;

            if(blk_id_ == ROOT){
                newNode();
                Node_.pos.push_back(old_node);
                Node_.elem.push_back(k);
                Node_.pos.push_back(new_node);
                element_num = 1;
                Node_.parent.block_id = ROOT;
                Node_.type = NodeType::Root;
                updateBlock();
                blk_id_ = block_id_;

                switchToBlock(old_id);
                Node_.parent.block_id = blk_id_;
                if(Node_.type == NodeType::Root) Node_.type = NodeType::nonLeafNode;
                updateBlock();

                switchToBlock(new_id);
                Node_.parent.block_id = blk_id_;
                if(Node_.type == NodeType::Root) Node_.type = NodeType::nonLeafNode;
                updateBlock();
            }
            else{
                switchToBlock(blk_id_);
                int i;
                for(i=0; i<element_num; i++){
                    if(Node_.elem[i].Compare(Operator::GT,k)) break;
                }
                //insert into "Node_"
                auto e = Node_.elem.begin();
                Node_.elem.insert(e+i, k);
                auto p = Node_.pos.begin();
                Node_.pos.insert(p+i, new_node);
                element_num++;

                if(element_num <= Config::kNodeCapacity - 1){
                    updateBlock();
                }
                else{  //split the parent node
                    vector<SqlValue> temp_val;
                    vector<Position> temp_pos;
                    Position temp_parent = Node_.parent;
                    NodeType temp_type = Node_.type;
                    temp_val.insert(temp_val.begin(), Node_.elem.begin(), Node_.elem.end());
                    temp_pos.insert(temp_pos.begin(), Node_.pos.begin(), Node_.pos.end());

                    Node_.elem.clear();
                    Node_.pos.clear();
                    int left_ = element_num / 2;
                    int right_ = element_num - left_ - 1;
                    Node_.elem.insert(Node_.elem.begin(), temp_val.begin(), temp_val.begin()+left_);
                    Node_.pos.insert(Node_.pos.begin(), temp_pos.begin(), temp_pos.begin()+left_+1);
                    element_num = left_;
                    blk_id_ = block_id_;

                    updateBlock();
                    newNode();
                    Node_.elem.insert(Node_.elem.begin(), temp_val.begin()+left_+1, temp_val.end());
                    Node_.pos.insert(Node_.pos.begin(), temp_pos.begin()+left_+1, temp_pos.end());
                    Node_.parent = temp_parent;
                    Node_.type = temp_type;
                    element_num = right_;
                    updateBlock();

                    size_t t = block_id_;
                    int j = element_num + 1;
                    for(i=0;i<j;i++){
                        switchToBlock(Node_.pos[i].block_id);
                        Node_.parent.block_id = t;
                        switchToBlock(t);
                    }

                    insert_in_parent(blk_id_, t, temp_val[left_]);
                }
            }
        }

        Position* find(SqlValue val){
            int i = 0;
            while(Node_.type != NodeType::LeafNode){
                size_t Next_blk;
                while(i < element_num){
                    if(Node_.elem[i].Compare(Operator::GT,val)) break;
                    i++;
                }
                Next_blk = Node_.pos[i].block_id;
                switchToBlock(Next_blk);
            }
            i = 0;
            while(i < element_num){
                if(Node_.elem[i].Compare(Operator::EQ,val)) break;
                i++;
            }
            if(i == element_num) return nullptr;
            else return &Node_.pos[i];
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