#include "IndexManager.hpp"

#include <exception>
#include <fstream>
#include <iostream>
#include <string>
#include <map>

#include "BufferManager.hpp"
#include "CatalogManager.hpp"
#include "RecordManager.hpp"
#include "DataStructure.hpp"

using namespace std;
using std::cerr;
using std::endl;

getBplus::getBplus(size_t blk_id, SqlValueType p, string n)
        : block_id_(blk_id), value_type_(p), index_name(n) {
      if (block_id_ == NEWBLOCK) {
        cur_blk_ = nullptr;
        data_ = nullptr;
        return;
      }
      cur_blk_ = static_cast<IndexBlock *>(buffer_manager.Read(block_id_));
      cur_blk_->pin_ = true;
      data_ = cur_blk_->val_;
      getNodeInfo();
    }

void getBplus::releaseBlock() { if(cur_blk_) cur_blk_->pin_ = false; }

const bplusNode& getBplus::getNodeInfo() {
    Node_.elem.clear();
    Node_.pos.clear();
    char *tmp = data_;
    Position pos;
    SqlValue v;
    memcpy(&element_num, tmp, sizeof(element_num));
    tmp += sizeof(element_num);
    memcpy(&pos, tmp, sizeof(pos));
    tmp += sizeof(pos);
    Node_.pos.push_back(pos);
    if(element_num!=0){
    for (int i = 0; i < element_num; i++) {
        memcpy(&pos, tmp, sizeof(pos));
        tmp += sizeof(pos);
        Node_.pos.push_back(pos);
    }
    for (int i = 0; i < element_num; i++) {
        memcpy(&v.val, tmp, sizeof(v.val));
        tmp += sizeof(v.val);
        v.type = value_type_;
        Node_.elem.push_back(v);
    }
    }
    memcpy(&Node_.parent, tmp, sizeof(Node_.parent));
    tmp += sizeof(Node_.parent);
    memcpy(&Node_.type, tmp, sizeof(Node_.type));
    return Node_;
}

void getBplus::newNode(NodeType t) {
    size_t new_id = buffer_manager.NextId();
    if (cur_blk_) {
    releaseBlock();
    }
    cur_blk_ = static_cast<IndexBlock *>(buffer_manager.Read(new_id));
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
    Node_.type = t;
    updateBlock();
}

void getBplus::switchToBlock(size_t blk_id) {
    if (cur_blk_) {
    releaseBlock();
    }
    cur_blk_ = static_cast<IndexBlock *>(buffer_manager.Read(blk_id));
    cur_blk_->pin_ = true;
    block_id_ = blk_id;
    data_ = cur_blk_->val_;
    getNodeInfo();
}

void getBplus::updateBlock() {
    cur_blk_->dirty_ = true;
    char *tmp = data_;
    memcpy(tmp, &element_num, sizeof(element_num));
    tmp += sizeof(element_num);
    for (auto &v : Node_.pos) {
    memcpy(tmp, &v, sizeof(v));
    tmp += sizeof(v);
    }
    for (auto &v : Node_.elem) {
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
void getBplus::insert(SqlValue val, Position pos) {
    int maxElem = Config::kNodeCapacity - 1;
    // find the leafNode
    int i = 0;
    while (Node_.type != NodeType::LeafNode) {
    size_t Next_blk;
    while (i < element_num - 1) {
        if (Node_.elem[i].Compare(Operator::GT, val)) break;
        i++;
    }
    Next_blk = Node_.pos[i].block_id;
    switchToBlock(Next_blk);
    }
    // reach the leafNode
    // find insert position
    for (i = 0; i < element_num; i++) {
    if (Node_.elem[i].Compare(Operator::GT, val)) break;
    }
    // insert into "Node_"
    auto e = Node_.elem.begin();
    Node_.elem.insert(e + i, val);
    auto p = Node_.pos.begin();
    Node_.pos.insert(p + i, pos);
    element_num++;

    // insert into real Index
    if (element_num <= maxElem) {
    updateBlock();
    }
    // the leafnode needs to split
    else {
    splitLeaf();
    }
}

void getBplus::splitLeaf() {
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
    Node_.elem.insert(Node_.elem.begin(), temp_val.begin(),
                    temp_val.begin() + left_);
    Node_.pos.insert(Node_.pos.begin(), temp_pos.begin(),
                    temp_pos.begin() + left_);
    size_t blk_id_ = buffer_manager.NextId();
    Position p = {blk_id_, 0};
    Node_.pos.push_back(p);
    element_num = left_;
    blk_id_ = block_id_;

    updateBlock();
    newNode(Node_.type);
    Node_.elem.insert(Node_.elem.begin(), temp_val.begin() + left_,
                    temp_val.end());
    Node_.pos.insert(Node_.pos.begin(), temp_pos.begin() + left_,
                    temp_pos.end());
    Node_.parent = temp_parent;
    Node_.type = temp_type;
    element_num = right_;
    updateBlock();

    insert_in_parent(blk_id_, block_id_, Node_.elem[0]);
}

/**
 * the cur_Node_ should be set to
 * */
void getBplus::insert_in_parent(size_t old_id, size_t new_id, SqlValue k) {
    Position new_node = {new_id, 0};
    Position old_node = {old_id, 0};
    size_t blk_id_ = Node_.parent.block_id;

    if (blk_id_ == ROOT) {
    newNode(NodeType::Root);
    Node_.pos.push_back(old_node);
    Node_.elem.push_back(k);
    Node_.pos.push_back(new_node);
    element_num = 1;
    Node_.parent.block_id = ROOT;
    root_id = block_id_;
    updateBlock();
    blk_id_ = block_id_;

    switchToBlock(old_id);
    Node_.parent.block_id = blk_id_;
    if (Node_.type == NodeType::Root) Node_.type = NodeType::nonLeafNode;
    updateBlock();

    switchToBlock(new_id);
    Node_.parent.block_id = blk_id_;
    if (Node_.type == NodeType::Root) Node_.type = NodeType::nonLeafNode;
    updateBlock();
    } else {
    switchToBlock(blk_id_);
    int i;
    for (i = 0; i < element_num; i++) {
        if (Node_.elem[i].Compare(Operator::GT, k)) break;
    }
    // insert into "Node_"
    auto e = Node_.elem.begin();
    Node_.elem.insert(e + i, k);
    auto p = Node_.pos.begin();
    Node_.pos.insert(p + i, new_node);
    element_num++;

    if (element_num <= Config::kNodeCapacity - 1) {
        updateBlock();
    } else {  // split the parent node
        vector<SqlValue> temp_val;
        vector<Position> temp_pos;
        Position temp_parent = Node_.parent;
        NodeType temp_type = Node_.type;
        temp_val.insert(temp_val.begin(), Node_.elem.begin(),
                        Node_.elem.end());
        temp_pos.insert(temp_pos.begin(), Node_.pos.begin(), Node_.pos.end());

        Node_.elem.clear();
        Node_.pos.clear();
        int left_ = element_num / 2;
        int right_ = element_num - left_ - 1;
        Node_.elem.insert(Node_.elem.begin(), temp_val.begin(),
                        temp_val.begin() + left_);
        Node_.pos.insert(Node_.pos.begin(), temp_pos.begin(),
                        temp_pos.begin() + left_ + 1);
        element_num = left_;
        blk_id_ = block_id_;

        updateBlock();
        newNode(Node_.type);
        Node_.elem.insert(Node_.elem.begin(), temp_val.begin() + left_ + 1,
                        temp_val.end());
        Node_.pos.insert(Node_.pos.begin(), temp_pos.begin() + left_ + 1,
                        temp_pos.end());
        Node_.parent = temp_parent;
        Node_.type = temp_type;
        element_num = right_;
        updateBlock();

        size_t t = block_id_;
        int j = element_num + 1;
        for (i = 0; i < j; i++) {
        switchToBlock(Node_.pos[i].block_id);
        Node_.parent.block_id = t;
        updateBlock();
        switchToBlock(t);
        }

        insert_in_parent(blk_id_, t, temp_val[left_]);
    }
    }
}

Position* getBplus::find(SqlValue val) {
    int i = 0;
    while (Node_.type != NodeType::LeafNode) {
    size_t Next_blk;
    while (i < element_num) {
        if (Node_.elem[i].Compare(Operator::GT, val)) break;
        i++;
    }
    Next_blk = Node_.pos[i].block_id;
    switchToBlock(Next_blk);
    }
    i = 0;
    while (i < element_num) {
    if (Node_.elem[i].Compare(Operator::EQ, val)) break;
    i++;
    }
    if (i == element_num)
    return nullptr;
    else
    return &Node_.pos[i];
}

/**
 * @brief delete ONE key in the index at one time
 * please set cur_blk_ to the rootNode before erase
 * */
void getBplus::erase(SqlValue val) {
    //
    Position *D = find(val);
    if (!D) {
    cerr << "the value doesn't exist" << endl;
    return;
    }
    size_t targetLeaf = D->block_id;

    return;
}

void getBplus::deleteIndexRoot() {
    //
    memset(cur_blk_->val_, 0, Config::kBlockSize);
    cur_blk_->dirty_ = true;
    cur_blk_->pin_ = false;

    return;
}



IndexManager::IndexManager(){
    std::ifstream is(Config::kIndexFileName, std::ios::binary);
    if (!is) {
    std::cerr << "Couldn't find the Index file, assuming it is the first "
                    "time of running MiniSQL."
                << std::endl;
    return;
    }

    size_t index_count;
    is.read(reinterpret_cast<char *>(&index_count), sizeof(index_count));
    while (index_count--) {
        size_t index_name_size;
        string index_name;
        size_t blk;

        is.read(reinterpret_cast<char *>(&index_name_size),
                sizeof(index_name_size));
        index_name.resize(index_name_size);
        is.read(index_name.data(), index_name_size);
        is.read(reinterpret_cast<char *>(&blk), sizeof(blk));

        index_blocks[index_name] = blk;
    }
}

IndexManager::~IndexManager(){
    std::ofstream os(Config::kIndexFileName, std::ios::binary);

    size_t index_count = index_blocks.size();
    os.write(reinterpret_cast<char *>(&index_count), sizeof(index_count));

    for(const auto &v : index_blocks){
        auto &index_name = v.first;
        auto index_name_size = index_name.size();
        auto &blk = v.second;

        os.write(reinterpret_cast<char *>(&index_name_size),
                 sizeof(index_name_size));
        os.write(index_name.data(), index_name_size);
        os.write(reinterpret_cast<const char *>(&blk), sizeof(blk));
    }
}

bool IndexManager::CreateIndex(const Table &table, const string &index_name,
                 const string &column){
    if(get<2>(table.attributes.at(column)) == SpecialAttribute::None){
        cerr << "failed to build index: the attribute may not be unique" << endl;
        return true;
    }
    SqlValueType p = get<1>(table.attributes.at(column));
    getBplus newTree(NEWBLOCK, p, index_name);
    newTree.newNode(NodeType::LeafNode);
    newTree.Node_.parent.block_id = ROOT;
    newTree.root_id = newTree.block_id_;
    Position temp = {ROOT, 0};
    newTree.Node_.pos.push_back(temp);
    newTree.updateBlock();
    index_blocks.insert({index_name, newTree.block_id_});
    
    //not finished yet
    size_t idx = get<0>(table.attributes.at(column));
    size_t root_id;
    SqlValue data;
    Position pos;
    RecordAccessProxy rap = record_manager.getIterator(table);
    do{
        if(!rap.isCurrentSlotValid()) break;
        data = rap.extractData().values[idx];
        pos = rap.extractPostion();
        newTree.insert(data, pos);
        index_blocks[column] = newTree.root_id;
        root_id = index_blocks[column];
        if(newTree.block_id_ != root_id) newTree.switchToBlock(root_id);
    } while(rap.next());

    return true;
}

bool IndexManager::PrimaryKeyIndex(const Table &table){
    auto it = table.attributes.begin();
    while(it!=table.attributes.end()){
        auto &c = *it;
        if(get<2>(c.second)==SpecialAttribute::PrimaryKey) break;
        ++it;
    }
    if(it==table.attributes.end()){
        cerr << "failed to build an index: the table doesn't have a Primary Key" << endl;
        return true;
    }
    CreateIndex(table, it->first, it->first);
    return true;
}


bool IndexManager::DropIndex(const string &index_name){
    auto &block_id = index_blocks.at(index_name);
    getBplus byebye(block_id, 0, index_name);
    byebye.deleteIndexRoot();
    index_manager.index_blocks.erase(index_name);
    
    return true;
}

void IndexManager::DropAllIndex(const Table &table){
    for(const auto &v : table.indexes){
        auto &attribute_name = v.first;
        auto &index_name = v.second;
        DropIndex(index_name);        
    }
}

bool IndexManager::InsertKey(const Table &table,
                 const Tuple &tuple,
                 Position &pos){
    //
    for(const auto &v : table.indexes){
        auto &attribute_name = v.first;
        auto &index_name = v.second;
        auto &block_id = index_blocks[index_name];
        auto &attribute_index = get<0>(table.attributes.at(attribute_name));
        auto &attribute_type = get<1>(table.attributes.at(attribute_name));
        getBplus current(block_id, attribute_type, index_name);
        current.insert(tuple.values[attribute_index], pos);
    }
    return true;
}

bool IndexManager::RemoveKey(const Table &table,
                    const Tuple &tuple){
    //
    for(const auto &v : table.indexes){
        auto &attribute_name = v.first;
        auto &index_name = v.second;
        auto &block_id = index_blocks.at(index_name);
        auto &attribute_index = get<0>(table.attributes.at(attribute_name));
        auto &attribute_type = get<1>(table.attributes.at(attribute_name));
        getBplus current(block_id, attribute_type, index_name);
        current.erase(tuple.values[attribute_index]);
    }
    return true;
}

vector<Tuple> IndexManager::SelectRecord(const string &index,
                             const vector<Condition> &conditions) {
    vector<Tuple> res;
    //
    return res;
}

IndexManager index_manager;