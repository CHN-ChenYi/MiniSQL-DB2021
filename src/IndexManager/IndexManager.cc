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
        #ifdef _indexDEBUG
        cout << "get null Node" << endl;
        #endif
        return;
      }
      #ifdef _indexDEBUG
    cout << "get node" << endl;
    #endif
      root_id = blk_id;
      cur_blk_ = static_cast<IndexBlock *>(buffer_manager.Read(block_id_));
      cur_blk_->pin_ = true;
      data_ = cur_blk_->val_;
      getNodeInfo();
    }

void getBplus::releaseBlock() { if(cur_blk_) cur_blk_->pin_ = false; }

const bplusNode& getBplus::getNodeInfo() {
    #ifdef _indexDEBUG
    cout << "getting node info..." << endl;
    #endif
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
    #ifdef _indexDEBUG
    cout << element_num << " " << Node_.pos[element_num].block_id << endl;
    #endif
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
    #ifdef _indexDEBUG
    cout << "get info! " << Node_.pos[element_num].block_id << endl;
    #endif
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
    #ifdef _indexDEBUG
    cout << "block switching..." << endl;
    #endif
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
    #ifdef _indexDEBUG
    cout << "start insert()" << endl;
    #endif
    int maxElem = Config::kNodeCapacity - 1;
    // find the leafNode
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
    #ifdef _indexDEBUG
    cout << "reach the leaf" << endl;
    #endif
    // reach the leafNode
    // find insert position
    for (i = 0; i < element_num; i++) {
    if (Node_.elem[i].Compare(Operator::GT, val)) break;
    }
    // insert into "Node_"
    Node_.elem.insert(Node_.elem.begin() + i, val);
    Node_.pos.insert(Node_.pos.begin() + i, pos);
    element_num++;

    // insert into real Index
    if (element_num <= maxElem) {
    updateBlock();
    #ifdef _indexDEBUG
    cout << "insert without split" << endl;
    #endif
    }
    // the leafnode needs to split
    else {
    #ifdef _indexDEBUG
    cout << "need to split" << endl;
    #endif
    splitLeaf();
    }
    #ifdef _indexDEBUG
    cout << "insert new record, now root id: " << root_id << endl;
    #endif
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
    newNode(temp_type);
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
    #ifdef _indexDEBUG
    cout << "need to create new root" << endl;
    #endif
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
    } 
    else {
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

size_t getBplus::findLeaf(SqlValue val) {
    int i = 0;
    while (Node_.type != NodeType::LeafNode) {
    #ifdef _indexDEBUG
    cout << "not leafNode, get down" << endl;
    #endif
    size_t Next_blk;
    i = 0;
    while (i < element_num) {
        #ifdef _indexDEBUG
        cout << "current node: " << block_id_ << " " << Node_.elem[i].val.String << endl;
        #endif
        if (Node_.elem[i].Compare(Operator::GT, val)) break;
        i++;
    }
    #ifdef _indexDEBUG
    cout << "reach Node: " << block_id_ << " " << Node_.elem[i].val.String << " " << Node_.pos[i].block_id << endl;
    #endif
    Next_blk = Node_.pos[i-1].block_id;
    switchToBlock(Next_blk);
    }
    #ifdef _indexDEBUG
    cout << "reach leaf: " << block_id_ << endl;
    #endif
    return block_id_;
}

Position getBplus::find(SqlValue val){
    size_t leaf = findLeaf(val);
    switchToBlock(leaf);
    int i = 0;
    while (i < element_num) {
    if (Node_.elem[i].Compare(Operator::EQ, val)) break;
    i++;
    }
    
    return Node_.pos[i];
}

Position getBplus::findMin(){
    size_t blk_id;
    while(Node_.type!=NodeType::LeafNode){
        blk_id = Node_.pos[0].block_id;
        switchToBlock(blk_id);
    }
    return Node_.pos[0];
}

/**
 * @brief delete ONE key in the index at one time
 * please set cur_blk_ to the rootNode before erase
 * */
void getBplus::erase(SqlValue val) {
    size_t targetLeaf = findLeaf(val);
    delete_entry(targetLeaf, val);
}

void getBplus::delete_entry(size_t N, SqlValue K){
    switchToBlock(N);
    int i = 0;
    while(i < element_num){
        if(Node_.elem[i].Compare(Operator::EQ, K)) break;
        i++;
    }
    if(i==element_num) return;

    if(Node_.type == NodeType::LeafNode){
        Node_.elem.erase(Node_.elem.begin() + i);
        Node_.pos.erase(Node_.pos.begin() + i);
    }
    else{
        Node_.elem.erase(Node_.elem.begin() + i);
        Node_.pos.erase(Node_.pos.begin() + i + 1);
    }
    element_num--;
    updateBlock();

    int minElem;
    switch (Node_.type){
        case NodeType::LeafNode : minElem = Config::kNodeCapacity / 2; break;
        default: minElem = (Config::kNodeCapacity + 1) / 2;
    }
    size_t blk_id;
    if(Node_.type == NodeType::Root && element_num==0){
        blk_id = Node_.pos[0].block_id;
        memset(cur_blk_->val_, 0, Config::kBlockSize);
        cur_blk_->dirty_ = true;
        cur_blk_->pin_ = false;
        updateBlock();

        switchToBlock(blk_id);
        root_id = block_id_;
        if(Node_.type != NodeType::LeafNode) Node_.type = NodeType::Root;
        updateBlock();
    }
    else if((Node_.type==NodeType::LeafNode && element_num<minElem) || 
            (Node_.type!=NodeType::LeafNode && element_num+1<minElem)){
        blk_id = block_id_;   //target Node
        size_t N_ = Node_.parent.block_id;
        SqlValue K_;
        int Elem_ = element_num;   //element of target Node
        switchToBlock(N_);
        for(i=0;i<=element_num;i++){   //find position of target Node in N_
            if(Node_.pos[i].block_id == blk_id) break;
        }
        if(i==0){
            N_ = Node_.pos[i+1].block_id;
            K_ = Node_.elem[i];
        }
        else {
            N_ = Node_.pos[i-1].block_id;
            K_ = Node_.elem[i-1];
        }
        switchToBlock(N_);
        if(element_num + Elem_ <= Config::kNodeCapacity - 1){ //merge
            if(i==0){
                N = N_;
                N_ = blk_id;
            }
            switchToBlock(N);
            vector<SqlValue> temp_val;
            vector<Position> temp_pos;
            temp_val.insert(temp_val.begin(),Node_.elem.begin(),Node_.elem.end());
            temp_pos.insert(temp_pos.begin(),Node_.pos.begin(),Node_.pos.end());
            switchToBlock(N_);
            if(Node_.type!=NodeType::LeafNode){
                Node_.elem.push_back(K_);
                Node_.elem.insert(Node_.elem.end(),temp_val.begin(),temp_val.end());
                Node_.pos.insert(Node_.pos.end(),temp_pos.begin(),temp_pos.end());
            }
            else{
                Node_.pos.erase(Node_.pos.end()-1);
                Node_.elem.insert(Node_.elem.end(),temp_val.begin(),temp_val.end());
                Node_.pos.insert(Node_.pos.end(),temp_pos.begin(),temp_pos.end());
            }
            element_num = Node_.elem.size();
            updateBlock();
            delete_entry(Node_.parent.block_id, K_);
            switchToBlock(blk_id);
            memset(cur_blk_->val_, 0, Config::kBlockSize);
            cur_blk_->dirty_ = true;
            cur_blk_->pin_ = false;
            updateBlock();
        }
        else{ //rearrange
            if(i!=0){
                if(Node_.type!=NodeType::LeafNode){
                    SqlValue temp_K = Node_.elem[element_num-1];
                    Position temp_p = Node_.pos[element_num];
                    Node_.elem.erase(Node_.elem.end()-1);
                    Node_.pos.erase(Node_.pos.end()-1);
                    element_num--;
                    updateBlock();
                    switchToBlock(N);
                    Node_.elem.insert(Node_.elem.begin(),K_);
                    Node_.pos.insert(Node_.pos.begin(),temp_p);
                    element_num++;
                    updateBlock();
                    switchToBlock(Node_.parent.block_id);
                    for(i=0;i<element_num;i++){
                        if(Node_.elem[i].Compare(Operator::EQ,K_)) break;
                    }
                    Node_.elem[i] = temp_K;
                    updateBlock();
                }
                else{
                    SqlValue temp_K = Node_.elem[element_num-1];
                    Position temp_p = Node_.pos[element_num-1];
                    Node_.elem.erase(Node_.elem.end()-1);
                    Node_.pos.erase(Node_.pos.end()-2);
                    element_num--;
                    updateBlock();
                    switchToBlock(N);
                    Node_.elem.insert(Node_.elem.begin(),temp_K);
                    Node_.pos.insert(Node_.pos.begin(),temp_p);
                    element_num++;
                    updateBlock();
                    switchToBlock(Node_.parent.block_id);
                    for(i=0;i<element_num;i++){
                        if(Node_.elem[i].Compare(Operator::EQ,K_)) break;
                    }
                    Node_.elem[i] = temp_K;
                    updateBlock();
                }
            }
            else{
                if(Node_.type!=NodeType::LeafNode){
                    SqlValue temp_K = Node_.elem[0];
                    Position temp_p = Node_.pos[0];
                    Node_.elem.erase(Node_.elem.begin());
                    Node_.pos.erase(Node_.pos.begin());
                    element_num--;
                    updateBlock();
                    switchToBlock(N);
                    Node_.elem.push_back(K_);
                    Node_.pos.push_back(temp_p);
                    element_num++;
                    updateBlock();
                    switchToBlock(Node_.parent.block_id);
                    for(i=0;i<element_num;i++){
                        if(Node_.elem[i].Compare(Operator::EQ,K_)) break;
                    }
                    Node_.elem[i] = temp_K;
                    updateBlock();
                }
                else{
                    SqlValue temp_K = Node_.elem[1];
                    Position temp_p = Node_.pos[0];
                    Node_.elem.erase(Node_.elem.begin());
                    Node_.pos.erase(Node_.pos.begin());
                    element_num--;
                    updateBlock();
                    switchToBlock(N);
                    Node_.elem.push_back(K_);
                    Node_.pos.insert(Node_.pos.end()-1,temp_p);
                    element_num++;
                    updateBlock();
                    switchToBlock(Node_.parent.block_id);
                    for(i=0;i<element_num;i++){
                        if(Node_.elem[i].Compare(Operator::EQ,K_)) break;
                    }
                    Node_.elem[i] = temp_K;
                    updateBlock();
                }
            }
        }
    }
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
    #ifdef _indexDEBUG
    cout << "creating index..." << endl;
    #endif

    if(get<2>(table.attributes.at(column)) == SpecialAttribute::None){
        cerr << "failed to build index: the attribute may not be unique" << endl;
        return true;
    }
    SqlValueType p = get<1>(table.attributes.at(column));
    #ifdef _indexDEBUG
    cout << "creating newNode..." << endl;
    #endif
    
    getBplus newTree(NEWBLOCK, p, index_name);
    newTree.newNode(NodeType::LeafNode);
    newTree.Node_.parent.block_id = ROOT;
    newTree.root_id = newTree.block_id_;   //new Tree, new id, new block
    Position temp = {NULLBLOCK, 0};
    newTree.Node_.pos.push_back(temp);
    newTree.updateBlock();
    index_blocks.insert({index_name, newTree.root_id});
    #ifdef _indexDEBUG
    cout << "new block id: " << newTree.block_id_ << endl;
    #endif
    
    #ifdef _indexDEBUG
    cout << "inserting formal element..." << endl;
    #endif
    //not finished yet
    size_t idx = get<0>(table.attributes.at(column));
    size_t root_id;
    SqlValue data;
    Position pos;
    RecordAccessProxy rap = record_manager.getIterator(table);
    do{
        if(!rap.isCurrentSlotValid()) continue;
        #ifdef _indexDEBUG
        cout << "get into loop" << endl;
        #endif
        data = rap.extractData().values[idx];
        pos = rap.extractPostion();
        newTree.insert(data, pos);
        //attention: there might be something wrong blow
        index_blocks[column] = newTree.root_id;
        root_id = index_blocks[column];
        if(newTree.block_id_ != root_id) newTree.switchToBlock(root_id);
        #ifdef _indexDEBUG
        cout << "end of one loop" << endl;
        #endif
    } while(rap.next());
    newTree.releaseBlock();
    #ifdef _indexDEBUG
    cout << "finish create index" << endl;
    #endif

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
    byebye.releaseBlock();
    
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
    #ifdef _indexDEBUG
    cout << "starting insert..." << endl;
    #endif
    for(const auto &v : table.indexes){
        auto &attribute_name = v.first;
        auto &index_name = v.second;
        auto &block_id = index_blocks[index_name];
        auto &attribute_index = get<0>(table.attributes.at(attribute_name));
        auto &attribute_type = get<1>(table.attributes.at(attribute_name));
        #ifdef _indexDEBUG
        cout << "insert into: " << attribute_name << index_name << block_id << attribute_index << attribute_type << endl;
        #endif
        getBplus current(block_id, attribute_type, index_name);
        current.insert(tuple.values[attribute_index], pos);
        index_blocks[index_name] = current.root_id;
        current.releaseBlock();
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
        current.releaseBlock();
    }
    return true;
}

bool IndexManager::checkCondition(const Table &table, const vector<Condition> &condition){
    #ifdef _indexDEBUG
    cout << "check Condition" << endl;
    #endif
    for(auto &v : condition){
        if(table.indexes.contains(v.attribute)) return true;
    }
    return false;
}

bool IndexManager::judgeCondition(string attribute, const SqlValue& val, Condition& condition){
    if(condition.attribute!=attribute) return false;
    return val.Compare(condition.op, condition.val);
}

bool IndexManager::judgeConditions(const Table &table, Position pos , const vector<Condition>& conditions){
    #ifdef _indexDEBUG
    cout << "judging conditions" << endl;
    #endif
    Tuple tuple_ = extractData(table, pos);
    for(int i=0; i<conditions.size();i++){
        size_t idx = get<0>(table.attributes.at(conditions[i].attribute));
        SqlValue comp = {tuple_.values[idx].type, conditions[i].val.val};
        if(!tuple_.values[idx].Compare(conditions[i].op, comp)) {
            #ifdef _indexDEBUG
            cout << "false" << endl;
            #endif
            return false;}
    }
    #ifdef _indexDEBUG
    cout << "true" << endl;
    #endif
    return true;
}


const Tuple IndexManager::extractData(const Table& table, const Position& pos){
    #ifdef _indexDEBUG
    cout << "extracting data: " << pos.block_id << " " << pos.offset << endl;
    #endif
  Block* blk = buffer_manager.Read(pos.block_id);
    #ifdef _indexDEBUG
    cout << "reach the block"  << endl;
    #endif
  char *data = blk->val_ + pos.offset;
  char *tmp = data;
  int size = table.attributes.size();
  #ifdef _indexDEBUG
    cout << "attribute number: " << size << endl;
    #endif
  Tuple tuple_;
  SqlValue t;
  for (auto &v : table.attributes) {
    switch (get<1>(v.second)) {
      case static_cast<SqlValueType>(SqlValueTypeBase::Integer):
        #ifdef _indexDEBUG
        cout << "type : integer " << endl;
        #endif
        memcpy(&t.val.Integer, tmp, sizeof(t.val.Integer));
        t.type = static_cast<SqlValueType>(SqlValueTypeBase::Integer);
        #ifdef _indexDEBUG
        cout << t.val.Integer << endl;
        #endif
        tuple_.values.push_back(t);
        tmp += sizeof(t.val.Integer);
        break;
      case static_cast<SqlValueType>(SqlValueTypeBase::Float):
        #ifdef _indexDEBUG
        cout << "type : float " << endl;
        #endif
        memcpy(&t.val.Float, tmp, sizeof(t.val.Float));
        t.type = static_cast<SqlValueType>(SqlValueTypeBase::Float);
        #ifdef _indexDEBUG
        cout << setprecision(2) << setiosflags(ios::fixed) << t.val.Float << endl;
        #endif
        tuple_.values.push_back(t);
        tmp += sizeof(t.val.Float);
        break;
      default: {
        size_t len =
            get<1>(v.second) - static_cast<SqlValueType>(SqlValueTypeBase::String);
            #ifdef _indexDEBUG
        cout << "type : string " << endl;
        #endif
        memcpy(t.val.String, tmp, len);
        t.type = static_cast<SqlValueType>(len);
        #ifdef _indexDEBUG
        cout << t.val.String << endl;
        #endif
        tuple_.values.push_back(t);
        tmp += len;
        break;
      }
    }
  }
    #ifdef _indexDEBUG
    cout << "obtain data!" << endl;
    #endif
  return tuple_;
}

vector<Tuple> IndexManager::SelectRecord(const Table &table,
                             const vector<Condition> &conditions) {
    #ifdef _indexDEBUG
    cout << "start selecting..." << endl;
    #endif
    vector<Tuple> res;
    vector<Condition> good;
    int i;
    #ifdef _indexDEBUG
    cout << "searching good conditions" << endl;
    #endif
    for(i; i<conditions.size();i++){
        if(index_blocks.contains(conditions[i].attribute)){
            good.push_back(conditions[i]);
        }
    }
    Condition perfect = good[0];
    #ifdef _indexDEBUG
    cout << "searching perfect condition" << endl;
    #endif
    for(i; i<good.size();i++){
        if(good[i].op == Operator::EQ) {
            perfect = good[i]; 
            break;
        }
        if(good[i].op < perfect.op) perfect = good[i];
    }
    #ifdef _indexDEBUG
    cout << "find perfect: " << perfect.val.val.String << endl;
    #endif
    getBplus index(index_blocks[perfect.attribute], perfect.val.type, table.indexes.at(perfect.attribute));
    index.getNodeInfo();
    index.findLeaf(perfect.val);
    size_t blk = index.block_id_;
    int offset;
    for(offset=0;offset<index.element_num;offset++){
        #ifdef _indexDEBUG
        cout << "compare: " << index.Node_.elem[offset].val.String << endl;
        #endif
        if(index.Node_.elem[offset].Compare(Operator::EQ, perfect.val)) break;
    }
    switch(perfect.op){
        case Operator::EQ : {
            #ifdef _indexDEBUG
            cout << "get equal condition" << endl;
            #endif
            if(judgeConditions(table, index.Node_.pos[offset], conditions))
            res.push_back(extractData(table, index.Node_.pos[offset])); 
            break;}
        case Operator::GT : {
            do{
                index.switchToBlock(blk);
                for(offset=0;offset<index.element_num;offset++){
                    if(judgeConditions(table, index.Node_.pos[offset], conditions))
                    res.push_back(extractData(table, index.Node_.pos[offset])); 
                }
                blk = index.Node_.pos[offset].block_id;
            }while(blk != ROOT);
            break;
        }
        case Operator::GE : {
            do{
                index.switchToBlock(blk);
                for(offset=0;offset<index.element_num;offset++){
                    if(judgeConditions(table, index.Node_.pos[offset], conditions))
                    res.push_back(extractData(table, index.Node_.pos[offset])); 
                }
                blk = index.Node_.pos[offset].block_id;
            }while(blk != ROOT);
            break;
        }
        case Operator::LE : {
            index.findMin();
            size_t cur = index.block_id_;
            do{
                index.switchToBlock(cur);
                for(offset=0;offset<index.element_num;offset++){
                    if(judgeConditions(table, index.Node_.pos[offset], conditions))
                    res.push_back(extractData(table, index.Node_.pos[offset])); 
                }
                cur = index.Node_.pos[offset].block_id;
            }while(cur != blk);
                index.switchToBlock(cur);
                for(offset=0;offset<index.element_num;offset++){
                    if(judgeConditions(table, index.Node_.pos[offset], conditions))
                    res.push_back(extractData(table, index.Node_.pos[offset])); 
                }
            break;
        }
        case Operator::LT : {
            index.findMin();
            size_t cur = index.block_id_;
            do{
                index.switchToBlock(cur);
                for(offset=0;offset<index.element_num;offset++){
                    if(judgeConditions(table, index.Node_.pos[offset], conditions))
                    res.push_back(extractData(table, index.Node_.pos[offset])); 
                }
                cur = index.Node_.pos[offset].block_id;
            }while(cur != blk);
                index.switchToBlock(cur);
                for(offset=0;offset<index.element_num;offset++){
                    if(judgeConditions(table, index.Node_.pos[offset], conditions))
                    res.push_back(extractData(table, index.Node_.pos[offset])); 
                }
            break;
        }
        case Operator::NE : {
            index.findMin();
            size_t cur = index.block_id_;
            do{
                index.switchToBlock(cur);
                for(offset=0;offset<index.element_num;offset++){
                    if(judgeConditions(table, index.Node_.pos[offset], conditions))
                    res.push_back(extractData(table, index.Node_.pos[offset])); 
                }
                cur = index.Node_.pos[offset].block_id;
            }while(cur != ROOT);
            break;
        }
    }
    #ifdef _indexDEBUG
    cout << "returning selecting result   " << res[0].values[1].val.String << endl;
    cout << (res[0].values[1].type == static_cast<SqlValueType>(SqlValueTypeBase::String)) << endl;
    #endif
    return res;
}

IndexManager index_manager;
