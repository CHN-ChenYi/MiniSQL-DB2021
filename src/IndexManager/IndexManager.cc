#include "IndexManager.hpp"

#include <exception>
#include <fstream>
#include <iostream>
#include <string>
#include <map>

#include "BufferManager.hpp"
#include "CatalogManager.hpp"
#include "DataStructure.hpp"

using namespace std;
using std::cerr;
using std::endl;

IndexManager::IndexManager(){
    std::ifstream is(kIndexFileName, std::ios::binary);
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
    std::ofstream os(kIndexFileName, std::ios::binary);

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
    if(table.indexes.contains(index_name) ||
       table.attributes.contains(column)){
        cerr << "such a index already exists" << endl;
        return true;
    }
    if(get<2>(table.attributes.at(column)) == SpecialAttribute::None){
        cerr << "failed to build index: the attribute may not be unique" << endl;
        return true;
    }
    
    SqlValueType p = get<1>(table.attributes.at(column));
    //something goes wrong down below
    //table.indexes.insert({std::pair<string,string>(column,index_name)});
    getBplus newTree(NEWBLOCK, p);
    newTree.newNode();
    index_blocks.insert({column, newTree.block_id_});

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


bool IndexManager::DropIndex(const Table &table, const string &index_name){
    if(!table.indexes.contains(index_name)){
        cerr << "such a index doesn't exists" << endl;
        return true;
    }
    auto &block_id = index_blocks.at(index_name);
    getBplus byebye(block_id, 0);
    byebye.deleteIndex();
    
    return true;
}

void IndexManager::DropAllIndex(const Table &table){
    for(const auto &v : table.indexes){
        auto &attribute_name = v.first;
        auto &index_name = v.second;
        DropIndex(table, index_name);        
    }
}

bool IndexManager::InsertKey(const Table &table,
                 const Tuple &tuple,
                 Position &pos){
    //
    for(const auto &v : table.indexes){
        auto &attribute_name = v.first;
        auto &index_name = v.second;
        auto &block_id = index_blocks.at(index_name);
        auto &attribute_index = get<0>(table.attributes.at(attribute_name));
        auto &attribute_type = get<1>(table.attributes.at(attribute_name));
        getBplus current(block_id, attribute_type);
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
        getBplus current(block_id, attribute_type);
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