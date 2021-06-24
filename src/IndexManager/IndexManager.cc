#include "IndexManager.hpp"

#include <exception>
#include <fstream>
#include <iostream>

#include "BufferManager.hpp"
#include "CatalogManager.hpp"
#include "DataStructure.hpp"

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
        cerr << "the attribute may not be unique" << endl;
        return false;
    }

    //

    return true;
}

bool IndexManager::DropIndex(const Table &table, const string &index_name){
    if(!table.indexes.contains(index_name)){
        cerr << "such a index doesn't exists" << endl;
        return true;
    }

    //

    return true;
}

bool IndexManager::InsertKey(const string &index_name,
                 tuple<string, SqlValueType, SpecialAttribute> &attributes,
                 Position &pos){}

bool IndexManager::RemoveKey(const string &index_name,
                    tuple<string, SqlValueType, SpecialAttribute> &attributes){}

vector<Tuple> IndexManager::SelectRecord(const string &index,
                             const vector<Condition> &conditions) {}

IndexManager index_manager;