#include "RecordManager.hpp"

#include <fstream>
#include <iostream>
#include <ostream>
#include <utility>

#include "BufferManager.hpp"
#include "CatalogManager.hpp"
#include "DataStructure.hpp"

using std::cerr;
using std::endl;
using std::ifstream;
using std::ostream;

RecordManager::RecordManager() {
  ifstream is(kRecordFileName, std::ios::binary);
  if (!is) {
    cerr << "Couldn't find the record file, assuming it is the first "
            "time of running MiniSQL."
         << endl;
    return;
  }
  size_t table_count;
  is.read(reinterpret_cast<char *>(&table_count), sizeof(table_count));
  for (size_t i = 0; i < table_count; ++i) {
    size_t table_name_size;
    size_t block_count;
    string table_name;
    vector<size_t> blks;

    is.read(reinterpret_cast<char *>(&table_name_size),
            sizeof(table_name_size));
    table_name.resize(table_name_size);
    is.read(table_name.data(), table_name_size);

    is.read(reinterpret_cast<char *>(&block_count), sizeof(block_count));
    for (size_t j = 0; j < block_count; ++j) {
      size_t block_id;
      is.read(reinterpret_cast<char *>(&block_id), sizeof(block_id));
      blks.push_back(block_id);
    }

    table_blocks[table_name] = blks;
  }
}

RecordManager::~RecordManager() {
  ofstream os(kRecordFileName, std::ios::binary);

  size_t table_count = table_blocks.size();
  os.write(reinterpret_cast<char *>(&table_count), sizeof(table_count));

  for (const auto &kv : table_blocks) {
    auto &table_name = kv.first;
    auto table_name_size = table_name.size();
    auto &blks = kv.second;
    auto block_count = blks.size();

    os.write(reinterpret_cast<char *>(&table_name_size),
             sizeof(table_name_size));
    os.write(table_name.data(), table_name_size);

    os.write(reinterpret_cast<char *>(&block_count), sizeof(block_count));
    for (size_t j = 0; j < block_count; ++j) {
      os.write(reinterpret_cast<const char *>(&blks[j]), sizeof(blks[j]));
    }
  }
}

bool RecordManager::createTable(const Table &table) {
  if (table_blocks.contains(table.table_name)) {
    cerr << "such a table already exists" << endl;
    return true;
  }
  RecordAccessProxy rap(&table_blocks[table.table_name], &table, 0);
  table_blocks.insert({table.table_name, {}});
  table_current.emplace(table.table_name, rap);
  table_current[table.table_name].newBlock();
  return true;
}

bool RecordManager::dropTable(const Table &table) {
  if (!table_blocks.contains(table.table_name)) {
    cerr << "such a table doesn't exist" << endl;
    return true;
  }
  for (auto &id : table_blocks[table.table_name]) {
    auto p = buffer_manager.Read(id);
    memset(p->val_, 0, Config::kBlockSize);
    p->dirty_ = true;
    p->pin_ = false;
  }
  table_blocks.erase(table.table_name);
  table_current.erase(table.table_name);
  return true;
}

RecordManager record_manager;
