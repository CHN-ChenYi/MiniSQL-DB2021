#include "RecordManager.hpp"

#include <fstream>
#include <iostream>
#include <ostream>

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

bool RecordManager::createTable(const std::string &table_name) {
  if (table_blocks.contains(table_name))
    throw std::runtime_error("such a table already exists");
  return table_blocks.insert({table_name, {}}).second;
}

bool RecordManager::dropTable(const std::string &table_name) {}

bool RecordManager::deleteTable(const std::string &table_name) {}

RecordManager::TableProxy RecordManager::operator[](
    const std::string &table_name) {
  if (!table_blocks.contains(table_name))
    throw std::runtime_error("no such a table");
  return TableProxy(&table_blocks.find(table_name)->second);
}

RecordManager record_manager;
