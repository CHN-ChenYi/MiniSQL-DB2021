#include "BufferManager.hpp"

#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <utility>

#ifdef _DEBUG
#include <iostream>
#endif

size_t BufferManager::max_block_id;
unordered_map<size_t, BufferManager::BlockInfo> BufferManager::buffer;
BufferManager buffer_manager;

static bool CheckFileExists(const std::string &filename) {
  return std::filesystem::exists(filename);
}

static void WriteToFile(const size_t &block_id, const Block *block) {
  std::ofstream os(Block::GetBlockFilename(block_id), std::ios::binary);
  block->write(os);
#ifdef _DEBUG
  std::cerr << "Write back block " << block_id << std::endl;
#endif
}

BufferManager::BufferManager() {
  size_t l = std::numeric_limits<size_t>::min(),
         r = std::numeric_limits<size_t>::max();
  while (l != r) {
    auto m = (l + r) >> 1;
    if (CheckFileExists(Block::GetBlockFilename(m)))
      l = m + 1;
    else
      r = m;
  }
  max_block_id = l;
}

BufferManager::~BufferManager() {
  for (const auto &it : buffer) {
    if (it.second.block->dirty_) WriteToFile(it.first, it.second.block);
    delete it.second.block;
  }
}

void BufferManager::AddBlockToBuffer(const size_t &block_id,
                                     Block *const block) {
  auto block_info = BlockInfo{block, std::chrono::high_resolution_clock::now()};
  if (buffer.size() == Config::kMaxBlockNum) {
    size_t least_recently_used_block_id;
    Block *least_recently_used_block = nullptr;
    auto least_recently_used_time =
        std::chrono::high_resolution_clock::now() +
        std::chrono::seconds(1);  // +1s to solve the resolution problem
    for (const auto &it : buffer) {
      if (!it.second.block->pin_ &&
          it.second.last_access_time < least_recently_used_time) {
        least_recently_used_block_id = it.first;
        least_recently_used_block = it.second.block;
        least_recently_used_time = it.second.last_access_time;
      }
    }
    if (least_recently_used_block == nullptr)
      throw std::overflow_error("buffer overflow");
#ifdef _DEBUG
    std::cerr << "Swap out block " << least_recently_used_block_id << std::endl;
#endif
    if (least_recently_used_block->dirty_)
      WriteToFile(least_recently_used_block_id, least_recently_used_block);
    delete least_recently_used_block;
    buffer.erase(least_recently_used_block_id);
  }
  buffer.insert(std::make_pair(block_id, block_info));
}

Block *BufferManager::Read(const size_t &block_id) {
  if (block_id >= max_block_id) throw std::out_of_range("block_id out of range");
  auto iter = buffer.find(block_id);
  if (iter == buffer.end()) {
    std::ifstream is(Block::GetBlockFilename(block_id), std::ios::binary);
    auto block = new Block;
    block->read(is);
    AddBlockToBuffer(block_id, block);
    return block;
  }
  iter->second.last_access_time = std::chrono::high_resolution_clock::now();
  return iter->second.block;
}

size_t BufferManager::Write(Block *const block) {
  const auto block_id = max_block_id++;
  WriteToFile(block_id, block);
  if (buffer.find(block_id) == buffer.end()) AddBlockToBuffer(block_id, block);
  return block_id;
}
