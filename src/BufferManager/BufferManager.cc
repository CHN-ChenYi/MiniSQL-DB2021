#include "BufferManager.hpp"

#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <utility>

#ifdef _DEBUG
// #define BufferDebug
#ifdef BufferDebug
#include <iostream>
#endif
#endif

#ifdef ParallelWrite
#include <sstream>
#endif

size_t BufferManager::max_block_id_;
unordered_map<size_t, BufferManager::BlockInfo> BufferManager::buffer_;
#ifdef ParallelWrite
TaskPool BufferManager::task_pool_;
#endif
BufferManager buffer_manager;

static bool CheckFileExists(const std::string &filename) {
  return std::filesystem::exists(filename);
}

void BufferManager::WriteToFile(const size_t &block_id, Block *block) {
#ifdef BufferDebug
  std::cerr << "Writing back block " << block_id << std::endl;
#endif
#ifdef ParallelWrite
  if (!task_pool_.Exec(block_id, [block_id, block]() {
#endif
#ifdef BufferDebug
        std::cerr << "Parallel writing back block " << block_id << std::endl;
#endif
        std::ofstream os(Block::GetBlockFilename(block_id), std::ios::binary);
        block->write(os);
        delete block;
#ifdef BufferDebug
        std::cerr << "Written back block " << block_id << std::endl;
#endif
#ifdef ParallelWrite
      })) {
    std::stringstream err_msg;
    err_msg << "Block " << block_id << " is busy";
    throw std::runtime_error(err_msg.str());
  }
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
  max_block_id_ = l;
}

BufferManager::~BufferManager() {
  for (const auto &it : buffer_) {
    if (it.second.block->dirty_)
      WriteToFile(it.first, it.second.block);
    else
      delete it.second.block;
  }
}

void BufferManager::AddBlockToBuffer(const size_t &block_id,
                                     Block *const block) {
  auto block_info = BlockInfo{block, std::chrono::high_resolution_clock::now()};
  if (buffer_.size() == Config::kMaxBlockNum) {
    size_t least_recently_used_block_id;
    Block *least_recently_used_block = nullptr;
    auto least_recently_used_time =
        std::chrono::high_resolution_clock::now() +
        std::chrono::seconds(1);  // +1s to solve the resolution problem
    for (const auto &it : buffer_) {
      if (!it.second.block->pin_ &&
          it.second.last_access_time < least_recently_used_time) {
        least_recently_used_block_id = it.first;
        least_recently_used_block = it.second.block;
        least_recently_used_time = it.second.last_access_time;
      }
    }
    if (least_recently_used_block == nullptr)
      throw std::overflow_error("buffer overflow");
#ifdef BufferDebug
    std::cerr << "Swap out block " << least_recently_used_block_id << std::endl;
#endif
    buffer_.erase(least_recently_used_block_id);
    if (least_recently_used_block->dirty_)
      WriteToFile(least_recently_used_block_id, least_recently_used_block);
    else
      delete least_recently_used_block;
  }
  buffer_.insert(std::make_pair(block_id, block_info));
}

Block *BufferManager::Read(const size_t &block_id) {
#ifdef BufferDebug
  std::cerr << "Read block " << block_id << std::endl;
#endif
  if (block_id == max_block_id_) {
    Block *block = new Block;
    Create(block);
    return block;
  }
  if (block_id > max_block_id_)
    throw std::out_of_range("block_id out of range");
  auto iter = buffer_.find(block_id);
  if (iter == buffer_.end()) {
#ifdef ParallelWrite
    task_pool_.Wait(block_id);
#endif
    std::ifstream is(Block::GetBlockFilename(block_id), std::ios::binary);
    auto block = new Block;
    block->read(is);
    AddBlockToBuffer(block_id, block);
    return block;
  }
  iter->second.last_access_time = std::chrono::high_resolution_clock::now();
  return iter->second.block;
}

size_t BufferManager::Create(Block *block) {
  const auto block_id = max_block_id_++;
#ifdef BufferDebug
  std::cerr << "Create block " << block_id << std::endl;
#endif
  block->dirty_ = true;
  AddBlockToBuffer(block_id, block);
  return block_id;
}

size_t BufferManager::NextId() { return max_block_id_; }
