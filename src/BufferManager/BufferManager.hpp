#pragma once

#include <chrono>
#include <unordered_map>
using std::unordered_map;

#include "DataStructure.hpp"
#ifdef ParallelWrite
#include "TaskPool.hpp"
#endif

// CAUTION: should have only **ONE** instance at a time
class BufferManager {
  static size_t max_block_id_;
  struct BlockInfo {
    Block *const block;
    std::chrono::time_point<std::chrono::system_clock> last_access_time;
  };
  static unordered_map<size_t, BlockInfo> buffer_;
#ifdef ParallelWrite
  static TaskPool task_pool_;
#endif

  /**
   * @brief add a block into the buffer
   *
   * @param block_id the id of the block
   * @param block a pointer to the block
   */
  static void AddBlockToBuffer(const size_t &block_id, Block *const block);

  static void WriteToFile(const size_t &block_id, Block *block);

 public:
  /**
   * @brief Construct a new Buffer Manager object. Open the files.
   *
   */
  BufferManager();

  /**
   * @brief Destroy the Buffer Manager object. Write back the data and close
   * the files.
   *
   */
  ~BufferManager();

  /**
   * @brief read a block
   *
   * @param block_id the id of the block
   * @return a pointer to the block
   */
  static Block *Read(const size_t &block_id);

  /**
   * @brief create a new block (CAUTION: the block should **NOT** be deleted)
   *
   * @param block a pointer to the block
   * @return the id of the block
   */
  static size_t Create(Block *block);

  /**
   * @brief get the id of the next new block
   *
   * @return the id of the next new block
   */
  static size_t NextId();
};

extern BufferManager buffer_manager;
