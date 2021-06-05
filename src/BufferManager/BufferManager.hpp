#pragma once

#include "DataStructure.hpp"

class BufferManager {
 public:
  /**
   * @brief Construct a new Buffer Manager object. Open the files.
   *
   */
  BufferManager();

  /**
   * @brief Destroy the Buffer Manager object. Write back the data and close the
   * files.
   *
   */
  ~BufferManager();

  /**
   * @brief read a block
   *
   * @param block_id the id of the block
   * @return a pointer points to the block
   */
  Block *const Read(const size_t &block_id);

  /**
   * @brief write a block
   *
   * @param block a pointer points to the block
   * @return the id of the block
   */
  size_t Write(Block *const block);
};

extern BufferManager buffer_manager;
