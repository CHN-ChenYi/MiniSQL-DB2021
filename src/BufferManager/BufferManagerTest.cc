#include "BufferManager.hpp"

void BufferManagerTest() {
  auto a = new Block;
  memset(a->val_, 0x3f, sizeof(a->val_));
  const auto a_id = buffer_manager.Create(a);
  a->pin_ = true;
  a->dirty_ = true;
  for (int i = 0; i < Config::kMaxBlockNum; i++) {
    auto b = new Block;
    buffer_manager.Create(b);
  }
  a->pin_ = false;
  auto b = new Block;
  buffer_manager.Create(b);
  auto c = buffer_manager.Read(a_id);
}
