#include <exception>
#include <iostream>

#include "Interpreter.hpp"

#ifdef _DEBUG
#include "BufferManager.hpp"
#endif

int main() {
  try {
    interpreter.Interpret();
#ifdef _DEBUG
    // auto a = new Block();
    // *reinterpret_cast<int*>(a->val_) = 1;
    // auto id = buffer_manager.Write(a);
    // auto b = buffer_manager.Read(id);
    auto b = buffer_manager.Read(0);
    std::cout << *reinterpret_cast<int *>(b->val_) << std::endl;
    *reinterpret_cast<int *>(b->val_) = 1;
    b->dirty_ = true;
    // b->pin_ = true;

    // a = new Block();
    // *reinterpret_cast<int*>(a->val_) = 2;
    // id = buffer_manager.Write(a);
    // b = buffer_manager.Read(id);
    b = buffer_manager.Read(1);
    std::cout << *reinterpret_cast<int *>(b->val_) << std::endl;
    // b->pin_ = true;

    b = buffer_manager.Read(0);

    // auto a = new Block();
    // *reinterpret_cast<int*>(a->val_) = 3;
    // auto id = buffer_manager.Write(a);
    // std::cout << id << std::endl;
    b = buffer_manager.Read(2);
    std::cout << *reinterpret_cast<int *>(b->val_) << std::endl;

    // while (1);
    throw "fuck";
#endif
  } catch (const std::exception &e) {
    std::cout << "Exception: " << e.what() << std::endl;
    return 1;
  } catch (...) {
    std::cout << "Unknown Error" << std::endl;
    return 2;
  }
  return 0;
}
