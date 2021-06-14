#include <exception>
#include <iostream>

#include "Interpreter.hpp"

int main() {
  try {
    interpreter.Interpret();
  } catch (const std::exception &e) {
    std::cout << "Exception: " << e.what() << std::endl;
    return 1;
  } catch (...) {
    std::cout << "Unknown Error" << std::endl;
    return 2;
  }
  return 0;
}
