#include <exception>
#include <ios>
#include <iostream>

#include "Interpreter.hpp"

int main() {
  std::ios_base::sync_with_stdio(false);
  try {
    interpreter.interpret();
  } catch (const std::exception &e) {
    std::cout << "Exception: " << e.what() << std::endl;
    return 1;
  } catch (...) {
    std::cout << "Unknown Error" << std::endl;
    return 2;
  }
  return 0;
}
