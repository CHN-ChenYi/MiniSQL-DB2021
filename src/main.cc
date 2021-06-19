#include <exception>
#include <filesystem>
#include <ios>
#include <iostream>

#include "Interpreter.hpp"

int main(int argc, char **argv) {
  std::ios_base::sync_with_stdio(false);
  try {
    if (argc == 1) {
      interpreter.interpret();
    } else {
      std::filesystem::path path = argv[1];
      interpreter.setWorkdir(path.parent_path());
      interpreter.interpretFile(path.filename());
    }
  } catch (const std::exception &e) {
    std::cout << "Exception: " << e.what() << std::endl;
    return 1;
  } catch (...) {
    std::cout << "Unknown Error" << std::endl;
    return 2;
  }
  return 0;
}
