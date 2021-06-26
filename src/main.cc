#include <csignal>
#include <exception>
#include <filesystem>
#include <ios>
#include <iostream>

#include "DataStructure.hpp"
#include "Interpreter.hpp"

volatile std::sig_atomic_t interrupt = 0;

int main(int argc, char **argv) {
  std::signal(SIGINT, [](int) {
    std::cout << ANSI_COLOR_RED
        "Please use `quit;` instead of `CTRL-C` the next time." ANSI_COLOR_RESET
              << std::endl;
    interrupt = 1;
  });

  if (!std::filesystem::exists(CommonPathPrefix))
    std::filesystem::create_directory(CommonPathPrefix);

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
    std::cout << "exception: " << e.what() << std::endl;
    return 1;
  } catch (...) {
    std::cout << "unknown error" << std::endl;
    return 2;
  }
  return 0;
}
