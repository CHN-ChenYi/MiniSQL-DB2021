#pragma once

class Interpreter {
 public:
  /**
   * @brief Read commands from stdin, interpret them and then execute them by
   * API module. Return when user quits.
   *
   */
  void Interpret() const;
};

extern Interpreter interpreter;
