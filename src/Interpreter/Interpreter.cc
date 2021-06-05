#include "Interpreter.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <iostream>
#include <iterator>
#include <regex>

using std::cerr;
using std::endl;
using std::isalnum;
using std::isalpha;
using std::min;
using std::string_view;

using namespace std::literals;

void Interpreter::Interpret() const {}

void Interpreter::handleError(std::string_view msg, InputType pos) const {
  cerr << msg << endl;
}

Interpreter::ParserState Interpreter::skipSpace(
    Interpreter::InputType input) const {
  input.remove_prefix(min(input.find_first_not_of(" \n\r\t"), input.size()));
  return {true, TokenNone, input};
}

Interpreter::ParserState Interpreter::expect(
    string_view lit, Interpreter::InputType input) const {
  auto [dummy1, dummy2, rest] = skipSpace(input);
  if (rest.starts_with(lit)) {
    rest.remove_prefix(lit.size());
    return {true, Token{.kind = TokenKind::KeyWord, .sv = lit}, rest};
  } else {
    return {false, TokenNone, rest};
  }
}

Interpreter::ParserState Interpreter::peek(string_view lit,
                                           Interpreter::InputType input) const {
  auto [dummy1, dummy2, rest] = skipSpace(input);
  if (rest.starts_with(lit)) {
    return {true, Token{.kind = TokenKind::KeyWord, .sv = lit}, rest};
  } else {
    return {false, TokenNone, rest};
  }
}

Interpreter::ParserState Interpreter::parseNumber(
    Interpreter::InputType input) const {
  auto [dummy1, dummy2, rest] = skipSpace(input);
  // TODO
  return {false, TokenNone, rest};
}

Interpreter::ParserState Interpreter::parseId(
    Interpreter::InputType input) const {
  auto [dummy1, dummy2, rest] = skipSpace(input);
  if (!rest.empty())
    if (isalpha(rest[0])) {
      size_t i = 1;
      for (; i < rest.size() && isalnum(rest[i]); ++i)
        ;
      auto tok = rest.substr(0, i);
      rest.remove_prefix(i);
      return {true, Token{.kind = TokenKind::Id, .sv = tok}, rest};
    }
  return {false, TokenNone, rest};
}

Interpreter::ParserState Interpreter::parsePath(
    Interpreter::InputType input) const {
  auto [dummy1, dummy2, rest] = skipSpace(input);
  // TODO
  return {false, TokenNone, rest};
}

Interpreter::ParserState Interpreter::parseCreateTable(
    Interpreter::InputType input) const {
  auto [flag1, _1, rest1] = expect("create"sv, input);
  if (!flag1) {
    handleError("expect token `create`", rest1);
    return {false, TokenNone, rest1};
  }
  auto [flag2, _2, rest2] = expect("table"sv, rest1);
  if (!flag2) {
    handleError("expect token `table`", rest2);
    return {false, TokenNone, rest2};
  }
  auto [flag3, id, rest3] = parseId(rest2);
  if (!flag3) {
    handleError("expect valid identifier like `[a-zA-Z][a-zA-Z0-9]*`", rest3);
    return {false, TokenNone, rest3};
  }
  // TODO
}
Interpreter::ParserState Interpreter::parseCreateIndex(
    Interpreter::InputType input) const {}
Interpreter::ParserState Interpreter::parseSelectStat(
    Interpreter::InputType input) const {}
Interpreter::ParserState Interpreter::parseDeleteStat(
    Interpreter::InputType input) const {}
Interpreter::ParserState Interpreter::parseInsertStat(
    Interpreter::InputType input) const {}
Interpreter::ParserState Interpreter::parseDropTable(
    Interpreter::InputType input) const {}
Interpreter::ParserState Interpreter::parseDropIndex(
    Interpreter::InputType input) const {}
Interpreter::ParserState Interpreter::parseExec(
    Interpreter::InputType input) const {}

Interpreter interpreter;
