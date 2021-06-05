#include "Interpreter.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cctype>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>

#include "API.hpp"
#include "DataStructure.hpp"

using std::cerr;
using std::cin;
using std::cout;
using std::endl;
using std::int64_t;
using std::isalnum;
using std::isalpha;
using std::isspace;
using std::string;
using std::string_view;
using std::strtod;
using std::vector;
using std::filesystem::path;

using namespace std::literals;

void Interpreter::Interpret() {
  for (;;) {
    cur_tok = table_name = index_name = indexed_column_name = TokenNone;
    cur_attributes.clear();
    cout << "MiniSQL > ";
    getline(cin, input);
    iter = input.begin();
    bool need_quit = false;
    try {
      need_quit = parse();
    } catch (...) {
    }
    if (need_quit) break;
  }
}

void Interpreter::skipSpace() {
  for (; iter != input.end() && isspace(*iter); ++iter)
    ;
}

void Interpreter::expect(string_view s) {
  skipSpace();
  if (input.end() - iter >= static_cast<string::difference_type>(s.length()) &&
      memcmp(&*iter, s.data(), s.size()) == 0) {
    iter += s.size();
    return;
  }
  cerr << "expect token `" << s << "`" << endl;
  throw std::runtime_error("can't find expected token");
}

bool Interpreter::consume(string_view s) {
  skipSpace();
  if (input.end() - iter >= static_cast<string::difference_type>(s.length()) &&
      memcmp(&*iter, s.data(), s.size()) == 0) {
    iter += s.size();
    return true;
  } else {
    return false;
  }
}

bool Interpreter::peek(string_view s) {
  skipSpace();
  if (input.end() - iter >= static_cast<string::difference_type>(s.length()) &&
      memcmp(&*iter, s.data(), s.size()) == 0) {
    return true;
  } else {
    return false;
  }
}

void Interpreter::skip(string_view s) { iter += s.size(); }

void Interpreter::parseAttribute() {
  Token attr_tok;
  SqlValueType val_type;
  SpecialAttribute spec_attr;

  skipSpace();
  parseId();
  attr_tok = cur_tok;

  if (peek("integer")) {
    skip("integer");
    val_type = static_cast<SqlValueType>(SqlValueTypeBase::Integer);
  } else if (peek("int")) {
    skip("int");
    val_type = static_cast<SqlValueType>(SqlValueTypeBase::Integer);
  } else if (peek("float")) {
    skip("float");
    val_type = static_cast<SqlValueType>(SqlValueTypeBase::Float);
  } else if (peek("char")) {
    skip("char");
    expect("(");
    parseNumber();
    assert(cur_tok.kind == TokenKind::Num);
    if (cur_tok.i < 0 || cur_tok.i > 255)
      throw std::runtime_error("invalid length of char");
    val_type = static_cast<SqlValueType>(SqlValueTypeBase::String) +
               (unsigned)cur_tok.i;
    expect(")");
  }

  if (peek("unique")) {
    skip("unique");
    spec_attr = SpecialAttribute::UniqueKey;
  } else {
    spec_attr = SpecialAttribute::None;
  }

  cur_attributes.push_back({string(attr_tok.sv), val_type, spec_attr});
}

void Interpreter::parseConstraint() {
  skipSpace();
  expect("primary");
  expect("key");
  expect("(");
  parseId();
  bool found = false;
  for (auto &[name, type, spec] : cur_attributes) {
    if (name == cur_tok.sv) {
      found = true;
      spec = SpecialAttribute::PrimaryKey;
    }
  }
  if (!found) {
    cerr << "no such attribute" << endl;
    throw std::runtime_error("constraint failed");
  }
  expect(")");
}

void Interpreter::parseAttributeList() {
  skipSpace();
  parseAttribute();
  while (consume(",")) {
    if (peek("primary")) {
      parseConstraint();
    } else {
      parseAttribute();
    }
  }
}

void Interpreter::parseNumber() {
  skipSpace();
  if (iter != input.end()) {
    char *end = nullptr;
    auto val = strtod(&*iter, &end);
    if (end != &*iter) {
      iter += end - &*iter;
      cur_tok = Token{
          .kind = TokenKind::Num,
          .sv = ""sv,
          .f = val,
          .i = static_cast<int64_t>(val),
      };
      return;
    }
  }
  cerr << "expect a number" << endl;
  throw std::runtime_error("can't find expected token");
}

void Interpreter::parseId() {
  skipSpace();
  if (iter != input.end()) {
    if (isalpha(iter[0])) {
      auto start = iter;
      for (; iter != input.end() && isalnum(*iter); ++iter)
        ;
      auto tok = string_view(start, iter);
      cur_tok = Token{.kind = TokenKind::Id, .sv = tok, .f = 0.0, .i = 0};
      return;
    }
  }
  cerr << "expect valid identifier like `[A-Za-z][A-Za-z0-9]*`" << endl;
  throw std::runtime_error("can't find expected token");
}

void Interpreter::parseCreateTable() {
  expect("create"sv);
  expect("table"sv);
  parseId();
  table_name = cur_tok;
  expect("("sv);
  parseAttributeList();
  expect(")"sv);
  parseStatEnd();
#ifdef _DEBUG
  cout << "DEBUG: create a table named `" << table_name.sv << "`" << endl;
#endif
}

void Interpreter::parseCreateIndex() {
  expect("create"sv);
  expect("index"sv);
  parseId();
  index_name = cur_tok;
  expect("on");
  parseId();
  table_name = cur_tok;
  expect("("sv);
  parseId();
  indexed_column_name = cur_tok;
  expect(")"sv);
  parseStatEnd();
#ifdef _DEBUG
  cout << "DEBUG: create an index on `" << table_name.sv << "."
       << indexed_column_name.sv << "` named `" << index_name.sv << "`" << endl;
#endif
}

void Interpreter::parseDropTable() {
  expect("drop"sv);
  expect("table"sv);
  parseId();
  table_name = cur_tok;
  parseStatEnd();
#ifdef _DEBUG
  cout << "DEBUG: drop a table named `" << table_name.sv << "`" << endl;
#endif
}

void Interpreter::parseDropIndex() {
  expect("drop"sv);
  expect("index"sv);
  parseId();
  index_name = cur_tok;
  parseStatEnd();
#ifdef _DEBUG
  cout << "DEBUG: drop a index named `" << index_name.sv << "`" << endl;
#endif
}

void Interpreter::parseExec() {
  expect("execfile");

  auto path_end_pos = input.find_last_of(";.");
  if (path_end_pos == input.npos) {
    cerr << "not correct format of execfile";
    throw std::runtime_error("wrong execfile sentence");
  }
  auto len = path_end_pos - (iter - input.begin());
  path file_path(string(&*iter, len));

#ifdef _DEBUG
  cout << "DEBUG: exec " << file_path << endl;
#endif
}

void Interpreter::parseSelectStat() {}
void Interpreter::parseDeleteStat() {}
void Interpreter::parseInsertStat() {}

void Interpreter::parseStatEnd() {
  if (!consume(";"sv)) expect("."sv);
}

bool Interpreter::parse() {
  auto backup = iter;
  if (consume("create")) {
    if (peek("table")) {
      iter = backup;
      parseCreateTable();
    } else if (peek("index")) {
      iter = backup;
      parseCreateIndex();
    } else {
      expect("table");
    }
  } else if (consume("drop")) {
    if (peek("table")) {
      iter = backup;
      parseDropTable();
    } else if (peek("index")) {
      iter = backup;
      parseDropIndex();
    } else {
      expect("table");
    }
  } else if (peek("execfile")) {
    parseExec();
  } else if (peek("quit")) {
    skip("quit");
    parseStatEnd();
    return true;
  }
  return false;
}

Interpreter interpreter;
