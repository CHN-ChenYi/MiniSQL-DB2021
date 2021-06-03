#include "API.hpp"
#include "BufferManager.hpp"
#include "CatalogManager.hpp"
#include "IndexManager.hpp"
#include "Interpreter.hpp"
#include "RecordManager.hpp"

int main() {
  API();
  BufferManager();
  CatalogManager();
  IndexManager();
  Interpreter();
  RecordManager();
  return 0;
}
