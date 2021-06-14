#pragma once

#ifdef _DEBUG
#include <iostream>
#endif

#include "DataStructure.hpp"

struct CatalogBlock : public Block {};

class CatalogManager { // TODO(TO/GA): test
  unordered_map<string, Table> tables_;
  inline static const string kCatalogFileName = "Catalog.data";

 public:
  /**
   * @brief Construct a new Catalog Manager object. Open the file.
   *
   */
  CatalogManager();

  /**
   * @brief Destroy the Catalog Manager object. Write back to the file.
   *
   */
  ~CatalogManager();

  /**
   * @brief Create a table
   *
   * @param table_name the name of the table
   * @param attributes the attributes of the table
   * @return true if successful
   */
  bool CreateTable(
      const string &table_name,
      const vector<tuple<string, SqlValueType, SpecialAttribute>> &attributes);

  /**
   * @brief Fetch a table's catalog
   *
   * @param table_name the name of the table
   * @return the information of the table
   */
  Table TableInfo(const string &table_name);

  void PrintTables() const {
    std::cerr << "Hello World!" << std::endl;
  }
};

extern CatalogManager catalog_manager;
