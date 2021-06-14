#include "DataStructure.hpp"

#ifdef _DEBUG
#include <iostream>
#endif

void Table::read(ifstream &is) {
#ifdef _DEBUG
  std::cerr << "Fuck" << std::endl;
#endif
  size_t size;
  is.read(reinterpret_cast<char *>(&size), sizeof(size));
  table_name.resize(size);
  is.read(reinterpret_cast<char *>(table_name.data()), sizeof(char) * size);

  is.read(reinterpret_cast<char *>(&size), sizeof(size));
  while (size--) {
    size_t length;
    is.read(reinterpret_cast<char *>(&length), sizeof(length));
    string attribute_name;
    attribute_name.resize(length);
    is.read(reinterpret_cast<char *>(attribute_name.data()),
            sizeof(char) * length);
    SqlValueType type;
    is.read(reinterpret_cast<char *>(&type), sizeof(type));
    unsigned special_attribute;
    is.read(reinterpret_cast<char *>(&special_attribute),
            sizeof(special_attribute));
    size_t offset;
    is.read(reinterpret_cast<char *>(&offset), sizeof(offset));
    attributes[attribute_name] = std::make_tuple(
        type, static_cast<SpecialAttribute>(special_attribute), offset);
  }

  is.read(reinterpret_cast<char *>(&size), sizeof(size));
  while (size--) {
    size_t length;
    is.read(reinterpret_cast<char *>(&length), sizeof(length));
    string column_name;
    column_name.resize(length);
    is.read(reinterpret_cast<char *>(column_name.data()),
            sizeof(char) * length);
    is.read(reinterpret_cast<char *>(&length), sizeof(length));
    string index_name;
    index_name.resize(length);
    is.read(reinterpret_cast<char *>(index_name.data()), sizeof(char) * length);
    indexes[column_name] = index_name;
  }
#ifdef _DEBUG
  std::cerr << "Fuck" << std::endl;
#endif
}

void Table::write(ofstream &os) const {
  auto size = table_name.length();
  os.write(reinterpret_cast<const char *>(&size), sizeof(size));
  os.write(table_name.c_str(), sizeof(char) * size);

  size = attributes.size();
  os.write(reinterpret_cast<const char *>(&size), sizeof(size));
  for (const auto &attribute : attributes) {
    size = attribute.first.length();
    os.write(reinterpret_cast<const char *>(&size), sizeof(size));
    os.write(attribute.first.c_str(), sizeof(char) * size);
    const auto type = std::get<0>(attribute.second);
    os.write(reinterpret_cast<const char *>(&type), sizeof(type));
    const unsigned special_attribute =
        static_cast<unsigned>(std::get<1>(attribute.second));
    os.write(reinterpret_cast<const char *>(&special_attribute),
             sizeof(special_attribute));
    const auto offset = std::get<2>(attribute.second);
    os.write(reinterpret_cast<const char *>(&offset), sizeof(offset));
  }

  size = indexes.size();
  os.write(reinterpret_cast<const char *>(&size), sizeof(size));
  for (const auto &index : indexes) {
    size = index.first.length();
    os.write(reinterpret_cast<const char *>(&size), sizeof(size));
    os.write(index.first.c_str(), sizeof(char) * size);
    size = index.second.length();
    os.write(reinterpret_cast<const char *>(&size), sizeof(size));
    os.write(index.second.c_str(), sizeof(char) * size);
  }
}
