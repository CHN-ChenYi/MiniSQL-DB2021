#include "DataStructure.hpp"

void Table::read(ifstream &is) {
  size_t size;
  is.read(reinterpret_cast<char *>(&size), sizeof(size));
  table_name.resize(size);
  is.read(reinterpret_cast<char *>(table_name.data()), sizeof(char) * size);

  is.read(reinterpret_cast<char *>(&size), sizeof(size));
  while (size--) {
    size_t index;
    is.read(reinterpret_cast<char *>(&index), sizeof(index));
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
        index, type, static_cast<SpecialAttribute>(special_attribute), offset);
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
}

void Table::write(ofstream &os) const {
  auto size = table_name.length();
  os.write(reinterpret_cast<const char *>(&size), sizeof(size));
  os.write(table_name.c_str(), sizeof(char) * size);

  size = attributes.size();
  os.write(reinterpret_cast<const char *>(&size), sizeof(size));
  for (const auto &attribute : attributes) {
    const auto index = std::get<0>(attribute.second);
    os.write(reinterpret_cast<const char *>(&index), sizeof(index));
    size = attribute.first.length();
    os.write(reinterpret_cast<const char *>(&size), sizeof(size));
    os.write(attribute.first.c_str(), sizeof(char) * size);
    const auto type = std::get<1>(attribute.second);
    os.write(reinterpret_cast<const char *>(&type), sizeof(type));
    const unsigned special_attribute =
        static_cast<unsigned>(std::get<2>(attribute.second));
    os.write(reinterpret_cast<const char *>(&special_attribute),
             sizeof(special_attribute));
    const auto offset = std::get<3>(attribute.second);
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

size_t Table::getAttributeSize() const {
  size_t len = 0;
  for (auto &[_1, attr] : attributes) {
    auto &[_2, type, _3, _4] = attr;
    switch (type) {
      case static_cast<SqlValueType>(SqlValueTypeBase::Integer):
        len += sizeof(int);
        break;
      case static_cast<SqlValueType>(SqlValueTypeBase::Float):
        len += sizeof(float);
        break;
      default: {
        len += type - static_cast<SqlValueType>(SqlValueTypeBase::String);
        break;
      }
    }
  }
  return len;
}

Tuple Table::makeEmptyTuple() const {
  static Tuple res;
  if (!res.values.empty()) res.values.clear();
  size_t cnt = attributes.size();
  res.values.resize(cnt);
  for (auto &[_1, attr] : attributes) {
    auto &[idx, type, _3, _4] = attr;
    res.values[idx].type = type;
  }
  return res;
}
