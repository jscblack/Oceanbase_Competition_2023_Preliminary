/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Wangyunlai on 2021/5/12.
//

#include <algorithm>
#include <common/lang/string.h>

#include "common/log/log.h"
#include "storage/table/table_meta.h"
#include "storage/trx/trx.h"
#include "json/json.h"

using namespace std;

static const Json::StaticString FIELD_TABLE_ID("table_id");
static const Json::StaticString FIELD_TABLE_NAME("table_name");
static const Json::StaticString FIELD_FIELDS("fields");
static const Json::StaticString FIELD_INDEXES("indexes");
static const Json::StaticString FIELD_IS_VIEW("is_view");
static const Json::StaticString FIELD_VIEW_SQL("view_sql");

TableMeta::TableMeta(const TableMeta &other)
    : table_id_(other.table_id_),
      name_(other.name_),
      fields_(other.fields_),
      indexes_(other.indexes_),
      record_size_(other.record_size_)
{}

void TableMeta::swap(TableMeta &other) noexcept
{
  name_.swap(other.name_);
  fields_.swap(other.fields_);
  indexes_.swap(other.indexes_);
  std::swap(record_size_, other.record_size_);
}

RC TableMeta::init(int32_t table_id, const char *name, int field_num, const AttrInfoSqlNode attributes[])
{
  if (common::is_blank(name)) {
    LOG_ERROR("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }

  if (field_num <= 0 || nullptr == attributes) {
    LOG_ERROR("Invalid argument. name=%s, field_num=%d, attributes=%p", name, field_num, attributes);
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;

  int field_offset = 0;
  // 添加系统字段，trxfield，不可见
  int                      sys_field_num = 0;
  int                      trx_field_num = 0;
  const vector<FieldMeta> *trx_fields    = TrxKit::instance()->trx_fields();
  if (trx_fields != nullptr) {
    fields_.resize(field_num + trx_fields->size());

    for (size_t i = 0; i < trx_fields->size(); i++) {
      const FieldMeta &field_meta = (*trx_fields)[i];
      fields_[i]                  = FieldMeta(
          field_meta.name(), field_meta.type(), field_offset, field_meta.len(), false /*nullable*/, false /*visible*/);
      field_offset += field_meta.len();
    }

    trx_field_num = static_cast<int>(trx_fields->size());
  } else {
    fields_.resize(field_num);
  }
  sys_field_num += trx_field_num;

  // 添加系统字段，nullfield，不可见
  // 是一个bitmap，每个bit对应一个字段，表示该字段是否为null
  // 一个字段占一个bit，使用chars去保存，每个char占8bit，所以需要的长度为ceil(field_num/8)
  fields_.resize(fields_.size() + 1);
  int null_field_len     = (field_num + 7) / 8;
  fields_[sys_field_num] = FieldMeta(
      "__field_is_null", AttrType::CHARS, field_offset, null_field_len, false /*nullable*/, false /*visible*/);
  field_offset += 1;
  sys_field_num += 1;

  // 添加数据字段
  for (int i = 0; i < field_num; i++) {
    const AttrInfoSqlNode &attr_info = attributes[i];
    rc                               = fields_[i + sys_field_num].init(
        attr_info.name.c_str(), attr_info.type, field_offset, attr_info.length, attr_info.nullable, true /*visible*/);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to init field meta. table name=%s, field name: %s", name, attr_info.name.c_str());
      return rc;
    }

    field_offset += attr_info.length;
  }

  record_size_ = field_offset;

  table_id_ = table_id;
  name_     = name;
  is_view_ = false;
  LOG_INFO("Sussessfully initialized table meta. table id=%d, name=%s", table_id, name);
  return RC::SUCCESS;
}

RC TableMeta::init(int32_t table_id, const char *name, int field_num, const AttrInfoSqlNode attributes[], const std::string &sql)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }

  if (field_num <= 0 || nullptr == attributes) {
    LOG_ERROR("Invalid argument. name=%s, field_num=%d, attributes=%p", name, field_num, attributes);
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;

  int field_offset = 0;
  // 添加系统字段，trxfield，不可见
  int                      sys_field_num = 0;
  int                      trx_field_num = 0;
  const vector<FieldMeta> *trx_fields    = TrxKit::instance()->trx_fields();
  if (trx_fields != nullptr) {
    fields_.resize(field_num + trx_fields->size());

    for (size_t i = 0; i < trx_fields->size(); i++) {
      const FieldMeta &field_meta = (*trx_fields)[i];
      fields_[i]                  = FieldMeta(
          field_meta.name(), field_meta.type(), field_offset, field_meta.len(), false /*nullable*/, false /*visible*/);
      field_offset += field_meta.len();
    }

    trx_field_num = static_cast<int>(trx_fields->size());
  } else {
    fields_.resize(field_num);
  }
  sys_field_num += trx_field_num;

  // 添加系统字段，nullfield，不可见
  // 是一个bitmap，每个bit对应一个字段，表示该字段是否为null
  // 一个字段占一个bit，使用chars去保存，每个char占8bit，所以需要的长度为ceil(field_num/8)
  fields_.resize(fields_.size() + 1);
  int null_field_len     = (field_num + 7) / 8;
  fields_[sys_field_num] = FieldMeta(
      "__field_is_null", AttrType::CHARS, field_offset, null_field_len, false /*nullable*/, false /*visible*/);
  field_offset += 1;
  sys_field_num += 1;

  // 添加数据字段
  for (int i = 0; i < field_num; i++) {
    const AttrInfoSqlNode &attr_info = attributes[i];
    rc                               = fields_[i + sys_field_num].init(
        attr_info.name.c_str(), attr_info.type, field_offset, attr_info.length, attr_info.nullable, true /*visible*/);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to init field meta. table name=%s, field name: %s", name, attr_info.name.c_str());
      return rc;
    }

    field_offset += attr_info.length;
  }

  record_size_ = field_offset;

  table_id_ = table_id;
  name_     = name;
  is_view_ = true;
  view_sql_ = sql;
  LOG_INFO("Sussessfully initialized table meta. table id=%d, name=%s", table_id, name);
  return RC::SUCCESS;
}

RC TableMeta::add_index(const IndexMeta &index)
{
  indexes_.push_back(index);
  return RC::SUCCESS;
}

const char *TableMeta::name() const { return name_.c_str(); }

const FieldMeta *TableMeta::trx_field() const { return &fields_[0]; }

const std::pair<const FieldMeta *, int> TableMeta::trx_fields() const
{
  return std::pair<const FieldMeta *, int>{fields_.data(), trx_field_num()};
}

const FieldMeta *TableMeta::field(int index) const { return &fields_[index]; }

// TODO 根据name找index
int TableMeta::find_field_index_by_name(const char *name) const
{
  if (nullptr == name) {
    return -1;
  }
  int i = 0;
  for (const FieldMeta &field : fields_) {
    if (0 == strcmp(field.name(), name)) {
      return i;
    }
    i++;
  }
  return -1;
}

int TableMeta::find_field_index_of_user_field_by_name(const char *name) const
{
  if (nullptr == name) {
    return -1;
  }
  int i = 0;
  for (const FieldMeta &field : fields_) {
    if (0 == strcmp(field.name(), name)) {
      return i - sys_field_num();
    }
    i++;
  }
  return -1;
}

const FieldMeta *TableMeta::field(const char *name) const
{
  if (nullptr == name) {
    return nullptr;
  }
  for (const FieldMeta &field : fields_) {
    if (0 == strcmp(field.name(), name)) {
      return &field;
    }
  }
  return nullptr;
}

const FieldMeta *TableMeta::find_field_by_offset(int offset) const
{
  for (const FieldMeta &field : fields_) {
    if (field.offset() == offset) {
      return &field;
    }
  }
  return nullptr;
}

const FieldMeta *TableMeta::null_field() const
{
  assert(strcmp(fields_[trx_field_num()].name(), "__field_is_null") == 0);
  return &fields_[trx_field_num()];
}

bool TableMeta::is_field_null(const char *data, const char *field_name) const
{
  const FieldMeta *null_field_meta = null_field();
  const char      *null_field_data = data + null_field_meta->offset();
  for (int i = 0; i < field_num(); i++) {
    const FieldMeta *field_meta = field(i + sys_field_num());
    if (strcmp(field_meta->name(), field_name) == 0) {
      return (null_field_data[i / CHAR_BIT] & (1 << i % CHAR_BIT)) != 0;
    }
  }
  // field_meta->offset()
  return true;
}

int TableMeta::field_num() const { return fields_.size(); }

int TableMeta::trx_field_num() const
{
  const vector<FieldMeta> *trx_fields = TrxKit::instance()->trx_fields();
  if (nullptr == trx_fields) {
    return 0;
  }
  return static_cast<int>(trx_fields->size());
}

int TableMeta::null_field_num() const { return 1; }

int TableMeta::sys_field_num() const { return trx_field_num() + null_field_num(); }

const IndexMeta *TableMeta::index(const char *name) const
{
  for (const IndexMeta &index : indexes_) {
    if (0 == strcmp(index.name(), name)) {
      return &index;
    }
  }
  return nullptr;
}

const IndexMeta *TableMeta::find_index_by_field(const char *field) const
{
  for (const IndexMeta &index : indexes_) {
    if (1 == index.fields().size() && 0 == strcmp(index.fields()[0].c_str(), field)) {
      return &index;
    }
  }
  return nullptr;
}

const IndexMeta *TableMeta::index(int i) const { return &indexes_[i]; }

int TableMeta::index_num() const { return indexes_.size(); }

int TableMeta::record_size() const { return record_size_; }

int TableMeta::serialize(std::ostream &ss) const
{

  Json::Value table_value;
  table_value[FIELD_TABLE_ID]   = table_id_;
  table_value[FIELD_TABLE_NAME] = name_;

  Json::Value fields_value;
  for (const FieldMeta &field : fields_) {
    Json::Value field_value;
    field.to_json(field_value);
    fields_value.append(std::move(field_value));
  }

  table_value[FIELD_FIELDS] = std::move(fields_value);

  Json::Value indexes_value;
  for (const auto &index : indexes_) {
    Json::Value index_value;
    index.to_json(index_value);
    indexes_value.append(std::move(index_value));
  }
  table_value[FIELD_INDEXES] = std::move(indexes_value);

  table_value[FIELD_IS_VIEW] = is_view_;

  if (is_view_) {
    table_value[FIELD_VIEW_SQL] = view_sql_;
  }

  Json::StreamWriterBuilder builder;
  Json::StreamWriter       *writer = builder.newStreamWriter();

  std::streampos old_pos = ss.tellp();
  writer->write(table_value, &ss);
  int ret = (int)(ss.tellp() - old_pos);

  delete writer;
  return ret;
}

int TableMeta::deserialize(std::istream &is)
{
  Json::Value             table_value;
  Json::CharReaderBuilder builder;
  std::string             errors;

  std::streampos old_pos = is.tellg();
  if (!Json::parseFromStream(builder, is, &table_value, &errors)) {
    LOG_ERROR("Failed to deserialize table meta. error=%s", errors.c_str());
    return -1;
  }

  const Json::Value &table_id_value = table_value[FIELD_TABLE_ID];
  if (!table_id_value.isInt()) {
    LOG_ERROR("Invalid table id. json value=%s", table_id_value.toStyledString().c_str());
    return -1;
  }

  int32_t table_id = table_id_value.asInt();

  const Json::Value &table_name_value = table_value[FIELD_TABLE_NAME];
  if (!table_name_value.isString()) {
    LOG_ERROR("Invalid table name. json value=%s", table_name_value.toStyledString().c_str());
    return -1;
  }

  std::string table_name = table_name_value.asString();

  const Json::Value &fields_value = table_value[FIELD_FIELDS];
  if (!fields_value.isArray() || fields_value.size() <= 0) {
    LOG_ERROR("Invalid table meta. fields is not array, json value=%s", fields_value.toStyledString().c_str());
    return -1;
  }

  RC                     rc        = RC::SUCCESS;
  int                    field_num = fields_value.size();
  std::vector<FieldMeta> fields(field_num);
  for (int i = 0; i < field_num; i++) {
    FieldMeta &field = fields[i];

    const Json::Value &field_value = fields_value[i];
    rc                             = FieldMeta::from_json(field_value, field);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to deserialize table meta. table name =%s", table_name.c_str());
      return -1;
    }
  }

  auto comparator = [](const FieldMeta &f1, const FieldMeta &f2) { return f1.offset() < f2.offset(); };
  std::sort(fields.begin(), fields.end(), comparator);

  table_id_ = table_id;
  name_.swap(table_name);
  fields_.swap(fields);
  record_size_ = fields_.back().offset() + fields_.back().len() - fields_.begin()->offset();

  const Json::Value &indexes_value = table_value[FIELD_INDEXES];
  if (!indexes_value.empty()) {
    if (!indexes_value.isArray()) {
      LOG_ERROR("Invalid table meta. indexes is not array, json value=%s", fields_value.toStyledString().c_str());
      return -1;
    }
    const int              index_num = indexes_value.size();
    std::vector<IndexMeta> indexes(index_num);
    for (int i = 0; i < index_num; i++) {
      IndexMeta &index = indexes[i];

      const Json::Value &index_value = indexes_value[i];
      rc                             = IndexMeta::from_json(*this, index_value, index);
      if (rc != RC::SUCCESS) {
        LOG_ERROR("Failed to deserialize table meta. table name=%s", table_name.c_str());
        return -1;
      }
    }
    indexes_.swap(indexes);
  }

  const Json::Value &is_view_value = table_value[FIELD_IS_VIEW];
  if (!is_view_value.isBool()) {
    LOG_ERROR("Invalid table is_view. json value=%s",is_view_value.toStyledString().c_str());
    return -1;
  }

  bool is_view = is_view_value.asBool();
  is_view_ = is_view;

  if (is_view) {
    const Json::Value &view_sql_value = table_value[FIELD_VIEW_SQL];
    if (!view_sql_value.isString()) {
      LOG_ERROR("Invalid view sql. json value=%s", view_sql_value.toStyledString().c_str());
      return -1;
    }
    std::string view_sql = view_sql_value.asString();
    view_sql_.swap(view_sql);
  }

  return (int)(is.tellg() - old_pos);
}

int TableMeta::get_serial_size() const { return -1; }

void TableMeta::to_string(std::string &output) const {}

void TableMeta::desc(std::ostream &os) const
{
  os << name_ << '(' << std::endl;
  for (const auto &field : fields_) {
    os << '\t';
    field.desc(os);
    os << std::endl;
  }

  for (const auto &index : indexes_) {
    os << '\t';
    index.desc(os);
    os << std::endl;
  }
  os << ')' << std::endl;
}
