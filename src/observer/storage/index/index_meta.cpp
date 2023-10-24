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
// Created by Wangyunlai.wyl on 2021/5/18.
//

#include "storage/index/index_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"
#include "json/json.h"

const static Json::StaticString FIELD_NAME("index_name");
const static Json::StaticString FIELD_COUNT("field_count");
const static Json::StaticString FIELD_FIELD_NAME("field_name");
const static Json::StaticString FIELD_IS_UNIQUE("is_unique");

RC IndexMeta::init(const char *name, const std::vector<const FieldMeta *> &fields, IndexType type)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  name_ = name;
  fields_.reserve(fields.size());
  for (int i = 0; i < fields.size(); i++) {
    fields_.push_back(fields[i]->name());
  }
  type_ = type;
  return RC::SUCCESS;
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME]  = name_;
  json_value[FIELD_COUNT] = fields_.size();
  std::string field_name_combined;
  for (int i = 0; i < fields_.size(); i++) {
    if (i > 0) {
      field_name_combined += "\n";
    }
    field_name_combined += fields_[i];
  }
  json_value[FIELD_FIELD_NAME] = field_name_combined.c_str();
  json_value[FIELD_IS_UNIQUE]  = type_ == IndexType::Unique ? true : false;
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value   = json_value[FIELD_NAME];
  const Json::Value &count_value  = json_value[FIELD_COUNT];
  const Json::Value &field_value  = json_value[FIELD_FIELD_NAME];
  const Json::Value &unique_value = json_value[FIELD_IS_UNIQUE];
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  if (!count_value.isInt()) {
    LOG_ERROR("Index count is not a int. json value=%s", count_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  if (!field_value.isString()) {
    LOG_ERROR("Field name of index [%s] is not a string. json value=%s",
        name_value.asCString(),
        field_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  if (!unique_value.isBool()) {
    LOG_ERROR("Index [%s] is_unique is not a bool. json value=%s",
        name_value.asCString(),
        unique_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  // get the fields
  std::istringstream             iss(field_value.asCString());
  std::vector<const FieldMeta *> fields;
  while (!iss.eof()) {
    std::string field_name;
    iss >> field_name;
    const FieldMeta *field = table.field(field_name.c_str());
    if (nullptr == field) {
      LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value.asCString());
      return RC::SCHEMA_FIELD_MISSING;
    }
    fields.push_back(field);
  }
  if (fields.size() != count_value.asInt()) {
    LOG_ERROR("Deserialize index [%s]: field count is not match. json value=%s",
        name_value.asCString(),
        json_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  return index.init(name_value.asCString(), fields, unique_value.asBool() ? IndexType::Unique : IndexType::NonUnique);
}

const IndexType IndexMeta::type() const { return type_; }

bool IndexMeta::is_unique() const { return type_ == IndexType::Unique; }

const char *IndexMeta::name() const { return name_.c_str(); }

const std::vector<std::string> &IndexMeta::fields() const { return fields_; }

void IndexMeta::desc(std::ostream &os) const
{
  os << "index name=" << name_;
  for (int i = 0; i < fields_.size(); i++) {
    os << ", field=" << fields_[i];
  }
}