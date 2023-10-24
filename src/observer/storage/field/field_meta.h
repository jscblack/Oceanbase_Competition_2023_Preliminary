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

#pragma once

#include <string>

#include "common/rc.h"
#include "sql/parser/parse_defs.h"

namespace Json {
class Value;
}  // namespace Json

/**
 * @brief 字段元数据
 *
 */
class FieldMeta
{
public:
  FieldMeta();
  FieldMeta(const char *name, AttrType attr_type, int attr_offset, int attr_len, bool nullable, bool visible);
  ~FieldMeta() = default;

  RC init(const char *name, AttrType attr_type, int attr_offset, int attr_len, bool nullable, bool visible);

public:
  const char *name() const;
  AttrType    type() const;
  int         offset() const;
  int         len() const;
  bool        nullable() const;
  bool        visible() const;

public:
  void desc(std::ostream &os) const;

public:
  void      to_json(Json::Value &json_value) const;
  static RC from_json(const Json::Value &json_value, FieldMeta &field);

protected:
  std::string name_;         // 字段名
  AttrType    attr_type_;    // 字段类型
  int         attr_offset_;  // 数据偏移量
  int         attr_len_;     // 数据长度
  bool        nullable_;     // 是否可空（默认可空）
  bool        visible_;      // 是否可见（默认可见）
};
