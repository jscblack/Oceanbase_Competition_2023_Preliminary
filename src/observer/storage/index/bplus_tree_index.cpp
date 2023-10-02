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
// Created by wangyunlai.wyl on 2021/5/19.
//

#include "storage/index/bplus_tree_index.h"
#include "common/log/log.h"

BplusTreeIndex::~BplusTreeIndex() noexcept { close(); }

RC BplusTreeIndex::create(const char *file_name, const IndexMeta &index_meta, const std::vector<FieldMeta> &fields_meta)
{
  if (inited_) {
    LOG_WARN("Failed to create index due to the index has been created before. file_name:%s, index:%s, fields:%s",
        file_name,
        index_meta.name(),
        index_meta.fields()[0].c_str());
    return RC::RECORD_OPENNED;
  }

  Index::init(index_meta, fields_meta);
  AttrType *types = new AttrType[fields_meta_.size()];
  int      *lens  = new int[fields_meta_.size()];
  for (int i = 0; i < fields_meta_.size(); i++) {
    types[i] = fields_meta_[i].type();
    lens[i]  = fields_meta_[i].len();
  }
  RC rc = index_handler_.create(file_name, fields_meta_.size(), types, lens);
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to create index_handler, file_name:%s, index:%s, field:%s, rc:%s",
        file_name,
        index_meta.name(),
        index_meta.fields()[0].c_str(),
        strrc(rc));
    return rc;
  }

  inited_ = true;
  LOG_INFO(
      "Successfully create index, file_name:%s, index:%s, field:%s", file_name, index_meta.name(), index_meta.fields()[0].c_str());
  return RC::SUCCESS;
}

RC BplusTreeIndex::open(const char *file_name, const IndexMeta &index_meta, const std::vector<FieldMeta> &fields_meta)
{
  if (inited_) {
    LOG_WARN("Failed to open index due to the index has been initedd before. file_name:%s, index:%s, field:%s",
        file_name,
        index_meta.name(),
        index_meta.fields()[0].c_str());
    return RC::RECORD_OPENNED;
  }

  Index::init(index_meta, fields_meta);

  RC rc = index_handler_.open(file_name);
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to open index_handler, file_name:%s, index:%s, field:%s, rc:%s",
        file_name,
        index_meta.name(),
        index_meta.fields()[0].c_str(),
        strrc(rc));
    return rc;
  }

  inited_ = true;
  LOG_INFO(
      "Successfully open index, file_name:%s, index:%s, field:%s", file_name, index_meta.name(), index_meta.fields()[0].c_str());
  return RC::SUCCESS;
}

RC BplusTreeIndex::close()
{
  if (inited_) {
    LOG_INFO("Begin to close index, index:%s, field:%s", index_meta_.name(), index_meta_.fields()[0].c_str());
    index_handler_.close();
    inited_ = false;
  }
  LOG_INFO("Successfully close index.");
  return RC::SUCCESS;
}

RC BplusTreeIndex::drop()
{
  if (inited_) {
    LOG_INFO("Begin to drop index, index:%s, field:%s", index_meta_.name(), index_meta_.fields()[0].c_str());
    index_handler_.drop();
    inited_ = false;
  }
  LOG_INFO("Successfully drop index.");
  return RC::SUCCESS;
}

int BplusTreeIndex::get_user_key(const char *record, char *&user_key)
{
  // 构造user_key
  int attr_length_sum = 0;
  for (int i = 0; i < fields_meta_.size(); i++) {
    attr_length_sum += fields_meta_[i].len();
  }
  user_key   = new char[attr_length_sum];
  int offset = 0;
  for (int i = 0; i < fields_meta_.size(); i++) {
    memcpy(user_key + offset, record + fields_meta_[i].offset(), fields_meta_[i].len());
    offset += fields_meta_[i].len();
  }
  return offset;
}

RC BplusTreeIndex::insert_entry(const char *record, const RID *rid)
{
  // 构造user_key
  char *user_key        = nullptr;
  int   attr_length_sum = get_user_key(record, user_key);
  return index_handler_.insert_entry(user_key, rid);
}

RC BplusTreeIndex::delete_entry(const char *record, const RID *rid)
{
  // 构造user_key
  char *user_key        = nullptr;
  int   attr_length_sum = get_user_key(record, user_key);
  return index_handler_.delete_entry(user_key, rid);
}

RC BplusTreeIndex::get_entry(const char *record, list<RID> &rids)
{
  // 构造user_key
  char *user_key        = nullptr;
  int   attr_length_sum = get_user_key(record, user_key);
  return index_handler_.get_entry(user_key, attr_length_sum, rids);
}

IndexScanner *BplusTreeIndex::create_scanner(
    const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len, bool right_inclusive)
{
  BplusTreeIndexScanner *index_scanner = new BplusTreeIndexScanner(index_handler_);
  RC rc = index_scanner->open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open index scanner. rc=%d:%s", rc, strrc(rc));
    delete index_scanner;
    return nullptr;
  }
  return index_scanner;
}

RC BplusTreeIndex::sync() { return index_handler_.sync(); }

////////////////////////////////////////////////////////////////////////////////
BplusTreeIndexScanner::BplusTreeIndexScanner(BplusTreeHandler &tree_handler) : tree_scanner_(tree_handler) {}

BplusTreeIndexScanner::~BplusTreeIndexScanner() noexcept { tree_scanner_.close(); }

RC BplusTreeIndexScanner::open(
    const char *left_key, int left_len, bool left_inclusive, const char *right_key, int right_len, bool right_inclusive)
{
  return tree_scanner_.open(left_key, left_len, left_inclusive, right_key, right_len, right_inclusive);
}

RC BplusTreeIndexScanner::next_entry(RID *rid) { return tree_scanner_.next_entry(*rid); }

RC BplusTreeIndexScanner::destroy()
{
  delete this;
  return RC::SUCCESS;
}
