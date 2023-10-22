/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Wangyunlai on 2021/5/13.
//

#include <algorithm>
#include <limits.h>
#include <string.h>

#include "common/defs.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "storage/common/condition_filter.h"
#include "storage/common/meta_util.h"
#include "storage/index/bplus_tree_index.h"
#include "storage/index/index.h"
#include "storage/record/record_manager.h"
#include "storage/table/table.h"
#include "storage/table/table_meta.h"
#include "storage/trx/trx.h"

Table::~Table()
{
  if (record_handler_ != nullptr) {
    delete record_handler_;
    record_handler_ = nullptr;
  }

  if (data_buffer_pool_ != nullptr) {
    data_buffer_pool_->close_file();
    data_buffer_pool_ = nullptr;
  }

  for (std::vector<Index *>::iterator it = indexes_.begin(); it != indexes_.end(); ++it) {
    Index *index = *it;
    delete index;
  }
  indexes_.clear();

  LOG_INFO("Table has been closed: %s", name());
}

RC Table::create(int32_t table_id, const char *path, const char *name, const char *base_dir, int attribute_count,
    const AttrInfoSqlNode attributes[])
{
  if (table_id < 0) {
    LOG_WARN("invalid table id. table_id=%d, table_name=%s", table_id, name);
    return RC::INVALID_ARGUMENT;
  }

  if (common::is_blank(name)) {
    LOG_WARN("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }
  LOG_INFO("Begin to create table %s:%s", base_dir, name);

  if (attribute_count <= 0 || nullptr == attributes) {
    LOG_WARN("Invalid arguments. table_name=%s, attribute_count=%d, attributes=%p", name, attribute_count, attributes);
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;

  // 使用 table_name.table记录一个表的元数据
  // 判断表文件是否已经存在
  int fd = ::open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    if (EEXIST == errno) {
      LOG_ERROR("Failed to create table file, it has been created. %s, EEXIST, %s", path, strerror(errno));
      return RC::SCHEMA_TABLE_EXIST;
    }
    LOG_ERROR("Create table file failed. filename=%s, errmsg=%d:%s", path, errno, strerror(errno));
    return RC::IOERR_OPEN;
  }

  close(fd);

  // 创建文件
  if ((rc = table_meta_.init(table_id, name, attribute_count, attributes)) != RC::SUCCESS) {
    LOG_ERROR("Failed to init table meta. name:%s, ret:%d", name, rc);
    return rc;  // delete table file
  }

  std::fstream fs;
  fs.open(path, std::ios_base::out | std::ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", path, strerror(errno));
    return RC::IOERR_OPEN;
  }

  // 记录元数据到文件中
  table_meta_.serialize(fs);
  fs.close();

  std::string        data_file = table_data_file(base_dir, name);
  BufferPoolManager &bpm       = BufferPoolManager::instance();
  rc                           = bpm.create_file(data_file.c_str());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create disk buffer pool of data file. file name=%s", data_file.c_str());
    return rc;
  }

  rc = init_record_handler(base_dir);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create table %s due to init record handler failed.", data_file.c_str());
    // don't need to remove the data_file
    return rc;
  }

  base_dir_ = base_dir;
  LOG_INFO("Successfully create table %s:%s", base_dir, name);
  return rc;
}

RC Table::drop(const char *path)
{
  LOG_INFO("Begin to drop table %s", name());
  RC rc = RC::SUCCESS;
  // 删除索引indexes_
  for (Index *index : indexes_) {
    // recast to BplusTreeIndex
    BplusTreeIndex *bpt_index  = dynamic_cast<BplusTreeIndex *>(index);
    std::string     index_file = table_index_file(base_dir_.c_str(), name(), bpt_index->index_meta().name());
    bpt_index->drop();
  }
  // 删除关联record_handler
  record_handler_->close();
  delete record_handler_;
  record_handler_ = nullptr;
  // 删除关联bufferpool，删除关联文件
  std::string        data_file = table_data_file(base_dir_.c_str(), name());
  BufferPoolManager &bpm       = BufferPoolManager::instance();
  rc                           = bpm.remove_file(data_file.c_str());
  // 删除源数据文件
  if (remove(path)) {
    // failed
    LOG_ERROR("Failed to drop table %s due to unknown error in deleting file.", name());
    rc = RC::FILE_ERROR;
    return rc;
  }

  LOG_INFO("Successfully drop table %s", name());
  return rc;
}

RC Table::open(const char *meta_file, const char *base_dir)
{
  // 加载元数据文件
  std::fstream fs;
  std::string  meta_file_path = std::string(base_dir) + common::FILE_PATH_SPLIT_STR + meta_file;
  fs.open(meta_file_path, std::ios_base::in | std::ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open meta file for read. file name=%s, errmsg=%s", meta_file_path.c_str(), strerror(errno));
    return RC::IOERR_OPEN;
  }
  if (table_meta_.deserialize(fs) < 0) {
    LOG_ERROR("Failed to deserialize table meta. file name=%s", meta_file_path.c_str());
    fs.close();
    return RC::INTERNAL;
  }
  fs.close();

  // 加载数据文件
  RC rc = init_record_handler(base_dir);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open table %s due to init record handler failed.", base_dir);
    // don't need to remove the data_file
    return rc;
  }

  base_dir_ = base_dir;

  const int              index_num = table_meta_.index_num();
  std::vector<FieldMeta> field_metas;

  for (int i = 0; i < index_num; i++) {
    const IndexMeta *index_meta = table_meta_.index(i);
    for (int j = 0; j < index_meta->fields().size(); j++) {
      const FieldMeta *field_meta = table_meta_.field(index_meta->fields()[j].c_str());
      if (field_meta == nullptr) {
        LOG_ERROR("Found invalid index meta info which has a non-exists field. table=%s, index=%s, field=%s",
                name(), index_meta->name(), index_meta->fields()[j].c_str());
        // skip cleanup
        //  do all cleanup action in destructive Table function
        return RC::INTERNAL;
      }
      field_metas.push_back(*field_meta);
    }
    BplusTreeIndex *index      = new BplusTreeIndex();
    std::string     index_file = table_index_file(base_dir, name(), index_meta->name());
    rc                         = index->open(index_file.c_str(), *index_meta, field_metas);
    if (rc != RC::SUCCESS) {
      delete index;
      LOG_ERROR("Failed to open index. table=%s, index=%s, file=%s, rc=%s",
                name(), index_meta->name(), index_file.c_str(), strrc(rc));
      // skip cleanup
      //  do all cleanup action in destructive Table function.
      return rc;
    }
    indexes_.push_back(index);
  }

  return rc;
}

RC Table::insert_record(Record &record)
{
  RC rc = RC::SUCCESS;
  // 检查索引中是否有存在的项
  for (Index *index : indexes_) {
    if (index->index_meta().is_unique()) {
      // null破坏唯一索引
      // 索引字段中带有null的record不保证唯一性，直接跳过
      bool has_null = false;
      for (auto field_name : index->index_meta().fields()) {
        if (table_meta_.is_field_null(record.data(), field_name.c_str())) {
          has_null = true;
          break;
        }
      }
      if (has_null) {
        continue;
      }

      // 需要首先检查插入后的情况是否会有键值重复的情况
      std::list<RID> rids;
      rc = index->get_entry(record.data(), rids);
      if (rc != RC::SUCCESS) {
        LOG_ERROR("Failed to get entry from index. table name=%s, index name=%s, rc=%s",
                  name(), index->index_meta().name(), strrc(rc));
        return rc;
      }
      // null破坏唯一索引，插入的0值与null值在索引层面是相同的，但是record层面是不同的
      // 也就意味着同样需要检查返回的这一堆看似重复的record中是否有null值
      // 如果有，那就不构成重复
      for (auto rid : rids) {
        RecordPageHandler record_page_handler;
        record_page_handler.cleanup();
        Record tmp_record;
        rc = record_handler_->get_record(record_page_handler, &rid, true /*readonly*/, &tmp_record);
        if (rc != RC::SUCCESS) {
          LOG_ERROR("Failed to get record from record handler. table name=%s, rc=%s",
                      name(), strrc(rc));
          return rc;
        }
        for (auto field_name : index->index_meta().fields()) {
          if (table_meta_.is_field_null(tmp_record.data(), field_name.c_str())) {
            rids.remove(rid);
          }
        }
      }

      if (rids.size() >= 1) {
        LOG_ERROR("Found duplicated key in unique index. table name=%s, index name=%s, rc=%s",
                  name(), index->index_meta().name(), strrc(rc));
        return RC::UNIQUE_INDEX_CONFLICT;
      }
    }
  }

  rc = record_handler_->insert_record(record.data(), table_meta_.record_size(), &record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert record failed. table name=%s, rc=%s", table_meta_.name(), strrc(rc));
    return rc;
  }

  rc = insert_entry_of_indexes(record.data(), record.rid());
  if (rc != RC::SUCCESS) {  // 可能出现了键值重复
    RC rc2 = delete_entry_of_indexes(record.data(), record.rid(), false /*error_on_not_exists*/);
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
    rc2 = record_handler_->delete_record(&record.rid());
    if (rc2 != RC::SUCCESS) {
      LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
  }
  return rc;
}

RC Table::visit_record(const RID &rid, bool readonly, std::function<RC(Record &)> visitor)
{
  return record_handler_->visit_record(rid, readonly, visitor);
}

RC Table::get_record(const RID &rid, Record &record)
{
  const int record_size = table_meta_.record_size();
  char     *record_data = (char *)malloc(record_size);
  ASSERT(nullptr != record_data, "failed to malloc memory. record data size=%d", record_size);

  auto copier = [&record, record_data, record_size](Record &record_src) -> RC {
    memcpy(record_data, record_src.data(), record_size);
    record.set_rid(record_src.rid());
    return RC::SUCCESS;
  };
  RC rc = record_handler_->visit_record(rid, true /*readonly*/, copier);
  if (rc != RC::SUCCESS) {
    free(record_data);
    LOG_WARN("failed to visit record. rid=%s, table=%s, rc=%s", rid.to_string().c_str(), name(), strrc(rc));
    return rc;
  }

  record.set_data_owner(record_data, record_size);
  return rc;
}

RC Table::recover_insert_record(Record &record)
{
  RC rc = RC::SUCCESS;
  rc    = record_handler_->recover_insert_record(record.data(), table_meta_.record_size(), record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert record failed. table name=%s, rc=%s", table_meta_.name(), strrc(rc));
    return rc;
  }

  rc = insert_entry_of_indexes(record.data(), record.rid());
  if (rc != RC::SUCCESS) {  // 可能出现了键值重复
    RC rc2 = delete_entry_of_indexes(record.data(), record.rid(), false /*error_on_not_exists*/);
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
    rc2 = record_handler_->delete_record(&record.rid());
    if (rc2 != RC::SUCCESS) {
      LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
  }
  return rc;
}

const char *Table::name() const { return table_meta_.name(); }

const TableMeta &Table::table_meta() const { return table_meta_; }

RC Table::make_record(int value_num, const Value *values, Record &record)
{
  // 检查字段类型是否一致
  if (value_num + table_meta_.sys_field_num() != table_meta_.field_num()) {
    LOG_WARN("Input values don't match the table's schema, table name:%s", table_meta_.name());
    return RC::SCHEMA_FIELD_MISSING;
  }

  const int normal_field_start_index = table_meta_.sys_field_num();
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field = table_meta_.field(i + normal_field_start_index);
    const Value     &value = values[i];
    if (value.attr_type() != AttrType::NONE && field->type() != value.attr_type()) {
      // 这里应该不会走到，前面都处理过了
      LOG_ERROR("Invalid value type. table name =%s, field name=%s, type=%d, but given=%d",
                table_meta_.name(), field->name(), field->type(), value.attr_type());
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }

  // 复制所有字段的值
  int   record_size = table_meta_.record_size();
  char *record_data = (char *)calloc(record_size, record_size);
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field = table_meta_.field(i + normal_field_start_index);
    const Value     &value = values[i];
    if (value.attr_type() == AttrType::NONE) {
      // 空值额外处理
      memset(record_data + field->offset(), 0, field->len());
      const FieldMeta *null_field = table_meta_.null_field();
      // 取出null_field的值
      char *null_field_data = record_data + null_field->offset();
      // 设置null_field的值
      null_field_data[i / CHAR_BIT] |= (1 << (i % CHAR_BIT));
    } else {
      size_t copy_len = field->len();
      if (field->type() == CHARS) {
        const size_t data_len = value.length();
        if (copy_len > data_len) {
          copy_len = data_len + 1;
        }
      }
      memcpy(record_data + field->offset(), value.data(), copy_len);
    }
  }

  record.set_data_owner(record_data, record_size);
  return RC::SUCCESS;
}

RC Table::init_record_handler(const char *base_dir)
{
  std::string data_file = table_data_file(base_dir, table_meta_.name());

  RC rc = BufferPoolManager::instance().open_file(data_file.c_str(), data_buffer_pool_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open disk buffer pool for file:%s. rc=%d:%s", data_file.c_str(), rc, strrc(rc));
    return rc;
  }

  record_handler_ = new RecordFileHandler();
  rc              = record_handler_->init(data_buffer_pool_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to init record handler. rc=%s", strrc(rc));
    data_buffer_pool_->close_file();
    data_buffer_pool_ = nullptr;
    delete record_handler_;
    record_handler_ = nullptr;
    return rc;
  }

  return rc;
}

RC Table::get_record_scanner(RecordFileScanner &scanner, Trx *trx, bool readonly)
{
  RC rc = scanner.open_scan(this, *data_buffer_pool_, trx, readonly, nullptr);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open scanner. rc=%s", strrc(rc));
  }
  return rc;
}

RC Table::create_index(Trx *trx, std::vector<const FieldMeta *> fields_meta, const char *index_name, bool is_unique)
{
  // 只有null的字段可以bypass掉unique的限制
  // 在当前情况下先做unique index
  // const FieldMeta *field_meta = field_metas[0];  // TODO: support multi-field index
  if (common::is_blank(index_name) || fields_meta.empty()) {
    LOG_INFO("Invalid input arguments, table name is %s, index_name is blank or attribute_name is blank", name());
    return RC::INVALID_ARGUMENT;
  }

  IndexMeta new_index_meta;
  RC        rc = new_index_meta.init(index_name, fields_meta, is_unique ? IndexType::Unique : IndexType::NonUnique);
  if (rc != RC::SUCCESS) {
    LOG_INFO("Failed to init IndexMeta in table:%s, index_name:%s, field_name:%s",
             name(), index_name, fields_meta[0]->name());
    return rc;
  }
  // 检查唯一性
  if (is_unique) {
    // 需要对于unique的问题单独处理，即如果两个record
    // 完全相等，但是但凡有一个是带null字段的，那么就可以插入
    std::unordered_set<std::string> unique_check_set;
    // 遍历当前的所有数据，插入这个索引
    RecordFileScanner scanner_unique;
    rc = get_record_scanner(scanner_unique, trx, true /*readonly*/);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create scanner while creating index. table=%s, index=%s, rc=%s",
              name(), index_name, strrc(rc));
      return rc;
    }

    Record record_unique;
    int    attr_length_sum = 0;
    for (int i = 0; i < fields_meta.size(); i++) {
      attr_length_sum += fields_meta[i]->len();
    }
    while (scanner_unique.has_next()) {
      rc = scanner_unique.next(record_unique);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to scan records while creating index. table=%s, index=%s, rc=%s",
                name(), index_name, strrc(rc));
        scanner_unique.close_scan();
        return rc;
      }

      // 索引字段中带有null的record不保证唯一性，直接跳过
      bool has_null = false;
      for (auto field : fields_meta) {
        if (table_meta_.is_field_null(record_unique.data(), field->name())) {
          has_null = true;
          break;
        }
      }
      if (has_null) {
        continue;
      }
      char *key_data = new char[attr_length_sum];
      int   offset   = 0;
      for (int i = 0; i < fields_meta.size(); i++) {
        memcpy(key_data + offset, record_unique.data() + fields_meta[i]->offset(), fields_meta[i]->len());
        offset += fields_meta[i]->len();
      }
      std::string key(key_data, attr_length_sum);
      delete[] key_data;
      if (unique_check_set.find(key) != unique_check_set.end()) {
        LOG_WARN("failed to create unique index. table=%s, index=%s, rc=%s",
                name(), index_name, strrc(rc));
        scanner_unique.close_scan();
        return RC::UNIQUE_INDEX_CONFLICT;
      }
      unique_check_set.insert(key);
    }
    scanner_unique.close_scan();
  }

  // 创建索引相关数据
  BplusTreeIndex        *index      = new BplusTreeIndex();
  std::string            index_file = table_index_file(base_dir_.c_str(), name(), index_name);
  std::vector<FieldMeta> ref_fields_meta(fields_meta.size());
  for (int i = 0; i < fields_meta.size(); i++) {
    ref_fields_meta[i] = *fields_meta[i];
  }
  rc = index->create(index_file.c_str(), new_index_meta, ref_fields_meta);
  if (rc != RC::SUCCESS) {
    delete index;
    LOG_ERROR("Failed to create bplus tree index. file name=%s, rc=%d:%s", index_file.c_str(), rc, strrc(rc));
    return rc;
  }

  // 遍历当前的所有数据，插入这个索引
  RecordFileScanner scanner;
  rc = get_record_scanner(scanner, trx, true /*readonly*/);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create scanner while creating index. table=%s, index=%s, rc=%s",
             name(), index_name, strrc(rc));
    return rc;
  }

  Record record;
  while (scanner.has_next()) {
    rc = scanner.next(record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to scan records while creating index. table=%s, index=%s, rc=%s",
               name(), index_name, strrc(rc));
      return rc;
    }
    rc = index->insert_entry(record.data(), &record.rid());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to insert record into index while creating index. table=%s, index=%s, rc=%s",
               name(), index_name, strrc(rc));
      return rc;
    }
  }
  scanner.close_scan();
  LOG_INFO("inserted all records into new index. table=%s, index=%s", name(), index_name);

  indexes_.push_back(index);

  /// 接下来将这个索引放到表的元数据中
  TableMeta new_table_meta(table_meta_);
  rc = new_table_meta.add_index(new_index_meta);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to add index (%s) on table (%s). error=%d:%s", index_name, name(), rc, strrc(rc));
    return rc;
  }

  /// 内存中有一份元数据，磁盘文件也有一份元数据。修改磁盘文件时，先创建一个临时文件，写入完成后再rename为正式文件
  /// 这样可以防止文件内容不完整
  // 创建元数据临时文件
  std::string  tmp_file = table_meta_file(base_dir_.c_str(), name()) + ".tmp";
  std::fstream fs;
  fs.open(tmp_file, std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", tmp_file.c_str(), strerror(errno));
    return RC::IOERR_OPEN;  // 创建索引中途出错，要做还原操作
  }
  if (new_table_meta.serialize(fs) < 0) {
    LOG_ERROR("Failed to dump new table meta to file: %s. sys err=%d:%s", tmp_file.c_str(), errno, strerror(errno));
    return RC::IOERR_WRITE;
  }
  fs.close();

  // 覆盖原始元数据文件
  std::string meta_file = table_meta_file(base_dir_.c_str(), name());
  int         ret       = rename(tmp_file.c_str(), meta_file.c_str());
  if (ret != 0) {
    LOG_ERROR("Failed to rename tmp meta file (%s) to normal meta file (%s) while creating index (%s) on table (%s). "
              "system error=%d:%s",
              tmp_file.c_str(), meta_file.c_str(), index_name, name(), errno, strerror(errno));
    return RC::IOERR_WRITE;
  }

  table_meta_.swap(new_table_meta);

  LOG_INFO("Successfully added a new index (%s) on the table (%s)", index_name, name());
  return rc;
}

RC Table::delete_record(const Record &record)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->delete_entry(record.data(), &record.rid());
    ASSERT(RC::SUCCESS == rc,
        "failed to delete entry from index. table name=%s, index name=%s, rid=%s, rc=%s",
        name(),
        index->index_meta().name(),
        record.rid().to_string().c_str(),
        strrc(rc));
  }
  rc = record_handler_->delete_record(&record.rid());
  return rc;
}

RC Table::update_record(const Record &record, const char *data)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    if (index->index_meta().is_unique()) {
      // 在这里需要针对支持null的情况做特殊处理
      // record是旧的值，这里检查新值是否有null
      bool has_null         = false;
      bool old_rec_has_null = false;
      for (auto field_name : index->index_meta().fields()) {
        if (table_meta_.is_field_null(data, field_name.c_str())) {
          has_null = true;
          break;
        }
      }
      for (auto field_name : index->index_meta().fields()) {
        if (table_meta_.is_field_null(record.data(), field_name.c_str())) {
          old_rec_has_null = true;
          break;
        }
      }
      if (has_null) {
        continue;
      }
      // 检查更新前后，索引对应的字段是否发生变化
      char *user_key        = nullptr;
      char *user_key_before = nullptr;
      int   attr_length_sum = index->get_user_key(record.data(), user_key_before);
      attr_length_sum       = index->get_user_key(data, user_key);

      int modified = memcmp(user_key, user_key_before, attr_length_sum);

      if (modified == 0 && !old_rec_has_null) {
        // FIX: 此时新值无null，因此必须要与现有检查冲突，事实上，就算字面量没变，新值与旧值可能在null上有区别
        // 比如将原先的null设置成新的0，这就产生的unique的冲突，依然需要检查
        // 旧值无null，（新值也无null）无任何变更，不需要检查
        continue;
      }
      // 需要首先检查插入后的情况是否会有键值重复的情况
      std::list<RID> rids;
      rc = index->get_entry(data, rids);
      if (rc != RC::SUCCESS) {
        LOG_ERROR("Failed to get entry from index. table name=%s, index name=%s, rc=%s",
                  name(), index->index_meta().name(), strrc(rc));
        return rc;
      }
      rids.remove_if([&record](const RID &rid) { return rid == record.rid(); });
      for (auto rid : rids) {
        RecordPageHandler record_page_handler;
        record_page_handler.cleanup();
        Record tmp_record;
        rc = record_handler_->get_record(record_page_handler, &rid, true /*readonly*/, &tmp_record);
        if (rc != RC::SUCCESS) {
          LOG_ERROR("Failed to get record from record handler. table name=%s, rc=%s",
                      name(), strrc(rc));
          return rc;
        }
        for (auto field_name : index->index_meta().fields()) {
          if (table_meta_.is_field_null(tmp_record.data(), field_name.c_str())) {
            rids.remove(rid);
          }
        }
      }
      if (rids.size() >= 1) {
        LOG_ERROR("Found duplicated key in unique index. table name=%s, index name=%s, rc=%s",
                  name(), index->index_meta().name(), strrc(rc));
        return RC::UNIQUE_INDEX_CONFLICT;
      }
    }
  }
  // 这里需要做update
  for (Index *index : indexes_) {
    rc = index->delete_entry(record.data(), &record.rid());
    ASSERT(RC::SUCCESS == rc,
        "failed to delete entry from index. table name=%s, index name=%s, rid=%s, rc=%s",
        name(),
        index->index_meta().name(),
        record.rid().to_string().c_str(),
        strrc(rc));
  }

  rc = record_handler_->update_record(record.rid(), data);

  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to update record. table name=%s, rc=%s", table_meta_.name(), strrc(rc));
    return rc;
  }
  // Losk:??????? 看不懂这里, record.data()难道被上面的record_handler_->update_record修改过?? 感觉并没有,
  // 那这个record.data()岂不是返回的仍然是旧data?
  rc = insert_entry_of_indexes(record.data(), record.rid());
  if (rc != RC::SUCCESS) {  // 可能出现了键值重复
    RC rc2 = delete_entry_of_indexes(record.data(), record.rid(), false /*error_on_not_exists*/);
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
    rc2 = record_handler_->delete_record(&record.rid());
    if (rc2 != RC::SUCCESS) {
      LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
  }
  return rc;
}

RC Table::insert_entry_of_indexes(const char *record, const RID &rid)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->insert_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      break;
    }
  }
  return rc;
}

RC Table::delete_entry_of_indexes(const char *record, const RID &rid, bool error_on_not_exists)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->delete_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      if (rc != RC::RECORD_INVALID_KEY || !error_on_not_exists) {
        break;
      }
    }
  }
  return rc;
}

Index *Table::find_index(const char *index_name) const
{
  for (Index *index : indexes_) {
    if (0 == strcmp(index->index_meta().name(), index_name)) {
      return index;
    }
  }
  return nullptr;
}
Index *Table::find_index_by_field(const char *field_name) const
{
  const TableMeta &table_meta = this->table_meta();
  const IndexMeta *index_meta = table_meta.find_index_by_field(field_name);
  if (index_meta != nullptr) {
    return this->find_index(index_meta->name());
  }
  return nullptr;
}

RC Table::sync()
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->sync();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to flush index's pages. table=%s, index=%s, rc=%d:%s",
                name(),
                index->index_meta().name(),
                rc,
                strrc(rc));
      return rc;
    }
  }
  LOG_INFO("Sync table over. table=%s", name());
  return rc;
}
