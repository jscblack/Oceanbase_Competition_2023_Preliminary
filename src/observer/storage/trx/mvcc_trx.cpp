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
// Created by Wangyunlai on 2023/04/24.
//

#include "storage/trx/mvcc_trx.h"
#include "event/sql_debug.h"
#include "storage/clog/clog.h"
#include "storage/db/db.h"
#include "storage/field/field.h"
#include <limits>

using namespace std;

MvccTrxKit::~MvccTrxKit()
{
  vector<Trx *> tmp_trxes;
  tmp_trxes.swap(trxes_);

  for (Trx *trx : tmp_trxes) {
    delete trx;
  }
}

RC MvccTrxKit::init()
{
  fields_ = vector<FieldMeta>{
      FieldMeta(
          "__trx_xid_begin", AttrType::INTS, 0 /*attr_offset*/, 4 /*attr_len*/, false /*nullable*/, false /*visible*/),
      FieldMeta(
          "__trx_xid_end", AttrType::INTS, 0 /*attr_offset*/, 4 /*attr_len*/, false /*nullable*/, false /*visible*/)};

  LOG_INFO("init mvcc trx kit done.");
  return RC::SUCCESS;
}

const vector<FieldMeta> *MvccTrxKit::trx_fields() const { return &fields_; }

int32_t MvccTrxKit::next_trx_id() { return ++current_trx_id_; }

int32_t MvccTrxKit::max_trx_id() const { return numeric_limits<int32_t>::max(); }

Trx *MvccTrxKit::create_trx(CLogManager *log_manager)
{
  Trx *trx = new MvccTrx(*this, log_manager);
  if (trx != nullptr) {
    lock_.lock();
    trxes_.push_back(trx);
    lock_.unlock();
  }
  return trx;
}

Trx *MvccTrxKit::create_trx(int32_t trx_id)
{
  Trx *trx = new MvccTrx(*this, trx_id);
  if (trx != nullptr) {
    lock_.lock();
    trxes_.push_back(trx);
    if (current_trx_id_ < trx_id) {
      current_trx_id_ = trx_id;
    }
    lock_.unlock();
  }
  return trx;
}

void MvccTrxKit::destroy_trx(Trx *trx)
{
  lock_.lock();
  for (auto iter = trxes_.begin(), itend = trxes_.end(); iter != itend; ++iter) {
    if (*iter == trx) {
      trxes_.erase(iter);
      break;
    }
  }
  lock_.unlock();

  delete trx;
}

Trx *MvccTrxKit::find_trx(int32_t trx_id)
{
  lock_.lock();
  for (Trx *trx : trxes_) {
    if (trx->id() == trx_id) {
      lock_.unlock();
      return trx;
    }
  }
  lock_.unlock();

  return nullptr;
}

void MvccTrxKit::all_trxes(std::vector<Trx *> &trxes)
{
  lock_.lock();
  trxes = trxes_;
  lock_.unlock();
}

////////////////////////////////////////////////////////////////////////////////

MvccTrx::MvccTrx(MvccTrxKit &kit, CLogManager *log_manager) : trx_kit_(kit), log_manager_(log_manager) {}

MvccTrx::MvccTrx(MvccTrxKit &kit, int32_t trx_id) : trx_kit_(kit), trx_id_(trx_id)
{
  started_    = true;
  recovering_ = true;
}

MvccTrx::~MvccTrx() {}

RC MvccTrx::insert_record(Table *table, Record &record)
{
  Field begin_field;
  Field end_field;
  trx_fields(table, begin_field, end_field);

  begin_field.set_int(record, -trx_id_);
  end_field.set_int(record, trx_kit_.max_trx_id());

  RC rc = table->insert_record(this, record, false);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to insert record into table. rc=%s", strrc(rc));
    return rc;
  }

  rc = log_manager_->append_log(
      CLogType::INSERT, trx_id_, table->table_id(), record.rid(), record.len(), 0 /*offset*/, record.data());
  ASSERT(rc == RC::SUCCESS,
      "failed to append insert record log. trx id=%d, table id=%d, rid=%s, record len=%d, rc=%s",
      trx_id_,
      table->table_id(),
      record.rid().to_string().c_str(),
      record.len(),
      strrc(rc));

  pair<OperationSet::iterator, bool> ret = operations_.insert(Operation(Operation::Type::INSERT, table, record.rid()));
  if (!ret.second) {
    rc = RC::INTERNAL;
    LOG_WARN("failed to insert operation(insertion) into operation set: duplicate");
  }
  return rc;
}

RC MvccTrx::delete_record(Table *table, Record &record)
{
  Field begin_field;
  Field end_field;
  trx_fields(table, begin_field, end_field);

  [[maybe_unused]] int32_t end_xid = end_field.get_int(record);

  // 新版不需要这些判断
  // /// 在删除之前，第一次获取record时，就已经对record做了对应的检查，并且保证不会有其它的事务来访问这条数据
  // // 错误，现在修复成在这里判断是否有删除/更新的冲突
  // if (end_xid < 0 && -end_xid == trx_id_) {
  //   // 自己此前已经删除过
  //   return RC::SUCCESS;
  // }
  // 这个ASSERT会在release时被编译掉，所以没起作用
  // ASSERT(end_xid > 0,
  //     "concurrency conflit: other transaction is updating this record. end_xid=%d, current trx id=%d, rid=%s",
  //     end_xid,
  //     trx_id_,
  //     record.rid().to_string().c_str());
  if (end_xid != trx_kit_.max_trx_id()) {
    // 当前不是多版本数据中的最新记录，不需要删除
    return RC::SUCCESS;
  }

  // 新版：不需要在运行时设置标志，提交时再设置，可见性通过operationSet判断
  // end_field.set_int(record, -trx_id_);
  RC rc = log_manager_->append_log(CLogType::DELETE, trx_id_, table->table_id(), record.rid(), 0, 0, nullptr);
  ASSERT(rc == RC::SUCCESS,
      "failed to append delete record log. trx id=%d, table id=%d, rid=%s, record len=%d, rc=%s",
      trx_id_,
      table->table_id(),
      record.rid().to_string().c_str(),
      record.len(),
      strrc(rc));

  auto ret = operations_.insert(Operation(Operation::Type::DELETE, table, record.rid()));
  if (!ret.second) {
    if (ret.first->type() == Operation::Type::DELETE || ret.first->type() == Operation::Type::DELETE_AFTER_INSERT) {
      // delete后第二次delete，也应该是success
      rc = RC::SUCCESS;
    } else if (ret.first->type() == Operation::Type::INSERT) {
      // ret.first->set_type(Operation::Type::DELETE_AFTER_INSERT);
      operations_.erase(ret.first);
      ret = operations_.insert(Operation(Operation::Type::DELETE_AFTER_INSERT, table, record.rid()));
    }
  }
  return RC::SUCCESS;
}

RC MvccTrx::update_record(Table *table, Record &record, const char *data)
{
  // ！！！：注意传入的data参数是有毒的，它的值是对的，但事务元信息是错的（拷贝的record的元信息），不能直接使用，仅供拷贝参考
  // 获取指定表上的事务使用的字段
  Field begin_xid_field, end_xid_field;
  trx_fields(table, begin_xid_field, end_xid_field);

  int32_t begin_xid = begin_xid_field.get_int(record);
  int32_t end_xid   = end_xid_field.get_int(record);

  int new_record_size = table->table_meta().record_size();
  if (begin_xid == -trx_id_) {
    // 同一个事务的多次update， 或者因为追加写新版本导致的同一个update调用两次update_record
    // 为保证接口统一，在此不做区分，统一生成原位更新的update-log
    // 注意对于单个update操作调用两次update-record的情形, 这个update-log是可有可无的
    char *record_data = (char *)malloc(new_record_size);
    memcpy(record_data, data, new_record_size);
    record.set_data_owner(record_data, new_record_size);
    begin_xid_field.set_int(record, -trx_id_);
    end_xid_field.set_int(record, trx_kit_.max_trx_id());
    RC rc = table->update_record(this, record, record.data());
    if (rc != RC::SUCCESS) {
      sql_debug("MVCC: failed, %d", __LINE__);
      LOG_WARN("MVCC: failed to update new-version record into table when update. rc=%s", strrc(rc));
      return rc;
    }
    // 需要加入到update-log
    // 现有的table->update_record似乎并不会把data赋给record.data() 所以采取保守的做法,
    rc = log_manager_->append_log(
        CLogType::UPDATE, trx_id_, table->table_id(), record.rid(), new_record_size, 0 /*offset*/, record.data());
    ASSERT(rc == RC::SUCCESS,
        "failed to append update record log. trx id=%d, table id=%d, rid=%s, record len=%d, rc=%s",
        trx_id_,
        table->table_id(),
        new_record.rid().to_string().c_str(),
        new_record.len(),
        strrc(rc));
    // 注意不需要插入到operationSet，因为OperationSet主要为了提交时对时间戳的修改，
    // 这个由第一次update插入新版本的delete+insert来处理
    return rc;
  }

  /* // TODO 新版：需要延后检测到commit修改xid前
  // 检测并发事务冲突, 将旧版本的end_xid_field置为-trx_id_
  {
    // 读到了其他未提交事务更新的新版本, 不可能出现
    // TODO 新版：这一步可能仍然是不存在的，因为可见性判断会拦截
    if (begin_xid < 0 && begin_xid != -trx_id_) {
      sql_debug("MVCC: failed, %d", __LINE__);
      LOG_WARN("MVCC update: 读到了其他未提交事务更新的新版本, 不可能出现");
      return RC::CONCURRENCY_UPDATE_FAIL;
    }
    // ASSERT(!(begin_xid < 0 && begin_xid != -trx_id_), "MVCC update: 读到了其他未提交事务更新的新版本, 不可能出现");

    // 读到新版本的情况已处理完成, 下面是当前record为即将更新的旧版本
    // 其他事务正在删除/更新当前版本, 中止当前事务
    if (end_xid < 0 && -end_xid != trx_id_) {
      sql_debug("MVCC: failed, %d", __LINE__);
      LOG_WARN("MVCC update: 其他事务正在删除/更新当前版本, 中止当前事务");
      return RC::CONCURRENCY_UPDATE_FAIL;
    }
    // ASSERT(end_xid > 0,
    //     "concurrency conflit: other transaction is updating this record. end_xid=%d, current trx id=%d, rid=%s",
    //     end_xid,
    //     trx_id_,
    //     record.rid().to_string().c_str());

    // 其他并发事务已经更新并提交了新版本, 中止当前事务
    if (end_xid != trx_kit_.max_trx_id()) {
      sql_debug("MVCC: failed, %d", __LINE__);
      LOG_WARN("MVCC update: 其他并发事务已经更新并提交了新版本, 中止当前事务");
      return RC::CONCURRENCY_UPDATE_FAIL;
    }
    // ASSERT(end_xid != trx_kit_.max_trx_id(),
    //     "concurrency conflit: other transaction is updating this record. end_xid=%d, current trx id=%d, rid=%s",
    //     end_xid,
    //     trx_id_,
    //     record.rid().to_string().c_str());

    end_xid_field.set_int(record, -trx_id_);  // 假定这个设置是原子性的, 目前的实现似乎还不是

    // double-check, 如果自己的原子更新没有抢过其他并发事务, 自己abort
    if (end_xid_field.get_int(record) != -trx_id_) {
      sql_debug("MVCC: failed, %d", __LINE__);
      LOG_WARN("MVCC update: double-check, 如果自己的原子更新没有抢过其他并发事务, 自己abort");
      return RC::CONCURRENCY_UPDATE_FAIL;
    }
    // ASSERT(end_xid_field.get_int(record) == -trx_id_,
    //     "concurrency conflit: other transaction is updating this record. end_xid=%d, current trx id=%d, rid=%s",
    //     end_xid,
    //     trx_id_,
    //     record.rid().to_string().c_str());
  }
  */

  // 对异位更新即将插入的new_record设置事务相关元数据
  Record new_record(record);  // Copy-On-Write
  // 参考update_physical的写法来给长度
  char *record_data = (char *)malloc(new_record_size);
  memcpy(record_data, data, new_record_size);
  new_record.set_data_owner(record_data, new_record_size);
  begin_xid_field.set_int(new_record, -trx_id_);
  end_xid_field.set_int(new_record, trx_kit_.max_trx_id());

  RC rc = table->insert_record(this, new_record, true);
  if (rc != RC::SUCCESS) {
    sql_debug("MVCC: failed, %d", __LINE__);
    LOG_WARN("MVCC: failed to insert new-version record into table when update. rc=%s", strrc(rc));
    return rc;
  }
  ASSERT(new_record.rid() != record.rid(), "MVCC update: new-copy's RID must NOT EQUAL the old-copy's RID");
  // // 修改写法后已经不需要再update了
  // rc = table->update_record(new_record, new_record.data());
  // if (rc != RC::SUCCESS) {
  //   LOG_WARN("MVCC: failed to update new-version record into table when update. rc=%s", strrc(rc));
  //   return rc;
  // }
  // // 废弃的调试
  // int32_t new_begin_xid = begin_xid_field.get_int(new_record);
  // int32_t new_end_xid = end_xid_field.get_int(new_record);
  // ASSERT(new_begin_xid == -trx_id_, "MVCC: fail =========");
  // ASSERT(new_end_xid == trx_kit_.max_trx_id(), "MVCC: fail =====");

  // 此处更新顺序可能有影响,因为一般得保证日志比对应数据先落盘(此处代码顺序可能不关键,需要在另外地方保证),
  // 与前面的insert和delete的先table操作再append log统一
  // 唯一可能出现问题的地方就是这两条log只落盘了一个？（也好像不会有问题，因为没有提交会被回滚）
  rc = log_manager_->append_log(CLogType::DELETE, trx_id_, table->table_id(), record.rid(), 0, 0, nullptr);
  ASSERT(rc == RC::SUCCESS,
      "failed to append delete record log when update. trx id=%d, table id=%d, rid=%s, record len=%d, rc=%s",
      trx_id_,
      table->table_id(),
      record.rid().to_string().c_str(),
      record.len(),
      strrc(rc));
  // 注意前面的table->update_record是不会更新new_record结构体的，只是在物理层面复写，
  // 所以存进log的数据得是raw-data，而非从new_record获取的data。
  rc = log_manager_->append_log(
      CLogType::INSERT, trx_id_, table->table_id(), new_record.rid(), new_record_size, 0 /*offset*/, new_record.data());
  ASSERT(rc == RC::SUCCESS,
      "failed to append insert record log when update. trx id=%d, table id=%d, rid=%s, record len=%d, rc=%s",
      trx_id_,
      table->table_id(),
      new_record.rid().to_string().c_str(),
      new_record.len(),
      strrc(rc));

  // 注意, OperationSet的迭代顺序是比较随机的, 不一定先迭代delete 后迭代insert, 所以还是得自己处理
  // 但真的会有影响吗? 这个先后顺序? 如果原子提交好像没影响, 现在本来就不能保证原子提交也好像没影响
  pair<OperationSet::iterator, bool> ret = operations_.insert(Operation(Operation::Type::DELETE, table, record.rid()));
  if (!ret.second) {
    if (ret.first->type() == Operation::Type::DELETE || ret.first->type() == Operation::Type::DELETE_AFTER_INSERT) {
      // delete后第二次delete，也应该是success
      rc = RC::SUCCESS;
    } else if (ret.first->type() == Operation::Type::INSERT) {
      // ret.first->set_type(Operation::Type::DELETE_AFTER_INSERT);
      operations_.erase(ret.first);
      ret = operations_.insert(Operation(Operation::Type::DELETE_AFTER_INSERT, table, record.rid()));
    }

    // rc = RC::INTERNAL;
    // sql_debug("MVCC: failed, %d", __LINE__);
    // LOG_WARN("failed to insert operation(update) into operation set: duplicate");
  }
  ret = operations_.insert(Operation(Operation::Type::INSERT, table, new_record.rid()));
  if (!ret.second) {
    rc = RC::INTERNAL;
    sql_debug("MVCC: failed, %d", __LINE__);
    LOG_WARN("failed to insert operation(update) into operation set: duplicate");
  }

  return rc;
}

RC MvccTrx::visit_record(Table *table, Record &record, bool readonly)
{
  Field begin_field;
  Field end_field;
  trx_fields(table, begin_field, end_field);

  int32_t begin_xid = begin_field.get_int(record);
  int32_t end_xid   = end_field.get_int(record);

  RC rc = RC::SUCCESS;
  // FIXME 这个循环可能有错，是假定了先insert再delete这种情况下，会只保留最后一个operations
  for (const Operation &operation : operations_) {
    RID rid(operation.page_num(), operation.slot_num());
    if (record.rid() == rid) {
      if (operation.type() == Operation::Type::DELETE || operation.type() == Operation::Type::DELETE_AFTER_INSERT) {
        return RC::RECORD_INVISIBLE;
      } else if (operation.type() == Operation::Type::INSERT) {
        return RC::SUCCESS;
      }
    }
  }

  if (begin_xid > 0 && end_xid > 0) {
    if (trx_id_ >= begin_xid && trx_id_ <= end_xid) {
      rc = RC::SUCCESS;
    } else {
      rc = RC::RECORD_INVISIBLE;
    }
  } else {
    if (begin_xid < 0) {
      return RC::RECORD_INVISIBLE;  // 不是自己插入的
    }
    if (end_xid < 0) {
      return RC::SUCCESS;  // 不是自己删除的
    }
  }
  // 旧版的first-update-win的可见性逻辑
  /* else if (begin_xid < 0) {
    // begin xid 小于0说明是刚插入而且没有提交的数据
    rc = (-begin_xid == trx_id_) ? RC::SUCCESS : RC::RECORD_INVISIBLE;
  } else if (end_xid < 0) {  // begin > 0 and end_xid < 0
    // end xid 小于0 说明是正在删除但是还没有提交的数据
    if (readonly) {
      // 如果 -end_xid 就是当前事务的事务号，说明是当前事务删除的
      rc = (-end_xid != trx_id_) ? RC::SUCCESS : RC::RECORD_INVISIBLE;
    } else {
      // 如果当前想要修改此条数据，并且不是当前事务删除的，简单的报错
      // 这是事务并发处理的一种方式，非常简单粗暴。其它的并发处理方法，可以等待，或者让客户端重试
      // 或者等事务结束后，再检测修改的数据是否有冲突

      // 错误的处理！这里扫描的时候扫到的未必是自己想要删除的，删除冲突的解决方案应该在trx->delete_record中解决
      // 这里只做可见性的判断，而不做其他判断
      rc = (-end_xid != trx_id_) ? RC::SUCCESS : RC::RECORD_INVISIBLE;
      // rc = (-end_xid != trx_id_) ? RC::LOCKED_CONCURRENCY_CONFLICT : RC::RECORD_INVISIBLE;
    }
  } */
  return rc;
}

/**
 * @brief 获取指定表上的事务使用的字段
 *
 * @param table 指定的表
 * @param begin_xid_field 返回处理begin_xid的字段
 * @param end_xid_field   返回处理end_xid的字段
 */
void MvccTrx::trx_fields(Table *table, Field &begin_xid_field, Field &end_xid_field) const
{
  const TableMeta                        &table_meta = table->table_meta();
  const std::pair<const FieldMeta *, int> trx_fields = table_meta.trx_fields();
  ASSERT(trx_fields.second >= 2, "invalid trx fields number. %d", trx_fields.second);

  begin_xid_field.set_table(table);
  begin_xid_field.set_field(&trx_fields.first[0]);
  end_xid_field.set_table(table);
  end_xid_field.set_field(&trx_fields.first[1]);
}

RC MvccTrx::start_if_need()
{
  if (!started_) {
    ASSERT(operations_.empty(), "try to start a new trx while operations is not empty");
    trx_id_ = trx_kit_.next_trx_id();
    LOG_DEBUG("current thread change to new trx with %d", trx_id_);
    RC rc = log_manager_->begin_trx(trx_id_);
    ASSERT(rc == RC::SUCCESS, "failed to append log to clog. rc=%s", strrc(rc));
    started_ = true;
  }
  return RC::SUCCESS;
}

RC MvccTrx::commit()
{
  int32_t commit_id = trx_kit_.next_trx_id();
  return commit_with_trx_id(commit_id);
}

RC MvccTrx::commit_with_trx_id(int32_t commit_xid)
{
  // TODO 这里存在一个很大的问题，不能让其他事务一次性看到当前事务更新到的数据或同时看不到
  RC rc    = RC::SUCCESS;
  started_ = false;

  for (const Operation &operation : operations_) {
    switch (operation.type()) {
      case Operation::Type::DELETE: {
        Table *table = operation.table();
        RID    rid(operation.page_num(), operation.slot_num());

        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);

        auto record_checker = [this, &begin_xid_field, &end_xid_field, commit_xid](Record &record) -> RC {
          if (end_xid_field.get_int(record) != trx_kit_.max_trx_id()) {  // update的不是最新版，被其他并发事务先update了
            return RC::CONCURRENCY_UPDATE_FAIL;
          }
          return RC::SUCCESS;
        };

        rc = operation.table()->visit_record(rid, true /*readonly*/, record_checker);
      } break;
      default: {
      } break;
    }
  }
  if (rc != RC::SUCCESS) {
    RC tmp_rc = this->rollback();
    return (tmp_rc != RC::SUCCESS) ? tmp_rc : rc;
  }

  for (const Operation &operation : operations_) {
    switch (operation.type()) {
      case Operation::Type::INSERT: {
        RID    rid(operation.page_num(), operation.slot_num());
        Table *table = operation.table();
        Field  begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);

        auto record_updater = [this, &begin_xid_field, commit_xid](Record &record) -> RC {
          LOG_DEBUG("before commit insert record. trx id=%d, begin xid=%d, commit xid=%d, lbt=%s",
                    trx_id_, begin_xid_field.get_int(record), commit_xid, lbt());
          ASSERT(begin_xid_field.get_int(record) == -this->trx_id_,
              "got an invalid record while committing. begin xid=%d, this trx id=%d",
              begin_xid_field.get_int(record),
              trx_id_);

          begin_xid_field.set_int(record, commit_xid);
          return RC::SUCCESS;
        };

        rc = operation.table()->visit_record(rid, false /*readonly*/, record_updater);
        ASSERT(rc == RC::SUCCESS,
            "failed to get record while committing. rid=%s, rc=%s",
            rid.to_string().c_str(),
            strrc(rc));
      } break;

      case Operation::Type::DELETE: {
        Table *table = operation.table();
        RID    rid(operation.page_num(), operation.slot_num());

        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);

        auto record_updater = [this, &end_xid_field, commit_xid](Record &record) -> RC {
          (void)this;
          ASSERT(end_xid_field.get_int(record) == -trx_id_,
              "got an invalid record while committing. end xid=%d, this trx id=%d",
              end_xid_field.get_int(record),
              trx_id_);

          end_xid_field.set_int(record, commit_xid);
          return RC::SUCCESS;
        };

        rc = operation.table()->visit_record(rid, false /*readonly*/, record_updater);
        ASSERT(rc == RC::SUCCESS,
            "failed to get record while committing. rid=%s, rc=%s",
            rid.to_string().c_str(),
            strrc(rc));
      } break;

      case Operation::Type::DELETE_AFTER_INSERT: {
        Table *table = operation.table();
        RID    rid(operation.page_num(), operation.slot_num());

        Field begin_xid_field, end_xid_field;
        trx_fields(table, begin_xid_field, end_xid_field);
        auto record_updater = [this, &begin_xid_field, &end_xid_field, commit_xid](Record &record) -> RC {
          (void)this;
          ASSERT(end_xid_field.get_int(record) == -trx_id_,
              "got an invalid record while committing. end xid=%d, this trx id=%d",
              end_xid_field.get_int(record),
              trx_id_);
          begin_xid_field.set_int(record, commit_xid);
          end_xid_field.set_int(record, commit_xid);
          return RC::SUCCESS;
        };

        rc = operation.table()->visit_record(rid, false /*readonly*/, record_updater);
        ASSERT(rc == RC::SUCCESS,
            "failed to get record while committing. rid=%s, rc=%s",
            rid.to_string().c_str(),
            strrc(rc));
      } break;

      default: {
        ASSERT(false, "unsupported operation. type=%d", static_cast<int>(operation.type()));
      }
    }
  }

  operations_.clear();

  if (!recovering_) {
    rc = log_manager_->commit_trx(trx_id_, commit_xid);
  }
  LOG_TRACE("append trx commit log. trx id=%d, commit_xid=%d, rc=%s", trx_id_, commit_xid, strrc(rc));
  return rc;
}

RC MvccTrx::rollback()
{
  RC rc    = RC::SUCCESS;
  started_ = false;

  for (const Operation &operation : operations_) {
    switch (operation.type()) {
      case Operation::Type::DELETE_AFTER_INSERT:
      case Operation::Type::INSERT: {
        RID    rid(operation.page_num(), operation.slot_num());
        Record record;
        Table *table = operation.table();
        // TODO 这里虽然调用get_record好像多次一举，而且看起来放在table的实现中更好，
        // 而且实际上trx应该记录下来自己曾经插入过的数据
        // 也就是不需要从table中获取这条数据，可以直接从当前内存中获取
        // 这里也可以不删除，仅仅给数据加个标识位，等垃圾回收器来收割也行
        rc = table->get_record(rid, record);
        ASSERT(rc == RC::SUCCESS,
            "failed to get record while rollback. rid=%s, rc=%s",
            rid.to_string().c_str(),
            strrc(rc));
        rc = table->delete_record(record);
        ASSERT(rc == RC::SUCCESS,
            "failed to delete record while rollback. rid=%s, rc=%s",
            rid.to_string().c_str(),
            strrc(rc));
      } break;

        // 新版rollback不需要更改delete的时间戳
        // case Operation::Type::DELETE: {
        //   Table *table = operation.table();
        //   RID    rid(operation.page_num(), operation.slot_num());

        //   ASSERT(rc == RC::SUCCESS,
        //       "failed to get record while rollback. rid=%s, rc=%s",
        //       rid.to_string().c_str(),
        //       strrc(rc));
        //   Field begin_xid_field, end_xid_field;
        //   trx_fields(table, begin_xid_field, end_xid_field);

        //   auto record_updater = [this, &end_xid_field](Record &record) -> RC {
        //     ASSERT(end_xid_field.get_int(record) == -trx_id_,
        //         "got an invalid record while rollback. end xid=%d, this trx id=%d",
        //         end_xid_field.get_int(record),
        //         trx_id_);

        //     end_xid_field.set_int(record, trx_kit_.max_trx_id());
        //     return RC::SUCCESS;
        //   };

        //   rc = table->visit_record(rid, false /*readonly*/, record_updater);
        //   ASSERT(rc == RC::SUCCESS,
        //       "failed to get record while committing. rid=%s, rc=%s",
        //       rid.to_string().c_str(),
        //       strrc(rc));
        // } break;

      default: {
        ASSERT(false, "unsupported operation. type=%d", static_cast<int>(operation.type()));
      }
    }
  }

  operations_.clear();

  if (!recovering_) {
    rc = log_manager_->rollback_trx(trx_id_);
  }
  LOG_TRACE("append trx rollback log. trx id=%d, rc=%s", trx_id_, strrc(rc));
  return rc;
}

RC find_table(Db *db, const CLogRecord &log_record, Table *&table)
{
  switch (clog_type_from_integer(log_record.header().type_)) {
    case CLogType::INSERT:
    case CLogType::DELETE:
    case CLogType::UPDATE: {
      const CLogRecordData &data_record = log_record.data_record();
      table                             = db->find_table(data_record.table_id_);
      if (nullptr == table) {
        LOG_WARN("no such table to redo. table id=%d, log record=%s",
                 data_record.table_id_, log_record.to_string().c_str());
        return RC::SCHEMA_TABLE_NOT_EXIST;
      }
    } break;
    default: {
      // do nothing
    } break;
  }
  return RC::SUCCESS;
}

RC MvccTrx::redo(Db *db, const CLogRecord &log_record)
{
  Table *table = nullptr;
  RC     rc    = find_table(db, log_record, table);
  if (OB_FAIL(rc)) {
    return rc;
  }

  switch (log_record.log_type()) {
    case CLogType::INSERT: {
      const CLogRecordData &data_record = log_record.data_record();
      Record                record;
      record.set_data(const_cast<char *>(data_record.data_), data_record.data_len_);
      record.set_rid(data_record.rid_);
      RC rc = table->recover_insert_record(record);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to recover insert. table=%s, log record=%s, rc=%s",
                 table->name(), log_record.to_string().c_str(), strrc(rc));
        return rc;
      }
      operations_.insert(Operation(Operation::Type::INSERT, table, record.rid()));
    } break;

    case CLogType::DELETE: {
      const CLogRecordData &data_record = log_record.data_record();
      Field                 begin_field;
      Field                 end_field;
      trx_fields(table, begin_field, end_field);

      auto record_updater = [this, &end_field](Record &record) -> RC {
        (void)this;
        ASSERT(end_field.get_int(record) == trx_kit_.max_trx_id(),
            "got an invalid record while committing. end xid=%d, this trx id=%d",
            end_field.get_int(record),
            trx_id_);

        end_field.set_int(record, -trx_id_);
        return RC::SUCCESS;
      };

      RC rc = table->visit_record(data_record.rid_, false /*readonly*/, record_updater);
      ASSERT(rc == RC::SUCCESS,
          "failed to get record while committing. rid=%s, rc=%s",
          data_record.rid_.to_string().c_str(),
          strrc(rc));

      operations_.insert(Operation(Operation::Type::DELETE, table, data_record.rid_));
    } break;

    case CLogType::UPDATE: {
      // TODO
      const CLogRecordData &data_record = log_record.data_record();
      Record                record;
      auto                  record_size = data_record.data_len_;
      // auto record_size = table->table_meta().record_size(); // redo时读不到这个table_meta, 会段错误，why？
      char *record_data = (char *)malloc(record_size);
      memcpy(record_data, data_record.data_, record_size);
      // record.set_data(const_cast<char*>(data_record.data_), data_record.data_len_);
      record.set_data_owner(record_data, record_size);
      record.set_rid(data_record.rid_);
      RC rc = table->update_record(this, record, record.data());
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to recover update. table=%s, log record=%s, rc=%s",
                 table->name(), log_record.to_string().c_str(), strrc(rc));
        return rc;
      }
      // 不需要插入到operations中，时间戳由相应的delete-insert log实现恢复。
    } break;

    case CLogType::MTR_COMMIT: {
      const CLogRecordCommitData &commit_record = log_record.commit_record();
      commit_with_trx_id(commit_record.commit_xid_);
    } break;

    case CLogType::MTR_ROLLBACK: {
      rollback();
    } break;

    default: {
      ASSERT(false, "unsupported redo log. log_record=%s", log_record.to_string().c_str());
      return RC::INTERNAL;
    } break;
  }

  return RC::SUCCESS;
}
