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
// Created by WangYunlai on 2021/6/9.
//

#include "sql/operator/insert_physical_operator.h"
#include "sql/stmt/insert_stmt.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

using namespace std;

InsertPhysicalOperator::InsertPhysicalOperator(Table *table, std::vector<vector<Value>> &&values)
    : table_(table), values_(std::move(values))
{}

RC InsertPhysicalOperator::open(Trx *trx)
{
  if (values_.empty()) {
    LOG_WARN("no values to insert");
    return RC::INVALID_ARGUMENT;
  }

  // 第一步：创建所有记录（验证阶段）
  // 如果任何一条记录创建失败，直接返回，此时还没有插入任何记录
  std::vector<Record> records;
  records.reserve(values_.size());
  
  for (size_t i = 0; i < values_.size(); ++i) {
    Record record;
    RC rc = table_->make_record(static_cast<int>(values_[i].size()), values_[i].data(), record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to make record at row %zu. rc=%s", i, strrc(rc));
      return rc;
    }
    records.emplace_back(std::move(record));
  }

  // 第二步：使用事务的 insert_records 方法一次性插入所有记录
  // 如果插入失败，之前插入的记录已经在事务的操作列表中，执行器会自动回滚事务
  RC rc = trx->insert_records(table_, records);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to insert records by transaction. rc=%s", strrc(rc));
    // 返回错误，让执行器处理回滚（SqlResult::close() 会自动回滚失败的事务）
  return rc;
  }

  return RC::SUCCESS;
}

RC InsertPhysicalOperator::next()
{
  return RC::RECORD_EOF;
}

RC InsertPhysicalOperator::close()
{
  return RC::SUCCESS;
}
