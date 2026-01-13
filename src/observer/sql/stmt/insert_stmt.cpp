/* Copyright (c) 2021OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/insert_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

InsertStmt::InsertStmt(Table *table, std::vector<std::vector<Value>> &values, int value_amount)
    : table_(table), values_(values), value_amount_(value_amount)
{}

RC InsertStmt::create(Db *db, const InsertSqlNode &inserts, Stmt *&stmt)
{
  RC rc = RC::SUCCESS;
  const char *table_name = inserts.relation_name.c_str();
  if (nullptr == db || nullptr == table_name || inserts.values.empty()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d",
        db, table_name, static_cast<int>(inserts.values.size()));
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  std::vector<std::vector<Value>> rows;
  if (0 == inserts.attrs_name.size()) {
    rc = check_full_rows(table, inserts, rows);
  } else {
    rc = check_incomplete_rows(table, inserts, rows);
  }
  if (RC::SUCCESS != rc) {
    LOG_WARN("values not match schema, rc=%s", strrc(rc));
    return rc;
  }

  // everything alright
  int field_num = table->table_meta().field_num() - table->table_meta().sys_field_num();
  stmt = new InsertStmt(table, rows, field_num);
  return RC::SUCCESS;
}

RC InsertStmt::check_full_rows(Table *table, const InsertSqlNode &inserts, std::vector<std::vector<Value>> &rows)
{
  RC rc = RC::SUCCESS;

  const TableMeta &table_meta = table->table_meta();
  const int field_num = table_meta.field_num() - table_meta.sys_field_num();
  const int sys_field_num = table_meta.sys_field_num();

  // 检查每一行数据
  for (const std::vector<Value> &values : inserts.values) {
    const int value_num = static_cast<int>(values.size());
    if (field_num != value_num) {
      LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
      return RC::SCHEMA_FIELD_MISSING;
    }

    // 创建新的一行数据，用于存储处理后的值（主要是CHARS字段的固定长度处理）
    std::vector<Value> row;
    row.reserve(field_num);

    // check fields type
    for (int i = 0; i < field_num; i++) {
      const FieldMeta *field_meta = table_meta.field(i + sys_field_num);
      const AttrType field_type = field_meta->type();
      const AttrType value_type = values[i].attr_type();
      
      if (value_type == NULLS && field_meta->nullable()) {
        row.emplace_back(values[i]);
        continue;
      }
      
      // 严格类型检查：类型必须完全匹配，不允许类型转换
      if (field_type != value_type) {
        // 只允许 TEXTS 和 CHARS 之间的兼容（因为它们本质相同）
        if (TEXTS == field_type && CHARS == value_type) {
          if (MAX_TEXT_LENGTH < values[i].length()) {
            LOG_WARN("Text length:%d, over max_length 65535", values[i].length());
            return RC::INVALID_ARGUMENT;
          }
        } else {
          // 其他类型不匹配直接返回错误
          LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
            table->name(), field_meta->name(), field_type, value_type);
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
      }
      
      if (field_type == CHARS) {
        if (values[i].length() > field_meta->len()) {
          LOG_WARN("char field length mismatch. field=%s, value length=%d, field len=%d",
            field_meta->name(), values[i].length(), field_meta->len());
          return RC::INVALID_ARGUMENT;
        }
        // 将不确定长度的 char 改为固定长度的 char
        char *char_data = (char*)malloc(field_meta->len());
        memset(char_data, 0, field_meta->len());
        memcpy(char_data, values[i].data(), values[i].length());
        row.emplace_back(Value(CHARS, char_data, field_meta->len()));
        free(char_data);
      } else {
        row.emplace_back(values[i]);
      }
    }
    rows.emplace_back(std::move(row));
  }
  return rc;
}

RC InsertStmt::check_incomplete_rows(Table *table, const InsertSqlNode &inserts, std::vector<std::vector<Value>> &rows)
{
  RC rc = RC::SUCCESS;

  const TableMeta &table_meta = table->table_meta();
  const int field_num = table_meta.field_num() - table_meta.sys_field_num();
  const int sys_field_num = table_meta.sys_field_num();
  int col_name_num = inserts.attrs_name.size();
  const std::vector<std::string> &col_names = inserts.attrs_name;

  // 记录行中每一列是values中第几个，不存在的为-1
  std::vector<int> col_idx(field_num, -1);

  // 确认每个列名称都是正确的，以及确认列在行中位置
  for (int i = 0; i < col_names.size(); i++) {
    const std::string &col_name = col_names[i];
    int field_idx = table->table_meta().find_field_idx_by_name(col_name.c_str());
    if (-1 == field_idx) {
      LOG_ERROR("column not exist:%s", col_name.c_str());
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
    col_idx[field_idx - sys_field_num] = i;
  }

  // 检查每一行数据
  for (const std::vector<Value> &values : inserts.values) {
     const int value_num = static_cast<int>(values.size());
    if (value_num != inserts.attrs_name.size()) {
      LOG_WARN("value mismatch with attr_names. value num=%d, attr_names num=%d", value_num, inserts.attrs_name.size());
      return RC::INVALID_ARGUMENT;
    }

    // check fields type
    std::vector<Value> row(field_num, {NULLS, nullptr, 0});
    for (int i = 0; i < field_num; i++) {
      const FieldMeta *field_meta = table_meta.field(i + sys_field_num);
      if (-1 == col_idx[i]) {
        // 该列未指定
        if (!field_meta->nullable()) {
          LOG_WARN("field not allow NULL:%s", field_meta->name());;
          return RC::INVALID_ARGUMENT;
        }
      } else {
        // 指定了值的列，需要检查
        int name_idx = col_idx[i];  // 该列的值是第几个
        const AttrType field_type = field_meta->type();
        const AttrType value_type = values[name_idx].attr_type();

        if (value_type == NULLS && field_meta->nullable()) {
          continue;
        }
        // 严格类型检查：类型必须完全匹配，不允许类型转换
        if (field_type != value_type) {
          // 只允许 TEXTS 和 CHARS 之间的兼容（因为它们本质相同）
          if (TEXTS == field_type && CHARS == value_type) {
            if (MAX_TEXT_LENGTH < values[name_idx].length()) {
              LOG_WARN("Text length:%d, over max_length 65535", values[name_idx].length());
              return RC::INVALID_ARGUMENT;
            }
          } else {
            // 其他类型不匹配直接返回错误
            LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
              table->name(), field_meta->name(), field_type, value_type);
            return RC::SCHEMA_FIELD_TYPE_MISMATCH;
          }
        }
        if (field_type == CHARS) {
          if (values[name_idx].length() > field_meta->len()) {
            LOG_WARN("char field length mismatch. field=%s, value length=%d, field len=%d",
              field_meta->name(), values[name_idx].length(), field_meta->len());
            return RC::INVALID_ARGUMENT;
          }
          // 将不确定长度的 char 改为固定长度的 char
          char *char_data = (char*)malloc(field_meta->len());
          memset(char_data, 0, field_meta->len());
          memcpy(char_data, values[name_idx].data(), values[name_idx].length());
          row[i] = Value(CHARS, char_data, field_meta->len());
          free(char_data);
        } else {
          row[i] = values[name_idx];
        }
      }
    }
    rows.emplace_back(row);
  }
  return rc;
}
