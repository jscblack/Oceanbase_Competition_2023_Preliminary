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
// Created by WangYunlai on 2023/06/28.
//

#include "sql/parser/value.h"
#include "common/lang/comparator.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/field/field.h"
#include <cmath>
#include <sstream>

const char *ATTR_TYPE_NAME[] = {"undefined", "chars", "ints", "floats", "dates", "texts", "booleans"};

const char *attr_type_to_string(AttrType type)
{
  if (type >= UNDEFINED && type <= DATES) {
    return ATTR_TYPE_NAME[type];
  }
  return "unknown";
}
AttrType attr_type_from_string(const char *s)
{
  for (unsigned int i = 0; i < sizeof(ATTR_TYPE_NAME) / sizeof(ATTR_TYPE_NAME[0]); i++) {
    if (0 == strcmp(ATTR_TYPE_NAME[i], s)) {
      return (AttrType)i;
    }
  }
  return UNDEFINED;
}

Value::Value(int val) { set_int(val); }

Value::Value(float val) { set_float(val); }

Value::Value(bool val) { set_boolean(val); }

Value::Value(const char *s, int len /*= 0*/) { set_string(s, len); }

Value Value::clone() const
{
  Value new_val;
  new_val.attr_type_ = attr_type_;
  new_val.length_    = length_;
  new_val.num_value_ = num_value_;
  new_val.str_value_ = str_value_;
  return new_val;
}

void Value::set_data(char *data, int length)
{
  switch (attr_type_) {
    case TEXTS: {
      set_text(data, length);
    } break;
    case CHARS: {
      set_string(data, length);
    } break;
    case DATES: {
      set_date(data, length);
    } break;
    case INTS: {
      num_value_.int_value_ = *(int *)data;
      length_               = length;
    } break;
    case FLOATS: {
      num_value_.float_value_ = *(float *)data;
      length_                 = length;
    } break;
    case BOOLEANS: {
      num_value_.bool_value_ = *(int *)data != 0;
      length_                = length;
    } break;
    default: {
      LOG_WARN("unknown data type: %d", attr_type_);
    } break;
  }
}
void Value::set_int(int val)
{
  attr_type_            = INTS;
  num_value_.int_value_ = val;
  length_               = sizeof(val);
}
void Value::set_float(float val)
{
  attr_type_              = FLOATS;
  num_value_.float_value_ = val;
  length_                 = sizeof(val);
}
void Value::set_boolean(bool val)
{
  attr_type_             = BOOLEANS;
  num_value_.bool_value_ = val;
  length_                = sizeof(val);
}
void Value::set_text(const char *s, int len /*= 4096*/)
{
  attr_type_ = TEXTS;
  if (len > 0) {
    len = strnlen(s, len);
    str_value_.assign(s, len);
  } else {
    str_value_.assign(s);
  }
  length_ = str_value_.length();
}
void Value::set_string(const char *s, int len /*= 0*/)
{
  attr_type_ = CHARS;
  if (len > 0) {
    len = strnlen(s, len);
    str_value_.assign(s, len);
  } else {
    str_value_.assign(s);
  }
  length_ = str_value_.length();
}
void Value::set_date(const char *s, int len /*= 0*/)
{
  attr_type_ = DATES;
  if (len > 0) {
    len = strnlen(s, len);
    str_value_.assign(s, len);
  } else {
    str_value_.assign(s);
  }
  length_ = str_value_.length();
}

void Value::set_value(const Value &value)
{
  switch (value.attr_type_) {
    case INTS: {
      set_int(value.get_int());
    } break;
    case FLOATS: {
      set_float(value.get_float());
    } break;
    case TEXTS: {
      set_text(value.get_string().c_str());
    } break;
    case CHARS: {
      set_string(value.get_string().c_str());
    } break;
    case DATES: {
      set_date(value.get_string().c_str());
    } break;
    case BOOLEANS: {
      set_boolean(value.get_boolean());
    } break;
    case UNDEFINED: {
      ASSERT(false, "got an invalid value type");
    } break;
  }
}

const char *Value::data() const
{
  switch (attr_type_) {
    case NONE: {
      return nullptr;
    } break;
    case TEXTS:
    case CHARS: {
      return str_value_.c_str();
    } break;
    case DATES: {
      return str_value_.c_str();
    } break;
    default: {
      return (const char *)&num_value_;
    } break;
  }
}

std::string Value::to_string() const
{
  std::stringstream os;
  switch (attr_type_) {
    case INTS: {
      os << num_value_.int_value_;
    } break;
    case FLOATS: {
      os << common::double_to_str(num_value_.float_value_);
    } break;
    case BOOLEANS: {
      os << num_value_.bool_value_;
    } break;
    case TEXTS:
    case CHARS: {
      os << str_value_;
    } break;
    case DATES: {
      os << str_value_;
    } break;
    case NONE: {
      os << "NULL";
    } break;
    default: {
      LOG_WARN("unsupported attr type: %d", attr_type_);
    } break;
  }
  return os.str();
}

int Value::compare(const Value &other) const
{
  // 尚未处理text
  if (this->attr_type_ == other.attr_type_) {
    switch (this->attr_type_) {
      case INTS: {
        return common::compare_int((void *)&this->num_value_.int_value_, (void *)&other.num_value_.int_value_);
      } break;
      case FLOATS: {
        return common::compare_float((void *)&this->num_value_.float_value_, (void *)&other.num_value_.float_value_);
      } break;
      case CHARS: {
        return common::compare_string((void *)this->str_value_.c_str(),
            this->str_value_.length(),
            (void *)other.str_value_.c_str(),
            other.str_value_.length());
      } break;
      case DATES: {
        return common::compare_date((void *)this->str_value_.c_str(), (void *)other.str_value_.c_str());
      } break;
      case BOOLEANS: {
        return common::compare_int((void *)&this->num_value_.bool_value_, (void *)&other.num_value_.bool_value_);
      }
      case NONE: {
        return 0;
      }
      default: {
        LOG_WARN("unsupported type: %d", this->attr_type_);
      }
    }
  } else if (this->attr_type_ == NONE) {
    // 任何值理性上都是大于NULL的
    return -1;
  } else if (other.attr_type_ == NONE) {
    return 1;
  } else if (this->attr_type_ == INTS && other.attr_type_ == FLOATS) {
    float this_data = this->num_value_.int_value_;
    return common::compare_float((void *)&this_data, (void *)&other.num_value_.float_value_);
  } else if (this->attr_type_ == FLOATS && other.attr_type_ == INTS) {
    float other_data = other.num_value_.int_value_;
    return common::compare_float((void *)&this->num_value_.float_value_, (void *)&other_data);
  } else if (this->attr_type_ == CHARS && other.attr_type_ == INTS) {
    Value new_val = clone();
    // need to check if new_val is a float
    new_val.str_to_number();
    if (new_val.attr_type_ == INTS) {
      return common::compare_int((void *)&new_val.num_value_.int_value_, (void *)&other.num_value_.int_value_);
    } else if (new_val.attr_type_ == FLOATS) {
      float casted_int = other.num_value_.int_value_;
      return common::compare_float((void *)&new_val.num_value_.float_value_, (void *)&casted_int);
    }
  } else if (this->attr_type_ == INTS && other.attr_type_ == CHARS) {
    Value new_val = other.clone();
    new_val.str_to_number();
    if (new_val.attr_type_ == INTS) {
      return common::compare_int((void *)&this->num_value_.int_value_, (void *)&new_val.num_value_.int_value_);
    } else if (new_val.attr_type_ == FLOATS) {
      float casted_int = this->num_value_.int_value_;
      return common::compare_float((void *)&casted_int, (void *)&new_val.num_value_.float_value_);
    }
  } else if (this->attr_type_ == CHARS && other.attr_type_ == FLOATS) {
    Value new_val = clone();
    new_val.auto_cast(FLOATS);
    return common::compare_float((void *)&new_val.num_value_.float_value_, (void *)&other.num_value_.float_value_);
  } else if (this->attr_type_ == FLOATS && other.attr_type_ == CHARS) {
    Value new_val = other.clone();
    new_val.auto_cast(FLOATS);
    return common::compare_float((void *)&this->num_value_.float_value_, (void *)&new_val.num_value_.float_value_);
  } else if (this->attr_type_ == CHARS && other.attr_type_ == DATES) {
    Value new_val = clone();
    new_val.auto_cast(DATES);
    return common::compare_date((void *)new_val.str_value_.c_str(), (void *)other.str_value_.c_str());
  } else if (this->attr_type_ == DATES && other.attr_type_ == CHARS) {
    Value new_val = other.clone();
    new_val.auto_cast(DATES);
    return common::compare_date((void *)this->str_value_.c_str(), (void *)new_val.str_value_.c_str());
  }
  LOG_WARN("not supported");
  return -1;  // TODO return rc?
}

int Value::get_int() const
{
  switch (attr_type_) {
    case TEXTS:
    case CHARS: {
      try {
        return (int)(std::stol(str_value_));
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to number. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return 0;
      }
    }
    case INTS: {
      return num_value_.int_value_;
    }
    case FLOATS: {
      return (int)(num_value_.float_value_);
    }
    case BOOLEANS: {
      return (int)(num_value_.bool_value_);
    }
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

float Value::get_float() const
{
  switch (attr_type_) {
    case TEXTS:
    case CHARS: {
      try {
        return std::stof(str_value_);
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to float. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return 0.0;
      }
    } break;
    case INTS: {
      return float(num_value_.int_value_);
    } break;
    case FLOATS: {
      return num_value_.float_value_;
    } break;
    case BOOLEANS: {
      return float(num_value_.bool_value_);
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

std::string Value::get_string() const { return this->to_string(); }

bool Value::get_boolean() const
{
  switch (attr_type_) {
    case CHARS: {
      try {
        float val = std::stof(str_value_);
        if (val >= EPSILON || val <= -EPSILON) {
          return true;
        }

        int int_val = std::stol(str_value_);
        if (int_val != 0) {
          return true;
        }

        return !str_value_.empty();
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to float or integer. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return !str_value_.empty();
      }
    } break;
    case INTS: {
      return num_value_.int_value_ != 0;
    } break;
    case FLOATS: {
      float val = num_value_.float_value_;
      return val >= EPSILON || val <= -EPSILON;
    } break;
    case BOOLEANS: {
      return num_value_.bool_value_;
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return false;
    }
  }
  return false;
}

RC Value::str_to_date() const
{
  RC     rc             = RC::SUCCESS;
  Value *bypass_const_p = const_cast<Value *>(this);
  if (bypass_const_p->attr_type() != CHARS) {
    return RC::VALUE_CAST_FAILED;
  }
  bypass_const_p->set_type(DATES);
  // check if date is valid
  int year, month, day;
  int read_count = sscanf(bypass_const_p->data(), "%d-%d-%d", &year, &month, &day);
  if (read_count != 3) {
    return RC::VALUE_DATE_INVALID;
  }
  // use lamda to avoid name confliction
  auto is_date_validate = [](int year, int month, int day) -> RC {
    auto is_leap_year = [](int year) -> bool {
      if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
        return true;
      }
      return false;
    };
    if (year <= 0 || month <= 0 || day <= 0) {
      return RC::VALUE_DATE_INVALID;  // Negative or zero values are not valid.
    }

    if (month > 12) {
      return RC::VALUE_DATE_INVALID;  // Month should be between 1 and 12.
    }

    int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    if (is_leap_year(year)) {
      days_in_month[2] = 29;
    }

    if (day > days_in_month[month]) {
      return RC::VALUE_DATE_INVALID;  // Invalid day for the given month.
    }

    return RC::SUCCESS;  // If all checks passed, the date is valid.
  };
  rc = is_date_validate(year, month, day);

  if (rc != RC::SUCCESS) {
    LOG_WARN("date invalid. year=%d, month=%d, day=%d",year,month,day);
    return rc;
  }
  char *date_formatted = new char[11];
  sprintf(date_formatted, "%04d-%02d-%02d", year, month, day);
  bypass_const_p->set_string(date_formatted);
  bypass_const_p->set_type(DATES);
  return rc;
}

RC Value::str_to_number() const
{
  Value *bypass_const_p = const_cast<Value *>(this);
  if (bypass_const_p->attr_type() != CHARS) {
    return RC::VALUE_CAST_FAILED;
  }
  // check if number is valid
  int   int_val;
  float float_val;
  int   read_count = sscanf(bypass_const_p->data(), "%f", &float_val);
  if (read_count != 1) {
    bypass_const_p->set_int(0);
    return RC::SUCCESS;
  }
  // is a number
  // but not know int or float
  read_count = sscanf(bypass_const_p->data(), "%d", &int_val);
  for (int i = 0; i < bypass_const_p->length(); i++) {
    if (!isdigit(bypass_const_p->data()[i])) {
      // not a digit, over
      if (bypass_const_p->data()[i] == '.') {
        // float
        bypass_const_p->set_float(float_val);
        return RC::SUCCESS;
      } else {
        // int
        bypass_const_p->set_int(int_val);
        return RC::SUCCESS;
      }
      break;
    }
  }
  bypass_const_p->set_int(int_val);
  return RC::SUCCESS;
}

RC Value::number_to_str() const
{
  Value *bypass_const_p = const_cast<Value *>(this);
  if (bypass_const_p->attr_type() == INTS) {
    std::string tmp_str = std::to_string(bypass_const_p->get_int());
    bypass_const_p->set_string(tmp_str.c_str(), tmp_str.length());
    return RC::SUCCESS;
  } else if (bypass_const_p->attr_type() == FLOATS) {
    std::string tmp_str = common::double_to_str(bypass_const_p->get_float());
    bypass_const_p->set_string(tmp_str.c_str(), tmp_str.length());
    return RC::SUCCESS;

  } else {
    return RC::VALUE_CAST_FAILED;
  }
}

RC Value::float_to_int() const
{
  Value *bypass_const_p = const_cast<Value *>(this);
  if (bypass_const_p->attr_type() != FLOATS) {
    return RC::VALUE_CAST_FAILED;
  }
  bypass_const_p->set_int(roundf(bypass_const_p->get_float()));
  return RC::SUCCESS;
}

RC Value::int_to_float() const
{
  Value *bypass_const_p = const_cast<Value *>(this);
  if (bypass_const_p->attr_type() != INTS) {
    return RC::VALUE_CAST_FAILED;
  }
  bypass_const_p->set_float(bypass_const_p->get_int());
  return RC::SUCCESS;
}

RC Value::auto_cast(AttrType field_type) const
{
  Value   *bypass_const_p = const_cast<Value *>(this);
  AttrType value_type     = this->attr_type();
  RC       rc             = RC::SUCCESS;

  if (value_type == CHARS && field_type == TEXTS) {
    bypass_const_p->attr_type_ = TEXTS;
    bypass_const_p->length_    = 4096;
    return RC::SUCCESS;
  }

  if (value_type == CHARS) {
    // convert value to some specific type
    if (field_type == DATES) {
      // CHARS to DATES is ok
      rc = bypass_const_p->str_to_date();
      if (rc != RC::SUCCESS) {
        return rc;
      }
      return rc;
    }
    if (field_type == INTS) {
      rc = bypass_const_p->str_to_number();
      if (rc != RC::SUCCESS) {
        return rc;
      }
      if (attr_type() == FLOATS) {
        rc = bypass_const_p->float_to_int();
        if (rc != RC::SUCCESS) {
          return rc;
        }
      }
      return rc;
    }
    if (field_type == FLOATS) {
      rc = bypass_const_p->str_to_number();
      if (rc != RC::SUCCESS) {
        return rc;
      }
      if (attr_type() == INTS) {
        rc = bypass_const_p->int_to_float();
        if (rc != RC::SUCCESS) {
          return rc;
        }
      }
      return rc;
    }
  } else if (value_type == INTS) {
    if (field_type == CHARS) {
      rc = bypass_const_p->number_to_str();
      if (rc != RC::SUCCESS) {
        return rc;
      }
      return rc;
    }
    if (field_type == FLOATS) {
      rc = bypass_const_p->int_to_float();
      if (rc != RC::SUCCESS) {
        return rc;
      }
      return rc;
    }
  } else if (value_type == FLOATS) {
    if (field_type == CHARS) {
      rc = bypass_const_p->number_to_str();
      if (rc != RC::SUCCESS) {
        return rc;
      }
      return rc;
    }
    if (field_type == INTS) {
      rc = bypass_const_p->float_to_int();
      if (rc != RC::SUCCESS) {
        return rc;
      }
      return rc;
    }
  }
  return RC::SCHEMA_FIELD_TYPE_MISMATCH;
}
