#ifndef LOW_LEVEL_PROGRAMMING_1_SCHEMA_MANAGER_H
#define LOW_LEVEL_PROGRAMMING_1_SCHEMA_MANAGER_H

#include "../includes.h"
#include "../struct/table.h"

column *create_string_column(const char *, column_type, uint16_t);
column *create_column(const char *, column_type);
#endif //LOW_LEVEL_PROGRAMMING_1_SCHEMA_MANAGER_H
