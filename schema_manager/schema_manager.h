#ifndef LOW_LEVEL_PROGRAMMING_1_SCHEMA_MANAGER_H
#define LOW_LEVEL_PROGRAMMING_1_SCHEMA_MANAGER_H

#include "../includes.h"
#include "../struct/table.h"

column *create_string_column(const char *, column_type, uint16_t);

column *create_column(const char *, column_type);

table_schema *create_table_schema();

table_schema *add_column_to_schema(table_schema *, const char *, column_type);

table_schema *add_string_column_to_schema(table_schema *, const char *, column_type, uint16_t);

int32_t get_string_column_len(const column *, size_t, const char *);

void close_table_schema(table_schema *);

void close_table_schema(table_schema *);

void close_schema(table_schema *);

void destroy_column_list(column *);

#endif //LOW_LEVEL_PROGRAMMING_1_SCHEMA_MANAGER_H
