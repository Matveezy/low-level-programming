#ifndef LOW_LEVEL_PROGRAMMING_1_CONTENT_MANAGER_H
#define LOW_LEVEL_PROGRAMMING_1_CONTENT_MANAGER_H

#include "../includes.h"
#include "../struct/table.h"
#include "../file_manager/file_manager.h"
#include "../schema_manager/schema_manager.h"
#include "../struct/query.h"

row *create_row(table *);

void fill_int(row *, int32_t, uint32_t);

int32_t column_offset(column *, size_t, const char *);

void fill_attribute(row *, const char *, column_type, void *, bool);

void insert_row(row *);

void select_row_from_table(query *);

void close_row(row *);

void update_row_in_table(query *);

void delete_row_from_table(query*);

#endif //LOW_LEVEL_PROGRAMMING_1_CONTENT_MANAGER_H
