#ifndef LOW_LEVEL_PROGRAMMING_1_PAGE_MANAGER_H
#define LOW_LEVEL_PROGRAMMING_1_PAGE_MANAGER_H

#include "../includes.h"
#include "../struct/page.h"
#include "../file_manager/file_manager.h"
#include "../schema_manager/schema_manager.h"
#include "../struct/query.h"
#include "../content_manager/content_manager.h"
#include "../struct/container.h"

bool enough_free_space(page_header *, uint32_t);

page_header *add_meta_page(database_header *);

database *init_database(const char *, database_type);

table *create_table_from_schema(table_schema *, database *, const char *);

struct page_header *add_page(table_header *, database_header *);

void close_table(table *);

void close_database(database *);

query *create_query(query_type query_type, table *tables, char *column[], void *values[], int32_t row_count);

table *get_table(const char *, database *);

void run_query(query *, show_mode);

query_join *create_join_query(table *, table *, char *, char *);

void run_join_query(query_join *, show_mode);

void close_query(query *);

void close_join_query(query_join *);

#endif //LOW_LEVEL_PROGRAMMING_1_PAGE_MANAGER_H
