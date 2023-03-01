#ifndef LOW_LEVEL_PROGRAMMING_1_FILE_MANAGER_H
#define LOW_LEVEL_PROGRAMMING_1_FILE_MANAGER_H

#include "stdio.h"
#include "../includes.h"
#include "../struct/database.h"
#include "../struct/table.h"
#include "../struct/page.h"
#include "../page_manager/page_manager.h"

typedef enum open_status {
    OPEN_OK = 0,
    OPEN_ERROR
} open_status;

typedef enum close_status {
    CLOSE_OK = 0,
    CLOSE_ERROR
} close_status;

typedef enum write_status {
    WRITE_OK = 0,
    WRITE_ERROR
} write_status;

typedef enum read_status {
    READ_OK = 0,
    READ_ERROR
} read_status;

typedef enum state {
    EXCELLENT = 0,
    CREATED_FILE,
    CREATE_ERROR
} state;

typedef struct store_file {
    state state;
    FILE *file;
} store_file;

store_file file_open_or_create(const char *);

open_status open_file(FILE **in, const char *const, const char *const);

read_status read_database_header(FILE *, database_header *);

bool table_exists(FILE *, const size_t, const char *, table_header *);

write_status overwrite_previous_last_page(FILE *, uint32_t, uint32_t);

write_status overwrite_th_after_change(FILE *, table_header *);

write_status overwrite_dh_after_change(FILE *, database_header *);

write_status overwrite_previous_last_page_db(FILE *, database_header *, uint32_t);

write_status write_header_to_meta_page(FILE *, database_header *, table_header *);

write_status write_table_page(FILE *, page_header *, table_schema *);

read_status read_table_header(FILE *, const char *, table_header *, size_t);

close_status close_file(FILE *);

read_status read_columns_of_table(FILE *, table *);

#endif //LOW_LEVEL_PROGRAMMING_1_FILE_MANAGER_H
