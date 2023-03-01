#ifndef LOW_LEVEL_PROGRAMMING_1_DATABASE_H
#define LOW_LEVEL_PROGRAMMING_1_DATABASE_H

#include <stdio.h>
#include "../includes.h"

#define DB_NAME_LENGTH 32

typedef struct database database;

typedef struct database_header {
    database *db;
    char database_name[DB_NAME_LENGTH];
    uint32_t table_count;
    uint32_t page_count;
    uint32_t page_size;
    uint32_t last_page_number;
} database_header;

typedef struct database {
    database_header *database_header;
    FILE *database_file;
} database;


#endif //LOW_LEVEL_PROGRAMMING_1_DATABASE_H
