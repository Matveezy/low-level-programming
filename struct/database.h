#ifndef LOW_LEVEL_PROGRAMMING_1_DATABASE_H
#define LOW_LEVEL_PROGRAMMING_1_DATABASE_H

#include <stdio.h>
#include "../includes.h"

#define DB_NAME_LENGTH 32

typedef struct database database;
typedef struct database_header database_header;
typedef enum database_type database_type;

struct database_header {
    char database_name[DB_NAME_LENGTH];
    database *db;
    uint32_t table_count;
    uint32_t page_count;
    uint32_t page_size;
    uint32_t last_page_number;
};

struct database {
    database_header *database_header;
    FILE *database_file;
};

enum database_type {
    EXISTING = 0,
    TO_BE_CREATED
};

#endif //LOW_LEVEL_PROGRAMMING_1_DATABASE_H
