#ifndef LOW_LEVEL_PROGRAMMING_1_TABLE_H
#define LOW_LEVEL_PROGRAMMING_1_TABLE_H

#include "../includes.h"
#include "database.h"

#define COLUMN_NAME_LENGTH 32
#define TABLE_NAME_LENGTH 32

typedef uint64_t row_length;
typedef struct table_header table_header;

typedef enum column_type {
    TYPE_BOOL = 0,
    TYPE_INT32,
    TYPE_FLOAT,
    TYPE_STRING
} column_type;

typedef struct column {
    char column_name[COLUMN_NAME_LENGTH];
    column_type column_type;
    uint16_t column_size;
    uint32_t offset;
    struct column *next;
} column;

typedef struct table_schema {
    uint32_t column_count;
    row_length row_length;
    column *columns;
    column *last_column;

} table_schema;

typedef struct table {
    table_schema *table_schema;
    table_header *table_header;
} table;

typedef struct table_header {
    char name[COLUMN_NAME_LENGTH];
    table *table;
    database *database;
    table_schema table_schema;

    uint32_t number_in_meta_page;
    uint32_t page_count;
    uint32_t first_page_number;
    uint32_t last_page_number;
    bool valid;
} table_header;

typedef struct row {
    bool valid;
    table *table;
    void **data;
} row;
#endif //LOW_LEVEL_PROGRAMMING_1_TABLE_H
