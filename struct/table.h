#ifndef LOW_LEVEL_PROGRAMMING_1_TABLE_H
#define LOW_LEVEL_PROGRAMMING_1_TABLE_H

#include "../includes.h"
#include "database.h"

#define COLUMN_NAME_LENGTH 20
#define MAX_TABLE_HEADER_NAME_LENGTH 20
#define TABLE_NAME_LENGTH 32

typedef uint64_t row_length;
typedef struct table_header table_header;
typedef struct row_header row_header;
typedef struct row row;
typedef enum column_type column_type;
typedef struct column column;
typedef struct table_schema table_schema;
typedef struct table table;
typedef struct expanded_query expanded_query;

enum column_type {
    TYPE_BOOL = 0,
    TYPE_INT32,
    TYPE_FLOAT,
    TYPE_STRING
};

struct expanded_query {
    column_type column_type;
    char column_name[COLUMN_NAME_LENGTH];
    uint16_t column_size;
    uint32_t offset;
};

struct column {
    char column_name[COLUMN_NAME_LENGTH];
    column_type column_type;
    uint16_t column_size;
    uint32_t offset;
    struct column *next;
};

struct table_schema {
    uint32_t column_count;
    row_length row_length;
    column *columns;
    column *last_column;
};

struct table {
    table_schema *table_schema;
    table_header *table_header;
};

struct table_header {
    char name[MAX_TABLE_HEADER_NAME_LENGTH];
    table *table;
    database *database;
    table_schema table_schema;

    uint32_t number_in_meta_page;
    uint32_t page_count;
    uint32_t first_page_number;
    uint32_t last_page_number;
    bool valid;
};

struct row {
    row_header *row_header;
    table *table;
    void **data;
};

struct row_header {
    bool valid;
};


#endif //LOW_LEVEL_PROGRAMMING_1_TABLE_H
