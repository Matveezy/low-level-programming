#ifndef LOW_LEVEL_PROGRAMMING_1_QUERY_H
#define LOW_LEVEL_PROGRAMMING_1_QUERY_H

#include "../includes.h"
#include "table.h"

#define MAX_COLUMN_NAME_LEN 20
typedef enum query_type query_type;
typedef struct query query;
typedef struct query_join query_join;

enum query_type {
    SELECT_WHERE = 0,
    UPDATE_WHERE,
    DELETE_WHERE,
};

struct query {
    enum query_type query_type;

    struct table *table;
    char **column_name;
    void **column_value;

    int32_t rows_number;
};

struct query_join {
    struct table *left_table;
    struct table *right_table;
    char *left_column_name;
    char *right_column_name;
};

#endif //LOW_LEVEL_PROGRAMMING_1_QUERY_H
