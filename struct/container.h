#ifndef LOW_LEVEL_PROGRAMMING_1_CONTAINER_H
#define LOW_LEVEL_PROGRAMMING_1_CONTAINER_H

#include "database.h"
#include "table.h"

typedef struct container container;
typedef enum show_mode show_mode;

struct container {
    table_schema *table_schema;
    database *db;
    table *table;
    row *row;
};

enum show_mode {
    SHOW_ROWS,
    NO_ROWS
};
#endif //LOW_LEVEL_PROGRAMMING_1_CONTAINER_H
