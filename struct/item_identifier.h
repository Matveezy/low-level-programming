#ifndef LOW_LEVEL_PROGRAMMING_1_ITEM_IDENTIFIER_H
#define LOW_LEVEL_PROGRAMMING_1_ITEM_IDENTIFIER_H

#include "../includes.h"

typedef uint16_t item_offset;
typedef uint16_t item_length;

typedef struct item_id_data {
    item_offset offset; // offset to tuple from page start
    item_length length; // length of tuple
} item_id_data;

typedef struct item_id_data *item_id;
#endif //LOW_LEVEL_PROGRAMMING_1_ITEM_IDENTIFIER_H
