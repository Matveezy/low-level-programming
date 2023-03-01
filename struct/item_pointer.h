#ifndef LOW_LEVEL_PROGRAMMING_1_ITEM_POINTER_H
#define LOW_LEVEL_PROGRAMMING_1_ITEM_POINTER_H

#include "../includes.h"
#include "block.h"

typedef uint16_t item_id_mumber;

typedef struct item_pointer_data {
    struct block_id_data block_id;
    item_id_mumber item_id;
};

#endif //LOW_LEVEL_PROGRAMMING_1_ITEM_POINTER_H
