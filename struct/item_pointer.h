#ifndef LOW_LEVEL_PROGRAMMING_1_ITEM_POINTER_H
#define LOW_LEVEL_PROGRAMMING_1_ITEM_POINTER_H

#include "../includes.h"
#include "block.h"

typedef uint16_t ItemIdNumber;

typedef struct ItemPointerData {
    BlockIdData blockId;
    ItemIdNumber itemId;
} ItemPointerData;

#endif //LOW_LEVEL_PROGRAMMING_1_ITEM_POINTER_H
