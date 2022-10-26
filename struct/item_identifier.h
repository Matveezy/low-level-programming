#ifndef LOW_LEVEL_PROGRAMMING_1_ITEM_IDENTIFIER_H
#define LOW_LEVEL_PROGRAMMING_1_ITEM_IDENTIFIER_H

#include "../includes.h"

typedef uint16_t ItemOffset;
typedef uint16_t ItemLength;

typedef struct ItemIdData {
    ItemOffset offset; // offset to tuple from page start
    ItemLength length; // length of tuple
} ItemIdData;

typedef ItemIdData *ItemId;
#endif //LOW_LEVEL_PROGRAMMING_1_ITEM_IDENTIFIER_H
