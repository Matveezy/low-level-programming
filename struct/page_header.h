#ifndef LOW_LEVEL_PROGRAMMING_1_PAGE_HEADER_H
#define LOW_LEVEL_PROGRAMMING_1_PAGE_HEADER_H

#include "../includes.h"
#include "item_identifier.h"

typedef uint16_t LocationIndex;

typedef struct PageHeaderData {
    LocationIndex startFreeSpaceOffset;
    LocationIndex endFreeSpaceOffset;
    ItemIdData itemIds[];
} PageHeaderData;

typedef PageHeaderData *PageHeader;

#endif //LOW_LEVEL_PROGRAMMING_1_PAGE_HEADER_H
