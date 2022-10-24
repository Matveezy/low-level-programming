#ifndef LOW_LEVEL_PROGRAMMING_1_STRUCT_H
#define LOW_LEVEL_PROGRAMMING_1_STRUCT_H

#include "inttypes.h"
#include "stddef.h"

typedef uint16_t LocationIndex;

typedef uint16_t ItemOffset;
typedef uint16_t ItemLength;

typedef struct ItemIdData {
    ItemOffset offset;
    ItemLength length;
} ItemIdData;

typedef ItemIdData *ItemId;

typedef struct PageHeaderData {
    LocationIndex free_spase_start_offset;
    LocationIndex free_space_end_offset;
    ItemIdData itemIds[];
} PageHeaderData;

typedef PageHeaderData *PageHeader;

#endif //LOW_LEVEL_PROGRAMMING_1_STRUCT_H
