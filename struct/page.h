#ifndef LOW_LEVEL_PROGRAMMING_1_PAGE_H
#define LOW_LEVEL_PROGRAMMING_1_PAGE_H

#include "../includes.h"
#include "table.h"

#define PAGE_SIZE 4096

typedef struct page_header page_header;

struct page_header {
    uint16_t free_bytes;
    uint32_t page_number;
    bool is_dirty;
    uint32_t page_free_space_seek;
    char table_name[TABLE_NAME_LENGTH];
    uint32_t table_number_in_meta_page;
    uint32_t next_page_number;
};

#endif //LOW_LEVEL_PROGRAMMING_1_PAGE_H
