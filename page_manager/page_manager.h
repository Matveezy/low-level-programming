#ifndef LOW_LEVEL_PROGRAMMING_1_PAGE_MANAGER_H
#define LOW_LEVEL_PROGRAMMING_1_PAGE_MANAGER_H
#define PAGE_SIZE 8192

#include "../includes.h"
#include "../struct/page.h"
#include "../file_manager/file_manager.h"

bool enough_free_space(page_header *, uint32_t);

page_header *add_meta_page(database_header *);

#endif //LOW_LEVEL_PROGRAMMING_1_PAGE_MANAGER_H
