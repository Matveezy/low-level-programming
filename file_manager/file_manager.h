#ifndef LOW_LEVEL_PROGRAMMING_1_FILE_MANAGER_H

#include "stdio.h"

#define LOW_LEVEL_PROGRAMMING_1_FILE_MANAGER_H

typedef enum state {
    EXCELLENT = 0,
    OPEN_ERROR,
    CLOSE_ERROR,
    READ_ERROR,
    WRITE_ERROR
} state;

struct file {
    state state;
    FILE *file;
};


#endif //LOW_LEVEL_PROGRAMMING_1_FILE_MANAGER_H
