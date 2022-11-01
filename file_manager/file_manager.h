#ifndef LOW_LEVEL_PROGRAMMING_1_FILE_MANAGER_H
#define LOW_LEVEL_PROGRAMMING_1_FILE_MANAGER_H

#include "stdio.h"

typedef enum state {
    EXCELLENT = 0,
    OPEN_ERROR,
    CLOSE_ERROR,
    READ_ERROR,
    WRITE_ERROR,
    CREATED_FILE,
    CREATE_ERROR
} state;

typedef struct storeFile {
    state state;
    FILE *file;
} storeFile;

storeFile fileOpenOrCreate(const char *filename);

#endif //LOW_LEVEL_PROGRAMMING_1_FILE_MANAGER_H
