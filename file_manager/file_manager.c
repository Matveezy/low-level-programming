#include "file_manager.h"

storeFile fileOpenOrCreate(const char *filename) {
    FILE *file;
    if ((file = fopen(filename, "rb+")) == NULL) { // FILE DOESN'T EXIST
        file = fopen(filename, "w+b"); // CREATE FILE FOR READ/WRITE
        if (file) return (storeFile) {.state = CREATED_FILE, file = file};
        else return (storeFile) {.state = CREATE_ERROR, file = NULL};
    } else return (storeFile) {.state = EXCELLENT, file = file};
}

