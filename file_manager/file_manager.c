#include "file_manager.h"

struct file file_open(const char *file_path) {
    FILE *file;
    if ((file = fopen(file_path, "rw")) == NULL) return (struct file) {.state = OPEN_ERROR, file = NULL};
    return (struct file) {.state = EXCELLENT, file = file};
}

