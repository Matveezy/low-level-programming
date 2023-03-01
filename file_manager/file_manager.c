#include "file_manager.h"

store_file file_open_or_create(const char *filename) {
    FILE *storage;
    if ((storage = fopen(filename, "rb+")) == NULL) { // FILE DOESN'T EXIST
        storage = fopen(filename, "w+b"); // CREATE FILE FOR READ/WRITE
        if (storage) return (store_file) {.state = CREATED_FILE, .file = storage};
        else return (store_file) {.state = CREATE_ERROR, .file = NULL};
    } else return (store_file) {.state = EXCELLENT, .file = storage};
}

open_status open_file(FILE **storage, const char *const filename, const char *const mode) {
    *storage = fopen(filename, mode);
    if (*storage == NULL) return OPEN_ERROR;
    else return OPEN_OK;
}

close_status close_file(FILE *in) {
    if (fclose(in) == 0) return CLOSE_OK;
    else return CLOSE_ERROR;
}

read_status read_database_header(FILE *file, database_header *db_header) {
    fseek(file, 0, SEEK_SET);
    if (fread(db_header, sizeof(database_header), 1, file) == 1) {
        return READ_OK;
    } else return READ_ERROR;
}

read_status read_table_header(FILE *storage, const char *table_name, table_header *read_th, size_t table_count) {
    if (table_exists(storage, table_count, table_name, read_th)) {
        return READ_OK;
    } else {
        return READ_ERROR;
    }
}

read_status read_columns_of_table(FILE *storage, table *table) {
    uint16_t *size_of_column_array = malloc(sizeof(uint16_t));
    column *columns = malloc(sizeof(column) * table->table_header->table_schema.column_count);
    fseek(storage, (table->table_header->first_page_number - 1) * PAGE_SIZE + sizeof(page_header), SEEK_SET);
    if (fread(size_of_column_array, sizeof(uint16_t), 1, storage) != 1) {
        return READ_ERROR;
    }
    if (fread(columns, *size_of_column_array, 1, storage) != 1) {
        return READ_ERROR;
    }
    table->table_schema->columns = columns;
    table->table_schema->column_count = table->table_header->table_schema.column_count;
    table->table_schema->last_column = NULL;
    table->table_schema->row_length = table->table_header->table_schema.row_length;
    free(size_of_column_array);
    return READ_OK;
}

bool table_exists(FILE *storage, const size_t len, const char *name, table_header *cur_tb_header) {
    uint32_t index = 0;
    uint32_t current_pointer = sizeof(database_header) + sizeof(page_header);

    page_header *pg_header = malloc(sizeof(page_header));

    fseek(storage, sizeof(database_header), SEEK_SET);
    fread(pg_header, sizeof(page_header), 1, storage);

    if (len != 0) {
        fseek(storage, sizeof(database_header) + sizeof(page_header), SEEK_SET);

        while (index != len) {
            fread(cur_tb_header, sizeof(table_header), 1, storage);
            if (cur_tb_header->valid && strcmp(cur_tb_header->name, name) == 0) {
                free(pg_header);
                return true;
            }

            index++;
            current_pointer += sizeof(table_header);
            if (pg_header->next_page_number != 0 && current_pointer == pg_header->page_free_space_seek) {
                current_pointer = 0;
                fseek(storage, (pg_header->next_page_number - 1) * PAGE_SIZE, SEEK_SET);
                fread(pg_header, sizeof(page_header), 1, storage);
            }

        }
        free(pg_header);
        return false;
    } else {
        free(pg_header);
        return false;
    }
}

write_status overwrite_previous_last_page(FILE *storage, uint32_t previous_last_page, uint32_t new_page_number) {
    page_header *old = malloc(sizeof(page_header));

    fseek(storage, (previous_last_page - 1) * PAGE_SIZE, SEEK_SET);
    fread(old, sizeof(page_header), 1, storage);
    old->next_page_number = new_page_number;

    fseek(storage, (previous_last_page - 1) * PAGE_SIZE, SEEK_SET);
    if (fwrite(old, sizeof(page_header), 1, storage) == 1) {
        free(old);
        return WRITE_OK;
    } else {
        free(old);
        return WRITE_ERROR;
    }
}

write_status overwrite_previous_last_page_db(FILE *storage, database_header *db_header, uint32_t new_next_number) {
    uint32_t old_last_number = db_header->last_page_number;
    page_header *old = malloc(sizeof(page_header));

    if (db_header->last_page_number == 1) {
        fseek(storage, sizeof(database_header), SEEK_SET);
    } else {
        fseek(storage, (db_header->last_page_number - 1) * PAGE_SIZE, SEEK_SET);
    }

    fread(old, sizeof(page_header), 1, storage);
    old->next_page_number = new_next_number;

    if (old_last_number == 1) {
        fseek(storage, sizeof(database_header), SEEK_SET);
    } else {
        fseek(storage, (old_last_number - 1) * PAGE_SIZE, SEEK_SET);
    }

    if (fwrite(old, sizeof(page_header), 1, storage) == 1) {
        free(old);
        return WRITE_OK;
    } else {
        free(old);
        return WRITE_ERROR;
    }
}

write_status write_header_to_meta_page(FILE *storage, database_header *db_header, table_header *new_table_header) {
    page_header *new_page_header = NULL;

    if (db_header->last_page_number == 1) {
        fseek(storage, sizeof(database_header), SEEK_SET);
    } else {
        fseek(storage, (db_header->last_page_number - 1) * PAGE_SIZE, SEEK_SET);
    }

    page_header *meta_page_header = malloc(sizeof(page_header));
    fread(meta_page_header, sizeof(page_header), 1, storage);

    if (!enough_free_space(meta_page_header, sizeof(table_header))) {
        new_page_header = add_meta_page(db_header);
        meta_page_header = new_page_header;
        if (overwrite_dh_after_change(storage, db_header) != WRITE_OK) {
            return WRITE_ERROR;
        }
        fseek(storage, (meta_page_header->page_number - 1) * PAGE_SIZE, SEEK_SET);
        meta_page_header->page_free_space_seek += sizeof(page_header);
        meta_page_header->free_bytes -= sizeof(page_header);
        if (fwrite(meta_page_header, sizeof(page_header), 1, storage) != 1) {
            return WRITE_ERROR;
        }
    }

    fseek(storage, meta_page_header->page_free_space_seek, SEEK_SET);

    if (fwrite(new_table_header, sizeof(table_header), 1, storage) == 1) {
        meta_page_header->page_free_space_seek += sizeof(table_header);
        meta_page_header->free_bytes -= sizeof(table_header);
        if (meta_page_header->page_number == 1) {
            fseek(storage, sizeof(database_header), 1);
        } else {
            fseek(storage, (meta_page_header->page_number - 1) * PAGE_SIZE, SEEK_SET);
        }

        if (fwrite(meta_page_header, sizeof(page_header), 1, storage) == 1) {
            if (NULL != new_page_header) {
                free(new_page_header);
            }
            free(meta_page_header);
            return WRITE_OK;
        }
    }
    if (NULL != new_page_header) {
        free(new_page_header);
    }
    free(meta_page_header);
    return WRITE_ERROR;
}

write_status write_table_page(FILE *storage, page_header *page_to_write, table_schema *schema) {
    fseek(storage, (page_to_write->page_number - 1) * PAGE_SIZE, SEEK_SET);
    uint16_t size_of_column_array = schema->column_count * sizeof(column);
    page_to_write->free_bytes -= sizeof(page_header) + sizeof(uint16_t) + size_of_column_array;
    page_to_write->page_free_space_seek += sizeof(page_header) + sizeof(uint16_t) + size_of_column_array;

    if (fwrite(page_to_write, sizeof(page_header), 1, storage) != 1) {
        return WRITE_ERROR;
    }

    column *column_array = malloc(size_of_column_array);
    column *current = schema->columns;
    for (size_t i = 0; i < schema->column_count; i++) {
        column_array[i] = *current;
        current = current->next;
    }

    fseek(storage, (page_to_write->page_number - 1) * PAGE_SIZE + sizeof(page_header), SEEK_SET);
    if (fwrite(&size_of_column_array, sizeof(uint16_t), 1, storage) != 1) {
        return WRITE_ERROR;
    }
    fseek(storage, (page_to_write->page_number - 1) * PAGE_SIZE + sizeof(page_header) + sizeof(uint16_t), SEEK_SET);
    if (fwrite(column_array, size_of_column_array, 1, storage) != 1) {
        return WRITE_ERROR;
    }

    free(column_array);
    free(page_to_write);
    return WRITE_OK;
}

write_status overwrite_th_after_change(FILE *storage, table_header *updated_table_header) {
    fseek(storage,
          sizeof(database_header) + sizeof(page_header) +
          sizeof(table_header) * (updated_table_header->number_in_meta_page - 1),
          SEEK_SET);
    if (fwrite(updated_table_header, sizeof(table_header), 1, storage) == 1) {
        return WRITE_OK;
    } else {
        return WRITE_ERROR;
    }
}

write_status overwrite_dh_after_change(FILE *file, struct database_header *updated_db_header) {
    fseek(file, 0, SEEK_SET);
    if (fwrite(updated_db_header, sizeof(struct database_header), 1, file) == 1) return WRITE_OK;
    else return WRITE_ERROR;
}

