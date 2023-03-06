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
    if (fread(db_header, sizeof(struct database_header), 1, file) == 1) {
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
    column *columns = malloc(sizeof(struct column) * table->table_header->table_schema.column_count);
    fseek(storage, (table->table_header->first_page_number - 1) * PAGE_SIZE + sizeof(struct page_header), SEEK_SET);
    if (fread(size_of_column_array, sizeof(uint16_t), 1, storage) != 1) {
        return READ_ERROR;
    }
//    columns = (column *) mmap(0, *size_of_column_array , PROT_READ,
//                              MAP_PRIVATE,
//                              fileno(storage),
//                              (table->table_header->first_page_number - 1) * PAGE_SIZE + sizeof(struct page_header));
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
    uint32_t current_pointer = sizeof(struct database_header) + sizeof(struct page_header);

    page_header *pg_header = malloc(sizeof(struct page_header));

    fseek(storage, sizeof(struct database_header), SEEK_SET);
    fread(pg_header, sizeof(struct page_header), 1, storage);

    if (len != 0) {
        fseek(storage, sizeof(struct database_header) + sizeof(struct page_header), SEEK_SET);

        while (index != len) {
            fread(cur_tb_header, sizeof(struct table_header), 1, storage);
            if (cur_tb_header->valid && strcmp(cur_tb_header->name, name) == 0) {
                free(pg_header);
                return true;
            }

            index++;
            current_pointer += sizeof(struct table_header);
            if (pg_header->next_page_number != 0 && current_pointer == pg_header->page_free_space_seek) {
                current_pointer = 0;
                fseek(storage, (pg_header->next_page_number - 1) * PAGE_SIZE, SEEK_SET);
                fread(pg_header, sizeof(struct page_header), 1, storage);
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
    page_header *old = malloc(sizeof(struct page_header));

    fseek(storage, (previous_last_page - 1) * PAGE_SIZE, SEEK_SET);
    fread(old, sizeof(struct page_header), 1, storage);
    old->next_page_number = new_page_number;

    fseek(storage, (previous_last_page - 1) * PAGE_SIZE, SEEK_SET);
    if (fwrite(old, sizeof(struct page_header), 1, storage) == 1) {
        free(old);
        return WRITE_OK;
    } else {
        free(old);
        return WRITE_ERROR;
    }
}

write_status overwrite_previous_last_page_db(FILE *storage, database_header *db_header, uint32_t new_next_number) {
    uint32_t old_last_number = db_header->last_page_number;
    page_header *old = malloc(sizeof(struct page_header));

    if (db_header->last_page_number == 1) {
        fseek(storage, sizeof(struct database_header), SEEK_SET);
    } else {
        fseek(storage, (db_header->last_page_number - 1) * PAGE_SIZE, SEEK_SET);
    }

    fread(old, sizeof(struct page_header), 1, storage);
    old->next_page_number = new_next_number;

    if (old_last_number == 1) {
        fseek(storage, sizeof(struct database_header), SEEK_SET);
    } else {
        fseek(storage, (old_last_number - 1) * PAGE_SIZE, SEEK_SET);
    }

    if (fwrite(old, sizeof(struct page_header), 1, storage) == 1) {
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
        fseek(storage, sizeof(struct database_header), SEEK_SET);
    } else {
        fseek(storage, (db_header->last_page_number - 1) * PAGE_SIZE, SEEK_SET);
    }

    page_header *meta_page_header = malloc(sizeof(struct page_header));
    fread(meta_page_header, sizeof(struct page_header), 1, storage);

    if (!enough_free_space(meta_page_header, sizeof(struct table_header))) {
        new_page_header = add_meta_page(db_header);
        meta_page_header = new_page_header;
        if (overwrite_dh_after_change(storage, db_header) != WRITE_OK) {
            return WRITE_ERROR;
        }
        fseek(storage, (meta_page_header->page_number - 1) * PAGE_SIZE, SEEK_SET);
        meta_page_header->page_free_space_seek += sizeof(struct page_header);
        meta_page_header->free_bytes -= sizeof(struct page_header);
        if (fwrite(meta_page_header, sizeof(struct page_header), 1, storage) != 1) {
            return WRITE_ERROR;
        }
    }

    fseek(storage, meta_page_header->page_free_space_seek, SEEK_SET);

    if (fwrite(new_table_header, sizeof(struct table_header), 1, storage) == 1) {
        meta_page_header->page_free_space_seek += sizeof(struct table_header);
        meta_page_header->free_bytes -= sizeof(struct table_header);
        if (meta_page_header->page_number == 1) {
            fseek(storage, sizeof(struct database_header), SEEK_SET);
        } else {
            fseek(storage, (meta_page_header->page_number - 1) * PAGE_SIZE, SEEK_SET);
        }

        if (fwrite(meta_page_header, sizeof(struct page_header), 1, storage) == 1) {
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
    uint16_t size_of_column_array = schema->column_count * sizeof(struct column);
    page_to_write->free_bytes -= sizeof(struct page_header) + sizeof(uint16_t) + size_of_column_array;
    page_to_write->page_free_space_seek += sizeof(struct page_header) + sizeof(uint16_t) + size_of_column_array;

    if (fwrite(page_to_write, sizeof(struct page_header), 1, storage) != 1) {
        return WRITE_ERROR;
    }

    column *column_array = malloc(size_of_column_array);
    column *current = schema->columns;
    for (size_t i = 0; i < schema->column_count; i++) {
        column_array[i] = *current;
        current = current->next;
    }

    fseek(storage, (page_to_write->page_number - 1) * PAGE_SIZE + sizeof(struct page_header), SEEK_SET);
    if (fwrite(&size_of_column_array, sizeof(uint16_t), 1, storage) != 1) {
        return WRITE_ERROR;
    }
    fseek(storage, (page_to_write->page_number - 1) * PAGE_SIZE + sizeof(struct page_header) + sizeof(uint16_t),
          SEEK_SET);
    if (fwrite(column_array, size_of_column_array, 1, storage) != 1) {
        return WRITE_ERROR;
    }

    free(column_array);
    free(page_to_write);
    return WRITE_OK;
}

write_status overwrite_th_after_change(FILE *storage, table_header *updated_table_header) {
    fseek(storage,
          sizeof(struct database_header) + sizeof(struct page_header) +
          sizeof(struct table_header) * (updated_table_header->number_in_meta_page - 1),
          SEEK_SET);
    if (fwrite(updated_table_header, sizeof(struct table_header), 1, storage) == 1) {
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

write_status write_db_to_file(FILE *storage, database_header *db_header, page_header *meta_page_header) {
    fseek(storage, 0, SEEK_SET);
    if (fwrite(db_header, sizeof(struct database_header), 1, storage) == 1) {
        meta_page_header->page_free_space_seek += sizeof(struct database_header) + sizeof(struct page_header);
        meta_page_header->free_bytes -= sizeof(struct database_header) + sizeof(struct page_header);
        if (fwrite(meta_page_header, sizeof(struct page_header), 1, storage) == 1) {
            return WRITE_OK;
        } else {
            return WRITE_ERROR;
        }
    } else {
        return WRITE_ERROR;
    }
}

enum write_status write_row_to_page(FILE *file, uint32_t page_to_write_num, struct row *row) {
    struct page_header *new_page_header = NULL;
    uint32_t row_len = row->table->table_header->table_schema.row_length;
    uint32_t sum_volume = sizeof(struct row_header) + row_len;
    fseek(file, (page_to_write_num - 1) * PAGE_SIZE, SEEK_SET);
    struct page_header *ph_to_write = malloc(sizeof(struct page_header));
    if (fread(ph_to_write, sizeof(struct page_header), 1, file) == 1) {
        if (!enough_free_space(ph_to_write, sum_volume)) {
            new_page_header = add_page(row->table->table_header, row->table->table_header->database->database_header);

            fseek(file, (page_to_write_num - 1) * PAGE_SIZE + sizeof(struct page_header), SEEK_SET);
            uint16_t size_of_column_array;
            fread(&size_of_column_array, sizeof(uint16_t), 1, file);
            struct column *columns = malloc(size_of_column_array);
            fseek(file, (page_to_write_num - 1) * PAGE_SIZE + sizeof(struct page_header) + sizeof(uint16_t), SEEK_SET);
            fread(columns, size_of_column_array, 1, file);

            page_to_write_num = new_page_header->page_number;
            ph_to_write = new_page_header;
            ph_to_write->free_bytes -= sizeof(struct page_header) + sizeof(uint16_t) + size_of_column_array;
            ph_to_write->page_free_space_seek += sizeof(struct page_header) + sizeof(uint16_t) + size_of_column_array;

            fseek(file, (page_to_write_num - 1) * PAGE_SIZE, SEEK_SET);
            fwrite(ph_to_write, sizeof(struct page_header), 1, file);
            fwrite(&size_of_column_array, sizeof(uint16_t), 1, file);
            fwrite(columns, size_of_column_array, 1, file);
            free(columns);
        }
        fseek(file, (page_to_write_num - 1) * PAGE_SIZE + ph_to_write->page_free_space_seek, SEEK_SET);
        if (fwrite(row->row_header, sizeof(struct row_header), 1, file) == 1) {
            fseek(file,
                  (page_to_write_num - 1) * PAGE_SIZE + ph_to_write->page_free_space_seek + sizeof(struct row_header),
                  SEEK_SET);
            if (fwrite(row->data, row_len, 1, file) == 1) {
                ph_to_write->free_bytes -= sizeof(struct row_header) + row_len;
                ph_to_write->page_free_space_seek += sizeof(struct row_header) + row_len;

                fseek(file, (page_to_write_num - 1) * PAGE_SIZE, SEEK_SET);
                if (fwrite(ph_to_write, sizeof(struct page_header), 1, file) != 1) {
                    if (new_page_header != NULL) free(new_page_header);
                    free(ph_to_write);
                    return WRITE_ERROR;
                }
                if (new_page_header != NULL) free(new_page_header);
                free(ph_to_write);
                return WRITE_OK;
            }
        }
        if (new_page_header != NULL) free(new_page_header);
        free(ph_to_write);
        return WRITE_ERROR;
    } else {
        if (new_page_header != NULL) free(new_page_header);
        free(ph_to_write);
        return WRITE_ERROR;
    }
}

void select_where(FILE *storage, table *table, uint32_t offset, uint16_t column_size, void *column_value,
                  column_type column_type, int32_t row_count) {
    uint32_t selected_count = 0;
    uint32_t current_pointer =
            sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->table_schema->column_count;
    row_header *row_header = malloc(sizeof(struct row_header));
    char *pointer_to_read = malloc(table->table_schema->row_length);

    page_header *page_header = malloc(sizeof(struct page_header));
    fseek(storage, (table->table_header->first_page_number - 1) * PAGE_SIZE, SEEK_SET);
    fread(page_header, sizeof(struct page_header), 1, storage);

    fseek(storage,
          (table->table_header->first_page_number - 1) * PAGE_SIZE + sizeof(struct page_header) + sizeof(uint16_t) +
          sizeof(struct column) * table->table_schema->column_count, SEEK_SET);

    while (current_pointer != page_header->page_free_space_seek) {
        fseek(storage, (page_header->page_number - 1) * PAGE_SIZE + current_pointer, SEEK_SET);
        fread(row_header, sizeof(struct row_header), 1, storage);
        if (row_header->valid) {
            fseek(storage, (page_header->page_number - 1) * PAGE_SIZE + current_pointer + sizeof(struct row_header),
                  SEEK_SET);
            fread(pointer_to_read, table->table_schema->row_length, 1, storage);
            switch (column_type) {
                case TYPE_INT32:
                    if (int_equals(pointer_to_read, column_value, offset)) {
                        print_data(pointer_to_read, table->table_schema->columns, table->table_schema->column_count);
                        selected_count++;
                    }
                    break;
                case TYPE_FLOAT:
                    if (float_equals(pointer_to_read, column_value, offset)) {
                        print_data(pointer_to_read, table->table_schema->columns, table->table_schema->column_count);
                        selected_count++;
                    }
                    break;
                case TYPE_BOOL:
                    if (bool_equals(pointer_to_read, column_value, offset)) {
                        print_data(pointer_to_read, table->table_schema->columns, table->table_schema->column_count);
                        selected_count++;
                    }
                    break;
                case TYPE_STRING:
                    if (string_equals(pointer_to_read, column_value, offset)) {
                        print_data(pointer_to_read, table->table_schema->columns, table->table_schema->column_count);
                        selected_count++;
                    }
                    break;
            }
        }

        current_pointer += sizeof(struct row_header) + table->table_schema->row_length;

        if (page_header->next_page_number != 0 && current_pointer != page_header->page_free_space_seek) {
            current_pointer +=
                    sizeof(struct page_header) + sizeof(uint16_t) +
                    sizeof(struct column) * table->table_schema->column_count;
            fseek(storage, (page_header->next_page_number - 1) * PAGE_SIZE, SEEK_SET);
            fread(page_header, sizeof(struct page_header), 1, storage);
            fseek(storage, (page_header->next_page_number - 1) * PAGE_SIZE + sizeof(uint16_t) +
                           sizeof(struct column) * table->table_schema->column_count, SEEK_SET);
        }
    }

    free(row_header);
    free(page_header);
    free(pointer_to_read);
    printf("\n");
    printf("Amount of rows:%d\n", selected_count);
}

void
update_where(FILE *file, struct table *table, expanded_query *first, expanded_query *second, void **column_values) {
    uint32_t updated_count = 0;
    uint32_t current_pointer =
            sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->table_schema->column_count;

    struct row_header *rh = malloc(sizeof(struct row_header));
    char *pointer_to_read_row = malloc(table->table_schema->row_length);
    struct page_header *ph = malloc(sizeof(struct page_header));

    fseek(file, (table->table_header->first_page_number - 1) * PAGE_SIZE, SEEK_SET);
    fread(ph, sizeof(struct page_header), 1, file); //прочитали заголовок страницы

    fseek(file,
          (table->table_header->first_page_number - 1) * PAGE_SIZE + sizeof(struct page_header) + sizeof(uint16_t) +
          sizeof(struct column) * table->table_schema->column_count, SEEK_SET); //передвинулись на начало строк

    while (current_pointer != ph->page_free_space_seek) {

        fseek(file, (ph->page_number - 1) * PAGE_SIZE + current_pointer, SEEK_SET);
        fread(rh, sizeof(struct row_header), 1, file);
        if (rh->valid) {
            uint32_t pointer_to_update = current_pointer + sizeof(struct row_header);
            fread(pointer_to_read_row, table->table_schema->row_length, 1,
                  file); //прочитали всю строку и у нас есть указатель на нее

            switch (first->column_type) {
                case TYPE_INT32:
                    if (int_equals(pointer_to_read_row, column_values[0], first->offset)) {
                        update_content(pointer_to_read_row, column_values[1], second, table, pointer_to_update,
                                       ph->page_number);
                        updated_count++;
                    }
                    break;
                case TYPE_BOOL:
                    if (bool_equals(pointer_to_read_row, column_values[0], first->offset)) {
                        update_content(pointer_to_read_row, column_values[1], second, table, pointer_to_update,
                                       ph->page_number);
                        updated_count++;
                    }
                    break;
                case TYPE_STRING:
                    if (string_equals(pointer_to_read_row, column_values[0], first->offset)) {
                        update_content(pointer_to_read_row, column_values[1], second, table, pointer_to_update,
                                       ph->page_number);
                        updated_count++;
                    }
                    break;
                case TYPE_FLOAT:
                    if (float_equals(pointer_to_read_row, column_values[0], first->offset)) {
                        update_content(pointer_to_read_row, column_values[1], second, table, pointer_to_update,
                                       ph->page_number);
                        updated_count++;
                    }
                    break;
            }
            current_pointer += sizeof(struct row_header) + table->table_schema->row_length;
        } else {
            current_pointer += sizeof(struct row_header) + table->table_schema->row_length;
        }

        if (ph->next_page_number != 0 && current_pointer == ph->page_free_space_seek) {
            current_pointer = sizeof(struct page_header) + sizeof(uint16_t) +
                              sizeof(struct column) * table->table_schema->column_count;
            fseek(file, (ph->next_page_number - 1) * PAGE_SIZE, SEEK_SET);
            fread(ph, sizeof(struct page_header), 1, file);
            fseek(file, (ph->page_number - 1) * PAGE_SIZE + sizeof(struct page_header) + sizeof(uint16_t) +
                        sizeof(struct column) * table->table_schema->column_count, SEEK_SET);
        }

    }
    free(rh);
    free(ph);
    free(pointer_to_read_row);
    printf("=====================\n");
    printf("Всего %d строк\n", updated_count);
}

void delete_where(FILE *file, struct table *table, struct expanded_query *expanded, void *column_value) {
    uint32_t current_pointer =
            sizeof(struct page_header) + sizeof(uint16_t) + sizeof(struct column) * table->table_schema->column_count;
    uint32_t count_of_deleted = 0;

    struct row_header *rh = malloc(sizeof(struct row_header));
    char *pointer_to_read_row = malloc(table->table_schema->row_length);
    struct page_header *ph = malloc(sizeof(struct page_header));

    fseek(file, (table->table_header->first_page_number - 1) * PAGE_SIZE, SEEK_SET);
    fread(ph, sizeof(struct page_header), 1, file); //прочитали заголовок страницы

    fseek(file,
          (table->table_header->first_page_number - 1) * PAGE_SIZE + sizeof(struct page_header) + sizeof(uint16_t) +
          sizeof(struct column) * table->table_schema->column_count, SEEK_SET); //передвинулись на начало строк

    while (current_pointer != ph->page_free_space_seek) {

        fseek(file, (ph->page_number - 1) * PAGE_SIZE + current_pointer, SEEK_SET);
        fread(rh, sizeof(struct row_header), 1, file);
        if (rh->valid) {
            fseek(file, (ph->page_number - 1) * PAGE_SIZE + current_pointer + sizeof(struct row_header), SEEK_SET);
            fread(pointer_to_read_row, table->table_schema->row_length, 1,
                  file); //прочитали всю строку и у нас есть указатель на нее

            switch (expanded->column_type) {
                case TYPE_INT32:
                    if (int_equals(pointer_to_read_row, column_value, expanded->offset)) {
                        delete_row(pointer_to_read_row, table, current_pointer, ph->page_number);
                        count_of_deleted += 1;
                    }
                    break;
                case TYPE_BOOL:
                    if (bool_equals(pointer_to_read_row, column_value, expanded->offset)) {
                        delete_row(pointer_to_read_row, table, current_pointer, ph->page_number);
                        count_of_deleted += 1;
                    }
                    break;
                case TYPE_STRING:
                    if (string_equals(pointer_to_read_row, column_value, expanded->offset)) {
                        delete_row(pointer_to_read_row, table, current_pointer, ph->page_number);
                        count_of_deleted += 1;
                    }
                    break;
                case TYPE_FLOAT:
                    if (float_equals(pointer_to_read_row, column_value, expanded->offset)) {
                        delete_row(pointer_to_read_row, table, current_pointer, ph->page_number);
                        count_of_deleted += 1;
                    }
                    break;
            }
        }

        current_pointer += sizeof(struct row_header) + table->table_schema->row_length;

        if (ph->next_page_number != 0 && current_pointer == ph->page_free_space_seek) {
            current_pointer = sizeof(struct page_header) + sizeof(uint16_t) +
                              sizeof(struct column) * table->table_schema->column_count;
            fseek(file, (ph->next_page_number - 1) * PAGE_SIZE, SEEK_SET);
            fread(ph, sizeof(struct page_header), 1, file);
            fseek(file, (ph->page_number - 1) * PAGE_SIZE + sizeof(struct page_header) + sizeof(uint16_t) +
                        sizeof(struct column) * table->table_schema->column_count, SEEK_SET);
        }
    }
    free(rh);
    free(ph);
    free(pointer_to_read_row);
    printf("Было удалено %d строк\n", count_of_deleted);
}

void delete_row(char *row_start, table *table, uint32_t pointer_to_delete, uint32_t page_general_number) {
    struct row_header rh = {false};

    fseek(table->table_header->database->database_file, (page_general_number - 1) * PAGE_SIZE + pointer_to_delete,
          SEEK_SET);
    fwrite(&rh, sizeof(struct row_header), 1, table->table_header->database->database_file);
}

void
join(FILE *file, table *left_table, table *right_table, expanded_query *left_expanded, expanded_query *right_expanded) {
    uint32_t joined_count = 0;
    uint32_t current_pointer = sizeof(struct page_header) + sizeof(uint16_t) +
                               sizeof(struct column) * left_table->table_schema->column_count;
    struct row_header *rh = malloc(sizeof(struct row_header));
    char *pointer_to_read_row = malloc(left_table->table_schema->row_length);

    struct page_header *ph = malloc(sizeof(struct page_header));
    fseek(file, (left_table->table_header->first_page_number - 1) * PAGE_SIZE, SEEK_SET);
    fread(ph, sizeof(struct page_header), 1, file); //прочитали заголовок страницы

    fseek(file, (left_table->table_header->first_page_number - 1) * PAGE_SIZE + sizeof(struct page_header) +
                sizeof(uint16_t) + sizeof(struct column) * left_table->table_schema->column_count,
          SEEK_SET); //передвинулись на начало строк

    while (current_pointer != ph->page_free_space_seek) {

        fseek(file, (ph->page_number - 1) * PAGE_SIZE + current_pointer, SEEK_SET);
        fread(rh, sizeof(struct row_header), 1, file);
        if (rh->valid) {
            fseek(file, (ph->page_number - 1) * PAGE_SIZE + current_pointer + sizeof(struct row_header), SEEK_SET);
            fread(pointer_to_read_row, left_table->table_schema->row_length, 1,
                  file); //прочитали всю строку и у нас есть указатель на нее
            joined_count += try_connect_with_right_table(file, left_table, right_table, left_expanded, right_expanded,
                                                         pointer_to_read_row);
        }

        current_pointer += sizeof(struct row_header) + left_table->table_schema->row_length;

        if (ph->next_page_number != 0 && current_pointer == ph->page_free_space_seek) {
            current_pointer = sizeof(struct page_header) + sizeof(uint16_t) +
                              sizeof(struct column) * left_table->table_schema->column_count;
            fseek(file, (ph->next_page_number - 1) * PAGE_SIZE, SEEK_SET);
            fread(ph, sizeof(struct page_header), 1, file);
            fseek(file, (ph->page_number - 1) * PAGE_SIZE + sizeof(struct page_header) + sizeof(uint16_t) +
                        sizeof(struct column) * left_table->table_schema->column_count, SEEK_SET);
        }

    }
    free(rh);
    free(ph);
    free(pointer_to_read_row);
    printf("=====================\n");
    printf("Всего %d строк\n", joined_count);

}

void print_joined_content(char *row_start_left, char *row_start_right, table *left_table, table *right_table,
                          uint32_t left_offset, uint32_t right_offset) {
    uint16_t offset = 0;
    uint16_t second_offset = 0;
    for (size_t i = 0; i < left_table->table_schema->column_count; i++) {
        if (offset != left_offset) {
            switch (left_table->table_schema->columns[i].column_type) {
                case TYPE_INT32:
                    print_int(row_start_left, offset);
                    break;
                case TYPE_BOOL:
                    print_bool(row_start_left, offset);
                    break;
                case TYPE_STRING:
                    print_string(row_start_left, offset);
                    break;
                case TYPE_FLOAT:
                    print_float(row_start_left, offset);
                    break;
            }
        } else {
            for (size_t j = 0; j < right_table->table_schema->column_count; j++) {
                if (second_offset != right_offset) {
                    switch (right_table->table_schema->columns[j].column_type) {
                        case TYPE_INT32:
                            print_int(row_start_right, second_offset);
                            break;
                        case TYPE_BOOL:
                            print_bool(row_start_right, second_offset);
                            break;
                        case TYPE_STRING:
                            print_string(row_start_right, second_offset);
                            break;
                        case TYPE_FLOAT:
                            print_float(row_start_right, second_offset);
                            break;
                    }
                }
                second_offset += right_table->table_schema->columns[j].column_size;
            }
        }
        offset += left_table->table_schema->columns[i].column_size;
    }
    printf("\n");
}

bool join_compare_int(char *row_from_left_table, char *row_from_right_table, struct expanded_query *left_expanded,
                      struct expanded_query *right_expanded, struct table *left_table, struct table *right_table) {
    int32_t *left_value = (int32_t *) (row_from_left_table + left_expanded->offset);
    int32_t *right_value = (int32_t *) (row_from_right_table + right_expanded->offset);
    if (*left_value == *right_value) {
        print_joined_content(row_from_left_table, row_from_right_table, left_table, right_table, left_expanded->offset,
                             right_expanded->offset);
        return true;
    } else return false;
}

bool join_compare_bool(char *row_from_left_table, char *row_from_right_table, struct expanded_query *left_expanded,
                       struct expanded_query *right_expanded, struct table *left_table, struct table *right_table) {
    bool *left_value = (bool *) (row_from_left_table + left_expanded->offset);
    bool *right_value = (bool *) (row_from_right_table + right_expanded->offset);
    if (*left_value == *right_value) {
        print_joined_content(row_from_left_table, row_from_right_table, left_table, right_table, left_expanded->offset,
                             right_expanded->offset);
        return true;
    } else return false;
}

bool join_compare_string(char *row_from_left_table, char *row_from_right_table, struct expanded_query *left_expanded,
                         struct expanded_query *right_expanded, struct table *left_table, struct table *right_table) {
    char *left_value = (char *) (row_from_left_table + left_expanded->offset);
    char *right_value = (char *) (row_from_right_table + right_expanded->offset);
    if (strcmp(left_value, right_value) == 0) {
        print_joined_content(row_from_left_table, row_from_right_table, left_table, right_table, left_expanded->offset,
                             right_expanded->offset);
        return true;
    } else return false;
}

bool join_compare_float(char *row_from_left_table, char *row_from_right_table, struct expanded_query *left_expanded,
                        struct expanded_query *right_expanded, struct table *left_table, struct table *right_table) {
    double *left_value = (double *) (row_from_left_table + left_expanded->offset);
    double *right_value = (double *) (row_from_right_table + right_expanded->offset);
    if (*left_value == *right_value) {
        print_joined_content(row_from_left_table, row_from_right_table, left_table, right_table, left_expanded->offset,
                             right_expanded->offset);
        return true;
    } else return false;
}


uint32_t try_connect_with_right_table(FILE *file, table *left_table, table *right_table, expanded_query *left_expanded,
                                      expanded_query *right_expanded, char *row_from_left_table) {
    uint32_t current_pointer = sizeof(struct page_header) + sizeof(uint16_t) +
                               sizeof(struct column) * right_table->table_schema->column_count;
    struct row_header *rh = malloc(sizeof(struct row_header));
    char *pointer_to_read_row = malloc(right_table->table_schema->row_length);

    struct page_header *ph = malloc(sizeof(struct page_header));
    fseek(file, (right_table->table_header->first_page_number - 1) * PAGE_SIZE, SEEK_SET);
    fread(ph, sizeof(struct page_header), 1, file); //прочитали заголовок страницы

    fseek(file, (right_table->table_header->first_page_number - 1) * PAGE_SIZE + sizeof(struct page_header) +
                sizeof(uint16_t) + sizeof(struct column) * right_table->table_schema->column_count,
          SEEK_SET); //передвинулись на начало строк

    while (current_pointer != ph->page_free_space_seek) {

        fseek(file, (ph->page_number - 1) * PAGE_SIZE + current_pointer, SEEK_SET);
        fread(rh, sizeof(struct row_header), 1, file);
        if (rh->valid) {
            fseek(file, (ph->page_number - 1) * PAGE_SIZE + current_pointer + sizeof(struct row_header), SEEK_SET);
            fread(pointer_to_read_row, right_table->table_schema->row_length, 1,
                  file); //прочитали всю строку и у нас есть указатель на нее
            switch (right_expanded->column_type) {
                case TYPE_INT32:
                    if (join_compare_int(row_from_left_table, pointer_to_read_row, left_expanded, right_expanded,
                                         left_table, right_table)) {
                        return 1;
                    }
                    break;
                case TYPE_BOOL:
                    if (join_compare_bool(row_from_left_table, pointer_to_read_row, left_expanded, right_expanded,
                                          left_table, right_table)) {
                        return 1;
                    }
                    break;
                case TYPE_STRING:
                    if (join_compare_string(row_from_left_table, pointer_to_read_row, left_expanded, right_expanded,
                                            left_table, right_table)) {
                        return 1;
                    }
                    break;
                case TYPE_FLOAT:
                    if (join_compare_float(row_from_left_table, pointer_to_read_row, left_expanded, right_expanded,
                                           left_table, right_table)) {
                        return 1;
                    }
                    break;
            }
        }

        current_pointer += sizeof(struct row_header) + right_table->table_schema->row_length;

        if (ph->next_page_number != 0 && current_pointer == ph->page_free_space_seek) {
            current_pointer = sizeof(struct page_header) + sizeof(uint16_t) +
                              sizeof(struct column) * right_table->table_schema->column_count;
            fseek(file, (ph->next_page_number - 1) * PAGE_SIZE, SEEK_SET);
            fread(ph, sizeof(struct page_header), 1, file);
            fseek(file, (ph->page_number - 1) * PAGE_SIZE + sizeof(struct page_header) + sizeof(uint16_t) +
                        sizeof(struct column) * right_table->table_schema->column_count, SEEK_SET);
        }

    }
    free(rh);
    free(ph);
    free(pointer_to_read_row);
}

bool int_equals(char *pointer_to_read, void *column_value, uint32_t offset) {
    int32_t *value_to_compare = (int32_t *) (pointer_to_read + offset);
    int32_t given_value = *((int32_t *) column_value);
    return *value_to_compare == given_value;
}

bool bool_equals(char *pointer_to_read, void *column_value, uint32_t offset) {
    bool *value_to_compare = (bool *) (pointer_to_read + offset);
    bool given_value = *((bool *) column_value);
    return *value_to_compare == given_value;
}

bool string_equals(char *pointer_to_read, void *column_value, uint32_t offset) {
    char *value_to_compare = (char *) (pointer_to_read + offset);
    char *given_value = *((char **) (column_value));
    return strcmp(value_to_compare, given_value) == 0;
}

bool float_equals(char *pointer_to_read, void *column_value, uint32_t offset) {
    float *value_to_compare = (float *) (pointer_to_read + offset);
    float given_value = *((float *) (column_value));
    return *value_to_compare == given_value;
}

void update_int(char *pointer_to_read_row, void *column_value, uint32_t offset) {
    int32_t *value_to_change = (int32_t *) (pointer_to_read_row + offset);
    int32_t given_value = *((int32_t *) column_value);
    *value_to_change = given_value;
}

void update_bool(char *pointer_to_read_row, void *column_value, uint32_t offset) {
    bool *value_to_change = (bool *) (pointer_to_read_row + offset);
    bool given_value = *((bool *) column_value);
    *value_to_change = given_value;
}

void update_string(char *pointer_to_read_row, void *column_value, uint32_t offset, uint16_t column_size) {
    char *value_to_change = (char *) pointer_to_read_row + offset;
    char *given_value = *((char **) column_value);
    strcpy(value_to_change, given_value);
}

void update_float(char *pointer_to_read_row, void *column_value, uint32_t offset) {
    double *value_to_change = (double *) (pointer_to_read_row + offset);
    double given_value = *((double *) column_value);
    *value_to_change = given_value;
}

void print_int(char *row_start, uint32_t offset) {
    int32_t *value_to_print = (int32_t *) (row_start + offset);
    printf("%" PRId32 ";", *value_to_print);
}

void print_bool(char *row_start, uint32_t offset) {
    bool *value_to_print = (bool *) (row_start + offset);
    printf("%s;", *value_to_print ? "true" : "false");
}

void print_string(char *row_start, uint32_t offset) {
    char *value_to_print = (char *) row_start + offset;
    printf("%s;", value_to_print);
}

void print_float(char *row_start, uint32_t offset) {
    double *value_to_print = (double *) (row_start + offset);
    printf("%f;", *value_to_print);
}

void print_data(char *row_start, column *columns, uint16_t len) {
    uint16_t offset = 0;

    for (size_t i = 0; i < len; i++) {
        switch (columns[i].column_type) {
            case TYPE_INT32:
                print_int(row_start, offset);
                break;
            case TYPE_FLOAT:
                print_float(row_start, offset);
                break;
            case TYPE_BOOL:
                print_bool(row_start, offset);
                break;
            case TYPE_STRING:
                print_string(row_start, offset);
                break;
        }
        offset += columns[i].column_size;
    }
    printf("\n");
}

void
update_content(char *row_start, void *column_value, expanded_query *second, table *table, uint32_t pointer_to_update,
               uint32_t page_general_number) {
    switch (second->column_type) {
        case TYPE_INT32:
            update_int(row_start, column_value, second->offset);
            break;
        case TYPE_BOOL:
            update_bool(row_start, column_value, second->offset);
            break;
        case TYPE_STRING:
            update_string(row_start, column_value, second->offset, second->column_size);
            break;
        case TYPE_FLOAT:
            update_float(row_start, column_value, second->offset);
            break;
    }

    fseek(table->table_header->database->database_file, (page_general_number - 1) * PAGE_SIZE + pointer_to_update,
          SEEK_SET);
    fwrite(row_start, table->table_schema->row_length, 1, table->table_header->database->database_file);

    print_data(row_start, table->table_schema->columns, table->table_schema->column_count);
}


