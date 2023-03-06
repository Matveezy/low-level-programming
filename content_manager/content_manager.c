#include "content_manager.h"

row *create_row(table *table) {
    row *created_row = malloc(sizeof(struct row));
    row_header *row_header = malloc(sizeof(struct row_header));
    created_row->table = table;
    created_row->data = malloc(table->table_schema->row_length);

    row_header->valid = true;
    created_row->row_header = row_header;
    return created_row;
}

void close_row(row *row) {
    free(row->row_header);
    free(row->data);
    free(row);
}

void fill_int(row *row, int32_t value, uint32_t offset) {
    char *pointer_to_write = (char *) row->data + offset;
    *((int32_t *) (pointer_to_write)) = value;
}

void fill_bool(row *row, bool value, uint32_t offset) {
    char *pointer_to_write = (char *) row->data + offset;
    *((bool *) (pointer_to_write)) = value;
}

void fill_string(row *row, char *value, uint32_t offset, uint32_t len) {
    if (len != -1) {
        char *pointer_to_write = (char *) row->data + offset;
        strncpy(pointer_to_write, "", len);
        strncpy(pointer_to_write, value, strlen(value));
    }
}

void fill_float(row *row, float value, uint32_t offset) {
    char *pointer_to_write = (char *) row->data + offset;
    *((float *) (pointer_to_write)) = value;
}

void fill_attribute(row *row, const char *column_name, column_type column_type, void *value, bool is_table_new) {

    read_columns_of_table(row->table->table_header->database->database_file, row->table);

    uint32_t offset = column_offset(row->table->table_schema->columns, row->table->table_schema->column_count,
                                    column_name);
    if (offset != -1) {
        switch (column_type) {
            case TYPE_INT32:
                fill_int(row, *((int32_t *) value), offset);
                break;
            case TYPE_FLOAT:
                fill_float(row, *((float *) (value)), offset);
                break;
            case TYPE_BOOL:
                fill_bool(row, *((bool *) (value)), offset);
                break;
            case TYPE_STRING:
                fill_string(row,
                            *((char **) (value)),
                            offset,
                            get_string_column_len(row->table->table_schema->columns,
                                                  row->table->table_schema->column_count,
                                                  column_name)
                );
                break;
        }
    } else {
        printf("\nКолонки с таким названием уже есть!\n");
    }
}

int32_t column_offset(column *column_list, size_t len, const char *name) {
    int32_t index = 0;
    uint32_t offset = 0;
    if (NULL != column_list) {
        while (index != len) {
            uint32_t size = column_list[index].column_size;
            if (strcmp(column_list[index].column_name, name) == 0) {
                return offset;
            }
            index++;
            offset += size;
        }
        return -1;
    } else {
        return -1;
    }
}

void insert_row(row *row) {
    uint32_t page_with_free_space_num = row->table->table_header->last_page_number;
    write_status response = write_row_to_page(row->table->table_header->database->database_file,
                                              page_with_free_space_num,
                                              row);
    if (WRITE_ERROR == response) {
        printf("\nНе удалось записать строку!\n");
    }
}

void select_row_from_table(query *query) {
    bool column_exists = false;
    column_type column_type;
    char column_name[COLUMN_NAME_LENGTH];

    uint16_t column_size = 0;

    for (size_t i = 0; i < query->table->table_schema->column_count; i++) {
        if (strcmp(query->table->table_schema->columns[i].column_name, query->column_name[0]) == 0) {
            column_exists = true;
            column_type = query->table->table_schema->columns[i].column_type;
            strncpy(column_name, query->table->table_schema->columns[i].column_name, MAX_COLUMN_NAME_LEN);
            column_size = query->table->table_schema->columns[i].column_size;
            break;
        }
    }
    if (column_exists) {
        uint32_t offset = column_offset(query->table->table_schema->columns, query->table->table_schema->column_count,
                                        query->column_name[0]);
        select_where(query->table->table_header->database->database_file, query->table, offset, column_size,
                     query->column_value[0],
                     column_type, query->rows_number);
    } else {
        printf("\nКолонки из запроса нет в таблице!\n");
    }
}

void update_row_in_table(struct query* query) {
    bool first_column_exists = false;
    bool second_column_exists = false;
    column_type first_column_type;
    column_type second_column_type;
    char first_column_name[MAX_COLUMN_NAME_LEN];
    char second_column_name[MAX_COLUMN_NAME_LEN];
    uint16_t first_column_size = 0;
    uint16_t second_column_size = 0;

    for (size_t i=0; i<query->table->table_schema->column_count; i++) {
        if (strcmp(query->table->table_schema->columns[i].column_name, query->column_name[0]) == 0) {
            first_column_exists = true;
            first_column_type = query->table->table_schema->columns[i].column_type;
            strncpy(first_column_name, query->table->table_schema->columns[i].column_name, MAX_COLUMN_NAME_LEN);
            first_column_size = query->table->table_schema->columns[i].column_size;
        } else if (strcmp(query->table->table_schema->columns[i].column_name, query->column_name[1]) == 0) {
            second_column_exists = true;
            second_column_type = query->table->table_schema->columns[i].column_type;
            strncpy(second_column_name, query->table->table_schema->columns[i].column_name, MAX_COLUMN_NAME_LEN);
            second_column_size = query->table->table_schema->columns[i].column_size;
        };
        if (first_column_exists && second_column_exists) break;
    }

    if (first_column_exists && second_column_exists) {
        uint32_t first_offset = column_offset(query->table->table_schema->columns, query->table->table_schema->column_count, first_column_name);
        uint32_t second_offset = column_offset(query->table->table_schema->columns, query->table->table_schema->column_count, second_column_name);
        struct expanded_query* first_expanded = malloc(sizeof(struct expanded_query));
        struct expanded_query* second_expanded = malloc(sizeof(struct expanded_query));

        first_expanded->column_type = first_column_type;
        first_expanded->column_size = first_column_size;
        first_expanded->offset = first_offset;
        strncpy(first_expanded->column_name, "", MAX_COLUMN_NAME_LEN);
        strncpy(first_expanded->column_name, first_column_name, MAX_COLUMN_NAME_LEN);

        second_expanded->column_type = second_column_type;
        second_expanded->column_size = second_column_size;
        second_expanded->offset = second_offset;
        strncpy(second_expanded->column_name, "", MAX_COLUMN_NAME_LEN);
        strncpy(second_expanded->column_name, second_column_name, MAX_COLUMN_NAME_LEN);

        update_where(query->table->table_header->database->database_file, query->table, first_expanded, second_expanded, query->column_value);
        free(first_expanded);
        free(second_expanded);
    } else printf("Невозможно выполнить запрос по вашему условию: колонки_ок из запроса нет в таблице\n");
}

void delete_row_from_table(query* query) {
    bool column_exists = false;
    column_type column_type;
    char column_name[MAX_COLUMN_NAME_LEN];
    uint16_t column_size = 0;

    for (size_t i=0; i<query->table->table_schema->column_count; i++) {
        if (strcmp(query->table->table_schema->columns[i].column_name, query->column_name[0]) == 0) {
            column_exists = true;
            column_type = query->table->table_schema->columns[i].column_type;
            strncpy(column_name, query->table->table_schema->columns[i].column_name, MAX_COLUMN_NAME_LEN);
            column_size = query->table->table_schema->columns[i].column_size;
            break;
        };
    }

    if (column_exists) {
        uint32_t offset = column_offset(query->table->table_schema->columns, query->table->table_schema->column_count, column_name);
        struct expanded_query* expanded = malloc(sizeof(struct expanded_query));

        expanded->column_type = column_type;
        expanded->column_size = column_size;
        expanded->offset = offset;
        strncpy(expanded->column_name, "", MAX_COLUMN_NAME_LEN);
        strncpy(expanded->column_name, column_name, MAX_COLUMN_NAME_LEN);

        delete_where(query->table->table_header->database->database_file, query->table, expanded, query->column_value[0]);
        free(expanded);
    } else printf("Невозможно выполнить запрос по вашему условию: колонки из запроса нет в таблице\n");
}
