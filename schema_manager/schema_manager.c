#include "schema_manager.h"

int32_t get_string_column_len(const column *column_list, size_t len, const char *name) {
    int32_t index = 0;
    if (NULL != column_list) {
        while (index != len) {
            if (strcmp(column_list[index].column_name, name) == 0) {
                return column_list[index].column_size;
            }
            index++;
        }
        return -1;
    } else {
        return -1;
    }
}

uint32_t column_exists(const column *column_list, size_t len, const char *name) {
    int32_t index = 0;
    const column *cur = column_list;
    if (NULL != column_list) {
        while (index != len) {
            if (strcmp(cur->column_name, name) == 0) {
                return cur->column_size;
            }
            index++;
            cur = cur->next;
        }
        return 0;
    } else {
        return 0;
    }
}

void destroy_column_list(column *column_list) {
    column *next;
    column *cur = column_list;
    while (cur) {
        next = cur->next;
        free(cur);
        cur = next;
    }
}

void
add_back_column_to_list(table_schema *table_schema, const char *column_name, column_type column_type, uint16_t size) {
    column *new_column = create_column(column_name, column_type);
    if (NULL != table_schema->last_column) {
        table_schema->last_column->next = new_column;
    } else {
        table_schema->columns = new_column;
    }
    table_schema->last_column = new_column;
}

void add_back_string_column_to_list(table_schema *table_schema, const char *column_name, column_type column_type,
                                    uint16_t size) {
    struct column *new_column = create_string_column(column_name, column_type, size);
    if (table_schema->last_column != NULL) {
        table_schema->last_column->next = new_column;
    } else table_schema->columns = new_column;
    table_schema->last_column = new_column;
}

column *create_string_column(const char *column_name, column_type column_type, uint16_t size) {
    if (column_type != TYPE_STRING) {
        printf("Неправильно указан тип!");
        return NULL;
    }
    column *created_column = malloc(sizeof(column));
    if (NULL == created_column) {
        return NULL;
    }
    strncpy(created_column->column_name, "", COLUMN_NAME_LENGTH);
    strncpy(created_column->column_name, column_name, strlen(column_name));
    created_column->column_type = TYPE_STRING;
    created_column->column_size = sizeof(char) * size;
    created_column->next = NULL;
    return created_column;
}

column *delete_column_from_list(column *cur, const char *column_name, table_schema *table_schema) {
    column *next = NULL;
    if (NULL == cur) {
        return NULL;
    } else if (strcmp(cur->column_name, column_name) == 0) {
        next = cur->next;
        free(cur);
        return next;
    } else {
        cur->next = delete_column_from_list(cur->next, column_name, table_schema);
        if (NULL == cur->next) {
            table_schema->last_column = cur;
        }
        return cur;
    }
}

column *create_column(const char *column_name, column_type column_type) {
    column *new_column = malloc(sizeof(column));
    if (NULL == new_column) {
        return NULL;
    }
    strncpy(new_column->column_name, "", COLUMN_NAME_LENGTH);
    strncpy(new_column->column_name, column_name, strlen(column_name));
    new_column->column_type = column_type;
    switch (column_type) {
        case TYPE_INT32:
            new_column->column_size = sizeof(int32_t);
            break;
        case TYPE_BOOL:
            new_column->column_size = sizeof(bool);
            break;
        case TYPE_FLOAT:
            new_column->column_size = sizeof(float);
            break;
        case TYPE_STRING:
            free(new_column);
            break;
    }
    new_column->next = NULL;
    return new_column;
}

table_schema *create_table_schema() {
    table_schema *table_schema = malloc(sizeof(table_schema));
    if (NULL == table_schema) {
        return NULL;
    }
    table_schema->columns = NULL;
    table_schema->last_column = NULL;
    table_schema->column_count = 0;
    table_schema->row_length = 0;
    return table_schema;
}

void close_table_schema(table_schema *table_schema) {
    destroy_column_list(table_schema->columns);
    free(table_schema);
}

table_schema *
add_column_to_schema(table_schema *table_schema, const char *column_name, column_type column_type, uint16_t size) {
    if (column_exists(table_schema->columns, table_schema->column_count, column_name) == 0) {
        add_back_column_to_list(table_schema, column_name, column_type, size);
        table_schema->column_count += 1;
        table_schema->row_length = table_schema->last_column->column_size;
        return table_schema;
    } else {
        printf("Колонка с данным названием уже есть в схеме!");
        return table_schema;
    }
}

table_schema *
add_string_column_to_schema(table_schema *table_schema, const char *column_name, column_type column_type,
                            uint16_t size) {
    if (column_exists(table_schema->columns, table_schema->column_count, column_name) == 0) {
        add_back_string_column_to_list(table_schema, column_name, column_type, size);
        table_schema->column_count += 1;
        table_schema->row_length = table_schema->last_column->column_size;
        return table_schema;
    } else {
        printf("Колонка с данным названием уже есть в схеме!");
        return table_schema;
    }
}

table_schema *delete_column_from_schema(table_schema *table_schema, const char *column_name) {
    uint32_t column_size = column_exists(table_schema->columns, table_schema->column_count, column_name);
    if (column_size != 0) {
        column *new_column_list = delete_column_from_list(table_schema->columns, column_name, table_schema);
        table_schema->columns = new_column_list;
        table_schema->column_count -= 1;
        table_schema->row_length -= column_size;
        return table_schema;
    } else {
        printf("Колонки с таким названием не существует!");
        return table_schema;
    }
}

void close_schema(table_schema *table_schema) {
    destroy_column_list(table_schema->columns);
    free(table_schema);
}