#include "page_manager.h"


page_header *create_page(database_header *db_header, table_header *table_header) {
    page_header *new_page_header = malloc(sizeof(struct page_header));
    if (NULL == new_page_header) {
        return NULL;
    }
    new_page_header->is_dirty = false;
    new_page_header->free_bytes = PAGE_SIZE;
    new_page_header->page_free_space_seek = 0;
    new_page_header->next_page_number = 0;
    new_page_header->page_number = db_header->page_count;

    if (NULL == table_header) {
        strncpy(new_page_header->table_name, "", TABLE_NAME_LENGTH);
    } else {
        new_page_header->table_number_in_meta_page = table_header->number_in_meta_page;
        strncpy(new_page_header->table_name, "", TABLE_NAME_LENGTH);
        strncpy(new_page_header->table_name, table_header->name, TABLE_NAME_LENGTH);
    }

    return new_page_header;
}

page_header *add_meta_page(database_header *db_header) {
    db_header->page_count = db_header->page_count + 1;
    page_header *new_page_header = create_page(db_header, NULL);
    if (NULL == new_page_header) {
        return NULL;
    }
    if (db_header->last_page_number != 0) {
        overwrite_previous_last_page_db(db_header->db->database_file, db_header, new_page_header->page_number);
    }
    db_header->last_page_number = new_page_header->page_number;
    if (db_header->page_count != 1) {
        overwrite_dh_after_change(db_header->db->database_file, db_header);
    }
    return new_page_header;
}

struct page_header *add_page(table_header *table_header, database_header *db_header) {
    db_header->page_count += 1;
    table_header->page_count += 1;
    struct page_header *new_page_header = create_page(db_header, table_header);
    if (new_page_header != NULL) {
        if (table_header->last_page_number != 0) {
            overwrite_previous_last_page(table_header->database->database_file, table_header->last_page_number,
                                         new_page_header->page_number);
        } else {
            table_header->first_page_number = new_page_header->page_number;
        }
        table_header->last_page_number = new_page_header->page_number;
        if (table_header->page_count != 1) {
            overwrite_th_after_change(db_header->db->database_file, table_header);
            overwrite_dh_after_change(db_header->db->database_file, db_header);
        }
        return new_page_header;
    } else {
        return NULL;
    }
}

database *create_database(char *filename) {
    database *created_db = malloc(sizeof(struct database));

    store_file file = file_open_or_create(filename);
    if (file.state == EXCELLENT || file.state == CREATED_FILE) {
        printf("Файл успешно создан!\n");
    } else if (file.state == CREATE_ERROR) {
        printf("Не удалось создать файл!\n");
    }

    database_header *db_header = malloc(sizeof(struct database_header));
    strncpy(db_header->database_name, "", DB_NAME_LENGTH);
    strncpy(db_header->database_name, filename, strlen(filename));

    db_header->table_count = 0;
    db_header->page_count = 0;
    db_header->page_size = PAGE_SIZE;
    db_header->last_page_number = 0;
    db_header->db = created_db;

    page_header *meta_page_header = add_meta_page(db_header);

//    write_db_to_file(in, db_header, tech_page_header);

    return created_db;
}

database *get_database_from_file(const char *filename) {
    database *db = malloc(sizeof(struct database));
    database_header *db_header = malloc(sizeof(struct database_header));

    FILE *storage = NULL;

    switch (open_file(&storage, filename, "rb+")) {
        case OPEN_ERROR:
            printf("\nФайл не удалось открыть!\n");
            break;
        case OPEN_OK:
            printf("\nФайл успешно открыт!\n");
            break;
    }

//    storage = fopen(filename, "rb+");

    read_status db_header_read_status = read_database_header(storage, db_header);
    if (READ_OK == db_header_read_status) {
        db_header->db = db;
        db->database_header = db_header;
        db->database_file = storage;
        return db;
    } else {
        printf("\nОшибка с прочтением заголовка базы данных!\n");
        return NULL;
    }
}

table *create_table_from_schema(table_schema *table_schema, database *db, const char *table_name) {
    table_header *th = malloc(sizeof(struct table_header));

    if (table_exists(db->database_file, db->database_header->table_count, table_name, th)) {
        printf("\nТаблица с таким именем уже существует!\n");
        free(th);
        return NULL;
    }

    table *created_table = malloc(sizeof(struct table));
    created_table->table_schema = table_schema;

    table_header *table_header = malloc(sizeof(struct table_header));
    strncpy(table_header->name, "", MAX_TABLE_HEADER_NAME_LENGTH);
    strncpy(table_header->name, table_name, strlen(table_name));

    table_header->database = db;
    table_header->page_count = 0;
    table_header->valid = true;
    table_header->table = created_table;
    table_header->table_schema = *table_schema;
    table_header->first_page_number = 0;
    table_header->last_page_number = 0;

    page_header *new_page_header = add_page(table_header, db->database_header);
    created_table->table_header = table_header;

    db->database_header->table_count += 1;
    table_header->number_in_meta_page = db->database_header->table_count;

    write_header_to_meta_page(db->database_file, db->database_header, table_header);
    overwrite_dh_after_change(db->database_file, db->database_header);
    write_table_page(db->database_file, new_page_header, created_table->table_schema);

    return created_table;
}

void close_table(table *table) {
//    destroy_column_list(table->table_schema->columns);
//    free(table->table_schema->columns);
    free(table->table_header);
    free(table->table_schema);
    free(table);
}

bool delete_table(const char *table_name, database *db) {
    table_header *table_header_to_delete = malloc(sizeof(struct table_header));

    if (read_table_header(db->database_file, table_name, table_header_to_delete, db->database_header->table_count) ==
        READ_OK) {
        table_header_to_delete->valid = false;
        db->database_header->table_count = -1;
        db->database_header->page_count -= table_header_to_delete->page_count;

        overwrite_th_after_change(db->database_file, table_header_to_delete);
        overwrite_dh_after_change(db->database_file, db->database_header);
        free(table_header_to_delete);
        return true;
    } else {
        printf("\nПроизошла ошибка с удаление таблицы\n");
        return false;
    }
}

bool enough_free_space(page_header *page_header, uint32_t desired_size) {
    if (page_header->free_bytes < desired_size) return false;
    else return true;
}

void close_database(database *db) {
    close_file(db->database_file);
    free(db->database_header);
    free(db);
}

table *get_table(const char *table_name, database *db) {
    table *new_table = malloc(sizeof(struct table));
    table_header *new_th = malloc(sizeof(struct table_header));
    table_schema *new_ts = malloc(sizeof(struct table_schema));

    if (READ_OK == read_table_header(db->database_file, table_name, new_th, db->database_header->table_count)) {
        new_table->table_header = new_th;
        new_table->table_schema = new_ts;
        read_columns_of_table(db->database_file, new_table);
        new_table->table_header->database = db;
        return new_table;
    } else {
        printf("\nПроблема с прочтением таблицы!\n");
        return NULL;
    }
}

database *get_prepared_database(const char *filename, database_type database_type) {
    switch (database_type) {
        case EXISTING:
            return get_database_from_file(filename);
        case TO_BE_CREATED:
            return create_database_in_file(filename);
    }
}

database *create_database_in_file(const char *filename) {
    database *db = malloc(sizeof(struct database) + sizeof(struct database_header));

    FILE *storage = NULL;

    switch (open_file(&storage, filename, "wb+")) {
        case OPEN_ERROR:
            printf("\nОшибка с открытием файла!\n");
            return NULL;
        case OPEN_OK:
            printf("\nФайл создан!\n");
            break;
    }

    database_header *db_header = malloc(sizeof(struct database_header));
    strncpy(db_header->database_name, "", DB_NAME_LENGTH);
    strncpy(db_header->database_name, filename, strlen(filename));
    db_header->table_count = 0;
    db_header->page_count = 0;
    db_header->page_size = PAGE_SIZE;
    db_header->last_page_number = 0;
    db_header->db = db;

    page_header *meta_page_header = add_meta_page(db_header);

    db->database_file = storage;
    db->database_header = db_header;

    write_db_to_file(storage, db_header, meta_page_header);

    return db;
}

query *create_query(query_type query_type, table *tables, char *column[], void *values[], int32_t row_count) {
    query *new_query = malloc(sizeof(struct query));
    new_query->q_type = query_type;
    new_query->table = tables;
    new_query->column_name = column;
    new_query->column_value = values;
    new_query->rows_number = row_count;

    return new_query;
}

query_join *create_join_query(table *left_table, table *right_table, char *left_column, char *right_column) {
    query_join *join_query = malloc(sizeof(struct query_join));
    join_query->left_table = left_table;
    join_query->right_table = right_table;
    join_query->left_column_name = left_column;
    join_query->right_column_name = right_column;
    return join_query;
}

void run_query(query *query) {
    switch (query->q_type) {
        case SELECT_WHERE:
            select_row_from_table(query);
            break;
        case UPDATE_WHERE:
            update_row_in_table(query);
            break;
        case DELETE_WHERE:
            delete_row_from_table(query);
            break;
    }
}

void run_join_query(query_join* query) {
    bool first_column_exists = false;
    bool second_column_exists = false;
    column_type first_column_type;
    column_type second_column_type;
    char first_column_name[MAX_COLUMN_NAME_LEN];
    char second_column_name[MAX_COLUMN_NAME_LEN];
    uint16_t first_column_size = 0;
    uint16_t second_column_size = 0;

    for (size_t i=0; i<query->left_table->table_schema->column_count; i++) {
        if (strcmp(query->left_table->table_schema->columns[i].column_name, query->left_column_name) == 0) {
            first_column_exists = true;
            first_column_type = query->left_table->table_schema->columns[i].column_type;
            strncpy(first_column_name, query->left_table->table_schema->columns[i].column_name, MAX_COLUMN_NAME_LEN);
            first_column_size = query->left_table->table_schema->columns[i].column_size;
        }
        if (first_column_exists) break;
    }

    for (size_t i=0; i<query->right_table->table_schema->column_count; i++) {
        if (strcmp(query->right_table->table_schema->columns[i].column_name, query->right_column_name) == 0) {
            second_column_exists = true;
            second_column_type = query->right_table->table_schema->columns[i].column_type;
            strncpy(second_column_name, query->right_table->table_schema->columns[i].column_name, MAX_COLUMN_NAME_LEN);
            second_column_size = query->right_table->table_schema->columns[i].column_size;
        }
        if (second_column_exists) break;
    }

    if (first_column_exists && second_column_exists) {
        uint32_t first_offset = column_offset(query->left_table->table_schema->columns, query->left_table->table_schema->column_count, first_column_name);
        uint32_t second_offset = column_offset(query->right_table->table_schema->columns, query->right_table->table_schema->column_count, second_column_name);
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

        join(query->left_table->table_header->database->database_file, query->left_table, query->right_table, first_expanded, second_expanded);
        free(first_expanded);
        free(second_expanded);
    } else printf("Невозможно выполнить запрос по вашему условию: колонки_ок из запроса нет в таблице\n");
}


void close_query(query *query){
    free(query);
}

void close_join_query(query_join *join_query){
    free(join_query);
}