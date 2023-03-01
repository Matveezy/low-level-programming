#include "page_manager.h"


page_header *create_page(database_header *db_header, table_header *table_header) {
    page_header *new_page_header = malloc(sizeof(page_header));
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
    }
}

database *create_database(char *filename) {
    database *created_db = malloc(sizeof(database));

    store_file file = file_open_or_create(filename);
    if (file.state == EXCELLENT || file.state == CREATED_FILE) {
        printf("Файл успешно создан!\n");
    } else if (file.state == CREATE_ERROR) {
        printf("Не удалось создать файл!\n");
    }

    database_header *db_header = malloc(sizeof(database_header));
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

database *get_database_from_file(char *filename) {
    database *db = malloc(sizeof(database));
    database_header *db_header = malloc(sizeof(database_header));

    FILE *storage = NULL;

    switch (open_file(&storage, filename, "rb+")) {
        case OPEN_ERROR:
            printf("Файл не удалось открыть!");
            break;
        case OPEN_OK:
            printf("Файл успешно открыт!");
            break;
    }

    read_status db_header_read_status = read_database_header(storage, db);
    if (READ_OK == db_header_read_status) {
        db_header->db = db;
        db->database_header = db_header;
        db->database_file = storage;
        return db;
    } else {
        printf("Ошибка с прочтением заголовка базы данных!");
        return NULL;
    }
}

table *create_table_from_schema(table_schema *table_schema, database *db, const char *table_name) {
    table_header *th = malloc(sizeof(struct table_header));

    if (table_exists(db->database_file, db->database_header->table_count, table_name, th)) {
        printf("Таблица с таким именем уже существует!");
        free(th);
        return NULL;
    }

    table *created_table = malloc(sizeof(table));
    created_table->table_schema = table_schema;
    table_header *table_header = malloc(sizeof(table_header));
    strncpy(table_header->name, "", TABLE_NAME_LENGTH);
    strncpy(table_header->name, table_name, TABLE_NAME_LENGTH);

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
    free(table->table_schema->columns);
    free(table->table_header);
    free(table->table_schema);
    free(table);
}

bool delete_table(const char *table_name, database *db) {
    table_header *table_header_to_delete = malloc(sizeof(table_header));

    if (read_table_header(db->database_file, table_name, db->database_header, db->database_header->table_count) ==
        READ_OK) {
        table_header_to_delete->valid = false;
        db->database_header->table_count = -1;
        db->database_header->page_count -= table_header_to_delete->page_count;

        overwrite_th_after_change(db->database_file, table_header_to_delete);
        overwrite_dh_after_change(db->database_file, db->database_header);
        free(table_header_to_delete);
        return true;
    } else {
        printf("Произошла ошибка с удаление таблицы");
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
    table *new_table = malloc(sizeof(table));
    table_header *new_th = malloc(sizeof(table_header));
    table_schema *new_ts = malloc(sizeof(table_schema));

    if (READ_OK == read_table_header(db->database_file, table_name, new_th, db->database_header->table_count)) {
        new_table->table_header = new_th;
        new_table->table_schema = new_ts;
        read_columns_of_table(db->database_file, new_table);
        new_table->table_header->database = db;
        return new_table;
    } else {
        printf("Проблема с прочтением таблицы!");
        return NULL;
    }
}

