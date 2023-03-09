#include "tests.h"

container *create_cars_test_container(database *db) {
    container *container = malloc(sizeof(struct container));
    table_schema *table_schema_first = create_table_schema();
    table_schema_first = add_column_to_schema(table_schema_first, "id", TYPE_INT32);
    table_schema_first = add_string_column_to_schema(table_schema_first, "brand", TYPE_STRING, 20);
    table_schema_first = add_string_column_to_schema(table_schema_first, "model", TYPE_STRING, 10);
    table_schema_first = add_column_to_schema(table_schema_first, "power", TYPE_INT32);
    table_schema_first = add_column_to_schema(table_schema_first, "is_release", TYPE_BOOL);
    table *cars_table = create_table_from_schema(table_schema_first, db, "cars");
    row *cars_row = create_row(cars_table);
    container->table_schema = table_schema_first;
    container->db = db;
    container->table = cars_table;
    container->row = cars_row;
    return container;
}

container *create_people_test_container(database *db) {
    container *container = malloc(sizeof(struct container));
    table_schema *table_schema_second = create_table_schema();
    table_schema_second = add_column_to_schema(table_schema_second, "id", TYPE_INT32);
    table_schema_second = add_string_column_to_schema(table_schema_second, "name", TYPE_STRING, 20);
    table_schema_second = add_column_to_schema(table_schema_second, "car_id", TYPE_INT32);
    table *people_table = create_table_from_schema(table_schema_second, db, "people");
    row *people_row = create_row(people_table);
    container->table_schema = table_schema_second;
    container->db = db;
    container->table = people_table;
    container->row = people_row;
    return container;
}

void write_to_db() {
    printf("Write db test!\n");
    database *my_db = init_database("db.data", CREATE);

    container *cars_container = create_cars_test_container(my_db);
    container *people_container = create_people_test_container(my_db);

    uint32_t car_id = 1;
    char *brand = "AUDI";
    char *model = "RS6";
    uint32_t power = 605;
    bool is_release = true;
    fill_data(cars_container->row, "brand", TYPE_STRING, (void *) &brand);
    fill_data(cars_container->row, "model", TYPE_STRING, (void *) &model);
    fill_data(cars_container->row, "power", TYPE_FLOAT, (void *) &power);
    fill_data(cars_container->row, "is_release", TYPE_BOOL, (void *) &is_release);
    fill_data(cars_container->row, "id", TYPE_INT32, (void *) &car_id);

    for (size_t i = 0; i < 10; i++) {
        fill_data(cars_container->row, "id", TYPE_INT32, (void *) &car_id);
        insert_row(cars_container->row);
        car_id++;
    }

    uint32_t person_id = 1;
    char *name = "Jack";
    uint32_t car_id_ref = 2;
    fill_data(people_container->row, "id", TYPE_INT32, (void *) &person_id);
    fill_data(people_container->row, "name", TYPE_STRING, (void *) &name);
    fill_data(people_container->row, "car_id", TYPE_INT32, (void *) &car_id_ref);
    insert_row(people_container->row);

    person_id++;
    name = "Bob";
    car_id_ref = 3;
    fill_data(people_container->row, "id", TYPE_INT32, (void *) &person_id);
    fill_data(people_container->row, "name", TYPE_STRING, (void *) &name);
    fill_data(people_container->row, "car_id", TYPE_INT32, (void *) &car_id_ref);
    insert_row(people_container->row);

    person_id++;
    name = "Michele";
    car_id_ref = 5;
    fill_data(people_container->row, "id", TYPE_INT32, (void *) &person_id);
    fill_data(people_container->row, "name", TYPE_STRING, (void *) &name);
    fill_data(people_container->row, "car_id", TYPE_INT32, (void *) &car_id_ref);
    insert_row(people_container->row);

    person_id++;
    name = "Matthew";
    car_id_ref = 1;
    fill_data(people_container->row, "id", TYPE_INT32, (void *) &person_id);
    fill_data(people_container->row, "name", TYPE_STRING, (void *) &name);
    fill_data(people_container->row, "car_id", TYPE_INT32, (void *) &car_id_ref);
    insert_row(people_container->row);

    close_database(my_db);
    close_row(cars_container->row);
    close_row(people_container->row);
    close_table_schema(cars_container->table_schema);
    close_table_schema(people_container->table_schema);
    close_table(cars_container->table);
    close_table(people_container->table);
    free(cars_container);
    free(people_container);
}


void read_from_db() {

    database *my_existing_db = init_database("db.data", EXIST);
    table *cars_table = get_table("cars", my_existing_db);
    row *cars_row = create_row(cars_table);

    table *people_table = get_table("people", my_existing_db);

    uint32_t id = 1;
    char *brand = "AUDI";
    char *model = "RS5";
    uint32_t power = 600;
    bool is_release = true;
    fill_data(cars_row, "brand", TYPE_STRING, (void *) &brand);
    fill_data(cars_row, "model", TYPE_STRING, (void *) &model);
    fill_data(cars_row, "power", TYPE_FLOAT, (void *) &power);
    fill_data(cars_row, "is_release", TYPE_BOOL, (void *) &is_release);
    fill_data(cars_row, "id", TYPE_INT32, (void *) &id);
    insert_row(cars_row);

    id = 1;
    brand = "AUDI";
    power = 650;
    is_release = true;
    fill_data(cars_row, "brand", TYPE_STRING, (void *) &brand);
    fill_data(cars_row, "model", TYPE_STRING, (void *) &model);
    fill_data(cars_row, "power", TYPE_FLOAT, (void *) &power);
    fill_data(cars_row, "is_release", TYPE_BOOL, (void *) &is_release);
    fill_data(cars_row, "id", TYPE_INT32, (void *) &id);
    insert_row(cars_row);

    printf("\nSELECT * from cars WHERE model = RS5;\n");
    char *column[1] = {"model"};
    void *value[1] = {&model};
    query *select_query = create_query(SELECT_WHERE, cars_table, column, value, -1);
    run_query(select_query, SHOW_ROWS);


    printf("\nSELECT * from cars WHERE power = 600\n");
    char *column2[1] = {"power"};
    void *value2[1] = {&power};
    query *select_query2 = create_query(SELECT_WHERE, cars_table, column2, value2, -1);
    run_query(select_query2, SHOW_ROWS);

    printf("\n SELECT * from cars WHERE is_release IS TRUE\n");
    char *column3[1] = {"is_release"};
    void *value3[1] = {&is_release};
    query *select_query3 = create_query(SELECT_WHERE, cars_table, column3, value3, -1);
    run_query(select_query3, SHOW_ROWS);


    printf("\nUPDATE cars SET power = 650 where brand = AUDI\n");
    char *columns[2] = {"brand", "power"};
    char *rs6_model = "RS6";
    void *values[2] = {&brand, &power};
    query *update_query = create_query(UPDATE_WHERE, cars_table, columns, values, -1);
    run_query(update_query, SHOW_ROWS);

    printf("\nDELETE * FROM cars WHERE model = RS5\n");
    char *rs5_model = "RS5";
    char *column_model[1] = {"model"};
    void *value_rs5[1] = {&rs5_model};
    query *delete_query = create_query(DELETE_WHERE, cars_table, column_model, value_rs5, -1);
    run_query(delete_query, SHOW_ROWS);

    printf("\nSELECT * FROM people WHERE people.car_id = 1\n");
    char *column5[1] = {"car_id"};
    uint car_id = 1;
    void *column_value5[1] = {&car_id};
    query *select_people_query = create_query(SELECT_WHERE, people_table, column5, column_value5, -1);
    run_query(select_people_query, SHOW_ROWS);

    close_database(my_existing_db);
    close_row(cars_row);
    close_query(select_query);
    close_query(select_query2);
    close_query(select_query3);
    close_query(select_people_query);
    close_query(delete_query);
    close_table(cars_table);
    close_table(people_table);
}


void insert() {
    printf("\nINSERT TEST\n");

    database *insert_db_test = init_database("db_insert.data", CREATE);

    container *cars_container = create_cars_test_container(insert_db_test);
    container *people_container = create_people_test_container(insert_db_test);

    uint32_t id = 1;
    char *brand = "AUDI";
    char *model = "A5";
    float power = 650;
    bool is_release = true;
    fill_data(cars_container->row, "brand", TYPE_STRING, (void *) &brand);
    fill_data(cars_container->row, "model", TYPE_STRING, (void *) &model);
    fill_data(cars_container->row, "power", TYPE_FLOAT, (void *) &power);
    fill_data(cars_container->row, "is_release", TYPE_BOOL, (void *) &is_release);
    fill_data(cars_container->row, "id", TYPE_INT32, (void *) &id);

    uint32_t person_id = 1;
    char *name = "Jack";
    uint32_t car_id_ref = 2;
    fill_data(people_container->row, "id", TYPE_INT32, (void *) &person_id);
    fill_data(people_container->row, "name", TYPE_STRING, (void *) &name);
    fill_data(people_container->row, "car_id", TYPE_INT32, (void *) &car_id_ref);

    clock_t begin;
    clock_t end;
    float time_spent = 0.0;

    for (size_t j = 1; j < 25; j++) {
        begin = clock();
        for (size_t i = 0; i < 1000 * j; i++) {
            insert_row(cars_container->row);
        }
        end = clock();
        time_spent = (float) (end - begin) / CLOCKS_PER_SEC;
        printf("Вставка %d строк заняла %f секунд\n", 1000 * j, time_spent);
    }

    close_database(insert_db_test);
    close_table_schema(cars_container->table_schema);
    close_table_schema(people_container->table_schema);
    close_row(cars_container->row);
    close_row(people_container->row);
    close_table(cars_container->table);
    close_table(people_container->table);
    free(cars_container);
    free(people_container);
}


void select() {
    printf("\nSELECT TEST\n");

    database *select_db_test = init_database("db_select.data", CREATE);
    FILE *select_report;
    select_report = fopen("select_report.csv", "w");
    container *cars_container = create_cars_test_container(select_db_test);

    uint32_t id[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    char *brand[5] = {"AUDI", "BMW", "TOYOTA", "NISSAN", "LEXUS"};
    char *model[10] = {"A4", "A5", "A6", "A7", "A8", "S4", "S5", "S6", "S7", "RS4"};
    uint32_t power[4] = {600, 650, 500, 550};
    bool is_release[2] = {true, false};
    fill_data(cars_container->row, "brand", TYPE_STRING, (void *) &brand);
    fill_data(cars_container->row, "model", TYPE_STRING, (void *) &model);
    fill_data(cars_container->row, "power", TYPE_INT32, (void *) &power);
    fill_data(cars_container->row, "is_release", TYPE_BOOL, (void *) &is_release);
    fill_data(cars_container->row, "id", TYPE_INT32, (void *) &id);

    clock_t begin;
    clock_t end;
    float time = 0.0;

    printf("\nSELECT QUERY: SELECT * FROM cars WHERE brand = AUDI");
    char *select_brand = "AUDI";
    char *column[1] = {"brand"};
    void *value[1] = {&select_brand};
    query *select_query = create_query(SELECT_WHERE, cars_container->table, column, value, -1);

    for (size_t j = 1; j < 25; j++) {
        for (size_t i = 0; i < 1000 * j; i++) {
            fill_data(cars_container->row, "id", TYPE_INT32, (void *) &id[i % 10]);
            fill_data(cars_container->row, "brand", TYPE_STRING, (void *) &brand[i % 5]);
            fill_data(cars_container->row, "model", TYPE_STRING, (void *) &model[i % 10]);
            fill_data(cars_container->row, "power", TYPE_FLOAT, (void *) &power[i % 4]);
            fill_data(cars_container->row, "is_release", TYPE_BOOL, (void *) &is_release[i % 2]);
            insert_row(cars_container->row);
        }
        select_brand = brand[j % 5];
        begin = clock();
        run_query(select_query, NO_ROWS);
        end = clock();
        time = (float) (end - begin) / CLOCKS_PER_SEC;
        fprintf(select_report, "%d,%f\n", 1000 * (j + 1), time);
    }

    close_database(select_db_test);
    close_table_schema(cars_container->table_schema);
    close_row(cars_container->row);
    close_query(select_query);
    close_table(cars_container->table);
    free(cars_container);
}

void delete() {
    printf("TEST DELETE\n");

    database *delete_db_test = init_database("db_delete.data", CREATE);
    FILE *delete_report;
    delete_report = fopen("delete_report.csv", "w");

    container *cars_container = create_cars_test_container(delete_db_test);
    uint32_t id[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    char *brand[5] = {"AUDI", "BMW", "TOYOTA", "NISSAN", "LEXUS"};
    char *model[10] = {"A4", "A5", "A6", "A7", "A8", "S4", "S5", "S6", "S7", "S9"};
    uint32_t power[4] = {600, 650, 500, 550};
    bool is_release[2] = {true, false};

    uint32_t power_value = 600;
    char *column[1] = {"power"};
    void *value[1] = {&power_value};

    query *delete_query = create_query(DELETE_WHERE, cars_container->table, column, value, -1);

    clock_t begin;
    clock_t end;
    double time = 0.0;

    for (size_t j = 1; j < 25; j++) {
        for (size_t i = 0; i < 1000 * j; i++) {
            fill_data(cars_container->row, "id", TYPE_INT32, (void *) &id[i % 10]);
            fill_data(cars_container->row, "brand", TYPE_STRING, (void *) &brand[i % 5]);
            fill_data(cars_container->row, "model", TYPE_STRING, (void *) &model[i % 10]);
            fill_data(cars_container->row, "power", TYPE_FLOAT, (void *) &power[i % 4]);
            fill_data(cars_container->row, "is_release", TYPE_BOOL, (void *) &is_release[i % 2]);
            insert_row(cars_container->row);
        }
        power_value = power[j % 4];
        begin = clock();
        run_query(delete_query, NO_ROWS);
        end = clock();
        time = (float) (end - begin) / CLOCKS_PER_SEC;
        fprintf(delete_report, "%d,%f\n", 1000 * (j + 1), time);
    }

    close_database(delete_db_test);
    close_table_schema(cars_container->table_schema);
    close_row(cars_container->row);
    close_query(delete_query);
    close_table(cars_container->table);
    free(cars_container);
}

void update() {
    printf("TEST UPDATE\n");

    FILE *update_report;
    update_report = fopen("update_report.csv", "w");


    database *update_db_test = init_database("db_update.data", CREATE);
    container *cars_container = create_cars_test_container(update_db_test);

    uint32_t id[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    char *brand[5] = {"AUDI", "BMW", "TOYOTA", "NISSAN", "LEXUS"};
    char *model[10] = {"A4", "A5", "A6", "A7", "A8", "S4", "S5", "S6", "S7", "S9"};
    uint32_t power[4] = {600, 650, 500, 550};
    bool is_release[2] = {true, false};

    printf("\nUPDATE cars SET power = 650 where brand = AUDI\n");
    uint32_t update_power = 0;
    char *update_brand = "BMW";
    char *columns[2] = {"brand", "power"};
    void *values[2] = {&update_brand, &update_power};
    query *update_query = create_query(UPDATE_WHERE, cars_container->table, columns, values, -1);


    clock_t begin;
    clock_t end;
    double time = 0.0;

    for (size_t j = 1; j < 25; j++) {
        for (size_t i = 0; i < 100 * j; i++) {
            fill_data(cars_container->row, "id", TYPE_INT32, (void *) &id[i % 10]);
            fill_data(cars_container->row, "brand", TYPE_STRING, (void *) &brand[i % 5]);
            fill_data(cars_container->row, "model", TYPE_STRING, (void *) &model[i % 10]);
            fill_data(cars_container->row, "power", TYPE_FLOAT, (void *) &power[i % 4]);
            fill_data(cars_container->row, "is_release", TYPE_BOOL, (void *) &is_release[i % 2]);
            insert_row(cars_container->row);
        }
        update_brand = brand[j % 5];
        update_power = power[j % 4];
        begin = clock();
        run_query(update_query, NO_ROWS);
        end = clock();
        time = (float) (end - begin) / CLOCKS_PER_SEC;
        fprintf(update_report, "%d,%f\n", 1000 * (j + 1), time);
    }

    run_query(update_query, NO_ROWS);

    close_database(update_db_test);
    close_table_schema(cars_container->table_schema);
    close_row(cars_container->row);
    close_query(update_query);
    close_table(cars_container->table);
    free(cars_container);
}

void join_tables() {
    printf("TEST JOIN\n");


    FILE *join_report;
    join_report = fopen("join_report.csv", "w");
    database *join_db_test = init_database("db_join.data", CREATE);

    container *cars_container = create_cars_test_container(join_db_test);
    container *people_container = create_people_test_container(join_db_test);

    uint32_t car_id = 1;
    char *brand = "AUDI";
    char *model = "RS6";
    uint32_t power = 600;
    bool is_release = true;
    fill_data(cars_container->row, "id", TYPE_INT32, (void *) &car_id);
    fill_data(cars_container->row, "brand", TYPE_STRING, (void *) &brand);
    fill_data(cars_container->row, "model", TYPE_STRING, (void *) &model);
    fill_data(cars_container->row, "power", TYPE_FLOAT, (void *) &power);
    fill_data(cars_container->row, "is_release", TYPE_BOOL, (void *) &is_release);
    insert_row(cars_container->row);

    car_id++;
    brand = "BMW";
    model = "M5CS";
    power = 635;
    fill_data(cars_container->row, "id", TYPE_INT32, (void *) &car_id);
    fill_data(cars_container->row, "brand", TYPE_STRING, (void *) &brand);
    fill_data(cars_container->row, "model", TYPE_STRING, (void *) &model);
    fill_data(cars_container->row, "power", TYPE_FLOAT, (void *) &power);
    fill_data(cars_container->row, "is_release", TYPE_BOOL, (void *) &is_release);
    insert_row(cars_container->row);

    car_id++;
    brand = "MERCEDES";
    model = "E63AMG";
    power = 612;
    is_release = false;
    fill_data(cars_container->row, "id", TYPE_INT32, (void *) &car_id);
    fill_data(cars_container->row, "brand", TYPE_STRING, (void *) &brand);
    fill_data(cars_container->row, "model", TYPE_STRING, (void *) &model);
    fill_data(cars_container->row, "power", TYPE_FLOAT, (void *) &power);
    fill_data(cars_container->row, "is_release", TYPE_BOOL, (void *) &is_release);
    insert_row(cars_container->row);

    car_id++;
    brand = "LADA";
    model = "VESTA";
    power = 106;
    is_release = true;
    fill_data(cars_container->row, "id", TYPE_INT32, (void *) &car_id);
    fill_data(cars_container->row, "brand", TYPE_STRING, (void *) &brand);
    fill_data(cars_container->row, "model", TYPE_STRING, (void *) &model);
    fill_data(cars_container->row, "power", TYPE_FLOAT, (void *) &power);
    fill_data(cars_container->row, "is_release", TYPE_BOOL, (void *) &is_release);
    insert_row(cars_container->row);

    car_id++;
    brand = "HONDA";
    model = "ACCORD";
    power = 281;
    is_release = true;
    fill_data(cars_container->row, "id", TYPE_INT32, (void *) &car_id);
    fill_data(cars_container->row, "brand", TYPE_STRING, (void *) &brand);
    fill_data(cars_container->row, "model", TYPE_STRING, (void *) &model);
    fill_data(cars_container->row, "power", TYPE_FLOAT, (void *) &power);
    fill_data(cars_container->row, "is_release", TYPE_BOOL, (void *) &is_release);
    insert_row(cars_container->row);

    char *people_names[10] = {"Michele", "Dima", "Kate", "Matthew", "Kirill", "Darya", "Bob", "Jackson", "Anastasiya",
                              "Oleg"};
    uint32_t ids[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    uint32_t car_ids[5] = {1, 2, 3, 4, 5};
    query_join *join_query = create_join_query(people_container->table, cars_container->table, "car_id", "id");


    clock_t begin;
    clock_t end;
    double time = 0.0;
    for (size_t j = 1; j < 10; j++) {
        for (size_t i = 0; i < 1000 * j; i++) {
            fill_data(people_container->row, "id", TYPE_INT32, (void *) &ids[i % 10]);
            fill_data(people_container->row, "name", TYPE_STRING, (void *) &people_names[i % 5]);
            fill_data(people_container->row, "car_id", TYPE_INT32, (void *) &car_ids[i % 5]);
            insert_row(people_container->row);
        }
        begin = clock();
        run_join_query(join_query);
        end = clock();
        time = (float) (end - begin) / CLOCKS_PER_SEC;
        fprintf(join_report, "%d,%f\n", 1000 * (j + 1), time);
    }

    close_database(join_db_test);
    close_schema(cars_container->table_schema);
    close_schema(people_container->table_schema);
    close_table(cars_container->table);
    close_table(people_container->table);
    close_row(cars_container->row);
    close_row(people_container->row);
    close_join_query(join_query);
    free(cars_container);
    free(people_container);
    fclose(join_report);
}

void file_size() {
    database *file_test_db = init_database("db_file_test.data", CREATE);

    container *cars_container = create_cars_test_container(file_test_db);
    FILE *file_size_report;
    file_size_report = fopen("file_size_report.csv", "w");

    uint32_t car_id = 1;
    char *brand = "AUDI";
    char *model = "RS6";
    uint32_t power = 600;
    bool is_release = true;
    fill_data(cars_container->row, "id", TYPE_INT32, (void *) &car_id);
    fill_data(cars_container->row, "brand", TYPE_STRING, (void *) &brand);
    fill_data(cars_container->row, "model", TYPE_STRING, (void *) &model);
    fill_data(cars_container->row, "power", TYPE_FLOAT, (void *) &power);
    fill_data(cars_container->row, "is_release", TYPE_BOOL, (void *) &is_release);
    insert_row(cars_container->row);

    for (size_t j = 1; j < 25; j++) {
        for (size_t i = 0; i < 100 * j; i++) {
            fill_data(cars_container->row, "id", TYPE_INT32, (void *) &car_id);
            fill_data(cars_container->row, "brand", TYPE_STRING, (void *) &brand);
            fill_data(cars_container->row, "model", TYPE_STRING, (void *) &model);
            fill_data(cars_container->row, "power", TYPE_FLOAT, (void *) &power);
            fill_data(cars_container->row, "is_release", TYPE_BOOL, (void *) &is_release);
            insert_row(cars_container->row);
        }
        fseek(file_test_db->database_file, 0, SEEK_END);
        long file_size = ftell(file_test_db->database_file);
        fprintf(file_size_report, "%d,%d\n", 1000 * (j + 1), file_size);
    }

    close_database(file_test_db);
    close_table(cars_container->table);
    close_schema(cars_container->table_schema);
    close_row(cars_container->row);
    free(cars_container);
}
