import threading
import time
import psycopg2

# Підключення до бази даних
def connection():
    username = 'Inna'
    password = '111'
    database = 'Inna_DB6_1'
    host = 'localhost'
    port = '5432'
    conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    return conn


def setup_database():
    conn = connection()
    with conn:
        with conn.cursor() as cursor:
            # Очистка даних у таблиці
            cursor.execute("DELETE FROM user_counter;")
            # Вставка початкового рядка даних
            cursor.execute("INSERT INTO user_counter (user_id, counter, version) VALUES (1, 0, 0);")


# Отримання значення лічильника(counter) з бази даних
def inner_func():
    conn = connection()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
            counter = cursor.fetchone()[0]
            return counter


def lost_update():
    conn = connection()
    with conn:
        with conn.cursor() as cursor:
            for _ in range(10000):
                cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
                counter = cursor.fetchone()[0]
                counter += 1
                cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
                conn.commit()


def in_place_update():
    conn = connection()
    with conn:
        with conn.cursor() as cursor:
            for _ in range(10000):
                cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s", (1,))
                conn.commit()


def	row_level_locking():
    conn = connection()
    with conn:
        with conn.cursor() as cursor:
            for _ in range(10000):
                cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
                counter = cursor.fetchone()[0]
                counter += 1
                cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
                conn.commit()


def update_counter_optimistic_locking():
    conn = connection()
    with conn:
        with conn.cursor() as cursor:
            for _ in range(10000):
                while True:
                    cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
                    counter, version = cursor.fetchone()
                    counter += 1
                    cursor.execute(
                        "UPDATE user_counter SET counter = %s, version = %s WHERE user_id = %s AND version = %s",
                        (counter, version + 1, 1, version))
                    conn.commit()
                    if cursor.rowcount > 0: break



# Функція, яка запускає функцію `func` у 10 потоках паралельно
def update_method(func):
    setup_database()
    threads = [threading.Thread(target=func) for _ in range(10)]

    start_time = time.time()
    # Запуск потоків
    for thread in threads:
        thread.start()
    # Очікування завершення потоків
    for thread in threads:
        thread.join()

    end_time = time.time()

    # Вивід інформації про виконану операцію
    print(f"\n{'=' * 50}")

    if func == lost_update:
        print(f"This method demonstrates a {func.__name__} scenario.")
    elif func == in_place_update:
        print(f"This method demonstrates an {func.__name__} scenario.")
    elif func == row_level_locking:
        print(f"This method demonstrates {func.__name__}")
    elif func == update_counter_optimistic_locking:
        print(f"This method demonstrates {func.__name__}")

    print(f"Total execution time: {round(end_time - start_time, 2)} seconds")

    print(f"Final counter value: {inner_func()}")
    print('=' * 50 + "\n")



if __name__ == "__main__":

    print("┌──────────────────────────────────────────────────────┐")
    print("│                                                      │")
    print("│                 Лабораторна робота №1                │")
    print("│                із дисципліни Бази даних              │")
    print("│                                                      │")
    print("│ Тема: Реалізація каунтера з використанням PostgreSQL │")
    print("│                                                      │")
    print("│                                                      │")
    print("│ Виконала ст.гр КМ-11             Шушпаннікова І.В.   │")
    print("│                                                      │")
    print("│                                                      │")
    print("│                         2024                         │")
    print("└──────────────────────────────────────────────────────┘")
    # Запуск функцій з різними сценаріями оновлення у потоках
    update_method(lost_update)
    update_method(in_place_update)
    update_method(row_level_locking)
    update_method(update_counter_optimistic_locking)


