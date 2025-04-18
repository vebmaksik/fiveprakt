import multiprocessing
import os
import psutil
import random
import logging
from datetime import datetime

log_queue = multiprocessing.Queue()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def setup_logger_process(queue):
    while True:
        try:
            record = queue.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception:
            import traceback
            print(traceback.format_exc())
            break

def log_message(level, message):
    record = logging.LogRecord(
        name=__name__,
        level=level,
        pathname=__file__,
        lineno=0,
        msg=message,
        args=(),
        exc_info=None,
    )
    log_queue.put(record)

def get_cpu_usage_percentage():
    return psutil.cpu_percent()

def get_max_processes():
    cpu_usage = get_cpu_usage_percentage()
    num_cores = multiprocessing.cpu_count()
    available_processes = int(num_cores * (1 - (cpu_usage / 100)))
    return max(1, available_processes)

def generate_matrix(rows, cols):
    return [[random.randint(1, 10) for _ in range(cols)] for _ in range(rows)]

def matrix_multiply_task(matrix1, matrix2, start_row, end_row, result_queue):
    log_message(logging.INFO, f"Процесс {os.getpid()} начал обработку строк с {start_row} по {end_row}")

    partial_result = []
    for i in range(start_row, end_row):
        row_result = []
        for j in range(len(matrix2[0])):
            element_sum = 0
            for k in range(len(matrix2)):
                element_sum += matrix1[i][k] * matrix2[k][j]
            row_result.append(element_sum)
        partial_result.append(row_result)

    result_queue.put((start_row, partial_result))
    log_message(logging.INFO, f"Процесс {os.getpid()} завершил обработку строк с {start_row} по {end_row}")

def save_partial_result(results, filename):
    try:
        with open(filename, 'w') as f:
            for row in results:
                f.write(','.join(map(str, row)) + '\n')
        log_message(logging.INFO, f"Частичные результаты успешно сохранены в файл {filename}")
    except Exception as e:
        log_message(logging.ERROR, f"Ошибка при сохранении частичных результатов в файл {filename}: {e}")

def main():
    logger_process = multiprocessing.Process(target=setup_logger_process, args=(log_queue,))
    logger_process.start()

    try:
        rows1 = int(input("Введите количество строк для первой матрицы: "))
        cols1 = int(input("Введите количество столбцов для первой матрицы: "))
        rows2 = int(input("Введите количество строк для второй матрицы: "))
        cols2 = int(input("Введите количество столбцов для второй матрицы: "))

        if cols1 != rows2:
            log_message(logging.ERROR, "Матрицы нельзя перемножить. Недопустимые размеры.")
            print("Матрицы нельзя перемножить. Недопустимые размеры.")
            return

        matrix1 = generate_matrix(rows1, cols1)
        matrix2 = generate_matrix(rows2, cols2)

        max_processes = get_max_processes()
        num_processes = int(input(f"Введите количество процессов для использования (максимум {max_processes}): "))
        num_processes = min(num_processes, max_processes)

        log_message(logging.INFO, f"Начинаем умножение матриц с {num_processes} процессами.")

        result_queue = multiprocessing.Queue()
        processes = []
        rows_per_process = rows1 // num_processes
        remaining_rows = rows1 % num_processes
        start_row = 0

        for i in range(num_processes):
            end_row = start_row + rows_per_process + (1 if i < remaining_rows else 0)
            p = multiprocessing.Process(target=matrix_multiply_task,
                                        args=(matrix1, matrix2, start_row, end_row, result_queue))
            processes.append(p)
            start_row = end_row
            p.start()

        for p in processes:
            p.join()

        log_message(logging.INFO, "Все процессы завершены.")

        results = [None] * rows1
        while not result_queue.empty():
            start_row, partial_result = result_queue.get()
            for i, row in enumerate(partial_result):
                results[start_row + i] = row

        log_message(logging.INFO, "Собираем финальную матрицу результатов.")

        filename = f"результат_умножения_матриц_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        save_partial_result(results, filename)

        print(f"Умножение матриц завершено. Результат сохранен в файл {filename}")

    except ValueError:
        log_message(logging.ERROR, "Неверный ввод. Пожалуйста, введите целые числа.")
        print("Неверный ввод. Пожалуйста, введите целые числа.")

    finally:
        log_queue.put(None)
        logger_process.join()

if __name__ == "__main__":
    main()