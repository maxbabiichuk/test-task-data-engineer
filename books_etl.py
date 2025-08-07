import os
import sys

import pandas as pd
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError



def connect_to_db():
    """
    Створити SQLAlchemy engine для підключення до PostgreSQL
    Використовуйте environment variables для параметрів підключення
    Поверніть engine об'єкт
    Обробіть помилки підключення
    """
    if not find_dotenv():
        print("Файл .env не знайдено в директорії з проектом. Це може бути проблемою.")
    else:
        load_dotenv()

    required_vars = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        raise RuntimeError(f"Не вказані параметри підключення до БД: {missing}")

    # Отримуємо змінні середовища
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    connection_url = (
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    try:
        engine = create_engine(connection_url)
        engine.connect().execute(text("SELECT 1"))  # Тестове підключення
        print(f"Підключено до бази даних {db_name} успішно")
        return engine
    except SQLAlchemyError as e:
        raise RuntimeError(f"Помилка підключення до БД: {e}")

def extract_books(engine, cutoff_date):
    """
    Витягнути книги з таблиці books де last_updated >= cutoff_date

    Параметри:
    - engine: SQLAlchemy engine
    - cutoff_date: рядок в форматі 'YYYY-MM-DD'

    Поверніть: pandas DataFrame з колонками:
    book_id, title, price, genre, stock_quantity, last_updated

    Виведіть кількість знайдених записів
    """


def main():
    """
    Головна функція:
    1. Перевірити аргументи командного рядка (має бути рівно 1 - дата)
    2. Валідувати формат дати (YYYY-MM-DD)
    3. Викликати всі ETL функції в правильному порядку
    4. Обробити помилки та вивести підсумкову статистику
    """
    if len(sys.argv) != 2:
        print("Використання: python books_etl.py YYYY-MM-DD")
        print("Приклад: python books_etl.py 2025-01-01")
        sys.exit(1)

    engine = connect_to_db()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Логування або алерт
        print(f"Помилка ETL: {e}")
        exit(1)
