import os
import sys

import pandas as pd
import numpy as np
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

CHUNK_SIZE = 1000


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


def extract_books(engine, cutoff_date, last_processed_id):
    """
    Витягнути книги з таблиці books де last_updated >= cutoff_date

    Параметри:
    - engine: SQLAlchemy engine
    - cutoff_date: рядок в форматі 'YYYY-MM-DD'

    Поверніть: pandas DataFrame з колонками:
    book_id, title, price, genre, stock_quantity, last_updated

    Виведіть кількість знайдених записів
    """

    query = text(
        """
        SELECT 
            book_id
            , title
            , price
            , genre
            , stock_quantity
            , last_updated
        FROM books
        WHERE 
            last_updated >= :cutoff_date
            AND
            book_id > :last_processed_id
        order by book_id
        LIMIT :limit
        """
    )

    try:
        chunk = pd.read_sql_query(
            query,
            engine,
            params={
                "cutoff_date": cutoff_date,
                "last_processed_id": last_processed_id,
                "limit": CHUNK_SIZE,
            },
        )
        if (len(chunk) > 0):
            print(f"Витягнуто {len(chunk)} записів з таблиці books")
        return chunk
    except Exception as e:
        raise RuntimeError(f"Помилка читання даних з books: {e}")


def is_valid_date(date_string):
    try:
        datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def transform_data(df):
    """
    Трансформувати дані згідно бізнес-правил:

    1. Створити колонку 'original_price' (копія 'price')
    2. Округлити 'price' до 1 знака після коми та зберегти як нову колонку з назвою 'rounded_price'
    3. Створити 'price_category':
       - 'budget' якщо rounded_price < 500
       - 'premium' якщо rounded_price >= 500
    4. Видалити оригінальну колонку 'price'

    Поверніть: трансформований DataFrame
    Виведіть кількість оброблених записів
    """

    # згідно п.1 та п.4 можна просто змінити назву колонки
    df.rename(columns={"price": "original_price"}, inplace=True)
    # п.2
    df["rounded_price"] = df["original_price"].round(1)
    # п.3
    df["price_category"] = np.where(df["rounded_price"] < 500, "budget", "premium")
    print(f"Трансформовано {len(df)} записів")
    return df


def load_data(df, engine):
    """
    Зберегти оброблені дані в таблицю books_processed

    Використовуйте df.to_sql() з параметрами:
    - if_exists='append' (додавати до існуючих даних)
    - index=False (не зберігати індекс DataFrame)
    - chunksize=1000 (пакетна обробка)

    Виведіть кількість збережених записів
    Обробіть помилки збереження
    """

    columns_to_save = [
        "book_id",
        "title",
        "original_price",
        "rounded_price",
        "genre",
        "price_category",
    ]

    try:
        df[columns_to_save].to_sql(
            name="books_processed",
            con=engine,
            if_exists="append",
            index=False,
            chunksize=CHUNK_SIZE,
        )
        print(f"Збережено {len(df)} записів в books_processed")
    except Exception as e:
        raise RuntimeError(f"Помилка збереження даних в books_processed: {e}")


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

    cutoff_date = sys.argv[1]
    if not is_valid_date(cutoff_date):
        print(f"Неправильний формат дати: {cutoff_date}")
        sys.exit(1)

    engine = connect_to_db()

    # обробка записів відбувається пакетами розміром CHUNK_SIZE, щоб запобігти переповнення пам'яті
    rows_processed = 0
    last_processed_id = 0
    df = extract_books(engine, cutoff_date, last_processed_id)
    while not df.empty:
        last_processed_id = int(df["book_id"].max())
        rows_processed += len(df)
        # трансформація згідно бізнес-правил
        transform_data(df)
        # збереження
        load_data(df, engine)
        df = extract_books(engine, cutoff_date, last_processed_id)
    else:
        if rows_processed == 0:
            print(
                "Нових книг для обробки за вказану дату не знайдено. Роботу завершено."
            )
        else:
            print(
                f"Загалом оброблено {rows_processed} записів. ETL процес завершено успішно"
            )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Логування або алерт
        print(f"Помилка ETL: {e}")
        exit(1)
