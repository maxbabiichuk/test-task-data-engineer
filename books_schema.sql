-- таблиця books
CREATE TABLE IF NOT EXISTS books (
	book_id SERIAL PRIMARY KEY,
	title VARCHAR(500) NOT NULL,
	price NUMERIC(10, 2) NOT NULL,
	genre VARCHAR(100) NULL,
	stock_quantity INTEGER DEFAULT(0),
	last_updated TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
);

-- таблиця books_processed
CREATE TABLE IF NOT EXISTS books_processed (
    processed_id SERIAL PRIMARY KEY,
    book_id INTEGER NOT NULL, -- можливо, варто ще створити FOREIGN KEY, але в завданні опис виглядаж так, що не варто
   	title VARCHAR(500) NOT NULL,
    original_price NUMERIC(10, 2) NOT NULL,
    rounded_price NUMERIC(9, 1) NOT NULL,
    genre VARCHAR(100) NULL,
    price_category VARCHAR(12) NOT NULL CHECK (price_category IN ('budget', 'premium')),
    processed_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
);

-- 1. idx_books_genre - для швидкого пошуку книг за жанром
CREATE INDEX IF NOT EXISTS idx_books_genre ON books (lower(genre));
-- створив B-tree індекс він підійде для пошуку по чіткому співпадінню і для пошуку по префіксу.
-- додатково привів поле genre до нижнього регістру, як на мене, такий варіант більш оптимальний для простих задач пошуку.
-- загалом, пошук по текстовим полям - це досить шарока задача і її варто вирішувати згідно відповідних потреб

-- 2. idx_books_last_updated - для пошуку книг за датою останнього оновлення
CREATE INDEX IF NOT EXISTS idx_books_last_updated ON books (last_updated);

-- 3. idx_books_price_range - для пошуку книг у певному ціновому діапазоні
CREATE INDEX IF NOT EXISTS idx_books_price_range ON books (price);
-- B-tree індекс підходить для пошуку по діапазону

-- додавання рівно 6 записів в таблицю books
INSERT INTO books (title, price, genre, stock_quantity, last_updated) VALUES
('Назва книги 1', 299.99, 'фантастика', 15, '2025-01-15 10:30:00'),
('Назва книги 2', 435.87, 'детектив', 20, '2025-07-15 12:45:00'),
('Назва книги 3', 625.50, 'романтика', 45, '2025-06-07 16:27:00'),
('Назва книги 4', 500.00, 'фентезі', 73, '2025-08-05 12:33:00'),
('Назва книги 5', 735.99, 'детектив', 52, '2025-08-07 09:16:00'),
('Назва книги 6', 305.45, 'романтика', 37, '2025-04-18 14:43:00')


