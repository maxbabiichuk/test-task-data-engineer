# Data Engineer Interview Test Tasks

Цей проєкт містить завдання для перевірки навичок Data Engineer. Основний скрипт виконує ETL-процес вибірки та обробки даних з бази PostgreSQL, починаючи з вказаної дати.


## Встановлення

Клонуйте репозиторій та встановіть залежності:

```bash
cd your-repo
pip install -r requirements.txt
```

## Конфігурація
Для роботи скрипта необхідно задати змінні оточення для підключення до бази даних. Для тесту, можна створити файл .env у корені проєкту з таким вмістом:
```
DB_HOST=your_db_host
DB_PORT=your_db_port
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_db_password
```

## Запуск
Запустіть ETL-скрипт, передавши дату у форматі YYYY-MM-DD (дата від якої буде вибір записів):
```bash
python books_etl.py 2025-01-01
```