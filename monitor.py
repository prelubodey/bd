import sqlite3
import time
from datetime import datetime

MAX_ROWS = 14
CHECK_INTERVAL = 5
DB_NAME = 'cars.db'
OUTPUT_FILE = 'allowed.txt'

def get_rows():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT type, model, number, event_time
        FROM events
        WHERE event_date = ?
        ORDER BY id DESC
        LIMIT ?
    """, (datetime.now().strftime("%d.%m.%Y"), MAX_ROWS))
    
    rows = cursor.fetchall()
    conn.close()
    return list(reversed(rows))

def format_row(index, move_type, model, number, event_time):
    short_model = str(model).split()[0] if model else "---"
    return f"{str(index):<3} | {move_type:<7} | {short_model:<10} | {number:<12} | {event_time:<9}"

def write_file(rows):
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    header = (
        f"ПРОПУСКА КРЫМ-АВТО | Дата: {now_str}\n"
        + "-" * 55 + "\n"
        + f"{'№':<3} | {'Тип':<7} | {'Модель':<10} | {'Номер':<12} | {'Время':<9}\n"
        + "-" * 55 + "\n"
    )

    content = ""
    for i, row in enumerate(rows, 1):
        move_type, model, number, event_time = row
        content += format_row(i, move_type, model, number, event_time) + "\n"

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(header + content)

def main():
    print(f"Монитор запущен. Вывод по {MAX_ROWS} строк в {OUTPUT_FILE}")

    while True:
        try:
            rows = get_rows()
            write_file(rows)
        except Exception as e:
            print(f"Ошибка: {e}")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()