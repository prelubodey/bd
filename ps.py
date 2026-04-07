import imaplib
import email
import sqlite3
import time
import os
import sys
import re
from email.header import decode_header
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASS = os.getenv('EMAIL_PASS')
IMAP_SERVER = 'mail.rbauto.ru'
TARGET_FOLDER = 'INBOX.kpp_simferopol.INBOX'
DB_NAME = 'cars.db'
CHECK_INTERVAL = 15

def init_db():
    """Инициализация БД: создание таблиц и индексов"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # Таблица данных
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            msg_id TEXT UNIQUE,
            type TEXT,
            model TEXT,
            number TEXT,
            vin TEXT,
            client TEXT,
            document TEXT,
            repair_type TEXT,
            event_date TEXT,
            event_time TEXT,
            authorized_by TEXT,
            reason TEXT,
            raw_subject TEXT
        )
    ''')
    # Таблица состояния для хранения последнего UID
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    # Индекс для быстрого поиска по msg_id
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_msg_id ON events(msg_id)')
    conn.commit()
    return conn

def get_state(cursor, key, default="0"):
    cursor.execute("SELECT value FROM state WHERE key = ?", (key,))
    row = cursor.fetchone()
    return row[0] if row else default

def set_state(cursor, key, value):
    cursor.execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", (key, str(value)))

def extract_field(field_name, text):
    """Универсальный экстрактор на регулярных выражениях"""
    pattern = fr"{field_name}\s*:\s*(.+)"
    match = re.search(pattern, text, re.IGNORECASE)
    return match.group(1).strip() if match else "---"

def parse_mail_body(msg):
    """Надежное извлечение текста из HTML или Plain частей"""
    html_body = None
    text_body = None

    for part in msg.walk():
        ctype = part.get_content_type()
        payload = part.get_payload(decode=True)
        if not payload: continue

        if ctype == "text/html":
            html_body = payload.decode('utf-8', 'replace')
        elif ctype == "text/plain":
            text_body = payload.decode('utf-8', 'replace')

    content = html_body or text_body
    if not content: return None

    # Очистка от HTML-тегов, если это HTML
    if html_body:
        soup = BeautifulSoup(content, 'html.parser')
        for br in soup.find_all("br"): br.replace_with("\n")
        content = soup.get_text()

    lines = content.split('\n')
    clean_text = "\n".join([l.strip() for l in lines if l.strip()])

    data = {
        'model': extract_field("Модель", clean_text),
        'number': extract_field("номер", clean_text).upper().replace(" ", ""),
        'vin': extract_field("VIN", clean_text),
        'client': extract_field("Клиент", clean_text),
        'document': extract_field("Документ", clean_text),
        'repair_type': extract_field("Вид ремонта", clean_text),
        'auth': extract_field("Разрешил", clean_text),
        'reason': extract_field("Причина", clean_text)
    }
    
    # Обработка даты и времени отдельно
    date_line = extract_field("Дата", clean_text)
    parts = date_line.split()
    data['date'] = parts[0] if len(parts) >= 1 else "---"
    data['time'] = parts[1] if len(parts) >= 2 else "--:--:--"
    
    return data

def process_emails():
    conn = init_db()
    cursor = conn.cursor()
    
    last_uid = int(get_state(cursor, 'last_uid', "0"))
    added_count = 0

    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select(f'"{TARGET_FOLDER}"', readonly=True)

        # Поиск только новых UID через сервер
        search_query = f"UID {last_uid + 1}:*" if last_uid > 0 else "ALL"
        status, data = mail.uid('search', None, search_query)

        if status != "OK" or not data[0]:
            mail.logout()
            return 0

        uids = data[0].split()
        # Исключаем текущий последний UID, если сервер вернул его в диапазоне
        new_uids = [u for u in uids if int(u) > last_uid]
        total = len(new_uids)

        if total == 0:
            mail.logout()
            return 0

        for idx, uid_bytes in enumerate(new_uids, 1):
            uid_str = uid_bytes.decode()
            status, msg_data = mail.uid('fetch', uid_str, '(RFC822)')
            
            if status != "OK": continue

            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)
            
            # Декодирование темы
            subject = ""
            for part, enc in decode_header(msg.get("Subject", "")):
                if isinstance(part, bytes):
                    subject += part.decode(enc or 'utf-8', 'replace')
                else:
                    subject += str(part)

            move_type = "ЗАЕЗД" if "ЗАЕЗД" in subject.upper() else "ВЫЕЗД" if "ВЫЕЗД" in subject.upper() else "---"
            
            info = parse_mail_body(msg)
            if info:
                cursor.execute('''
                    INSERT OR IGNORE INTO events (
                        msg_id, type, model, number, vin, client, 
                        document, repair_type, event_date, event_time, 
                        authorized_by, reason, raw_subject
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    uid_str, move_type, info['model'], info['number'], 
                    info['vin'], info['client'], info['document'], 
                    info['repair_type'], info['date'], info['time'], 
                    info['auth'], info['reason'], subject
                ))
                
                # Обновляем состояние после каждого успешно обработанного письма
                set_state(cursor, 'last_uid', uid_str)
                added_count += 1
                
                print(f" Обработка: [{idx}/{total}] UID {uid_str}...", end="\r")

        conn.commit()
        mail.logout()
        if added_count > 0:
            print(f"\n[OK] Добавлено записей: {added_count}")
        return added_count

    except Exception as e:
        print(f"\n[Error] Ошибка IMAP/DB: {e}")
        return 0
    finally:
        conn.close()

if __name__ == "__main__":
    print(f"--- Мониторинг запущен ({TARGET_FOLDER}) ---")
    while True:
        process_emails()
        # Компактный таймер ожидания
        for i in range(CHECK_INTERVAL, 0, -1):
            print(f" Следующая проверка через {i} сек...  ", end="\r")
            time.sleep(1)