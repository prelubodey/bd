import imaplib
import email
import sqlite3
import time
import os
import sys
from email.header import decode_header
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Загружаем переменные из файла .env
load_dotenv()

# Настройки
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASS = os.getenv('EMAIL_PASS')
IMAP_SERVER = 'mail.rbauto.ru'
TARGET_FOLDER = 'INBOX.kpp_simferopol.INBOX'
DB_NAME = 'cars.db'
CHECK_INTERVAL = 15

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
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
    conn.commit()
    return conn

def get_last_saved_uid(cursor):
    cursor.execute("SELECT MAX(CAST(msg_id AS INTEGER)) FROM events")
    result = cursor.fetchone()[0]
    return result if result else 0

def parse_full_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    for br in soup.find_all("br"):
        br.replace_with("\n")
    text = soup.get_text()
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    data = {
        'model': '---', 'number': '---', 'vin': '---', 
        'client': '---', 'document': '---', 'repair_type': '---',
        'date': '---', 'time': '--:--:--', 'auth': '---', 'reason': '---'
    }

    for line in lines:
        if "Модель:" in line: data['model'] = line.split("Модель:")[1].strip()
        elif "номер:" in line: data['number'] = line.split("номер:")[1].strip().upper().replace(" ", "")
        elif "VIN:" in line: data['vin'] = line.split("VIN:")[1].strip()
        elif "Клиент:" in line: data['client'] = line.split("Клиент:")[1].strip()
        elif "Документ:" in line: data['document'] = line.split("Документ:")[1].strip()
        elif "Вид ремонта:" in line: data['repair_type'] = line.split("Вид ремонта:")[1].strip()
        elif "Дата:" in line:
            parts = line.split("Дата:")[1].strip().split()
            if len(parts) >= 1: data['date'] = parts[0]
            if len(parts) >= 2: data['time'] = parts[1]
        elif "Разрешил:" in line: data['auth'] = line.split("Разрешил:")[1].strip()
        elif "Причина:" in line: data['reason'] = line.split("Причина:")[1].strip()
    return data

def process_emails():
    conn = init_db()
    cursor = conn.cursor()
    last_uid = get_last_saved_uid(cursor)
    added_count = 0
    
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select(f'"{TARGET_FOLDER}"', readonly=True)
        
        search_query = f"UID {last_uid + 1}:*" if last_uid > 0 else "ALL"
        status, data = mail.uid('search', None, search_query)
        
        uids = data[0].split()
        if not uids or (len(uids) == 1 and int(uids[0]) <= last_uid):
            return 0

        # Фильтруем только те UID, которые действительно больше последнего
        new_uids = [u for u in uids if int(u) > last_uid]
        total_new = len(new_uids)
        
        if total_new == 0:
            return 0

        for uid_bytes in new_uids:
            uid_str = uid_bytes.decode()
            status, msg_data = mail.uid('fetch', uid_str, '(RFC822)')
            
            for response_part in msg_data:
                if not isinstance(response_part, tuple): continue
                
                msg = email.message_from_bytes(response_part[1])
                subject = ""
                for part, enc in decode_header(msg.get("Subject", "")):
                    if isinstance(part, bytes):
                        subject += part.decode(enc or 'utf-8', 'replace')
                    else:
                        subject += str(part)
                
                move_type = "ЗАЕЗД" if "ЗАЕЗД" in subject.upper() else "ВЫЕЗД" if "ВЫЕЗД" in subject.upper() else "---"
                
                html_body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/html":
                            html_body = part.get_payload(decode=True).decode('utf-8', 'replace')
                            break
                else:
                    html_body = msg.get_payload(decode=True).decode('utf-8', 'replace')

                if html_body:
                    info = parse_full_html(html_body)
                    cursor.execute('''
                        INSERT OR IGNORE INTO events (
                            msg_id, type, model, number, vin, client, 
                            document, repair_type, event_date, event_time, 
                            authorized_by, reason, raw_subject
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        uid_str, move_type, info['model'], info['number'], 
                        info['vin'], info['client'], info['document'], 
                        info['repair_type'], info['date'], info['time'], 
                        info['auth'], info['reason'], subject
                    ))
                    
                    added_count += 1
                    sys.stdout.write(f"\rОбработка: [{added_count}/{total_new}] новых записей...")
                    sys.stdout.flush()

        conn.commit()
        mail.logout()
        if added_count > 0:
            print(f"\nЗавершено. Всего добавлено: {added_count}")
        return added_count

    except Exception as e:
        print(f"\nОшибка при работе с почтой: {e}")
        return 0
    finally:
        conn.close()

if __name__ == "__main__":
    print(f"Мониторинг запущен. Папка: {TARGET_FOLDER}")
    while True:
        process_emails()
        for i in range(CHECK_INTERVAL, 0, -1):
            sys.stdout.write(f"\rСледующая проверка через {i} сек...   ")
            sys.stdout.flush()
            time.sleep(1)