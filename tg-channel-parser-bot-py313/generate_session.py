from telethon.sync import TelegramClient
from telethon.sessions import StringSession

def main():
    print("=== Генерация строки Telethon-сессии ===")
    api_id = int(input("API_ID: ").strip())
    api_hash = input("API_HASH: ").strip()
    print("\nВойдите в Telegram-аккаунт: телефон, код, при необходимости 2FA-пароль.")
    with TelegramClient(StringSession(), api_id, api_hash) as client:
        print("\n=== СКОПИРУЙТЕ ЭТУ СТРОКУ В .env КАК TELETHON_SESSION ===\n")
        print(client.session.save())
        print("\n========================================")

if __name__ == "__main__":
    main()
