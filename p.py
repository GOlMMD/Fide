import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup

# تابع برای خواندن فایل و استخراج ID‌ها
def extract_ids_from_txt(file_path):
    ids = []
    with open(file_path, 'r') as file:
        lines = file.readlines()
        for line in lines[1:]:  # خط اول را نادیده می‌گیریم چون عنوان‌هاست
            id_part = line.split()[0]  # اولین ستون که ID است
            ids.append(id_part)
    return ids

# تابع async برای دریافت HTML از URL و استخراج جدول با ستون 'Period'
async def fetch_table_with_period(session, player_id, semaphore):
    print(player_id)
    url = f"https://ratings.fide.com/profile/{player_id}/calculations"
    async with semaphore:  # محدود کردن تعداد درخواست‌ها به 1000 همزمان
        async with session.get(url) as response:
            if response.status == 200:
                html = await response.text()
                tables = pd.read_html(html)
                for table in tables:
                    if 'Period' in table.columns:  # چک کردن وجود ستون 'Period'
                        print(f"Player ID {player_id} has 'Period' table:\n")
                        print(table)
                        table.to_excel(f"A:/payan_name/python-scrapping/excel/{player_id}.xlsx")
                        return table
            else:
                print(f"Failed to fetch data for Player ID: {player_id} - Status code: {response.status}")

# تابع async برای مدیریت دانلودها
async def fetch_all_tables_with_period(player_ids, max_concurrent_tasks=1):
    semaphore = asyncio.Semaphore(max_concurrent_tasks)  # محدودیت 1000 درخواست همزمان
    async with aiohttp.ClientSession() as session:
        tasks = []
        for player_id in player_ids:
            task = fetch_table_with_period(session, player_id, semaphore)
            tasks.append(task)
        await asyncio.gather(*tasks)  # منتظر تمام شدن همه وظایف async

# تابع اصلی برای اجرا (همراه با بررسی حلقه رویداد موجود)
def main(file_path):
    player_ids = extract_ids_from_txt(file_path)
    print(len(player_ids))
    player_ids=player_ids[:1]
    try:
        # بررسی اینکه آیا حلقه رویداد در حال اجراست یا خیر
        loop = asyncio.get_running_loop()
    except RuntimeError:  # اگر حلقه‌ای وجود نداشت، یکی ایجاد می‌کنیم
        loop = None
    
    if loop and loop.is_running():
        print("Using existing event loop.")
        # در صورتی که حلقه‌ای در حال اجرا باشد، از create_task برای اجرای وظیفه استفاده می‌کنیم
        task = loop.create_task(fetch_all_tables_with_period(player_ids))
    else:
        print("Starting new event loop.")
        # اگر حلقه‌ای در حال اجرا نبود، از asyncio.run استفاده می‌کنیم
        asyncio.run(fetch_all_tables_with_period(player_ids))

# اجرا
file_path = 'player.txt'
main(file_path)
