import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
import os


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
            print(url)
            if response.status == 200:
                try:
                    html = await response.text()
                    tables = pd.read_html(html)
                    for table in tables:
                        if 'Period' in table.columns:  # چک کردن وجود ستون 'Period'
                            # print(f"Player ID {player_id} has 'Period' table:\n")
                            # display(table.head(10))
                            table.to_excel(f"C:/Users/pc/Desktop/New folder/excel/{player_id}.xlsx")
                            return table
                except Exception as e:
                    print(e)
                    pass
            else:
                print(f"Failed to fetch data for Player ID: {player_id} - Status code: {response.status}")

# تابع async برای مدیریت دانلودها
async def fetch_all_tables_with_period(player_ids, max_concurrent_tasks=100):
    semaphore = asyncio.Semaphore(max_concurrent_tasks)  # محدودیت 1000 درخواست همزمان
    async with aiohttp.ClientSession() as session:
        tasks = []
        for player_id in player_ids:
            task = fetch_table_with_period(session, player_id, semaphore)
            tasks.append(task)
        await asyncio.gather(*tasks)  # منتظر تمام شدن همه وظایف async


# تابع برای دریافت نام فایل‌های موجود در فولدر 'excel'
def get_existing_files_in_excel_folder(folder_path):
    files = os.listdir(folder_path)  # گرفتن تمام فایل‌های موجود در فولدر
    excel_files = [f.replace('.xlsx', '') for f in files if f.endswith('.xlsx')]  # حذف پسوند '.xlsx' از نام فایل‌ها
    return excel_files


# تابع اصلی برای اجرا (همراه با بررسی حلقه رویداد موجود)
def main(file_path):
        # استخراج ID‌ها از فایل
    player_ids = extract_ids_from_txt(file_path)
    print(f"Initial Player IDs: {len(player_ids)}")

    # خواندن فایل‌های موجود در پوشه excel و حذف آن‌ها از player_ids
    existing_files = get_existing_files_in_excel_folder('excel')
    print(f"Existing files in excel folder: {len(existing_files)}")
    player_ids=player_ids[len(existing_files):len(existing_files)+50000]

    # حذف بازیکنان که فایل مربوطه‌شان در فولدر موجود است
    player_ids = [player_id for player_id in player_ids if player_id not in existing_files]
    print(f"Remaining Player IDs after removing existing files: {len(player_ids)}")

    print(len(player_ids))
    player_ids=player_ids[1000:10005]
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
