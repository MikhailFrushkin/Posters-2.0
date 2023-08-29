import asyncio
import os
import re
from urllib.parse import quote
import httplib2
import apiclient
from oauth2client.service_account import ServiceAccountCredentials
import aiohttp
import pandas as pd
from loguru import logger

from config import token, df_in_xlsx, path_base_y_disc, ready_path, id_google_table, path_root


def read_codes_on_google(CREDENTIALS_FILE='google_acc.json'):
    os.makedirs('files', exist_ok=True)
    logger.debug('Читаю гугл таблицу')
    spreadsheet_id = f'{id_google_table}'
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            CREDENTIALS_FILE,
            ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive'])
        httpAuth = credentials.authorize(httplib2.Http())
        service = apiclient.discovery.build('sheets', 'v4', http=httpAuth, static_discovery=False)
        values = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range='A1:O30000',

        ).execute()
    except Exception as ex:
        logger.error(f'Ошибка чтения гуглтаблицы {ex}')
    data = values.get('values', [])
    headers = data[0]
    for row in data:
        missing_columns = len(headers) - len(row)
        if missing_columns > 0:
            row += [''] * missing_columns

    headers = data[0]  # Заголовки столбцов из первого элемента списка значений
    rows = data[1:]
    # Проверка количества столбцов и создание DataFrame
    lines_list = []
    if len(headers) != len(rows[0]):
        print("Ошибка: количество столбцов не совпадает с количеством значений.")
    else:
        df = pd.DataFrame(rows, columns=headers)
        df_in_xlsx(df, 'files\\Таблица гугл')

    list_art = []
    df = pd.read_excel('files\\Таблица гугл.xlsx', usecols=['Наименование', 'Артикул на ВБ'],
                       dtype=str)
    df = df[~df['Артикул на ВБ'].isna() &
            df['Наименование'].apply(lambda x: isinstance(x, str) and not x.startswith('https'))
            ]
    df['Артикул на ВБ'] = df['Артикул на ВБ'].apply(lambda x: re.sub(r'\s+', ' ', x))
    df['Артикул на ВБ'] = df['Артикул на ВБ'].apply(lambda x: x.replace(',', ' '))
    df['Артикул на ВБ'] = df['Артикул на ВБ'].apply(lambda x: list_art.extend(x.split()))
    list_art = [i for i in list_art if len(i) > 0 and '-' in i]
    logger.info(f'Найденно артикулов: {len(list_art)}')

    return list_art


async def traverse_yandex_disk(session, folder_path, target_folders, result_dict, progress=None):
    url = f"https://cloud-api.yandex.net/v1/disk/resources?path={quote(folder_path)}&limit=1000"
    headers = {"Authorization": f"OAuth {token}"}
    try:
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            tasks = []
            for item in data["_embedded"]["items"]:
                if item["type"] == "dir":
                    result_dict[item["name"].lower()] = item["path"]
                    task = traverse_yandex_disk(session, item["path"], target_folders, result_dict, progress)
                    tasks.append(task)
            if tasks:
                await asyncio.gather(*tasks)
    except Exception as ex:
        logger.debug(f'Ошибка при поиске папки {folder_path} {ex}')


async def main_search(self=None):
    target_folders = list(map(lambda x: x.lower(), read_codes_on_google()))
    logger.debug('Сканирую Яндекс диск...')
    if target_folders:
        folder_path = path_base_y_disc
        result_dict = {}
        async with aiohttp.ClientSession() as session:
            await traverse_yandex_disk(session, folder_path, target_folders, result_dict)

        df = pd.DataFrame(list(result_dict.items()), columns=['Артикул', 'Путь'])
        logger.info('Создан документ Пути к артикулам.xlsx')
        df_in_xlsx(df, 'files\\Пути к артикулам')

        def get_all_folder_names(directory):
            files_names = []
            for root, dirs, files in os.walk(directory):
                for file in files:
                    files_name = file
                    files_names.append(files_name.lower().replace('.pdf', ''))
            return files_names

        all_folder_names = get_all_folder_names(ready_path)
        df = df[df['Артикул'].isin(target_folders) & ~df['Артикул'].isin(all_folder_names)]
        df_in_xlsx(df, 'files\\Разница артикулов с гугл.таблицы и на я.диске')
        return True


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main_search())
