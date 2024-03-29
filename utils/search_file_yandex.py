import asyncio
import os
import re
from pprint import pprint
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
            range='Надо сделать',

        ).execute()
    except Exception as ex:
        logger.error(f'Ошибка чтения гуглтаблицы {ex}')
    data = values.get('values', [])
    headers = data[0]  # Заголовки столбцов из первого элемента списка значений
    headers.append(' ')
    rows = data[1:]
    # Проверка количества столбцов и создание DataFrame
    if len(headers) != len(rows[0]):
        logger.error("Ошибка: количество столбцов не совпадает с количеством значений.")
    else:
        df = pd.DataFrame(rows, columns=headers)
        df_in_xlsx(df, 'Таблица гугл')

    list_art = []

    df = df[~df['Артикул на ВБ'].isna() &
            df['Наименование'].apply(lambda x: isinstance(x, str) and not x.startswith('https'))
            ]
    df['Артикул на ВБ'] = df['Артикул на ВБ'].apply(lambda x: re.sub(r'\s+', ' ', x))
    df['Артикул на ВБ'] = df['Артикул на ВБ'].apply(lambda x: x.replace(',', ' '))
    df['Артикул на ВБ'] = df['Артикул на ВБ'].apply(lambda x: list_art.extend(x.split()))
    list_art = [i for i in list_art if len(i) > 0 and '-' in i]
    logger.info(f'Найденно артикулов: {len(list_art)}')

    return list_art


async def traverse_yandex_disk(session, folder_path, result_dict, progress=None):
    url = f"https://cloud-api.yandex.net/v1/disk/resources?path={quote(folder_path)}&limit=1000"
    headers = {"Authorization": f"OAuth {token}"}
    try:
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            tasks = []
            for item in data["_embedded"]["items"]:
                if item["type"] == "dir":
                    if '-' in item['name']:
                        result_dict[item["name"].lower()] = item["path"]
                    task = traverse_yandex_disk(session, item["path"], result_dict, progress)
                    tasks.append(task)
            if tasks:
                await asyncio.gather(*tasks)
    except Exception as ex:
        logger.debug(f'Ошибка при поиске папки {folder_path} {ex}')


async def main_search(self=None):
    english_letters_and_digits = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_@!#$%^&*(){}[]'''

    # target_folders = list(map(lambda x: x.lower(), read_codes_on_google()))
    folder_path = path_base_y_disc
    result_dict = {}
    async with aiohttp.ClientSession() as session:
        await traverse_yandex_disk(session, folder_path, result_dict)
    df = pd.DataFrame(list(result_dict.items()), columns=['Артикул', 'Путь'])
    logger.info('Создан документ Пути к артикулам.xlsx')
    df_in_xlsx(df, 'Пути к артикулам')

    def get_all_folder_names(directory):
        files_names = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                files_name = file
                files_names.append(files_name.lower().replace('.pdf', ''))
        return files_names

    all_folder_names = get_all_folder_names(ready_path)

    def contains_only_english_letters_and_digits(s):
        return all(char in english_letters_and_digits for char in s) and s.count('-') > 1 and s not in all_folder_names

    df = df[df['Артикул'].apply(contains_only_english_letters_and_digits)]
    df_in_xlsx(df, 'Разница артикулов с гугл.таблицы и на я.диске')
    return True


async def async_main():
    loop = asyncio.get_event_loop()
    await main_search()


if __name__ == '__main__':
    asyncio.run(async_main())
