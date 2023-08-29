import os

import pandas as pd
import requests
from PyQt5.QtWidgets import QMessageBox
from loguru import logger

from config import token, main_path, SearchProgress

count_glob = 0


def count_files_posters(folder: str, count_all_files, self) -> tuple:
    global count_glob
    exclude_keywords = ['размер', 'титул', 'мокап', 'moc', 'mos', 'рекомен', 'реки', 'подлож', 'шаблон']
    count = 0
    good_list_files = []
    for filename in os.listdir(folder):
        file_extension = os.path.splitext(filename)[1]
        if file_extension.lower() in ['.png', '.jpg'] and not any(
                keyword in filename.lower() for keyword in exclude_keywords):
            count += 1
            good_list_files.append(filename)

    count_glob += 1
    if count_all_files and count_all_files != count:
        logger.error(f"{folder} не соответствует количество файлов {count}/{count_all_files}")
        if self:
            QMessageBox.warning(self, 'Ошибка',
                                f"{folder} не соответствует количество файлов {count}/{count_all_files}")
    return tuple((folder, good_list_files), )


def rename_files(file_data):
    folder_path, file_list = file_data

    for index, filename in enumerate(file_list, start=1):
        file_extension = os.path.splitext(filename)[1]
        new_filename = f"{index}{file_extension}"
        old_filepath = os.path.join(folder_path, filename)
        new_filepath = os.path.join(folder_path, new_filename)
        os.rename(old_filepath, new_filepath)


def count_objects_in_folders(directory, self=None):
    folder_info = []

    for root, dirs, files in os.walk(directory):
        num_objects = len(dirs) + len(files)
        folder_info.append((root, num_objects, root.split('\\')[-1]))

    sorted_folder_info = sorted(folder_info, key=lambda x: x[1], reverse=True)

    for folder, num_objects, papka in sorted_folder_info:
        count = None
        papka = papka.split('-')[-2:]
        for element in papka:
            value = element[-2:]
            if value.endswith('3'):
                count = 3
            elif value.endswith('6'):
                count = 6
            elif value.endswith('10'):
                count = 10
        try:
            rename_files(count_files_posters(folder, count, self))
        except Exception as ex:
            logger.error(ex)


def dowloads_files(df_new, self=None):
    def download_files(source_folder, target_folder, token):
        url = 'https://cloud-api.yandex.net/v1/disk/resources'
        headers = {'Authorization': 'OAuth ' + token}
        params = {
            'path': target_folder[5:],
            'limit': 1000,  # Максимальное количество элементов в одном запросе
        }
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            resource_data = response.json()
            type_list = [item['type'] for item in resource_data['_embedded']['items']]
            print(type_list)
            for item in resource_data['_embedded']['items']:
                if item['type'] == 'file':
                    download_url = item['file']
                    download_response = requests.get(download_url)
                    if download_response.status_code == 200:
                        if item['name'][0].isdigit() and (os.path.splitext(item['name'])[1] == '.png' or
                                                          os.path.splitext(item['name'])[1] == '.jpg') and (
                                'блюр' not in item['name']):
                            file_path = os.path.join(source_folder, item['name'])
                            with open(file_path, 'wb') as file:
                                file.write(download_response.content)
                                logger.success(f'Загружен файл {item["name"]}')

                elif item['type'] == 'dir' and len(item['name']) < 6:
                    logger.info(f"Переход в папку {item['path']}")
                    download_files(source_folder, target_folder + "/" + item['name'], token)

    df = pd.read_excel(df_new)
    logger.debug(f'Количество новых артикулов для загрузки: {len(df)}')
    if self:
        progress = SearchProgress(len(df), self)
    # Итерация по строкам и передача значений в функцию
    for index, row in df.iterrows():
        vpr = row['Артикул']
        target_folder = row['Путь']
        if not os.path.exists(os.path.join(main_path, vpr)):
            os.makedirs(os.path.join(main_path, vpr))
            logger.debug(f"Скачивание артикула {vpr}")
            try:
                download_files(os.path.join(main_path, vpr), target_folder, token)
                if self:
                    progress.update_progress()
            except Exception as ex:
                logger.error(f'Ошибка загрузки {os.path.join(main_path, vpr)}|{target_folder}|{ex}')
            try:
                count_objects_in_folders(os.path.join(main_path, vpr))
            except Exception as ex:
                logger.error(f'Ошибка переименовывания папки {os.path.join(main_path, vpr)} {ex}')
        else:
            logger.debug(f'Папка существует {os.path.join(main_path, vpr)}')
