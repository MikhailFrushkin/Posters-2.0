import asyncio
import json
import os
import shutil
import time
from pprint import pprint
import pandas as pd
import aiohttp
import requests
from PIL import Image
from loguru import logger

from config import df_in_xlsx

token = 'y0_AgAAAABWiJ78AAnrzgAAAADjYERnWFIrtAnJTZmrrJ6nf5LPjn6zg6A'
base_folder = 'Новая база (1)/Старая база(производство)'

url_upload = 'https://cloud-api.yandex.net/v1/disk/resources'
url_publish = 'https://cloud-api.yandex.net/v1/disk/resources/publish'
headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': f'OAuth {token}'}


def main():
    good_folder = []
    for root, dirs, files in os.walk(directory):
        if files and 'Принт.png' in files and any([i for i in files if i.endswith('.pdf') and i != 'Макет.pdf']):
            good_folder.append(os.path.abspath(root))

    files_for_copy = {}
    print(len(good_folder))
    for index, folder in enumerate(good_folder, start=1):
        file_list = []
        file_list_ham = []

        for i in os.listdir(folder):
            path_file = os.path.join(folder, i)
            if os.path.isfile(path_file) and not i.endswith('.pdf') and 'принт' not in i.lower():
                if 'кружка-ха' in i.lower():
                    file_list_ham.append(i)
                else:
                    file_list.append(i)

        for i in os.listdir(folder):
            path_file = os.path.join(folder, i)
            if os.path.isfile(path_file) and 'макет' in i.lower() or 'принт' in i.lower():
                file_list.append(i)
                file_list_ham.append(i)

        for i in os.listdir(folder):
            path_file = os.path.join(folder, i)
            if os.path.isfile(path_file) and i.endswith('.pdf') and 'макет' not in i.lower():
                if '-hamel' in i.lower():
                    file_list_ham.append(i)
                    files_for_copy[(folder, i.replace('.pdf', ''))] = file_list_ham
                else:
                    file_list.append(i)
                    files_for_copy[(folder, i.replace('.pdf', ''))] = file_list

    count = 1
    for (folder, art), files in files_for_copy.items():
        print(count, folder, art, files)
        try:
            new_folder_path = os.path.join(out_directory, art)
            if not os.path.exists(new_folder_path):
                os.makedirs(new_folder_path, exist_ok=True)
                for i in files:
                    shutil.copy2(os.path.join(folder, i), new_folder_path)
        except Exception as ex:
            logger.error(ex)
        count += 1


def create_thumbnail(input_path, output_path, thumbnail_size=(140, 180)):
    """Создание превью"""
    try:
        with Image.open(input_path) as img:
            thumbnail = img.resize(thumbnail_size)
            thumbnail.save(output_path)
            return output_path
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")


async def fetch(session, semaphore, url):
    async with semaphore:
        async with session.get(url) as response:
            return await response.json()


async def get_info_publish_folder(semaphore, public_url, art):
    result_data = {}
    stickers = []
    images = []
    skin = []
    other = []
    all_files = []

    async with aiohttp.ClientSession() as session:
        async with session.get(
                f'https://cloud-api.yandex.net/v1/disk/public/resources?public_key={public_url}&fields=_embedded&limit=1000') as res:
            if res.status == 200:
                data = await res.json()
                items = data.get('_embedded', {}).get('items', [])

                for i in items:
                    file_name = i.get('name', None)
                    if file_name.endswith(
                            '.pdf') and 'изображения' not in file_name.lower() and 'макет' not in file_name.lower():
                        stickers.append(file_name)
                    elif '1' in file_name.lower() and 'кружка' in file_name.lower():
                        skin.append(file_name)
                    elif 'макет' in file_name.lower() and file_name.endswith('.pdf'):
                        all_files.append(file_name)
                    elif (file_name.endswith('.png') or file_name.endswith('.jpg')) and 'принт' in file_name.lower():
                        images.append(file_name)
                    else:
                        other.append(file_name)

                result_data[art] = {
                    'stickers': stickers,
                    'images': images,
                    'skin': skin,
                    'all_files': all_files,
                    'other': other,
                    'public_url': public_url,
                }
                return result_data
            else:
                logger.error(res.status)
                logger.error(await res.text())


async def process_chunk(semaphore, chunk):
    tasks = []
    for i in chunk:
        try:
            task = get_info_publish_folder(semaphore, i[1], i[0])
            tasks.append(task)
        except Exception as ex:
            logger.error(i)
            logger.error(ex)
    return await asyncio.gather(*tasks)


async def main_async():
    semaphore = asyncio.Semaphore(10)
    data = []
    with open(f'кружки урл.json', 'r', encoding='utf-8') as file:
        data_load = json.load(file)
    for key, value in data_load.items():
        data.append((key, value))
    all_data = {}
    len_data = len(data)
    chunk_size = 100
    pause_duration = 3

    for chunk_start in range(0, len_data, chunk_size):
        chunk_end = min(chunk_start + chunk_size, len_data)
        chunk = data[chunk_start:chunk_end]

        print(f'Processing chunk {chunk_start + 1}-{chunk_end} / {len_data}')
        results = await process_chunk(semaphore, chunk)

        for result in results:
            all_data.update(result)

        time.sleep(pause_duration)

    with open(f'scan.json', 'w', encoding='utf-8') as file:
        json.dump(all_data, file, indent=4, ensure_ascii=False)


def get_info(path):
    """Получение информации о файлах в папке на Яндекс.Диске.
    path: Путь к папке."""

    limit = 1000  # Максимальное количество элементов на странице
    offset = 0  # Начальное смещение
    all_files = []  # Список для хранения всех файлов
    data_dir = {}
    while True:
        url = f'https://cloud-api.yandex.net/v1/disk/resources?path={path}&limit={limit}&offset={offset}'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for i in data.get('_embedded').get('items', []):
                public_url = i.get('public_url', None)
                data_dir[i.get('name')] = public_url

            # Проверяем, есть ли еще файлы
            if len(data.get('_embedded').get('items', [])) < limit:
                break  # Если файлов меньше, чем limit, это последняя страница

            offset += limit  # Увеличиваем смещение для следующего запроса
            print(offset)
        else:
            logger.error(f"Error {response.status_code}: {response.text} \n{path}")
            return None
    return data_dir


if __name__ == '__main__':
    directory = r'C:\Кружки'
    out_directory = 'D:\База кружек для загрузки'
    # main()
    # for root, dirs, files in os.walk(out_directory):
    #     for i in files:
    #         file_name = i.lower().strip()
    #         if '1' in file_name and 'кружка' in file_name and not file_name.endswith('.pdf'):
    #             new_file_name = os.path.basename(root)
    #             create_thumbnail(input_path=os.path.join(root, i),
    #                              output_path=fr'D:\Превью кружек\{new_file_name}.png')
    #             break
    #     else:
    #         logger.error(root)

    # data = get_info(f'{base_folder}/Кружки')
    # with open(f'кружки урл.json', 'w', encoding='utf-8') as file:
    #     json.dump(data, file, indent=4, ensure_ascii=False)

    # asyncio.run(main_async())

    with open(f'scan.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
    df = pd.DataFrame(columns=[
        'art', 'category', 'brand', 'quantity', 'size', 'directory_url', 'preview', 'json_data_images',
        'json_data_skin', 'json_data_sticker'
    ])
    for index, (key, value) in enumerate(data.items(), start=1):
        df.at[index, 'art'] = key
        df.at[index, 'category'] = 'Кружки'
        df.at[index, 'brand'] = 'AniKoya'
        df.at[index, 'quantity'] = 1
        df.at[index, 'size'] = 'Кружка-хамелеон' if '-hamel' in key.lower() else 'Кружка'
        df.at[index, 'directory_url'] = value.get('public_url', None)
        df.at[index, 'preview'] = rf'\skins\old kruzhka\{key}.png'
        df.at[index, 'json_data_images'] = json.dumps([[i, None] for i in value.get('images', [])])
        df.at[index, 'json_data_skin'] = json.dumps([[i, None] for i in value.get('skin', [])])
        df.at[index, 'json_data_sticker'] = json.dumps([[i, None] for i in value.get('stickers', [])])
    df_in_xlsx(df, 'Кружки')

