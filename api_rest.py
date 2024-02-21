import json
import os
import shutil
from pprint import pprint
from loguru import logger
import requests

from config import ready_path, main_path, sticker_path
from utils.created_pdf import one_pdf

headers = {'Content-Type': 'application/json'}
# domain = 'http://127.0.0.1:8000/api_rest'


domain = 'https://mycego.online/api_rest'


def get_products(categories: list):
    url = f'{domain}/products/'
    try:
        json_data = json.dumps(categories)
        response = requests.get(url, data=json_data, headers=headers)
        return response.json().get('data', [])
    except Exception as ex:
        logger.error(f'Ошибка в запросе по api {ex}')


def get_info_publish_folder(public_url, files):
    result_data = []
    res = requests.get(
        f'https://cloud-api.yandex.net/v1/disk/public/resources?public_key={public_url}&fields=_embedded&limit=1000')
    if res.status_code == 200:
        data = res.json().get('_embedded', {}).get('items', [])
        for i in data:
            file_name = i.get('name', None)
            if file_name in files:
                try:
                    result_data.append({
                        'name': i.get('name'),
                        'file': i.get('file')
                    })
                except:
                    pass
        return result_data


def create_download_data(item):
    download_files = []
    if len(item.get('images')) != item['quantity'] and not item['the_same']:
        logger.error(f'Не совпадает количество\n{item}')
    else:
        download_files.extend(item['images'])
        download_files.extend(item['sticker'])

        url_data = get_info_publish_folder(item['directory_url'], download_files)
        if url_data:
            item['url_data'] = url_data
            return item


def download_file(destination_path, url):
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(destination_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        file.write(chunk)
            # logger.info(f"File downloaded successfully: {destination_path}")
        else:
            logger.error(f"Error {response.status_code} while downloading file: {url}")
    except requests.RequestException as e:
        logger.error(f"Error during downloading file: {e}")


def copy_image(image_path, count):
    folder_art = os.path.dirname(image_path)
    exp = image_path.split('.')[-1]
    for i in range(count - 1):
        shutil.copy2(image_path, os.path.join(folder_art, f'{i + 2}.{exp}'))


def main_download_site(categories, dir_path):
    shutil.rmtree(main_path, ignore_errors=True)
    result_dict_arts = []

    art_list = [os.path.splitext(i)[0].upper() for i in os.listdir(dir_path)]
    print(art_list)
    data = get_products(categories)

    logger.debug(f'Артикулов в ответе с сайта:{len(data)}')
    data = [item for item in data if item['art'] not in art_list]
    logger.success(f'Артикулов для загрузки:{len(data)}')
    for item in data:
        download_data = create_download_data(item)
        if download_data:
            result_dict_arts.append(download_data)
    # with open('json.json', 'w') as f:
    #     json.dump(result_dict_arts, f, indent=4, ensure_ascii=False)
    #
    # with open('json.json', 'r') as f:
    #     data = json.load(f)
    count_task = len(result_dict_arts)

    for index, item in enumerate(result_dict_arts, start=1):
        art = item['art']
        count = item['quantity']
        folder = os.path.join(main_path, art)
        try:
            if int(art.split('-')[-1]) != count:
                logger.error(f'Не совпадает кол-во {art}')
        except Exception as ex:
            logger.error(f'Не понятный {art} {ex}')
        os.makedirs(folder, exist_ok=True)
        for i in item['url_data']:
            destination_path = os.path.join(folder, i['name'])
            download_file(destination_path, i['file'])

        if item['the_same']:
            try:
                image_path = os.path.join(folder, '1.png')
                if os.path.exists(image_path):
                    copy_image(image_path, count)
                else:
                    image_path = os.path.join(folder, '1.jpg')
                    if os.path.exists(image_path):
                        copy_image(image_path, count)
                    else:
                        raise ValueError(f'Нет файла для копирования артикул: {item["art"]}')
            except Exception as ex:
                logger.error(ex)

        try:
            for file in os.listdir(folder):
                if file.endswith('.pdf'):
                    try:
                        shutil.move(os.path.join(folder, file), sticker_path)
                    except Exception as ex:
                        logger.error(ex)
                        os.remove(os.path.join(folder, file))
            filename = f'{dir_path}\\{art}.pdf'
            logger.debug(filename)
            if not os.path.exists(filename) and os.path.isdir(folder):
                one_pdf(folder_path=folder, art=art)
            else:
                logger.error(f'файл существует {filename}')
            logger.success(f'{index}/{count_task} - {item["art"]}')
        except Exception as ex:
            logger.error(ex)


if __name__ == '__main__':
    main_download_site()
