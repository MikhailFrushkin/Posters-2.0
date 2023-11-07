import asyncio
import os
from urllib.parse import quote

import aiofiles
import aiohttp
from loguru import logger

from config import token, ready_path, path_ready_posters_y_disc

semaphore = asyncio.Semaphore(2)


async def traverse_yandex_disk(session, folder_path, result_dict, offset=0):
    limit = 1000
    url = f"https://cloud-api.yandex.net/v1/disk/resources?path={quote(folder_path)}&limit={limit}"
    headers = {"Authorization": f"OAuth {token}"}
    try:
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            tasks = []
            for item in data["_embedded"]["items"]:
                if item["type"] == "file" and item["name"].endswith(".pdf"):
                    if not os.path.exists(os.path.join(ready_path, item["name"])):
                        print(item["name"])
                        result_dict[item["name"].lower()] = item["path"]
                elif item["type"] == "dir":
                    task = traverse_yandex_disk(session, item["path"], result_dict)
                    tasks.append(task)
            if tasks:
                await asyncio.gather(*tasks)

            # Проверяем, есть ли ещё элементы для сканирования
            total = data["_embedded"]["total"]
            offset += limit
            if offset < total:
                # Рекурсивно вызываем функцию для следующей порции элементов
                await traverse_yandex_disk(session, folder_path, result_dict, offset)

    except Exception as ex:
        logger.error(f'Ошибка при поиске папки {folder_path} {ex}')


async def main_search():
    result_dict = {}
    async with aiohttp.ClientSession() as session:
        await traverse_yandex_disk(session, path_ready_posters_y_disc, result_dict)
    return result_dict


async def get_download_link(session, token, file_path):
    headers = {"Authorization": f"OAuth {token}"}
    url = "https://cloud-api.yandex.net/v1/disk/resources/download"
    params = {"path": file_path}

    try:
        async with session.get(url, headers=headers, params=params) as response:
            if response.status == 200:
                data = await response.json()
                return data["href"]
    except asyncio.TimeoutError:
        logger.error(f"Время ожидания ответа от сервера истекло для файла '{file_path}'.")


async def download_file(session, url, filename):
    headers = {'Authorization': f'OAuth {token}'}

    async with semaphore:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    full_path = os.path.join(ready_path, filename)
                    print(f'Загрузка {filename}')
                    async with aiofiles.open(full_path, 'wb') as f:
                        while True:
                            chunk = await response.content.read(1024)
                            if not chunk:
                                break
                            await f.write(chunk)
        except Exception as e:
            logger.error(f"Error downloading {filename}: {e}")


async def main_ready_posters(result_dict):
    os.makedirs(ready_path, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        tasks = []
        batch_size = 10
        current_batch = []

        for filename, yandex_disk_path in result_dict.items():
            if not filename in os.listdir(ready_path):

                download_link = await get_download_link(session, token, yandex_disk_path)
                if download_link:
                    current_batch.append((download_link, filename))

                    if len(current_batch) >= batch_size:
                        download_tasks = [download_file(session, link, name) for link, name in current_batch]
                        tasks.extend(download_tasks)
                        current_batch = []

        # Завершите оставшиеся задачи для скачивания
        download_tasks = [download_file(session, link, name) for link, name in current_batch]
        tasks.extend(download_tasks)

        # Запустите задачи для скачивания файлов
        await asyncio.gather(*tasks)


async def async_main_ready_posters():
    result_dict = await main_search()
    await main_ready_posters(result_dict)
    return result_dict


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    asyncio.run(async_main_ready_posters())
    # asyncio.run(main_sh())
