import asyncio
import os
import time

import aiofiles
import aiohttp
from loguru import logger

from config import token, path_ready_posters_y_disc, ready_path

semaphore = asyncio.Semaphore(3)
headers = {"Authorization": f"OAuth {token}"}


async def get_download_link(session, file_path):
    url = "https://cloud-api.yandex.net/v1/disk/resources/download"
    params = {"path": file_path}

    try:
        async with session.get(url, headers=headers, params=params) as response:
            if response.status == 200:
                data = await response.json()
                return data["href"]
            else:
                logger.error(f"Не удалось получить ссылку для скачивания файла '{file_path}'. Код ошибки:",
                             response.status)
                return None
    except asyncio.TimeoutError:
        logger.error(f"Время ожидания ответа от сервера истекло для файла '{file_path}'.")
        time.sleep(20)
        return None


async def get_yandex_disk_files(session, folder_path):
    url = "https://cloud-api.yandex.net/v1/disk/resources"
    files_on_yandex_disk = []

    stack = [(folder_path, folder_path)]
    while stack:
        current_folder_path, base_folder_path = stack.pop()
        params = {"path": current_folder_path, "limit": 1000}
        while True:
            async with session.get(url, headers=headers, params=params, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    files = data['_embedded']["items"]
                    if files:
                        for item in files:
                            if item["type"] == "dir":
                                stack.append((item["path"], current_folder_path))
                            elif item["type"] == "file":
                                file_name = item["name"]
                                file_path = item["path"]
                                if not os.path.exists(os.path.join(ready_path, file_name)):
                                    if file_name.split('.')[-1] in ['pdf']:
                                        files_on_yandex_disk.append((file_name, file_path))
                        # Проверяем, есть ли еще файлы для получения
                        if "offset" in data['_embedded']:
                            params["offset"] = data['_embedded']["offset"] + data['_embedded']["limit"]
                        else:
                            break
                    else:
                        break

    logger.success(f'Найденно новых файлов: {len(files_on_yandex_disk)}')

    return files_on_yandex_disk


async def download_file(session, file_name, file_path, local_folder_path, progress=None):
    local_filepath = os.path.join(local_folder_path, file_name)
    if os.path.exists(local_filepath):
        return

    download_link = await get_download_link(session, file_path)

    if download_link:
        async with semaphore:  # Используйте семафор для ограничения параллельных загрузок
            async with session.get(download_link) as response:
                if response.status == 200:
                    async with aiofiles.open(local_filepath, "wb") as f:
                        while True:
                            chunk = await response.content.read(8192)
                            if not chunk:
                                break
                            await f.write(chunk)
                    logger.success(f"Загружен файл '{file_name}'")
                else:
                    logger.error(f"Не удалось загрузить файл '{file_name}'. Код ошибки:", response.status)
    else:
        logger.error(f"Не удалось получить ссылку для скачивания файла '{file_name}'.")


async def download_files_from_yandex_disk(files_to_download, local_folder_path=".", progress=None):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
        tasks = [download_file(session, file_name, file_path, local_folder_path, progress) for
                 file_name, file_path in
                 files_to_download]

        await asyncio.gather(*tasks)


async def scan_files(self=None):
    try:
        async with aiohttp.ClientSession() as session:
            files_to_download = await get_yandex_disk_files(session, path_ready_posters_y_disc)
            try:
                os.makedirs(ready_path, exist_ok=True)
                await download_files_from_yandex_disk(files_to_download, ready_path)
            except Exception as ex:
                logger.error(f'Ошибка  {ex}')

    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")


if __name__ == "__main__":
    asyncio.run(scan_files())
