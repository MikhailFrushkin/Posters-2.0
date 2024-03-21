import json
import os
from dataclasses import dataclass
from datetime import datetime

from loguru import logger
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from pathlib import Path

debug = False

logger.add(
    f"logs/check_bd_{datetime.now().date()}.log",
    rotation="200 MB",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {file!s} | {line} | {message}"
)

path_root = Path(__file__).resolve().parent

admin_name = 'Mikhail'
data = {}
try:
    with open('config.json', 'r', encoding='utf-8') as f:
        config_data = json.load(f)
except Exception as ex:
    logger.error(ex)

try:
    dbname = config_data['dbname']
    user = config_data['user']
    password = config_data['password']
    host = config_data['host']

    token = config_data['token']

    path_ready_posters_y_disc = config_data['path_ready_posters_y_disc']
    path_base_y_disc = config_data['path_base_y_disc']
    google_sticker_path = config_data['google_sticker_path']
    id_google_table = config_data['id_google_table']

    machine_name = config_data['machine_name']

    main_path = config_data['main_path']
    ready_path = config_data['ready_path']
    ready_path_kruzhka = config_data['ready_path_kruzhka']
    sticker_path = config_data['sticker_path']
    acrobat_path = config_data['acrobat_path']
    list_dirs = [main_path, ready_path, ready_path_kruzhka, sticker_path, 'files']

    for directory in list_dirs:
        os.makedirs(directory, exist_ok=True)

except Exception as ex:
    logger.error(f'Ошибка чтения файла настроек: {ex}')


class ProgressBar:
    def __init__(self, total, progress_bar, current=0):
        self.current = current
        self.total = total
        self.progress_bar = progress_bar

    def update_progress(self):
        self.current += 1
        self.progress_bar.update_progress(self.current, self.total)

    def __str__(self):
        return str(self.current)


class SearchProgress:
    def __init__(self, total_folders, progress_bar, current_folder=0):
        self.current_folder = current_folder
        self.total_folders = total_folders
        self.progress_bar = progress_bar

    def update_progress(self):
        self.current_folder += 1
        self.progress_bar.update_progress(self.current_folder, self.total_folders)

    def __str__(self):
        return str(self.current_folder)


@dataclass
class FilesOnPrint:
    art: str
    count: int
    file_path: list
    status: str = '❌'
    type: str = 'Постер'


def df_in_xlsx(df, filename, directory='files', max_width=50):
    workbook = Workbook()
    sheet = workbook.active
    for row in dataframe_to_rows(df, index=False, header=True):
        sheet.append(row)
    for column in sheet.columns:
        column_letter = column[0].column_letter
        max_length = max(len(str(cell.value)) for cell in column)
        adjusted_width = min(max_length + 2, max_width)
        sheet.column_dimensions[column_letter].width = adjusted_width

    os.makedirs(directory, exist_ok=True)
    workbook.save(f"{directory}\\{filename}.xlsx")
