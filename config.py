import os
from dataclasses import dataclass
from pathlib import Path
from loguru import logger
from environs import Env
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

path_root = Path(__file__).resolve().parent
env = Env()
env.read_env()

machine_name = env.str('machine_name')
dbname = env.str('dbname')
user = env.str('user')
password = env.str('password')
host = env.str('host')

token = env.str('token')
main_path = env.str('main_path')
ready_path = env.str('ready_path')
sticker_path = env.str('sticker_path')
path_ready_posters_y_disc = env.str('path_ready_posters_y_disc')
path_base_y_disc = env.str('path_base_y_disc')
acrobat_path = env.str('acrobat_path')
google_sticker_path = env.str('google_sticker_path')
id_google_table = env.str('id_google_table')

list_dirs = [main_path, ready_path, sticker_path, 'files']
admin_name = 'Ноут'

for directory in list_dirs:
    os.makedirs(directory, exist_ok=True)


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
    status: str = '❌'

    @staticmethod
    def set_status(status):
        return '✅' if status else '❌'


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
