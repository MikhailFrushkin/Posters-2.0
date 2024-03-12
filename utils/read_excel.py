import os
from typing import List

import pandas as pd
from loguru import logger

from config import FilesOnPrint, ready_path, ready_path_kruzhka, debug


def read_excel_file(file: str) -> tuple:
    # def search_file(art, directory):
    #     for filename in os.listdir(directory):
    #         base_name, extension = os.path.splitext(filename)
    #         if base_name.strip().lower() == art.strip().lower():
    #             return os.path.abspath(os.path.join(directory, filename))
    #     return None
    def search_file(art, directory):
        files = []
        for filename in os.listdir(directory):
            if art.strip().lower() in filename.lower():
                files.append(os.path.abspath(os.path.join(directory, filename)))
                if '2' not in filename:
                    break
        return files

    def set_dataclass(name, count):
        name = name.strip()
        if 'KRUZHKA' in name:
            type_art = 'Кружка'
            directory = ready_path_kruzhka
        else:
            type_art = 'Постер'
            directory = ready_path
        files = search_file(art=name, directory=directory)
        return FilesOnPrint(art=name, count=count,
                            status='✅' if files else '❌',
                            type=type_art,
                            file_path=files)

    df = pd.DataFrame()
    art_list = []
    if file.endswith('.csv'):
        try:
            df = pd.read_csv(file, delimiter=';')
            mask = df['Артикул'].str.startswith('POSTER')
            df = df[mask]
            for index, row in df.iterrows():
                art_list.append(row['Артикул продавца'])
            df = df.groupby('Артикул').agg({
                'Номер заказа': 'count',
            }).reset_index()

            df = df.rename(columns={'Номер заказа': 'Количество', 'Артикул': 'Артикул продавца'})
        except Exception as ex:
            logger.error(ex)
    else:
        try:
            df = pd.read_excel(file)
            columns_list = list(map(str.lower, df.columns))
            if len(columns_list) == 2:
                try:
                    df = df.rename(columns={df.columns[0]: 'Артикул продавца', df.columns[1]: 'Количество'})
                    for index, row in df.iterrows():
                        for _ in range(int(row['Количество'])):
                            art_list.append(row['Артикул продавца'])
                except Exception as ex:
                    logger.error(ex)
            else:
                for index, row in df.iterrows():
                    art_list.append(row['Артикул продавца'])
                df = df.groupby('Артикул продавца', sort=False).agg({
                    'Стикер': 'count',
                }).reset_index()
                df = df.rename(columns={'Стикер': 'Количество'})

        except Exception as ex:
            logger.error(ex)
    files_on_print = []
    files_on_print_single = []

    for index, row in df.iterrows():
        name = row['Артикул продавца']
        count = row['Количество']
        files_on_print.append(set_dataclass(name, count))
    for i in art_list:
        files_on_print_single.append(set_dataclass(i, 1))

    for i in files_on_print_single:
        i.art = i.art.strip()
    if debug:
        logger.debug(files_on_print)
        logger.debug(files_on_print_single)
    return files_on_print, files_on_print_single


if __name__ == '__main__':
    print(read_excel_file('../Заказы2.xlsx'))
