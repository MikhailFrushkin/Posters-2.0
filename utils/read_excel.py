import os
from typing import List

import pandas as pd
from loguru import logger

from config import FilesOnPrint, ready_path


def read_excel_file(file: str) -> tuple[List[FilesOnPrint], list]:
    def search_file(filename, directory):
        for root, dirs, files in os.walk(directory):
            if filename.lower() in list(map(str.lower, files)):
                return os.path.join(root, filename)
        return None
    all_files = []
    df = pd.DataFrame()
    if file.endswith('.csv'):
        try:
            df = pd.read_csv(file, delimiter=';')
            mask = df['Артикул'].str.startswith('POSTER')

            # Фильтрация DataFrame с использованием маски
            df = df[mask]
            all_files = df['Артикул'].tolist()
            df = df.groupby('Артикул').agg({
                'Номер заказа': 'count',
            }).reset_index()

            df = df.rename(columns={'Номер заказа': 'Количество', 'Артикул': 'Артикул продавца'})
        except Exception as ex:
            logger.error(ex)
    else:
        try:
            df = pd.read_excel(file)
            all_files = df['Артикул продавца'].tolist()
            df = df.groupby('Артикул продавца').agg({
                'Стикер': 'count',
            }).reset_index()
            df = df.rename(columns={'Стикер': 'Количество'})
        except Exception as ex:
            logger.error(ex)
    files_on_print = []
    for index, row in df.iterrows():
        file_on_print = FilesOnPrint(art=row['Артикул продавца'], count=row['Количество'])
        files_on_print.append(file_on_print)

    for item in files_on_print:
        status = search_file(filename=f"{item.art}.pdf", directory=ready_path)
        if status:
            item.status = '✅'
    return files_on_print, all_files


if __name__ == '__main__':
    print(read_excel_file('../Заказы.xlsx'))
