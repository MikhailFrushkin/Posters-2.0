import os
from typing import List

import pandas as pd
from loguru import logger

from config import FilesOnPrint, ready_path


def read_excel_file(file: str) -> list:
    def search_file(filename, directory):
        for root, dirs, files in os.walk(directory):
            if filename.lower() in list(map(str.lower, files)):
                return os.path.join(root, filename)
        return None
    df = pd.DataFrame()
    if file.endswith('.csv'):
        try:
            df = pd.read_csv(file, delimiter=';')
            mask = df['Артикул'].str.startswith('POSTER')
            df = df[mask]
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
                except Exception as ex:
                    logger.error(ex)
            else:
                df = df.groupby('Артикул продавца', sort=False).agg({
                    'Стикер': 'count',
                }).reset_index()
                df = df.rename(columns={'Стикер': 'Количество'})

        except Exception as ex:
            logger.error(ex)
    files_on_print = []
    for index, row in df.iterrows():
        art = row['Артикул продавца'].strip()
        status = search_file(filename=f"{art}.pdf", directory=ready_path)
        files_on_print.append(FilesOnPrint(art=art, count=row['Количество'], status=FilesOnPrint.set_status(status)))
    return files_on_print


if __name__ == '__main__':
    print(read_excel_file('../Заказы2.xlsx'))
