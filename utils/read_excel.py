import os
from typing import List

import pandas as pd
from config import FilesOnPrint, ready_path


def read_excel_file(file: str) -> List[FilesOnPrint]:
    def search_file(filename, directory):
        for root, dirs, files in os.walk(directory):
            if filename.lower() in list(map(str.lower, files)):
                return os.path.join(root, filename)
        return None

    df = pd.read_excel(file)
    df = df.groupby('Артикул продавца').agg({
        'Стикер': 'count',
    }).reset_index()
    df = df.rename(columns={'Стикер': 'Количество'})

    files_on_print = []
    for index, row in df.iterrows():
        file_on_print = FilesOnPrint(art=row['Артикул продавца'], count=row['Количество'])
        files_on_print.append(file_on_print)

    for item in files_on_print:
        status = search_file(filename=f"{item.art}.pdf", directory=ready_path)
        if status:
            item.status = '✅'

    return files_on_print


if __name__ == '__main__':
    print(read_excel_file('../Заказы.xlsx'))
