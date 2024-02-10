import pandas as pd
import os

from config import ready_path, df_in_xlsx


def main():
    df = pd.read_excel('постеры.xlsx')
    print(len(df))
    arts_list_in_file = df['Артикул продавца'].apply(lambda x: x.lower().strip()).tolist()

    arts_list_in_folder = [i.lower().replace('.pdf', '') for i in os.listdir(ready_path)]

    result = set(arts_list_in_file) - set(arts_list_in_folder)
    print(result)
    print(len(result))
    df = pd.DataFrame(list(map(str.upper, result)), columns=['Артикул продавца'])
    df_in_xlsx(df, 'Не найденные артикула')
    directory = r'D:\Новая база\Скачанные файлы'
    for i in result:
        os.makedirs(os.path.join(directory, i.upper()), exist_ok=True)


if __name__ == '__main__':
    main()