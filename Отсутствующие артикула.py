import pandas as pd
import os

from config import ready_path


def main():
    df = pd.read_excel('постеры.xlsx')
    print(df.columns)
    arts_list_in_file = df['Артикул продавца'].apply(lambda x: x.lower().strip()).tolist()
    # print(arts_list_in_file)

    arts_list_in_folder = [i.lower().replace('.pdf', '') for i in os.listdir(ready_path)]
    # print(arts_list_in_folder)

    result = set(arts_list_in_file) - set(arts_list_in_folder)
    print(result)
    print(len(result))

    directory = r'D:\Новая база\Скачанные файлы'
    for i in result:
        os.makedirs(os.path.join(directory, i.upper()), exist_ok=True)


if __name__ == '__main__':
    main()