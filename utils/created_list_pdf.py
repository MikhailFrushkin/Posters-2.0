import os
import shutil
import time

import PyPDF2
import pandas as pd
from PyQt5.QtWidgets import QMessageBox
from loguru import logger

from config import ready_path, ProgressBar, sticker_path, df_in_xlsx, machine_name
from db import orders_base_postgresql


def find_files_in_directory(directory, file_list):
    found_files = []
    not_found_files = file_list[:]
    for poster in file_list:
        for file in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, file)):
                file_name = '.'.join(file.split('.')[:-1]).lower()
                if poster.lower() == file_name:
                    found_files.append(os.path.join(directory, file))
                    not_found_files.remove(poster)
                    break

    return found_files, not_found_files


def merge_pdfs_stickers(arts_paths, output_path):
    pdf_writer = PyPDF2.PdfWriter()
    for index, input_path in enumerate(arts_paths, start=1):
        try:
            with open(input_path, 'rb') as pdf_file:
                pdf_reader = PyPDF2.PdfReader(pdf_file)

                # Add all pages from PdfReader to PdfWriter
                for page in pdf_reader.pages:
                    pdf_writer.add_page(page)
        except Exception:
            pass
    current_output_path = f"{output_path}.pdf"
    with open(current_output_path, 'wb') as output_file:
        pdf_writer.write(output_file)
    PyPDF2.PdfWriter()


def split_list(lst, num_parts):
    avg = len(lst) // num_parts
    remainder = len(lst) % num_parts
    split_indices = [0] + [avg * i + min(i, remainder) for i in range(1, num_parts + 1)]
    return [lst[split_indices[i]:split_indices[i + 1]] for i in range(num_parts)]


def merge_pdfs(input_paths, output_path, count, self):
    pdf_writer = PyPDF2.PdfWriter()
    split_lists = split_list(input_paths, count)
    progress = ProgressBar(len(input_paths), self, 1)

    all_arts = []

    for group_index, current_group_paths in enumerate(split_lists, start=1):
        for index, input_path in enumerate(current_group_paths, start=1):
            try:
                with open(input_path, 'rb') as pdf_file:
                    pdf_reader = PyPDF2.PdfReader(pdf_file)

                    for i, page in enumerate(pdf_reader.pages, start=1):
                        pdf_writer.add_page(page)

                    name = os.path.splitext(os.path.basename(input_path))[0]
                    if '-glos' in name or '-clos' in name or '_glos' in name:
                        type_list = 'gloss'
                    else:
                        type_list = 'mat'

                    all_arts.append((machine_name, name, type_list, i, self.name_doc))

            except Exception as ex:
                os.remove(input_path)
                logger.error(input_path)
                with open('проблемные пдф.txt', "a") as file:
                    # Записываем строки в файл
                    file.writelines(f'{input_path}\n')
                QMessageBox.warning(self, 'Ошибка',
                                    f'В файле {input_path} обнаружена ошибка, он удален, нужно пересоздать файл')
            progress.update_progress()
        # Write the merged pages to the output file with an index
        current_output_path = f"{output_path}_{group_index}.pdf"
        with open(current_output_path, 'wb') as output_file:
            pdf_writer.write(output_file)
        pdf_writer = PyPDF2.PdfWriter()
    try:
        orders_base_postgresql(all_arts)
    except Exception as ex:
        logger.error(ex)


def created_order(arts, self):
    try:
        shutil.rmtree('Файлы на печать')
    except:
        pass

    time.sleep(2)
    os.makedirs('Файлы на печать', exist_ok=True)

    # Создание файлов со стикерами
    if not self.checkBox.isChecked():
        created_mix_files(arts, ' ', self)

    else:
        arts_gloss = [i for i in arts if '-glos' in i.lower() or '-clos' in i.lower() or '_glos' in i.lower()]
        arts_mat = [i for i in arts if i.lower().endswith('-mat') or '-mat-' in i.lower()]
        arts_other = [i for i in arts if i not in arts_gloss and i not in arts_mat]
        created_mix_files(arts_gloss, 'Gloss', self)
        created_mix_files(arts_mat, 'Mat', self)
        created_mix_files(arts_other, 'Другие', self)


def created_mix_files(arts: list, name: str, self):
    if len(arts) > 0:
        file_new_name = f'Файлы на печать\\Постеры {name}.pdf'

        found_files_all, not_found_files = find_files_in_directory(ready_path, arts)

        merge_pdfs(found_files_all, file_new_name, self.spinBox.value(), self)

        df = pd.DataFrame(not_found_files, columns=['Артикул'])
        if len(df) > 0:
            df_in_xlsx(df, f'Файлы на печать\\Не найденные артикула {name}')
        logger.success(f'{name} Завершено!')

        found_files_stickers, not_found_stickers = find_files_in_directory(sticker_path, arts)
        df = pd.DataFrame(not_found_stickers, columns=['Артикул'])
        try:
            if len(df) > 0:
                df_in_xlsx(df, f'Не найденные шк {name}', directory='Файлы на печать')
        except Exception as ex:
            logger.error(ex)
        if found_files_stickers:
            merge_pdfs_stickers(found_files_stickers, f'Файлы на печать\\!ШК{name}')
    else:
        return
