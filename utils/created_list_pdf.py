import os

import PyPDF2
from PyQt5.QtWidgets import QMessageBox
from loguru import logger

from config import ready_path, ProgressBar


def find_files_in_directory(file_list):
    logger.debug('Проверка папки на компьютере ...')
    found_files = []
    not_found_files = file_list[:]
    for poster in file_list:
        for file in os.listdir(ready_path):
            if os.path.isfile(os.path.join(ready_path, file)):
                file_name = '.'.join(file.split('.')[:-1]).lower()
                if poster.lower() == file_name:
                    found_files.append(os.path.join(ready_path, file))
                    not_found_files.remove(poster)
                    break

    return found_files, not_found_files


def split_list(lst, num_parts):
    avg = len(lst) // num_parts
    remainder = len(lst) % num_parts
    split_indices = [0] + [avg * i + min(i, remainder) for i in range(1, num_parts + 1)]
    return [lst[split_indices[i]:split_indices[i + 1]] for i in range(num_parts)]


def merge_pdfs(input_paths, output_path, count, self):
    pdf_writer = PyPDF2.PdfWriter()
    split_lists = split_list(input_paths, count)
    progress = ProgressBar(len(input_paths), self, 1)

    for group_index, current_group_paths in enumerate(split_lists, start=1):
        for index, input_path in enumerate(current_group_paths, start=1):
            try:
                print(index, input_path)
                with open(input_path, 'rb') as pdf_file:
                    pdf_reader = PyPDF2.PdfReader(pdf_file)
                    # Add all pages from PdfReader to PdfWriter
                    for page in pdf_reader.pages:
                        pdf_writer.add_page(page)
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


def created_order(arts, self):
    file_new_name = 'Файлы на печать\\aaaa.pdf'
    os.makedirs('Файлы на печать', exist_ok=True)
    found_files_all, not_found_files = find_files_in_directory(arts)
    if len(not_found_files) > 0:
        logger.error(f'Длина найденных артикулов {len(found_files_all)}')
        logger.error(f'Длина не найденных артикулов {len(not_found_files)}')
        logger.error("Файлы не найдены:")
        for file_name in not_found_files:
            logger.error(file_name.replace('.pdf', ''))
    else:
        logger.success('Все артикула найденны')
        logger.success(f'Длина найденных артикулов {len(found_files_all)}')

        merge_pdfs(found_files_all, file_new_name, self.spinBox.value(), self)
        logger.success('Завершено!')
