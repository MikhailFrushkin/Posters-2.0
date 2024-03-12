import os
import shutil

import PyPDF2
import pandas as pd
from PIL import Image, ImageOps
from PyQt5.QtWidgets import QMessageBox
from loguru import logger
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from config import ready_path, ProgressBar, sticker_path, df_in_xlsx, machine_name, FilesOnPrint, config_data, \
    admin_name
from db import orders_base_postgresql

count_art = 1


def find_files_in_directory(directory, file_list):
    file_dict = {}
    found_files = []
    not_found_files = []

    for file in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, file)):
            file_name = file.replace('.pdf', '').lower().strip()
            file_dict[file_name] = os.path.join(directory, file)

    for poster in file_list:
        file_name = poster.lower().strip()
        if file_name in file_dict:
            found_files.append(file_dict[file_name])
        else:
            not_found_files.append(poster)

    return found_files, not_found_files


def merge_pdfs_stickers(arts_paths, output_path, progress=None):
    pdf_writer = PyPDF2.PdfWriter()
    arts_paths.reverse()
    for index, input_path in enumerate(arts_paths, start=1):
        try:
            with open(input_path, 'rb') as pdf_file:
                pdf_reader = PyPDF2.PdfReader(pdf_file)
                for page in pdf_reader.pages:
                    pdf_writer.add_page(page)
            # if progress:
            #     progress.update_progress()
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


def merge_pdfs(input_paths, output_path, count, self, progress):
    global count_art
    pdf_writer = PyPDF2.PdfWriter()
    split_lists = split_list(input_paths, count)

    all_arts = []

    for group_index, current_group_paths in enumerate(split_lists, start=1):
        for index, input_path in enumerate(current_group_paths, start=1):
            try:
                filename = input_path.split("\\")[-1]
                logger.success(f'Добавлен {filename}')
                with open(input_path, 'rb') as pdf_file:
                    pdf_reader = PyPDF2.PdfReader(pdf_file)

                    for i, page in enumerate(pdf_reader.pages, start=1):
                        pdf_writer.add_page(page)

                    name = os.path.splitext(os.path.basename(input_path))[0]
                    if '-glos' in name or '-clos' in name or '_glos' in name:
                        type_list = 'gloss'
                    else:
                        type_list = 'mat'

                    all_arts.append((machine_name, name, count_art, type_list, i, self.name_doc))
                    count_art += 1

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
    return all_arts


def created_order(self):
    arts_all = self.all_files
    shutil.rmtree('Файлы на печать', ignore_errors=True)
    os.makedirs('Файлы на печать', exist_ok=True)
    orders = []
    orders_posters = [i.art for i in arts_all if i.type == 'Постер']
    orders_kruzhka = [i for i in arts_all if i.type == 'Кружка']

    if orders_posters:
        progress = ProgressBar(len(orders_posters) * 2, self, 1)

        if not self.checkBox.isChecked():
            orders.extend(created_mix_files(orders_posters, '', self, progress))

        else:
            arts_gloss = [i for i in orders_posters if
                          '-glos' in i.lower() or '-clos' in i.lower() or '_glos' in i.lower()]
            arts_mat = [i for i in orders_posters if i.lower().endswith('-mat') or '-mat-' in i.lower()]
            arts_other = [i for i in orders_posters if i not in arts_gloss and i not in arts_mat]
            if arts_gloss:
                orders.extend(created_mix_files(arts_gloss, 'Gloss', self, progress))
            if arts_mat:
                orders.extend(created_mix_files(arts_mat, 'Mat', self, progress))
            if arts_other:
                orders.extend(created_mix_files(arts_other, 'Другие', self, progress))

    if orders_kruzhka:
        orders_kruzhka.reverse()
        progress = ProgressBar(len(orders_kruzhka), self, 1)
        orders.extend(created_lists_orders_kruzhka(orders_kruzhka, self, progress))

    if machine_name != admin_name:
        try:
            orders_base_postgresql(orders)
            global count_art
            count_art = 1
        except Exception as ex:
            logger.error(ex)


def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def created_lists_orders_kruzhka(orders, self, progress):
    global count_art
    image_paths = []
    all_arts = []
    not_found_arts = [i.art for i in orders if not i.file_path]
    found_arts = [i for i in orders if i.file_path]
    arts = [i.art for i in orders if i.file_path]
    num_string = 1

    for i in found_arts:
        count = 1
        temp_list = []
        try:
            quantity = int(i.art.split('-')[-1])
        except:
            quantity = 1

        if quantity == 2:
            for file in i.file_path:
                temp_list.append((i.art, file, num_string, count))
                count += 1
        else:
            for _ in range(quantity):
                temp_list.append((i.art, i.file_path[0], num_string, count))
                count += 1

        num_string += 1
        count_art += 1
        image_paths.extend(temp_list)
        all_arts.append((machine_name, i.art, count_art, 'Кружка', quantity, self.name_doc))

    chunks = list(chunk_list(image_paths, 3))
    output_path = 'Файлы на печать/Кружки.pdf'

    desired_width_mm = 203
    desired_height_mm = 91
    spacing_mm = 5
    points_per_inch = 72  # 1 дюйм = 72 пункта

    add_width = 0
    add_height = 0
    if config_data:
        try:
            add_width = config_data['Увеличить ширину макета кружки на']
            add_height = config_data['Увеличить высоту макета кружки на']
        except Exception as ex:
            logger.error(ex)

    desired_width_pt = int(desired_width_mm * points_per_inch / 25.4) + add_width
    desired_height_pt = int(desired_height_mm * points_per_inch / 25.4) + add_height

    spacing_pt = int(spacing_mm * points_per_inch / 25.4)
    a4_width, a4_height = A4
    c = canvas.Canvas(output_path, pagesize=A4)

    page_count = 1
    num_list = []
    for chunk in chunks:
        current_x = 10
        current_y = a4_height - spacing_pt - desired_height_pt
        for index, (art, img_path, num_string, count) in enumerate(chunk, start=1):
            try:
                image = Image.open(img_path)
                mirrored_image = ImageOps.mirror(image)
                title = f'{num_string} - {art}#{count}'
                c.drawInlineImage(mirrored_image, current_x, current_y, width=desired_width_pt,
                                  height=desired_height_pt)
                c.setFont("Helvetica", 10)  # Установите шрифт и размер, которые вам нравятся
                #Подпись под картинкой
                # c.drawString(current_x, current_y - 12, title)
                current_y += - spacing_pt - desired_height_pt
                if num_string not in num_list:
                    progress.update_progress()
                    num_list.append(num_string)

            except Exception as ex:
                logger.error(f"Error processing image {img_path}: {ex}")
        c.showPage()
        page_count += 1
    c.save()

    create_file_sh(arts, 'Кружки', progress)
    return all_arts


def created_mix_files(arts: list, name: str, self, progress):
    if arts:
        file_new_name = f'Файлы на печать\\Постеры {name}.pdf'

        found_files_all, not_found_files = find_files_in_directory(ready_path, arts)

        order = merge_pdfs(found_files_all, file_new_name, self.spinBox.value(), self, progress)
        df = pd.DataFrame(not_found_files, columns=['Артикул'])
        if len(df) > 0:
            df_in_xlsx(df, f'Не найденные артикула постеры_{name}_{self.name_doc}_{machine_name}',
                       directory='Файлы на печать')
        create_file_sh(arts, 'Постеры', progress)
        return order


def create_file_sh(arts, name, progress):
    found_files_stickers, not_found_stickers = find_files_in_directory(sticker_path, arts)
    df_not_found_stickers = pd.DataFrame(not_found_stickers, columns=['Артикул'])
    try:
        if len(df_not_found_stickers) > 0:
            df_in_xlsx(df_not_found_stickers, f'Не найденные шк {name}', directory='Файлы на печать')
    except Exception as ex:
        logger.error(ex)

    if found_files_stickers:
        merge_pdfs_stickers(found_files_stickers, f'Файлы на печать\\!ШК {name}', progress)
    logger.debug(f'{name} Завершено!')


if __name__ == '__main__':
    created_lists_orders_kruzhka(
        [
            FilesOnPrint(art='KRUZHKA-LOVE.IS-1', count=1, file_path=None, status='❌', type='Кружка'),
            FilesOnPrint(art='KRUZHKA-BRAWLSTARS-LOGO-1', count=1, file_path=None, status='❌', type='Кружка'),
            FilesOnPrint(art='KRUZHKA-SLOVAPATSANA-1', count=1, file_path=None, status='❌', type='Кружка'),
            FilesOnPrint(art='KRUZHKA-SLOVAPATSANA-1', count=1, file_path=None, status='❌', type='Кружка'),
            FilesOnPrint(art='KRUZHKA-LUBIMYIMUZH-1', count=1, file_path=None, status='❌', type='Кружка'),
            FilesOnPrint(art='KRUZHKA-LUTSHYIDED-1', count=1, file_path=None, status='❌', type='Кружка'),
            FilesOnPrint(art='KRUZHKA-IMENNAYA_JAROSLAVA-1', count=1,
                         file_path='D:\\База постеров\\Кружки\\KRUZHKA-IMENNAYA_JAROSLAVA-1.png', status='✅',
                         type='Кружка'), FilesOnPrint(art='KRUZHKA-HASK-HAZBIN-10', count=1,
                                                      file_path='D:\\База постеров\\Кружки\\KRUZHKA-HASK-HAZBIN-10.png',
                                                      status='✅', type='Кружка')]
    )
