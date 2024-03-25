import fitz
import os

from loguru import logger


def check_pdf(file_path):
    try:
        with fitz.open(file_path) as pdf_document:
            if pdf_document.page_count != 0:
                return True
    except Exception as e:
        logger.error(f"Ошибка при открытии файла {file_path}: {e}")
        return False


def check_pdfs(folder_path):
    for index, file_name in enumerate(os.listdir(folder_path), start=1):
        if file_name.endswith(".pdf"):
            print('\r', index, end='', flush=True)
            file_path = os.path.join(folder_path, file_name)
            if not check_pdf(file_path):
                logger.error(f"Файл {file_name} содержит ошибки, Удален!")
                os.remove(file_path)


if __name__ == "__main__":
    folder_path = r"E:\Новая база\Готовые pdf"
    check_pdfs(folder_path)
