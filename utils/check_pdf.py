import fitz
import os


def check_pdf(file_path):
    try:
        with fitz.open(file_path) as pdf_document:
            if pdf_document.page_count != 0:
                return True
    except Exception as e:
        print(f"Ошибка при открытии файла {file_path}: {e}")
        return False


def check_pdfs(folder_path):
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".pdf"):
            file_path = os.path.join(folder_path, file_name)
            if not check_pdf(file_path):
                print(f"Файл {file_name} содержит ошибки, Удален!")
                os.remove(file_path)


if __name__ == "__main__":
    folder_path = r"E:\Новая база\Готовые pdf"
    check_pdfs(folder_path)
