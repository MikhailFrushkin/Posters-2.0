from PIL import Image

image_list = ['D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\1.png', 'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\10.png',
              'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\11.png', 'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\12.png',
              'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\2.png', 'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\3.png',
              'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\4.png', 'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\5.png',
              'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\6.png', 'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\7.png',
              'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\6.png', 'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\7.png',
              'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\6.png', 'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\7.png',
              'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\6.png', 'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\7.png',
              'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\6.png', 'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\7.png',
              'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\6.png', 'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\7.png',
              'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\8.png', 'D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\9.png']

# Размеры PNG изображения в миллиметрах
width_mm = 1000
height_mm = 400

# Интервалы между картинками и рядами в миллиметрах
interval_x_mm = 6.65
interval_y_mm = 1.48

# Количество картинок в ряду и рядов
images_per_row = 24
rows = 10

# Разрешение изображения в пикселях на дюйм (PPI)
ppi = 72

# Преобразование размеров из миллиметров в пиксели
width_px = int(width_mm * (ppi / 25.4))
height_px = int(height_mm * (ppi / 25.4))

# Преобразование интервалов между картинками и рядами из миллиметров в пиксели
interval_x_px = interval_x_mm * (ppi / 25.4)
interval_y_px = interval_y_mm * (ppi / 25.4)

# Создание нового PNG изображения
new_image = Image.new("RGB", (width_px, height_px), (255, 255, 255))

x_offset = interval_x_px
y_offset = interval_y_px

for i in range(rows):
    for j in range(images_per_row):
        # Вычисляем индекс текущей картинки
        index = i * images_per_row + j
        if index >= len(image_list):
            break

        # Загрузка изображения
        image_path = image_list[index]
        image = Image.open(image_path)

        # Получение реальных размеров изображения в пикселях
        image_width_mm, image_height_mm = image.size
        image_width_px = int(image_width_mm * (ppi / 25.4))
        image_height_px = int(image_height_mm * (ppi / 25.4))

        # Вставка изображения на новое изображение
        new_image.paste(image, (int(x_offset), int(y_offset)))

        # Обновление смещения для следующего изображения
        x_offset += image_width_px + interval_x_px

        # Добавление интервала после каждой третьей картинки
        if (j + 1) % 3 == 0:
            x_offset += 14.04 * (ppi / 25.4)

    # Обновление смещения для следующего ряда
    y_offset += image_height_px + interval_y_px

    # Сброс смещения для следующего ряда
    x_offset = interval_x_px

# Сохранение результата в файл PNG
new_image.save("output.png")
