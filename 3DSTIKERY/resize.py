import cv2
import numpy as np

# Read the image using imread function
image = cv2.imread('D:\\Наклейки\\3D\\3DSTIKERY-5N.FREDDYS-12\\1.png')
cv2.imshow('Original Image', image)

# Разрешение изображения в пикселях на дюйм (PPI)
ppi = 72
width_mm = 31
height_mm = 31
# Преобразование размеров из миллиметров в пиксели
up_width = int(width_mm * (ppi / 25.4))
up_height = int(height_mm * (ppi / 25.4))

up_points = (up_width, up_height)
resized_up = cv2.resize(image, up_points, interpolation=cv2.INTER_LINEAR)

cv2.imshow('Resized Up image by defining height and width', resized_up)
cv2.waitKey()

# press any key to close the windows
cv2.destroyAllWindows()