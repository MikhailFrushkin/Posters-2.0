import asyncio
import os
import shutil
import time
from threading import Thread

import pandas as pd
from pathlib import Path

import qdarkstyle
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QAbstractTableModel, Qt
from PyQt5.QtWidgets import QApplication
from PyQt5.QtWidgets import QProgressBar, QFileDialog, QMessageBox
from loguru import logger

from config import main_path, ready_path, machine_name
from db import update_base_postgresql
from scan_ready_posters import async_main_ready_posters
from scan_shk import async_main_sh
from utils.check_pdf import check_pdfs
from utils.created_list_pdf import created_order
from utils.created_pdf import created_pdf
from utils.dow_stickers import main_download_stickers
from utils.dowloads_files_yzndex import dowloads_files
from utils.read_excel import read_excel_file
from utils.search_file_yandex import main_search, async_main
from utils.update_db import scan_files
import csv

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(448, 462)
        font = QtGui.QFont()
        font.setBold(True)
        font.setItalic(False)
        font.setUnderline(False)
        font.setWeight(75)
        font.setStrikeOut(False)
        MainWindow.setFont(font)
        MainWindow.setMouseTracking(False)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.verticalLayout = QtWidgets.QVBoxLayout(self.centralwidget)
        self.verticalLayout.setObjectName("verticalLayout")
        self.horizontalLayout_2 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_2.setObjectName("horizontalLayout_2")
        self.pushButton = QtWidgets.QPushButton(self.centralwidget)
        self.pushButton.setEnabled(True)
        self.pushButton.setMaximumSize(QtCore.QSize(200, 150))
        font = QtGui.QFont()
        font.setPointSize(14)
        self.pushButton.setFont(font)
        self.pushButton.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.pushButton.setAutoDefault(False)
        self.pushButton.setDefault(False)
        self.pushButton.setFlat(False)
        self.pushButton.setObjectName("pushButton")
        self.horizontalLayout_2.addWidget(self.pushButton)
        if machine_name != 'Ноут':
            self.pushButton.setEnabled(False)
        spacerItem = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_2.addItem(spacerItem)
        self.pushButton_4 = QtWidgets.QPushButton(self.centralwidget)
        self.pushButton_4.setEnabled(True)
        self.pushButton_4.setMaximumSize(QtCore.QSize(200, 150))
        font = QtGui.QFont()
        font.setPointSize(14)
        self.pushButton_4.setFont(font)
        self.pushButton_4.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.pushButton_4.setAutoDefault(False)
        self.pushButton_4.setDefault(False)
        self.pushButton_4.setFlat(False)
        self.pushButton_4.setObjectName("pushButton_4")
        self.horizontalLayout_2.addWidget(self.pushButton_4)
        self.verticalLayout.addLayout(self.horizontalLayout_2)
        self.tableView = QtWidgets.QTableView(self.centralwidget)
        self.tableView.setObjectName("tableView")
        self.verticalLayout.addWidget(self.tableView)
        self.label = QtWidgets.QLabel(self.centralwidget)
        font = QtGui.QFont()
        font.setPointSize(11)
        self.label.setFont(font)
        self.label.setObjectName("label")
        self.verticalLayout.addWidget(self.label)
        self.horizontalLayout = QtWidgets.QHBoxLayout()
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.lineEdit = QtWidgets.QLineEdit(self.centralwidget)
        font = QtGui.QFont()
        font.setPointSize(12)
        self.lineEdit.setFont(font)
        self.lineEdit.setObjectName("lineEdit")
        self.horizontalLayout.addWidget(self.lineEdit)
        self.pushButton_2 = QtWidgets.QPushButton(self.centralwidget)
        self.pushButton_2.setObjectName("pushButton_2")
        self.horizontalLayout.addWidget(self.pushButton_2)
        self.verticalLayout.addLayout(self.horizontalLayout)
        self.gridLayout = QtWidgets.QGridLayout()
        self.gridLayout.setObjectName("gridLayout")
        self.label_2 = QtWidgets.QLabel(self.centralwidget)
        font = QtGui.QFont()
        font.setPointSize(11)
        self.label_2.setFont(font)
        self.label_2.setObjectName("label_2")
        self.gridLayout.addWidget(self.label_2, 0, 0, 1, 1)
        self.spinBox = QtWidgets.QSpinBox(self.centralwidget)
        font = QtGui.QFont()
        font.setPointSize(12)
        self.spinBox.setFont(font)
        self.spinBox.setMaximum(10)
        self.spinBox.setProperty("value", 1)
        self.spinBox.setObjectName("spinBox")
        self.gridLayout.addWidget(self.spinBox, 0, 1, 1, 1)
        spacerItem1 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.gridLayout.addItem(spacerItem1, 0, 2, 1, 1)
        self.label_3 = QtWidgets.QLabel(self.centralwidget)
        font = QtGui.QFont()
        font.setPointSize(11)
        self.label_3.setFont(font)
        self.label_3.setObjectName("label_3")
        self.gridLayout.addWidget(self.label_3, 1, 0, 1, 1)
        self.checkBox = QtWidgets.QCheckBox(self.centralwidget)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Minimum, QtWidgets.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.checkBox.sizePolicy().hasHeightForWidth())
        self.checkBox.setSizePolicy(sizePolicy)
        self.checkBox.setMinimumSize(QtCore.QSize(50, 0))
        font = QtGui.QFont()
        font.setPointSize(11)
        self.checkBox.setFont(font)
        self.checkBox.setLayoutDirection(QtCore.Qt.LeftToRight)
        self.checkBox.setText("")
        self.checkBox.setIconSize(QtCore.QSize(20, 20))
        self.checkBox.setShortcut("")
        self.checkBox.setCheckable(True)
        self.checkBox.setChecked(False)
        self.checkBox.setAutoRepeat(False)
        self.checkBox.setObjectName("checkBox")
        self.gridLayout.addWidget(self.checkBox, 1, 1, 1, 1)
        self.pushButton_3 = QtWidgets.QPushButton(self.centralwidget)
        self.pushButton_3.setMinimumSize(QtCore.QSize(0, 4))
        self.pushButton_3.setMaximumSize(QtCore.QSize(200, 200))
        font = QtGui.QFont()
        font.setPointSize(14)
        self.pushButton_3.setFont(font)
        self.pushButton_3.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.pushButton_3.setLayoutDirection(QtCore.Qt.LeftToRight)
        self.pushButton_3.setText("Создать файлы")
        self.pushButton_3.setAutoExclusive(False)
        self.pushButton_3.setObjectName("pushButton_3")
        self.gridLayout.addWidget(self.pushButton_3, 2, 1, 1, 1)
        self.verticalLayout.addLayout(self.gridLayout)
        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "Печать постеров"))
        self.pushButton.setText(_translate("MainWindow", "Обновить базу"))
        self.pushButton_4.setText(_translate("MainWindow", "Статистика"))
        self.label.setText(_translate("MainWindow", "Загрузите файл заказа Excel"))
        self.pushButton_2.setText(_translate("MainWindow", "Выбрать файл"))
        self.label_2.setText(_translate("MainWindow", "Количество принтеров:"))
        self.label_3.setText(_translate("MainWindow", "Разделят матовые и глянцевые:"))


class CustomTableModel(QAbstractTableModel):
    def __init__(self, data, headers):
        super().__init__()
        self.data = data
        self.headers = headers

    def rowCount(self, parent=None):
        return len(self.data)

    def columnCount(self, parent=None):
        return len(self.headers)

    def data(self, index, role):
        if role == Qt.DisplayRole:
            if index.column() == 0:
                return str(index.row() + 1)  # Возвращаем номер строки + 1
            elif index.column() == 1:
                return self.data[index.row()].art
            elif index.column() == 2:
                return str(self.data[index.row()].count)
            elif index.column() == 3:  # Новый столбец с номером
                return self.data[index.row()].status

    def headerData(self, section, orientation, role):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self.headers[section]


class MainWindow(QtWidgets.QMainWindow, Ui_MainWindow):
    def __init__(self):
        super().__init__()
        self.setupUi(self)
        self.current_dir = Path.cwd()
        self.all_files = []

        self.headers = ['№', 'Артикул', 'Кол-во', 'Статус']

        self.progress_bar = QProgressBar(self)
        self.progress_bar.setGeometry(10, 10, 100, 25)
        self.progress_bar.setMaximum(100)
        self.statusbar.addWidget(self.progress_bar, 1)

        self.pushButton.clicked.connect(self.evt_btn_update)
        self.pushButton_2.clicked.connect(self.evt_btn_open_file_clicked)
        self.pushButton_3.clicked.connect(self.evt_btn_create_queue)
        self.pushButton_4.clicked.connect(self.evt_btn_statistic)

        self.dialogs = []
        self.name_doc = None

    def update_progress(self, current_value, total_value):
        progress = int(current_value / total_value * 100)
        self.progress_bar.setValue(progress)
        QApplication.processEvents()

    def evt_btn_update(self):
        self.progress_bar.setValue(0)
        try:
            shutil.rmtree(main_path, ignore_errors=True)
        except Exception as ex:
            logger.error(ex)
        os.makedirs(main_path, exist_ok=True)
        #
        # try:
        #     self.progress_bar.setValue(0)
        #
        #     logger.debug('Скачивание стикеров ШК...')
        #     main_download_stickers(self)
        # except Exception as ex:
        #     logger.error(ex)
        #
        # try:
        #     self.progress_bar.setValue(0)
        #
        #     os.makedirs(main_path, exist_ok=True)
        #     logger.debug('Поиск готовых pdf файлов на сервере...')
        #     asyncio.run(scan_files(self))
        # except Exception as ex:
        #     logger.error(ex)
        #
        try:
            self.progress_bar.setValue(0)

            logger.debug('Поиск новых артикулов на Яндекс диске дизайнеров...')
            asyncio.run(async_main())

        except Exception as ex:
            logger.error(ex)

        try:
            self.progress_bar.setValue(0)

            logger.debug('Загрузка...')
            dowloads_files(df_new='files/Разница артикулов с гугл.таблицы и на я.диске.xlsx', self=self)
            created_pdf(self)
        except Exception as ex:
            logger.error(ex)

        try:
            update_base_postgresql()
        except Exception as ex:
            logger.error(ex)
        self.progress_bar.setValue(100)

        QMessageBox.information(self, 'Инфо', 'Обновление завершено')

    def evt_btn_statistic(self):
        pass

    def evt_btn_open_file_clicked(self):
        """Ивент на кнопку загрузить файл"""

        file_name, _ = QFileDialog.getOpenFileName(self, 'Загрузить файл', str(self.current_dir),
                                                   'CSV файлы (*.csv *.xlsx)')
        if file_name:
            try:
                self.lineEdit.setText(file_name)
                data = read_excel_file(self.lineEdit.text())
                self.all_files = data[1]
                data[0].reverse()
                data[1].reverse()
                sorted_data = sorted(data[0], key=lambda x: x.status, reverse=True)
                self.model = CustomTableModel(sorted_data, self.headers)

                self.tableView.setModel(self.model)
                self.tableView.resizeColumnsToContents()
                self.tableView.setColumnWidth(0, int(self.tableView.width() * 0.1))
                self.tableView.setColumnWidth(1, int(self.tableView.width() * 0.55))
                self.tableView.setColumnWidth(2, int(self.tableView.width() * 0.16))
                self.tableView.setColumnWidth(3, int(self.tableView.width() * 0.16))
            except Exception as ex:
                logger.error(f'ошибка чтения файла {ex}')
                QMessageBox.information(self, 'Инфо', f'ошибка чтения xlsx {ex}')

    def evt_btn_create_queue(self):
        """Ивент на кнопку создать файлы"""
        filename = os.path.basename(self.lineEdit.text())
        if filename:
            try:
                self.name_doc = (os.path.abspath(self.lineEdit.text()).split('\\')[-1]
                                 .replace('.xlsx', '')
                                 .replace('.csv', ''))
                created_order(self.all_files, self)
                QMessageBox.information(self, 'Инфо', 'Завершено')
            except Exception as ex:
                logger.debug(ex)
        else:
            QMessageBox.information(self, 'Инфо', 'Загрузите заказ')


def get_art_column_data(self, colum):
    art_column_data = []

    for row in range(self.model.rowCount()):
        index = self.model.index(row, colum)  # Индекс ячейки во второй колонке (артикулы)
        art = index.data(Qt.DisplayRole)
        art_column_data.append(art)

    return art_column_data


def run_script():
    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    while True:
        logger.success('Проверка файлов на ошибки...')
        try:
            check_pdfs(ready_path)
        except Exception as ex:
            logger.error(ex)

        try:
            os.makedirs(main_path, exist_ok=True)
            logger.debug('Поиск готовых pdf файлов на сервере...')
            asyncio.run(scan_files())
        except Exception as ex:
            logger.error(ex)
            time.sleep(180)
            try:
                run_script()
            except Exception as ex:
                logger.error(ex)

        logger.success('Поиск новых стикеров ШК...')
        try:
            asyncio.run(async_main_sh())
            # loop.run_until_complete(async_main_sh())
        except Exception as ex:
            logger.error(ex)

        try:
            update_base_postgresql()
        except Exception as ex:
            logger.error(ex)

        logger.success('Обновление завершено')
        time.sleep(1800)


if __name__ == '__main__':
    import sys

    app = QtWidgets.QApplication(sys.argv)
    app.setStyleSheet(qdarkstyle.load_stylesheet_pyqt5())
    w = MainWindow()
    w.show()
    if machine_name != 'Ноут':
        script_thread = Thread(target=run_script)
        script_thread.daemon = True
        script_thread.start()
    sys.exit(app.exec())
