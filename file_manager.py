import os
import datetime


def copy_for_date(date_, path):
    date_ = date_ - datetime.timedelta(days=1)
    if isinstance(date_, datetime.datetime):
        date_list = str(date_.date()).split('-')
    elif isinstance(date_, datetime.date):
        date_list = str(date_).split('-')
    date_str = date_list[2] + date_list[1] + date_list[0]
    list_dir = os.listdir('data_all/')
    files_to_copy = []
    for file_name in list_dir:
        if file_name.find(date_str) > -1:
            files_to_copy.append(file_name)
    for file_name in files_to_copy:
        os.rename('data_all/' + file_name, path + file_name)


def copy_all_to_archive(path):
    list_dir = os.listdir(path)
    for file_name in list_dir:
        os.rename(path + file_name, 'archive/' + file_name)

