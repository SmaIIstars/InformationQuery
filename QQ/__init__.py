# !/usr/bin/env python 
# -*- coding: utf-8 -*-
# @version: v3.7
# @author: Small_stars
# @mailbox: smallstars.he@qq.com
# @site: 
# @software: PyCharm
# @file: __init__.py
# @time: 2021/2/9 15:59
import threading

from dataSource import data_source_path
from pieces import pieces_path
from database import sql_init

qq_data_path = data_source_path['QQ']
qq_piece_path = pieces_path['QQ']

split_size = 1000
thread_size = 200


def qq_main():

    # split big file
    # split_big_file(split_size, qq_data_path)

    # create table in database
    # create_table(50)

    # data Insert
    insert_data(thread_size)


def split_big_file(size, path):
    offsets_list = find_offsets(size, path)
    all_piece = [''] * size

    with open(qq_data_path, 'r') as big_file:
        for i in range(size):
            for j in range(size):
                all_piece[j] = ''
            print('{}: offset({})'.format(i, offsets_list[i]))
            # read() reads the specified number of bytes from a file, or all if not given or negative.
            str_piece = big_file.read(offsets_list[i + 1] - offsets_list[i])
            list_piece = str_piece.split('\n')

            for tmp in list_piece:
                if len(tmp) > 25:
                    print('error line 1：' + str(tmp))
                    continue
                items = tmp.split("----")
                if len(items[0]) < 5 or len(items) < 2:
                    print('error line 2：' + str(tmp))
                    continue
                try:
                    m = int(int(items[0]) % size)
                    all_piece[m] += tmp + '\n'
                except BaseException as e:
                    print('error line 3：' + str(tmp))
            print('list_piece split finish')

            for j in range(size):
                with open('{}/{}.txt'.format(qq_piece_path, j), 'a') as small_file:
                    print('{}: writing'.format(j))
                    small_file.write(all_piece[j])
                    small_file.close()

        big_file.close()


def find_offsets(thread_size, file_path):
    import os
    list_start_piece_offset = list()
    # The start offset
    list_start_piece_offset.append(0)
    file_size = os.path.getsize(file_path)
    piece_size = int(file_size / thread_size)
    # print("piece_size: {}".format(piece_size))
    with open(file_path, 'rb') as f:
        for piece_number in range(1, thread_size):
            f.seek(piece_number * piece_size)
            for offset in range(0, 27):
                if f.read(1) == b'\n':
                    list_start_piece_offset.append(f.tell())
        f.close()

    # The end offset
    list_start_piece_offset.append(file_size)
    return list_start_piece_offset


def create_table_thread(start_table, end_table):
    db = sql_init()
    cursor = db.cursor()
    counter = 0
    for i in range(start_table, end_table):
        sql = 'create table qq_' + str(i) + ' (qq varchar(12), phone varchar(11));'
        cursor.execute(sql)
        counter += 1
        if counter >= 5:
            try:
                print('{} Thread Commit'.format(i))
                db.commit()
            except BaseException as e:
                db.rollback()

    db.commit()
    print('Created {} to {} table'.format(start_table, end_table))
    cursor.close()
    db.close()


def create_table(thread_size):
    gap = int(split_size / thread_size)
    for i in range(0, thread_size):
        try:
            threading.Thread(target=create_table_thread, args=(i * gap, (i + 1) * gap)).start()
        except BaseException as e:
            print("Error: {} " + str(e))


piece_time_limit = 60


def insert_thread(start_piece, end_piece):
    import time
    thread_start_time = int(time.time())
    print('Start Time({} - {}): '.format(start_piece, end_piece), thread_start_time)
    db = sql_init()
    cursor = db.cursor()
    for i in range(start_piece, end_piece):
        piece_start_time = int(time.time())
        commit = 0
        counter = 0
        items = open_piece_file(i)
        sql = "insert into qq_{} (qq, phone) values".format(i)
        
        for item in items:
            item = item.replace('\n', '').replace('\r', '').replace('\r\n', '')
            [qq, phone] = item.split('----')
            sql += " ({}, {}),".format(qq, phone)
            counter += 1
            if counter >= 5000:
                try:
                    cursor.execute(sql)
                except BaseException as e:
                    print('execute error: {}'.format(e))
                    db.rollback()
                piece_end_time = int(time.time())
                if piece_end_time - piece_start_time >= piece_time_limit:
                    print('{} piece {} commit: {} items'.format(i, commit, counter))
                    try:
                        db.commit()
                    except BaseException as e:
                        print('commit error: {}'.format(e))
                    commit += 1
                    counter = 0
                    piece_start_time = int(time.time())

    try:
        db.commit()
    except BaseException as e:
        print('commit error: {}'.format(e))
    cursor.close()
    db.close()
    thread_elapsed_time = thread_start_time - int(time.time())
    print('Elapsed Time({} - {}): {}'.format(start_piece, end_piece, thread_elapsed_time))


def open_piece_file(num):
    with open('{}/{}.txt'.format(qq_piece_path, num), 'rb') as f:
        for item in f:
            yield str(item, encoding='utf-8')


def insert_data(thread_size):
    gap = int(split_size / thread_size)
    for i in range(0, thread_size):
        try:
            threading.Thread(target=insert_thread, args=(i*gap, (i+1)*gap)).start()
        except BaseException as e:
            print("Error: {} " + str(e))




