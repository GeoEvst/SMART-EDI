# ВАЖНО! Должна быть проверка по недопущению дублей, придумать selectЫ

import psycopg2
from decouple import config

try:
    con = psycopg2.connect(
        database=config('database', default=''),
        user=config('user_db', default=''),
        password=config('password_db', default=''),
        host=config('host_db', default=''),
        port=config('port_db', default='')
    )
    message = ['INFO', 'Database connection successful']
except:
    message = ['ERROR', 'Database connection failed']


def preparig_data_to_record(data):
    select_list = []
    result = []
    for i in data:
        inn = i[1]
        is_active = i[4]
        is_headoff = i[5]
        select_list.append(f"select * from ek.dim_kontragents where inn='{inn}' and is_active is {is_active} and is_headoff is {is_headoff}")

    cur = con.cursor()
    for i in select_list:
        cur.execute(i)
        result.append(cur.fetchall())
    for i in result:
        print(i)

