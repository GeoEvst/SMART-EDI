# ВАЖНО! Должна быть проверка по недопущению дублей, придумать selectЫ

import psycopg2
import psycopg2.extras
from decouple import config

# Регистрация адаптера JSON для psycopg2
# psycopg2.extras.register_json(con, globally=True)

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
        print(i)
        inn = i[1]
        is_active = i[4]
        is_headoff = i[5]
        select_list.append(f"select * from ek.dim_kontragents where inn='{inn}' and is_active is {is_active} and is_headoff is {is_headoff}")
    # print (data)
    cur = con.cursor()
    for i in select_list:
        cur.execute(i)
        result.append(cur.fetchall())
    # for i in result:
    #     print(i)



# Функция формирования и записи настроек потоков в таблицу ek.link_kontragents_flow

def insert_flow_groups(con, data):
    if con is None or con.closed:
        print("Ошибка: Подключение к базе отсутствует")
        return

    try:
        cur = con.cursor()

        # Проход по переданным данным
        for item in data:
            title, inn, kpp, GLN, is_active, is_headoff, flow_groups = item

            # Получение top_cd_dp из таблицы ek.dim_kontragents по ИНН + проверка что это активная запись "головы"
            cur.execute("SELECT top_cd_dp FROM ek.dim_kontragents WHERE inn = %s AND is_active = TRUE AND is_headoff = TRUE", (inn,))
            top_cd_dp_row = cur.fetchone()
            if top_cd_dp_row:
                top_cd_dp = top_cd_dp_row[0]
                print("top_cd_dp:", top_cd_dp)
            else:
                print("Ошибка: top_cd_dp не найден для ИНН", inn)
                continue

            # Получение id из таблицы ek.data_kontragents_guids
            cur.execute("SELECT id FROM ek.data_kontragents_guids WHERE top_cd_dp = %s AND is_active = TRUE", (top_cd_dp,))
            id_dim_provider_row = cur.fetchone()
            if id_dim_provider_row:
                id_dim_provider = id_dim_provider_row[0]
                print("id_dim_provider:", id_dim_provider)
            else:
                print("Ошибка: id_dim_provider не найден для top_cd_dp", top_cd_dp)
                continue

            # Вставка данных в таблицу ek.link_kontragents_flow
            for flow_group in flow_groups:
                for fns_participant_id, settings in flow_group.items():
                    for flag, flags in enumerate(settings):
                        if flags:
                            # Словарь с настройками потоков
                            settings_dict = {
                                # Ашан
                                0: [(top_cd_dp, 1, 7, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 8, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 9, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 100, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 113, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 115, True, 0, id_dim_provider),
                                   ],
                                # Атак
                                1: [(top_cd_dp, 1, 5, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 6, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 10, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 101, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 114, True, 1, id_dim_provider),
                                    (top_cd_dp, 1, 116, True, 0, id_dim_provider),
                                   ],
                                # Ашан Тех
                                2: [(top_cd_dp, 1, 123, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 124, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 125, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 126, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 127, True, 0, id_dim_provider),
                                   ],
                                # Ашан Флай
                                3: [(top_cd_dp, 1, 25, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 27, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 29, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 102, True, 0, id_dim_provider),
                                    ],
                                # Ашан Флай Сибирь
                                4: [(top_cd_dp, 1, 25, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 27, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 29, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 102, True, 0, id_dim_provider),
                                    ],
                                # Ашан Флай Импорт
                                5: [(top_cd_dp, 1, 128, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 129, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 130, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 131, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 132, True, 0, id_dim_provider),
                                    ],
                                # Филье
                                6: [(top_cd_dp, 1, 37, True, 1, id_dim_provider),
                                    (top_cd_dp, 1, 38, True, 1, id_dim_provider),
                                    (top_cd_dp, 1, 60, True, 1, id_dim_provider),
                                    (top_cd_dp, 1, 104, True, 1, id_dim_provider),
                                    ],
                                # Хладокомбинат
                                7: [(top_cd_dp, 1, 91, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 92, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 93, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 105, True, 0, id_dim_provider),
                                    ],
                                # РПК
                                8: [(top_cd_dp, 1, 94, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 95, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 96, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 106, True, 0, id_dim_provider),
                                    ],
                                # Элм Строй
                                9: [(top_cd_dp, 1, 97, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 98, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 99, True, 0, id_dim_provider),
                                    (top_cd_dp, 1, 107, True, 0, id_dim_provider),
                                    ],
                                #тут можно добавить настройки для остальных юридических лиц если будут новые
                            }

                            for setting in settings_dict[flag]:
                                # print (setting)
                                cur.execute("""
                                    INSERT INTO ek.link_kontragents_flow 
                                        (kontrag_top_cd_dp, id_dim_provider, id_flow_grp, is_active, is_test, id_kontragents_guids)
                                    VALUES 
                                        (%s, %s, %s, %s, %s, %s)
                                """, setting)
                                print("Добавлена запись:", setting, title)

        # Завершение транзакции и закрытие курсора
        con.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Ошибка:", error)

    finally:
        if con is not None:
            con.close()

# Алгоритм работы:
# Проверяет, существует ли соединение с базой данных.
# Проходится по каждой записи в данных.
# Проверяет, существует ли уже запись с такими же данными в таблице ek.dim_kontragents.
# Если запись не найдена, добавляет ее в таблицу ek.dim_kontragents и получает соответствующий top_cd_dp.
# Затем проверяет, существует ли запись с таким top_cd_dp в таблице ek.data_kontragents_guids.
# Если запись существует, обновляет GUID (fns_participant_id).
# Если запись не существует, добавляет новую запись.
# Фиксирует изменения и закрывает соединение с базой данных.

# Функция добавления контрагента и его абонентского ящика в таблицу ek.dim_kontragents и ek.data_kontragents_guids
def insert_changed_data(con, data):
    print (con, type (con))
    if con is None or con.closed:
        print("Ошибка: Подключение к базе отсутствует")
        return

    try:
        cur = con.cursor()
        # for row in data:
            # print (row)
            # print("title -", row[0])
            # print("inn -", row[1])
            # print("kpp -", row[2])
            # print("fns_participant_id -", row[6])
            # print("is_active -", row[4])
            # print("is_headoff -", row[5])
            # print("fns_participant_id_list -", row[6])
            # print()
        for row in data:
            title = row[0]
            inn = row[1]
            is_active = row[4]
            is_headoff = row[5]
            fns_participant_id_list = row[6]

            # Проверяем, есть ли уже данные с такими значениями title, inn, is_active и is_headoff
            cur.execute(
                "SELECT * FROM ek.dim_kontragents WHERE title = %s AND inn = %s AND is_active = %s AND is_headoff = %s",
                (title, inn, is_active, is_headoff))
            existing_data = cur.fetchone()

            if not existing_data:
                # Добавляем новую запись в dim_kontragents
                cur.execute(
                    "INSERT INTO ek.dim_kontragents (title, inn, is_active, is_headoff) VALUES (%s, %s, %s, %s) RETURNING top_cd_dp;",
                    (title, inn, is_active, is_headoff))
                top_cd_dp = cur.fetchone()[0]

                print(f"Добавлена новая запись в dim_kontragents: {title}, {inn}, {is_active}, {is_headoff}")

            else:
                # Если запись уже существует, используем ее top_cd_dp
                top_cd_dp = existing_data[2]

            # Получаем список всех записей с таким top_cd_dp и is_active = true
            cur.execute(
                "SELECT * FROM ek.data_kontragents_guids WHERE top_cd_dp = %s AND is_active = true",
                (top_cd_dp,))
            active_guids = cur.fetchall()

            if active_guids:
                # Если есть активные записи, обновляем только их
                for guid_dict in fns_participant_id_list:
                    for guid, flags in guid_dict.items():
                        cur.execute(
                            "UPDATE ek.data_kontragents_guids SET guid = %s WHERE top_cd_dp = %s AND is_active = true;",
                            (guid, top_cd_dp))
                        print(f"Обновлен GUID в data_kontragents_guids для top_cd_dp {top_cd_dp}")
            else:
                # Если активных записей нет, добавляем новую
                for guid_dict in fns_participant_id_list:
                    for guid, flags in guid_dict.items():
                        cur.execute(
                            "INSERT INTO ek.data_kontragents_guids (top_cd_dp, provider_no, guid, is_active) VALUES (%s, '1', %s, true);",
                            (top_cd_dp, guid))
                        print(f"Добавлена новая запись в data_kontragents_guids для top_cd_dp {top_cd_dp}")

        con.commit()
        print("Данные успешно добавлены")
        return True
    except psycopg2.Error as e:
        con.rollback()
        print(f"Ошибка добавления данных: {e}")
        return False
    finally:
        if con is not None:
            cur.close()


# -------------- Оптимизация-----------


