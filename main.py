import json
from tkinter import *
from tkinter import ttk
from tkinter import filedialog
import csv
from tkinter.messagebox import showinfo, askyesno
import re
import datetime
import sys
import os
import api_kontur
import connect_db
from connect_db import con
from connect_db import insert_flow_groups
import time


# Параметры окна
root = Tk()
root.title("Smart EDI connect 0.9 (beta)")
root.geometry("1400x600")
root. resizable(False, False)

h = ttk.Scrollbar(orient=HORIZONTAL)
v = ttk.Scrollbar(orient=VERTICAL)


canvas = Canvas(scrollregion=(0, 0, 0, 0), bg="white", yscrollcommand=v.set, xscrollcommand=h.set)
h["command"] = canvas.xview
v["command"] = canvas.yview
canvas.grid(column=0, row=0, sticky=(N,W,E,S))
canvas.config(width=1380, height=580)
h.grid(column=0, row=1, sticky=(W,E))
v.grid(column=1, row=0, sticky=(N,S))


# Объявление списков хранящие объекты виджетов
global first_get_all
first_get_all = True
global final_data_to_record
global data_all
final_data_to_record = []
data_all = []
data_field_lable = {}
data_field_title = {}
data_field_inn = {}
data_field_kpp = {}
data_field_gln = {}
data_field_is_active_ka = {}
data_field_is_headoff = {}
data_field_guid = {}
data_compbox_guid = {}
data_compbox_name = {}
data_provider_name = {}
data_entity_0 = {}
data_entity_1 = {}
data_entity_2 = {}
data_entity_3 = {}
data_entity_4 = {}
data_entity_5 = {}
data_entity_6 = {}
data_entity_7 = {}
data_entity_8 = {}
data_entity_9 = {}
all_entity = [data_entity_0, data_entity_1, data_entity_2, data_entity_3, data_entity_4, data_entity_5,
              data_entity_6, data_entity_7, data_entity_8, data_entity_9]
parameters_field = [data_field_lable, data_field_title, data_field_inn, data_field_kpp, data_field_gln, data_field_guid]
parameters_checkbox = [data_field_is_active_ka, data_field_is_headoff]
parameters_compbox = [data_compbox_guid]
parameters_label = [data_compbox_name, data_provider_name]
provider_no_list = (('1', '2BM', 'Контур Диадок'), ('2', '2BK', 'КОРУС Консалтинг'), ('3', '2LD', 'E-COM Докробот'), ('4', '2JM', 'СИСЛИНК (DOCLINK)'), ('5', '2BE', 'Тензор СБИС'), ('6', '2IJ', 'Эдисофт'),
                    ('7', '2AL', 'Такском'), ('8', '2LH', 'LeraData ФораПром'), ('9', '2AE', 'Калуга Астрал'), ('10', '2KL', ''), ('11', '2LT', 'ЦРПТ ЭДО Lite'))


# Открытие и чтение файла
def open_file():
    clear_space()
    parse_file = []
    filepath = filedialog.askopenfilename()
    if filepath != "":
        with open(filepath, "r") as csvfile:
            csvreader = csv.reader(csvfile)
            for i in csvreader:
                parse_file.append(i[0].split(';'))
            parse_file = parse_file[1:]
            y = formation_of_fields(parse_file)
            csvfile.close()
    canvas.create_window(10, y + 10, anchor=NW, window=request_guid)
    canvas.create_window(110, y + 10, anchor=NW, window=get_all)
    canvas.create_window(235, y + 10, anchor=NW, window=record)
    open_file_btn.configure(state='disabled')


# Вывод полей
def display_to_fields(field_name, x, y, insert=''):
    data_field_lable[f'{field_name}'] = ttk.Label(text=f'{field_name[:-2]}', background='white')
    canvas.create_window(x, y - 20, anchor=NW, window=data_field_lable[f'{field_name}'])
    if 'КПП' in field_name:
        field_entry = ttk.Entry(validate="key", validatecommand=check_field)
    elif 'GUID' in field_name:
        field_entry = ttk.Entry(width=60)
    else:
        field_entry = ttk.Entry()
        field_entry.insert(1, insert)
        field_entry.configure(state='disabled')
    canvas.create_window(x, y, anchor=NW, window=field_entry)
    return field_entry


# Вывод вложенных списков
def display_to_compbox(name, x, y):
    compbox_var = StringVar()
    compbox = ttk.Combobox(root, textvariable=compbox_var, width=60)
    compbox['values'] = []
    # compbox['state'] = 'readonly'
    canvas.create_window(x, y, window=compbox)
    result = [compbox_var, compbox]
    return result


# Вывод label
def display_to_label(name, x, y):
    compbox_name = ttk.Label(text=name[:-1], background='white')
    canvas.create_window(x, y - 20, window=compbox_name)
    result = compbox_name
    return result


# Вывод чекбоксов
def display_to_checkbox(name, x, y, state):
    is_active_ka_enable = IntVar()
    is_active_ka_enable.set(state)
    checkbox = ttk.Checkbutton(text=name[:-2], variable=is_active_ka_enable)
    canvas.create_window(x, y, anchor=NW, window=checkbox)
    result = [is_active_ka_enable, checkbox]
    return result


# Формирование всех виджетов
def formation_of_fields(parse_file):
    global y
    y = 80
    for i in range(len(parse_file)):
        data_field_title[f'title{i}'] = display_to_fields(f'Наименование {i + 1}', 10, y, parse_file[i][1])
        data_field_inn[f'inn{i}'] = display_to_fields(f'ИНН {i + 1}', 150, y, parse_file[i][0])
        data_field_kpp[f'kpp{i}'] = display_to_fields(f'КПП {i + 1}', 290, y)
        data_field_gln[f'gln{i}'] = display_to_fields(f'GLN {i + 1}', 430, y, parse_file[i][2])
        data_field_is_active_ka[f'is_active_ka{i}'] = display_to_checkbox(f'Ликвидирован {i + 1}', 570, y, 0)
        data_field_is_headoff[f'is_headoff{i}'] = display_to_checkbox(f'Признак головы {i + 1}', 690, y, 1)
        data_field_is_headoff[f'is_headoff{i}'][0].trace(mode='w', callback=trace_to_headoff)
        # data_field_guid[f'guid{i}'] = display_to_fields(f'GUID {i + 1}', 870, y)
        data_compbox_guid[f'guid{i}'] = display_to_compbox(f'GUID{i + 1}', 1010, y + 10)
        data_compbox_guid[f'guid{i}'][1].bind("<<ComboboxSelected>>", selected)
        data_compbox_name[f'guid{i}'] = display_to_label(f'GUID{i + 1}', 835, y + 9)
        data_provider_name[f'prov_name{i}'] = display_to_label(f'{i + 1}', 1280, y + 30)
        data_entity_0[f'name{i}'] = display_to_checkbox(f'Ашан {i + 1}', 10, y + 30, 0)
        data_entity_1[f'name{i}'] = display_to_checkbox(f'Атак {i + 1}', 90, y + 30, 0)
        data_entity_2[f'name{i}'] = display_to_checkbox(f'Ашан Тех {i + 1}', 160, y + 30, 0)
        data_entity_3[f'name{i}'] = display_to_checkbox(f'Ашан Флай {i + 1}', 260, y + 30, 0)
        data_entity_4[f'name{i}'] = display_to_checkbox(f'Флай Сибирь {i + 1}', 370, y + 30, 0)
        data_entity_5[f'name{i}'] = display_to_checkbox(f'Флай Импорт {i + 1}', 490, y + 30, 0)
        data_entity_6[f'name{i}'] = display_to_checkbox(f'Филье {i + 1}', 610, y + 30, 0)
        data_entity_7[f'name{i}'] = display_to_checkbox(f'Хладокомбинат {i + 1}', 690, y + 30, 0)
        data_entity_8[f'name{i}'] = display_to_checkbox(f'РПК {i + 1}', 820, y + 30, 0)
        data_entity_9[f'name{i}'] = display_to_checkbox(f'ЭЛМ Строй {i + 1}', 890, y + 30, 0)
        y = 80 + (i + 1) * 90
        canvas.config(scrollregion=(0, 0, 10, y + 50))
    return y


# Получение всего что есть в список
def get_all():
    global first_get_all
    global final_data_to_record
    data_compbox_guid_list = []
    if first_get_all is True:
        flow_groups = {}
        data_title_list = []
        data_inn_list = []
        data_kpp_list = []
        data_gln_list = []
        data_field_is_active_ka_list = []
        data_field_is_headoff_list = []
        data_field_guid_list = []
        cnt = 0
        for field_name, field_link in data_field_title.items():
            cnt += 1
            data_title_list.append(field_link.get())
        for field_name, field_link in data_field_inn.items():
            data_inn_list.append(field_link.get())
        for field_name, field_link in data_field_kpp.items():
            data_kpp_list.append(field_link.get())
        for field_name, field_link in data_field_gln.items():
            data_gln_list.append(field_link.get())
        for field_name, field_link in data_field_is_active_ka.items():
            data_field_is_active_ka_list.append(not bool(field_link[0].get()))
        for field_name, field_link in data_field_is_headoff.items():
            data_field_is_headoff_list.append(bool(field_link[0].get()))
        # for field_name, field_link in data_field_guid.items():
        #     data_field_guid_list.append(field_link.get())
        for i in range(cnt):
            data_all.append([data_title_list[i], data_inn_list[i], data_kpp_list[i], data_gln_list[i],
                             data_field_is_active_ka_list[i], data_field_is_headoff_list[i]])
            first_get_all = False
    for field_name, field_link in data_compbox_guid.items():
        data_compbox_guid_list.append(field_link[0].get())
    flow_groups = get_flows()
    for i in range(len(data_compbox_guid_list)):
        flow_group_dict = {}
        flow_group_dict[data_compbox_guid_list[i]] = flow_groups[i]
        data_all[i].append(flow_group_dict)
    final_data_to_record = prepare_flows(data_all)
    record.configure(state='enabled')


# Дичь
def to_record():
    entity_names = ['Ашан', 'Атак', 'Ашан Тех', 'Ашан Флай', 'Флай Сибирь', 'Флай Импорт',
                    'Филье', 'Хладокомбинат', 'РПК', 'ЭЛМ Строй']
    record.configure(state='disabled')
    get_all.configure(state='disabled')
    request_guid.configure(state='disabled')
    info = []
    errors = []
    errors_str = ''
    info_str = 'Для записи будут добавлены следующие группы потоков \n'
    for i in range(len(final_data_to_record)):
        info.append([final_data_to_record[i][0]])
        for j in final_data_to_record[i][-1]:
            for guid, flows in j.items():
                for k in range(len(flows)):
                    if flows[k] is True:
                        info[i].append([entity_names[k], guid])
    for i in range(len(info)):
        errors.append([info[i][0]])
        for j in range(1, len(info[i])):
            if len(errors[i]) == 1:
                errors[i].append([])
            errors[i][1].append(info[i][j][0])
    for i in errors:
        if len(i) > 1 and sorted(i[1]) != sorted(list(set(i[1]))):
            errors_str += str(i) + '\n'
    if errors_str != '':
        showinfo('Ошибка', f'Попытка добавить несколько GUID для одного юр. лица АРР \n {errors_str}')
        return False
    for i in info:
        info_str += str(i) + '\n'
        info_str += '-' * 50 + '\n'
    askyesno('Подтверждение операции', info_str)
    connect_db.preparig_data_to_record(final_data_to_record)
    # Никитин: добавил вызов функции записи в БД (первая функция обновляет GUID, вторая записывает настройки потоков)

    connect_db.insert_changed_data(con, final_data_to_record)
    x = connect_db.insert_changed_data(con, final_data_to_record)
    if x is True:
        connect_db.insert_flow_groups(con, final_data_to_record)




def get_flows():
    entity_names = ['Ашан', 'Атак', 'Ашан Тех', 'Ашан Флай', 'Флай Сибирь', 'Флай Импорт',
                    'Филье', 'Хладокомбинат', 'РПК', 'ЭЛМ Строй']
    entity = {}
    for i in range(len(all_entity)):
        entity_list = []
        for field_name, field_link in all_entity[i].items():
            entity_list.append(bool(field_link[0].get()))
        entity[entity_names[i]] = entity_list

    keys = list(entity.keys())
    length = len(entity['Ашан'])
    flow_groups = []
    for i in range(length):
        prom_list = []
        for j in keys:
            prom_list.append(entity[j][i])
        flow_groups.append(prom_list)
    return flow_groups


def prepare_flows(data):
    guid_to_record = []
    data_to_record = []
    for i in data:
        dedupped = [json.loads(i) for i in set(json.dumps(item, sort_keys=True) for item in i[6:])]
        guid_to_record.append(dedupped)
    for i in range(len(data)):
        data_to_record.append(data[i][:6])
        data_to_record[i].append(guid_to_record[i])
    return data_to_record


# Запрос guid по API
def request_guid():
    global final_data_to_record
    global data_all
    final_data_to_record = []
    data_all = []

    if is_second_validate() is False:
        view_errors()
        return False
    data_inn_kpp = []
    data_inn = []
    data_kpp = []
    for field_name, field_link in data_field_inn.items():
        data_inn.append(field_link.get())
    for field_name, field_link in data_field_kpp.items():
        data_kpp.append(field_link.get())
    for i in range(len(data_inn)):
        data_inn_kpp.append([data_inn[i], data_kpp[i]])
    # print (data_inn)
    res = api_kontur.search_api_kas(data_inn_kpp)
    for i in range(len(res)):
        data_compbox_guid[f'guid{i}'][1]['values'] = res[i]
        if bool(data_field_is_headoff[f'is_headoff{i}'][0].get()) is False:
            clean_guid(i)
        try:
            data_compbox_guid[f'guid{i}'][1].current(0)
            data_compbox_guid[f'guid{i}'][1]['state'] = 'readonly'
            ins_prov_name(i)
        except:
            data_compbox_guid[f'guid{i}'][1]['state'] = ''
            pass
    get_all.configure(state='enabled')


def clean_guid(i):
    data_compbox_guid[f'guid{i}'][1]['state'] = 'readonly'
    data_compbox_guid[f'guid{i}'][1]['values'] = []
    data_compbox_guid[f'guid{i}'][1].set('')
    get_all.configure(state='disabled')
    data_provider_name[f'prov_name{i}'].configure(text=f'', foreground='green', anchor=NW)


# Очистка всего
def clear_space():
    for i in parameters_field:
        destroy_entry(i)
    for i in parameters_checkbox:
        destroy_checkbox(i)
    for i in parameters_compbox:
        destroy_compbox(i)
    for i in parameters_label:
        destroy_label(i)


# Дочерняя функция clear_space (ликвидация вложенных списков)
def destroy_compbox(compbox):
    for field_name, field_link in compbox.items():
        field_link[1].destroy()
    compbox.clear()


# Дочерняя функция clear_space (ликвидация полей)
def destroy_entry(field):
    for field_name, field_link in field.items():
        field_link.destroy()
    field.clear()


# Дочерняя функция clear_space (ликвидация чекбокс)
def destroy_checkbox(checkbox):
    for field_name, field_link in checkbox.items():
        field_link[1].destroy()
    checkbox.clear()


# Дочерняя функция clear_space (ликвидация label)
def destroy_label(label):
    for field_name, field_link in label.items():
        field_link.destroy()
    label.clear()


# Валидация КПП в рельном времени
def is_first_valid_field(newval):
    get_all.configure(state='disabled')
    i = root.focus_get()
    nums = re.findall(r'\d+', str(i))[0]
    index = (int(nums) - 3) // 4
    clean_guid(index)
    return re.match(r"^\d{0,9}$", newval) is not None


# Вторичная валидация полей
def is_second_validate():
    global error_list_all
    error_list_all = []
    error_list = []
    for field_name, field_link in data_field_kpp.items():
        if 0 < len(field_link.get()) < 9:
            error_list.append(f'КПП {int(field_name[len(field_name) - 1:]) + 1} - Ошибка, КПП менее 9 знаков, текущее значение: {field_link.get()}')
    error_list_all.append(error_list)
    if error_list_all[0]:
        return False


def view_errors():
    global error_list_all
    errors = ''
    for i in error_list_all:
        for j in i:
            errors += j + '\n'
    showinfo('Найденные ошибки', f'{errors}')


check_field = (root.register(is_first_valid_field), "%P")


def trace_to_headoff(*args):
    i = root.focus_get()
    nums = re.findall(r'\d+', str(i))[0]
    index = (int(nums) - 2) // 2
    clean_guid(index)


##### TESTs #####


def selected(*args):
    i = root.focus_get()
    try:
        nums = re.findall(r'\d+', str(i))[0]
    except:
        nums = 1
    index = (int(nums) - 1)
    ins_prov_name(index)


def ins_prov_name(index):
    prefix = data_compbox_guid[f'guid{index}'][1].get()[0:3]
    for i in provider_no_list:
        if prefix == i[1]:
            data_provider_name[f'prov_name{index}'].configure(text=f'{i[2]}', foreground='green', anchor=NW)


def to_click():
    text = ('Для добавления новых контрагентов: \n'
            '1) Нажмите кнопку "Открыть файл" и выберете файл с выгрузкой КА \n'
            '2) После разбора файла укажите недостающие данные -> КПП, признаки активности и головной организации \n'
            '3) Нажмите кнопку "Получить GUID" - через API Диадока подтянутся GUID, если они будут найдены по комбинации '
            'ИНН/КПП или только по ИНН, если КПП не указан \n'
            '4) Отметьте ЮР лица АРР, на которых будет распространяться выбранный GUID контрагента и нажмите "Добавить к выгрузке" \n'
            '5) Если для контрагента необходимо добавить еще один GUID, тогда выберете другой GUID из выпадающего списка,'
            'снимите предыдущий выбор ЮР лица АРР и укажите новое ЮР лицо АРР для нового GUID, еще раз нажмите "Добавить к выгрузке" \n'
            '6) Нажмите кнопку "Записать", если нет ошибок при подготовке данных, ВНИМАТЕЛЬНО причитайте информационное сообщение \n'
            'Подтвердите операцию если данные для записи КОРРЕКТНЫЕ \n'
            '\n'
            'ВАЖНО: Нельзя добавлять для одного ЮР лица АРР несколько разных GUID \n'
            'Для одного КА может быть найдено несколько GUID \n'
            'Если необходимо повторить запись, нажмите "Рестарт" и повторите запись с новым файлом')
    showinfo('Справка', text)


click = ttk.Button(text="Справка", command=to_click)
canvas.create_window(990, 20, anchor=NW, window=click)

##### TESTs #####


def restart():
    root.destroy()
    os.startfile('main.exe')


restart = ttk.Button(text="Рестарт", command=restart)
canvas.create_window(900, 20, anchor=NW, window=restart)


open_file_btn = ttk.Button(text="Открыть файл", command=open_file)
canvas.create_window(10, 20, anchor=NW, window=open_file_btn)

request_guid = ttk.Button(root, text="Получить GUID", command=request_guid)
get_all = ttk.Button(root, text="Добавить к загрузке", command=get_all)
get_all.configure(state='disabled')
record = ttk.Button(root, text="Записать", command=to_record)
record.configure(state='disabled')

# data_field_is_headoff['is_headoff1'][0].trace(mode='w', callback=trace_to_headoff)

root.mainloop()


