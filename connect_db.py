def show_flows(data):
     entity_names = ['Ашан', 'Атак', 'Ашан Тех', 'Ашан Флай', 'Флай Сибирь', 'Флай Импорт',
                     'Филье', 'Хладокомбинат', 'РПК', 'ЭЛМ Строй']
     select_flows = []
     for i in data:
          res = [i[0]]
          flow_group = i[-1]
          for j in range(len(flow_group)):
               if flow_group[j] is True:
                    res.append(entity_names[j])
          select_flows.append(res)
          print(res)
     return select_flows


def preparing_data(data):
     for i in data:
          flow_gtoup = i[-1]

