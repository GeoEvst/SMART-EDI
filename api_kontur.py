import http.client
import json
import http.client
from decouple import config


ddauth_api_client_id = config('ddauth_api_client_id', default='')
Gravitee_Api_Key = config('X-Gravitee-Api-Key', default='')
login = config('login', default='')
password = config('password', default='')


# def authorization():
#   conn = http.client.HTTPSConnection("api-ext.ru.auchan.com")
#   payload = json.dumps({
#     "Login": login,
#     "Password": password
#   })
#   headers = {
#     'Content-Type': 'application/json',
#     'Authorization': f'DiadocAuth ddauth_api_client_id={ddauth_api_client_id}',
#     'X-Gravitee-Api-Key': Gravitee_Api_Key
#   }
#   conn.request("POST", "/edi/diadoc-out/v1/V3/Authenticate?type=password", payload, headers)
#   res = conn.getresponse()
#   data = res.read()
#   token = data.decode("utf-8")
#   return token

# x = authorization()
# print(x)
token = ''


# def get_to_counteragents(token, afterIndexKey = ''):
#   conn = http.client.HTTPSConnection("api-ext.ru.auchan.com")
#   boundary = ''
#   payload = ''
#   headers = {
#     'Authorization': f'DiadocAuth ddauth_api_client_id={ddauth_api_client_id}, ddauth_token={token}',
#     'Accept': 'application/json',
#     'Content-Type': 'application/json; charset=utf-8',
#     'X-Gravitee-Api-Key': Gravitee_Api_Key,
#     'Content-type': 'multipart/form-data; boundary={}'.format(boundary)
#   }
#   conn.request("GET", f"/edi/diadoc-out/v1/V2/GetCounteragents?myOrgId=13f51fdb-ee1e-43a8-bfd7-667f34b23246&counteragentStatus=IsMyCounteragent&afterIndexKey={afterIndexKey}", payload, headers)
#   res = conn.getresponse()
#   data = res.read()
#   json_data = json.loads(data.decode("utf-8"))
#
#   result = {}
#   for i in json_data['Counteragents']:
#     result[i['Organization']['Inn']] = ([i['Organization']['Inn'], i['Organization']['ShortName'], i['Organization']['Kpp'], i['Organization']['FnsParticipantId']])
#   return result, i['IndexKey']


def get_organization(token, inn, kpp=''):
  try:
    conn = http.client.HTTPSConnection("api-ext.ru.auchan.com")
    boundary = ''
    payload = ''
    headers = {
      'Authorization': f'DiadocAuth ddauth_api_client_id={ddauth_api_client_id}, ddauth_token={token}',
      'Accept': 'application/json',
      'Content-Type': 'application/json; charset=utf-8',
      'X-Gravitee-Api-Key': Gravitee_Api_Key,
      'Content-type': 'multipart/form-data; boundary={}'.format(boundary)
    }
    conn.request("GET",
                 f"/edi/diadoc-out/v1/GetOrganizationsByInnKpp?Inn={inn}&Kpp={kpp}",
                 payload, headers)
    res = conn.getresponse()
    data = res.read()
    json_data = json.loads(data.decode("utf-8"))
  except:
    json_data = {'Organizations':[]}
  return json_data


def search_api_ka(data_all):
  guid_ka_list = []
  for i in data_all:
    result = get_organization(token, i[0], i[1])
    if len(result['Organizations']) == 1:
      guid_ka_list.append(result['Organizations'][0]['FnsParticipantId'])
    elif len(result['Organizations']) > 1:
      guid_ka_list.append('')
    else:
      guid_ka_list.append('')
  return guid_ka_list
print (search_api_ka)


def search_api_kas(data_all):
  guid_ka_list = []
  for i in data_all:
    guid_ka = []
    result = get_organization(token, i[0], i[1])
    print (result)
    for i in range(len(result['Organizations'])):
      guid_ka.append(result['Organizations'][i]['FnsParticipantId'])
    guid_ka_list.append(guid_ka)
  return guid_ka_list