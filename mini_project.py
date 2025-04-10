# import json 
# column_name = ['name','height','weight']
# filename = 'source1.json'
# data = []

# d = {}
# with open('D:\Guvi\mini_project1\source1.json') as f:
#    name = []
#    height = [] 
#    weight = []
#    for i in f:
#     res = json.loads(i)
#     name.append(res['name'])
#     height.append(res['height'])
#     weight.append(res['weight'])
#    data.append(name)
#    data.append(height)
#    data.append(weight)
#    for i in range(len(column_name)):
#      d.update({(str(column_name[i])+"_"+filename):data[i]})
# print(d)

d = {'name_source1.json': ['jack', 'tom', 'tracy', 'john'],
 'height_source1.json': [68.7, 69.8, 70.01, 67.9],
 'weight_source1.json': [123.3, 141.49, 136.46, 112.37],
 'name_source2.xml': ['simon', 'jacob', 'cindy', 'ivan'],
 'height_source2.xml': ['67.90', '66.78', '66.49', '67.62'],
 'weight_source2.xml': ['112.37', '120.67', '127.45', '114.14'],
 'name_source2.csv': ['alex', 'ajay', 'alice', 'ravi', 'joe'],
 'height_source2.csv': [65.78, 71.52, 69.4, 68.22, 67.79],
 'weight_source2.csv': [112.99, 136.49, 153.03, 142.34, 144.3],
 'name_source2.json': ['jack', 'tom', 'tracy', 'john'],
 'height_source2.json': [68.7, 69.8, 70.01, 67.9],
 'weight_source2.json': [123.3, 141.49, 136.46, 112.37],
 'name_source1.xml': ['simon', 'jacob', 'cindy', 'ivan'],
 'height_source1.xml': ['67.90', '66.78', '66.49', '67.62'],
 'weight_source1.xml': ['112.37', '120.67', '127.45', '114.14'],
 'name_source3.xml': ['simon', 'jacob', 'cindy', 'ivan'],
 'height_source3.xml': ['67.90', '66.78', '66.49', '67.62'],
 'weight_source3.xml': ['112.37', '120.67', '127.45', '114.14'],
 'name_source3.csv': ['alex', 'ajay', 'alice', 'ravi', 'joe'],
 'height_source3.csv': [65.78, 71.52, 69.4, 68.22, 67.79],
 'weight_source3.csv': [112.99, 136.49, 153.03, 142.34, 144.3],
 'name_source1.csv': ['alex', 'ajay', 'alice', 'ravi', 'joe'],
 'height_source1.csv': [65.78, 71.52, 69.4, 68.22, 67.79],
 'weight_source1.csv': [112.99, 136.49, 153.03, 142.34, 144.3],
 'name_source3.json': ['jack', 'tom', 'tracy', 'john'],
 'height_source3.json': [68.7, 69.8, 70.01, 67.9],
 'weight_source3.json': [123.3, 141.49, 136.46, 112.37]}

column_name = ['name','height','weight']

name = []
height = []
weight = []

for i in column_name:
    for l,m in d.items():
        if i in l:
            if "name" == i:
                name.extend(m)
            if "height" == i:
                height.extend(m)
            if "weight" == i:
                weight.extend(m)

data = {}
for i in column_name:
    if "name" == i:
        data.update({i:name})
    if "height" == i:
        data.update({i:height})
    if "weight" == i:
        data.update({i:weight})

print(data)








