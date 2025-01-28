import random
import csv

random.seed(0)

data = []
# for t in range(65536):
#     v = t
#     data.append((t, v))

# # 将数据写入CSV文件
# with open('data.csv', 'w', newline='') as csvfile:
#     writer = csv.writer(csvfile)
#     writer.writerow(['t', 'v'])  # 写入表头
#     writer.writerows(data)  # 写入数据

    
for t in range(65536):
    v = 60000 - t
    data.append((t, v))

# 将数据写入CSV文件
with open('data2.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['t', 'v'])  # 写入表头
    writer.writerows(data)  # 写入数据
