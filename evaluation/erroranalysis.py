# -*- coding: utf-8 -*-
# @Time : 2025/5/22 14:42
# @Author : linli

# 统计 evaluation_results 中每个 jsonl 文件中 ex_bool 为 false 的数量，
# 以及 predict_exec_result 下 exec_able 为 false 和 true 的数量

# import os
# import json

# root_dir = "evaluation_results"
# stats = {}

# for db_name in os.listdir(root_dir):
#     db_path = os.path.join(root_dir, db_name)
#     if not os.path.isdir(db_path):
#         continue

#     for model_name in os.listdir(db_path):
#         model_path = os.path.join(db_path, model_name)
#         if not os.path.isdir(model_path):
#             continue

#         for filename in os.listdir(model_path):
#             if not filename.endswith(".jsonl"):
#                 continue

#             file_path = os.path.join(model_path, filename)
#             ex_bool_false = 0
#             exec_false = 0
#             exec_true = 0

#             with open(file_path, 'r', encoding='utf-8') as f:
#                 for idx, line in enumerate(f, 1):
#                     line = line.strip()
#                     if not line:
#                         continue
#                     try:
#                         data = json.loads(line)
#                     except Exception as e:
#                         print(f"跳过无效行: {file_path} 第{idx}行，错误: {e}")
#                         continue

#                     ex = data.get("EX", {})
#                     if not ex.get("ex_bool", True):
#                         ex_bool_false += 1

#                     pred_exec = ex.get("predict_exec_result", {})
#                     if pred_exec.get("exec_able") is False:
#                         exec_false += 1
#                     elif pred_exec.get("exec_able") is True:
#                         exec_true += 1

#             stats[f"{db_name}/{model_name}/{filename}"] = {
#                 "ex_bool_false": ex_bool_false,
#                 "predict_exec_result_false": exec_false,
#                 "predict_exec_result_true": exec_true,
#             }

# # 输出统计结果
# total_ex_bool_false = 0
# total_exec_false = 0
# total_exec_true = 0

# for file, result in stats.items():
#     print(f"{file}: ex_bool_false={result['ex_bool_false']}, "
#           f"predict_exec_result_false={result['predict_exec_result_false']}, "
#           f"predict_exec_result_true={result['predict_exec_result_true']}")
#     total_ex_bool_false += result['ex_bool_false']
#     total_exec_false += result['predict_exec_result_false']
#     total_exec_true += result['predict_exec_result_true']

# print("\n总计：")
# print(f"ex_bool_false 总数: {total_ex_bool_false}")
# print(f"predict_exec_result_false 总数: {total_exec_false}")
# print(f"predict_exec_result_true 总数: {total_exec_true}")


import matplotlib.pyplot as plt
from matplotlib.patches import Wedge
import numpy as np

# 子类标签与占比
labels = [
    "Wrong column", "Wrong table", "Wrong schema linking",
    "Incorrect dialect function", "Incorrect data calculation", "Incorrect planning", "Erroneous data analysis",
    "Join error", "Condition filter error",
    "Syntax error", "Excessive prompt length", "Misunderstanding, external knowledge"
]
sizes = [16.6, 10.1, 27.6, 10.3, 7.5, 17.7, 35.5, 8.3, 11.5, 7.4, 5.1, 4.7]
colors = ['#84c4e1', '#a5d6f1', '#68b0ce',  # 蓝色系
          '#f6cfcf', '#c3e0b8', '#c7e4aa', '#d4f2c6',  # 绿色系
          '#f7b8b5', '#ead7f0',  # 粉紫系
          '#666666', '#bbbbbb', '#f5e6a1']  # 灰黄系

# 创建图形
fig, ax = plt.subplots(figsize=(10, 8), subplot_kw=dict(aspect="equal"))
start_angle = 90
radius = 1.0
width = 0.3

# 绘制每一块
current_angle = start_angle
wedges = []
for i, (label, size, color) in enumerate(zip(labels, sizes, colors)):
    theta1 = current_angle
    theta2 = current_angle - (size / sum(sizes) * 360)
    wedge = Wedge(center=(0, 0), r=radius, theta1=theta1, theta2=theta2,
                  width=width, facecolor=color, edgecolor='white')
    ax.add_patch(wedge)

    # 添加标签
    angle = (theta1 + theta2) / 2
    x = (radius - width/2) * np.cos(np.deg2rad(angle))
    y = (radius - width/2) * np.sin(np.deg2rad(angle))
    ax.text(x, y, f"{label}\n{size}%", ha='center', va='center', fontsize=8)

    current_angle = theta2

# 添加标题等
plt.title("Error Category Distribution", fontsize=14)
plt.axis('off')
plt.tight_layout()
plt.show()
