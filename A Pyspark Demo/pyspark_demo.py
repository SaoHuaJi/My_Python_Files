# -*- coding:UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt
import numpy as np
import time


# 动态数据展示
def showDatas(narr):
    date = narr[0][0]
    # 数据列表设置
    showls = []     # 前十国家/地区列表
    shownl = []     # 前十国家/地区死伤数据列表
    showdl = []     # 前十国家/地区死亡数据列表
    showcl = []     # 前十国家/地区颜色对应列表
    # 剩余可用颜色列表
    colorls = ['royalblue', 'coral', 'c', 'g', 'orange', 'r', 'navy',
               'aqua', 'm', 'peru', 'tan', 'y', 'violet', 'silver']
    # 图形设置
    plt.rcParams['figure.figsize'] = (12, 12)           # 图像显示大小
    plt.rcParams['font.sans-serif'] = ['AR PL UKai CN'] # 防止中文标签乱码
    plt.rcParams['axes.unicode_minus'] = False          # 字符显示
    plt.rcParams['lines.linewidth'] = 0.8               # 设置曲线线条宽度
    plt.ion()                                           # 开启交互模式
    for i in range(len(narr)):
        if date != narr[i][0] or i == len(narr):
            # 清除上一时刻的图像
            plt.clf()
            # 从下往上绘制堆积图像
            # 死伤数据包含死亡数据，故后画死亡数据覆盖以实现堆积图效果
            plt.barh(showls, shownl, height=0.5, edgecolor='k', color=showcl, alpha=1)
            plt.barh(showls, showdl, height=0.5, edgecolor='k', color='k', alpha=1)
            plt.gca().invert_yaxis()                         # y轴反向
            plt.xlim(min(shownl),max(shownl)*1.2)            # 设置x轴的范围
            plt.xlabel("剩余阳性患者数", fontsize=18)          # 设置x轴标签
            plt.ylabel("国家/地区", fontsize=18,
                       labelpad=-100, y=1.02, rotation=0)    # 设置y轴标签
            plt.xticks(fontsize=18)                          # 设置x轴刻度字体大小
            plt.yticks(fontsize=18)                          # 设置y轴刻度字体大小
            title = "新冠重创排行    截至日期："+str2date(date)  # 构造图片标题
            plt.title(title, fontsize=18)                    # 给条形图添加标题
            plt.subplots_adjust(left=0.16, right=0.96)       # 调整图像边缘距离
            # 条形右侧添加数据
            for x, y in enumerate(showdl):
                plt.text(y+10, x+0.5, "死亡:\n"+str(showdl[x]),
                         fontsize=18, fontweight='heavy', alpha=1,
                         bbox=dict(boxstyle="round", alpha=0.2, fc="w", ec='k'),
                         horizontalalignment="right", verticalalignment="center")
            for x, y in enumerate(shownl):
                plt.text(y, x, "未治愈:\n"+str(shownl[x]),
                         fontsize=18, fontweight='heavy', alpha=1,
                         horizontalalignment="left", verticalalignment="center")
            plt.pause(0.1)
            date = narr[i][0]
        # 数据列表更新
        # 刚开始数据列表为空，直接添加元素即可
        if i == 0:
            showls.append(narr[0][1])
            showdl.append(int(narr[0][3]))
            shownl.append(int(narr[0][2]))
            showcl.append(colorls.pop(np.random.randint(0, len(colorls))))
        # 数据列表未满时无需替换
        elif (0 < i and i < 10) or len(shownl) < 10:
            # 若更新国家/地区已在国家/地区列表中则只需更新数据再排序而不必增添新元素
            if narr[i][1] in showls:
                ptr = showls.index(narr[i][1])
                shownl[ptr] = int(narr[i][2])
                showls.pop(ptr)
                showdl.pop(ptr)
                shownl.pop(ptr)
                color = showcl.pop(ptr) # 暂存国家/地区对应颜色
                for j in range(i-1):
                    if int(narr[i][2]) > shownl[j]:
                        showls.insert(j, narr[i][1])
                        showdl.insert(j, int(narr[i][3]))
                        shownl.insert(j, int(narr[i][2]))
                        showcl.insert(j, color)
                        break
                if len(shownl) == i-1:
                    showls.append(narr[i][1])
                    showdl.append(int(narr[i][3]))
                    shownl.append(int(narr[i][2]))
                    showcl.append(color)
            # 若更新国家/地区不在国家/地区列表中则需增添新元素并排序
            else:
                for j in range(i):
                    if int(narr[i][2]) > shownl[j]:
                        showls.insert(j, narr[i][1])
                        showdl.insert(j, int(narr[i][3]))
                        shownl.insert(j, int(narr[i][2]))
                        showcl.insert(j, colorls.pop(np.random.randint(0, len(colorls))))
                        break
                if len(shownl) == i:
                    showls.append(narr[i][1])
                    showdl.append(int(narr[i][3]))
                    shownl.append(int(narr[i][2]))
                    showcl.append(colorls.pop(np.random.randint(0, len(colorls))))
        # 数据列表满10时需弹出最末尾元素
        else:
            if narr[i][1] in showls:
                ptr = showls.index(narr[i][1])
                shownl[ptr] = int(narr[i][2])
                showdl.pop(ptr)
                showls.pop(ptr)
                shownl.pop(ptr)
                color = showcl.pop(ptr) # 暂存国家/地区对应颜色
                for j in range(9):
                    if int(narr[i][2]) > shownl[j]:
                        showls.insert(j, narr[i][1])
                        showdl.insert(j, int(narr[i][3]))
                        shownl.insert(j, int(narr[i][2]))
                        showcl.insert(j, color)
                        break
                if len(shownl) == 9:
                    showls.append(narr[i][1])
                    showdl.insert(j, int(narr[i][3]))
                    shownl.append(int(narr[i][2]))
                    showcl.append(color)
            else:
                for j in range(10):
                    if int(narr[i][2]) > shownl[j]:
                        showls.insert(j, narr[i][1])
                        showdl.insert(j, int(narr[i][3]))
                        shownl.insert(j, int(narr[i][2]))
                        showcl.insert(j, colorls.pop(np.random.randint(0, len(colorls))))
                        colorls.append(showcl.pop())
                        showdl.pop()
                        showls.pop()
                        shownl.pop()
                        break
    plt.ioff()  # 关闭交互模式


# udf作带参装饰器使其可以处理udf的返回类型
@ F.udf(returnType=IntegerType())
# str日期转换int
def date2int(x):
    x = x.split("/")
    x = x[2]+x[0]+x[1]
    return int(x)


# 日期格式化
def str2date(x):
    return x[0:4]+"年"+x[4:6]+"月"+x[6:8]+"日"

if __name__ == "__main__":
    # fp = "file:///home/saohuaji/桌面/covid/covid_19_data.csv"   # 数据文件路径
    fp = "hdfs://localhost:9000/covid/covid_19_data.csv"
    n_threads = 4  # 线程数
    # spark初始化
    spark = SparkSession.builder.master("local[%d]" % n_threads).appName("CDA").getOrCreate()
        
    time_start = time.time()

    # 读取csv文件数据
    """
    inferSchema=True会多读取一次文件以判断数据类型
    在此处会把int型数值数据判断为float，不符合预期
    """
    datas = spark.read.csv(path=fp, header=True, inferSchema=False)
    # 数值数据预处理
    datas = datas.filter((F.col("Country/Region") != "China")&
                        (F.col("Country/Region") != "Others"))
    datas = datas.withColumn("Confirmed", F.col("Confirmed").cast("Integer"))
    datas = datas.withColumn("Deaths", F.col("Deaths").cast("Integer"))
    datas = datas.withColumn("Recovered", F.col("Recovered").cast("Integer"))
    # 剩余感染者计算
    datas = datas.withColumn(
        "ConfirmedRemain", F.col("Confirmed")-F.col("Recovered"))
    # 根据日期和国家聚类
    SD = datas.groupBy(["ObservationDate", "Country/Region"]).agg(
        F.sum("ConfirmedRemain").alias("ConfirmedRemain"),
        F.sum("Deaths").alias("Deaths"))
    # 日期数据格式修正以便排序
    SD = SD.withColumn("ObservationDate", date2int(F.col("ObservationDate")))
    # 基于日期数据升序排序
    SD = SD.orderBy(["ObservationDate"], ascending=[True])
    # pyspark dataframe转pandas dataframe再转numpy数组
    narr = np.array(SD.toPandas().values.tolist())    # 此时数组中数据都是str
    showDatas(narr)

    time_end = time.time()  
    print("不算初始化Spark的程序执行耗时%s秒" % (time_end-time_start))

    input("Please press Enter key to continue.")
    plt.close("all")
    spark.stop()
