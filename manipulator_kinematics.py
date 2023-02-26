import numpy as np


"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
关节i处坐标系O_i-1沿z_i-1轴平移d_i后再绕z_i-1轴旋转theta_i得到中间坐标系O_i'，
坐标系O_i'沿x_i'轴平移a_i后再绕x_i'轴旋转alpha_i得到关节i+1处坐标系O_i。
其中z轴与对应关节所在平面垂直，x_i轴同时与z_i轴和z_i-1轴正交。
基座处旋转关节1对应坐标系O_0（空间基坐标系）。
机械臂末端虚构关节n+1对应坐标系O_n（腕部坐标系）。
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""


# DH参数列表转变换矩阵列表函数
# A1左乘向量r_i'得到O_i'中向量r_i'在O_i-1中的表示r_i-1
# A2左乘向量r_i得到O_i中向量r_i在O_i'中的表示r_i'
# A1*A2得到关节i+1到关节i的向量变换矩阵（O_i-1中表示O_i向量）
# 输出列表中依次为从O_n-1中表示O_n的变换矩阵到O_0中表示O_1的变换矩阵
def DHargs2matrix(arm_num, d_inlist, theta_inlist, a_inlist, alpha_inlist):
    out_matrix_list = []
    for i in range(arm_num):
        t = theta_inlist[i]
        alpha = alpha_inlist[i]
        A1 = np.mat([np.cos(t), -np.sin(t), 0, 0],
                    [np.sin(t), np.cos(t), 0, 0],
                    [0, 0, 1, d_inlist[i]],
                    [0, 0, 0, 1])
        A2 = np.mat([1, 0, 0, a_inlist[i]],
                    [0, np.cos(alpha), -np.sin(alpha), 0],
                    [0, np.sin(alpha), np.cos(alpha), 0]
                    [0, 0, 0, 1])
        out_matrix_list.insert(np.dot(A1, A2), 0)
    return out_matrix_list


# 正运动学求解函数
# 于末端虚构关节处坐标系中设无旋转初始向量(0, 0, 0)得初始变换矩阵为E
# 变换矩阵列表中各元素依次左乘可得O_n原点向量在O_0中的表示
def forward_analysis(arm_num, d_inlist, theta_inlist, a_inlist, alpha_inlist):
    matrix_list = DHargs2matrix(
        arm_num, d_inlist, theta_inlist, a_inlist, alpha_inlist)
    trans_matrix = np.mat([1, 0, 0, 0], [0, 1, 0, 0],
                          [0, 0, 1, 0], [0, 0, 0, 1])
    for x in matrix_list:
        trans_matrix = np.dot(x, trans_matrix)
    return trans_matrix


# 逆运动学求解函数
# 不知道机械臂的具体类型不好逆运动学求解诶……
def inverse_analysis(inlist):
    arm_num = inlist[0]
    return "not available yet"
