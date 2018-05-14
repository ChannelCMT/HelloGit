# encoding: utf-8
# 文件需要以utf-8格式编码

# 文件名代表因子名称，需满足命名规范，查看完整命名
__author__ = "袁皓玮" # 这里填下你的名字
default_params = {"t1":6} # 这里填写因子参数默认值，比如: {"t1": 10}
params_description = {"t1":"过去6天"} # 这里填写因子参数描述信息，比如: {"t1": "并没有用上的参数"}

def run_formula(dv):
    """
    当天的收盘价对比过去6天平均收盘价的变化率乘100
        """
    # 存入的数据命名要带来源(XXX_A or XXX_B)
    alpha66 = dv.add_formula('alpha66_B', "(close-Ts_Mean(close,%s))/Ts_Mean(close,%s)*100"%(default_params["t1"],default_params["t1"]),
                             is_quarterly=False,
                             add_data=True)

    return alpha66