#多因子组合
__author__ = "" # 这里填下你的名字
# Module
from jaqs_fxdayu.data.dataservice import LocalDataService
from jaqs_fxdayu.data import DataView
from jaqs_fxdayu.data.dataapi import DataApi
from jaqs_fxdayu.util import dp
from jaqs_fxdayu.research.signaldigger import process
from jaqs_fxdayu.research.signaldigger import multi_factor
import warnings

# Data and Process
def get_data(dv, ds):

	################################################################################
	#添加字段和因子公式
    dv.add_field('turnover_ratio',ds)
    dv.add_field('eps_diluted2',ds)
    dv.add_field('float_mv',ds)
    dv.add_field('eps_diluted2',ds)
    dv.add_field('tot_compreh_inc_parent_comp',ds)
    dv.add_field('total_share',ds)
    dv.add_field('surplus_rsrv',ds)
    dv.add_field('undistributed_profit',ds)
    dv.add_field('grossmargin',ds)
    dv.add_field(("sw1"),ds)

    dv.add_formula('alpha1',"Rank(Ts_Sum(turnover_ratio,20))",is_quarterly=False,add_data=True)
    dv.add_formula('alpha2',"-Rank(eps_diluted2)",is_quarterly=False,add_data=True)
    dv.add_formula('alpha3',"-Rank(grossmargin)",is_quarterly=False,add_data=True).tail()
    dv.add_formula('alpha4',"Ts_Mean(vwap*volume,{})+Ts_Mean(vwap*volume,{})/2".format(20,5),is_quarterly=False,add_data=True)
    dv.add_formula('alpha5',"-(eps_diluted2+tot_compreh_inc_parent_comp/total_share+(surplus_rsrv+undistributed_profit)/total_share)",is_quarterly=False,add_data=True)

    factors_list = ['alpha1','alpha2','alpha3','alpha4','alpha5']
    ################################################################################

    index_member = ds.query_index_member_daily('000906.SH',dv.extended_start_date_d, dv.end_date)

    factor_dict = dict()
    for name in factors_list:
        signal = dv.get_ts(name) # 不调整符号
        process.winsorize(factor_df=signal,alpha=0.05,index_member=index_member)#去极值

        signal = process.neutralize(signal,
                                    group=dv.get_ts("sw1"),# 行业分类标准
                                    float_mv = dv.get_ts("float_mv"), #流通市值 可为None 则不进行市值中性化#
                                    index_member=index_member,# 是否只处理时只考虑指数成份股
                                    )
        signal = process.standardize(signal, index_member) #z-score标准化 保留排序信息和分布信息
        factor_dict[name] = signal

    return factor_dict

# Algorithm
def multi_cal(factor_dict):
    new_factors = multi_factor.combine_factors(factor_dict,
                            standardize_type="rank",
                            winsorization=False,
                            weighted_method='equal_weight',
                            props=None)
    return new_factors

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    #################################################################
    dataview_folder = './data2010'    #修改数据存放路径
    #################################################################
    dv = DataView()
    ds = LocalDataService(fp=dataview_folder)
    factor_list = ["volume","sw1","float_mv"]
    check_factor = ','.join(factor_list)
    start = 20170101
    end = 20180213

    ZZ800_id = ds.query_index_member("000906.SH", start, end)
    stock_symbol = list(set(ZZ800_id))
    
    dv_props = {'start_date': start, 'end_date': end, 'symbol':','.join(stock_symbol),
             'fields': check_factor,
             'freq': 1,
             "prepare_fields": True}

    dv.init_from_config(dv_props, data_api=ds)
    dv.prepare_data()

    factor_dict = get_data(dv, ds)
    multi_result = multi_cal(factor_dict)

    print(multi_result)
    print('测试通过')