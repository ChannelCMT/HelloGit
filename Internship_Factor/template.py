# -*- coding: utf-8 -*-
"""
Created on Thu Feb 22 11:08:10 2018

@author: xinger
"""

#--------------------------------------------------------
#import

import os
import numpy as np
import pandas as pd
import jaqs_fxdayu
jaqs_fxdayu.patch_all()
from jaqs.data import DataView
from jaqs_fxdayu.data.dataservice import LocalDataService

import warnings
warnings.filterwarnings("ignore")

#--------------------------------------------------------
#define

start = 20170101
end = 20180101
factor_list  = ['BBI','RVI','Elder','ChaikinVolatility','EPS','PE','PS','ACCA','CTOP','MA10RegressCoeff12','AR','BR','ARBR','np_parent_comp_ttm','total_share','bps']
check_factor = ','.join(factor_list)

dataview_folder = r'E:/data'
ds = LocalDataService(fp = dataview_folder)

SH_id = ds.query_index_member("000001.SH", start, end)
SZ_id = ds.query_index_member("399106.SZ", start, end)
stock_symbol = list(set(SH_id)|set(SZ_id))

dv_props = {'start_date': start, 'end_date': end, 'symbol':','.join(stock_symbol),
         'fields': check_factor,
         'freq': 1,
         "prepare_fields": True}

dv = DataView()
dv.init_from_config(dv_props, data_api=ds)
dv.prepare_data()



#--------------------------------------------------------
"""
define factor caculation functions

input  :  dict
parameter of factor
-------
output :  pd.DataFrame
    factor_values , Index is trade_date, columns are symbols.

"""
#------------------------------------------------------------------------------    
#type1  -  simplest type,only use add_formula function without parameter

def EPSTTM():
    EPSTTM = dv.add_formula('EPSTTM', 
               "np_parent_comp_ttm/total_share"
               , is_quarterly=False)
    return EPSTTM    

def CETOP():
    dv.add_field('ncf_oper_ttm',ds)
    dv.add_field('total_mv',ds)
    CETOP = dv.add_formula('CETOP',"ncf_oper_ttm/total_mv",is_quarterly=False)
    return CETOP


#-------------------------------------------------------   
#type2  -  only use add_formula function with parameter
        
def BBI(param = None):
    defult_param = {'t1':3,'t2':6,'t3':12,'t4':24}
    if not param:
        param = defult_param
    
    BBI = dv.add_formula('BBI_J', 
           '''Ta('MA',0,open,high,low,close,volume,%s)+Ta('MA',0,open,high,low,close,volume,%s)+Ta('MA',0,open,high,low,close,volume,%s)+
           Ta('MA',0,open,high,low,close,volume,%s)/4'''%(param['t1'],param['t2'],param['t3'],param['t4']),
           is_quarterly=False)
    
    return BBI


def ChaikinVolatility(param = None):
    defult_param = {'t':10}
    if not param:
        param = defult_param
    
    dv.add_formula('HLEMA',                "Ta('EMA',0,high-low,high-low,high-low,high-low,high-low,10)"
               , is_quarterly=False, add_data=True)
    
    ChaikinVolatility = dv.add_formula('ChaikinVolatility_J', 

                   "100*(HLEMA-Delay(HLEMA,%s))/Delay(HLEMA,%s)"%(param['t'],param['t'])
                   , is_quarterly=False)
    return ChaikinVolatility


def EarnMom(param=None):
    default_param = {'t':7,'i':7}
    if not param:
        param = default_param
    
    EarnMom = dv.add_formula('EarnMom', 
       "Ts_Sum(Sign(oper_rev_ttm-Delay(oper_rev_ttm,%s)),%s)"%(param['t'],param['i'])
       , is_quarterly=True, add_data=True)
    
    return  EarnMom    


#-------------------------------------------------------    
#type3  -  the intermediate variable of the factor is also a factor
    

def RVI(param=None,ret = 'RVI'):
    defult_param = {'t1':10,'t2':14}      
    if not param:
        param = defult_param
        
    dv.add_formula('USD', 
           "If(Return(close,1)>0,StdDev(Return(close,1),%s),0)"%(param['t1'])
           , is_quarterly=False, add_data=True)

    dv.add_formula('DSD', 
                   "If(Return(close,1)<0,StdDev(Return(close,1),%s),0)"%(param['t1'])
                   , is_quarterly=False, add_data=True)
    
    UpRVI = dv.add_formula('UpRVI_J', 
                   "Ta('EMA',0,USD,USD,USD,USD,USD,%s)"%(param['t2'])
                   , is_quarterly=False, add_data=True)
    
    DownRVI = dv.add_formula('DownRVI_J', 
                   "Ta('EMA',0,DSD,DSD,DSD,DSD,DSD,%s)"%(param['t2'])
                   , is_quarterly=False, add_data=True)
    
    RVI = dv.add_formula('RVI_J', 
                   "100*UpRVI_J/(UpRVI_J+DownRVI_J)"
                   , is_quarterly=False)
    
    if ret == 'RVI':
        return RVI   
    elif ret =='UpRVI':
        return UpRVI  
    elif ret =='DownRVI':
        return DownRVI


def ARBR(param = None,ret = 'ARBR'):
    defult_param = {'t':26}      
    if not param:
        param = defult_param
        
    t = param['t']
    
    AR = dv.add_formula("AR_J","Ts_Sum(high-open,%s)/Ts_Sum(open-low,%s)"%(t,t),
           is_quarterly=False,
           add_data=True)
    
    BR = dv.add_formula("BR_J","Ts_Sum(Ts_Max(high-Delay(close,1),0),%s)/Ts_Sum(Ts_Max(Delay(close,1)-low,0),%s)"%(t,t),
           is_quarterly=False,
           add_data=True)
    
    ARBR = dv.add_formula("ARBR_J","AR_J-BR_J",
           is_quarterly=False)
    
    if ret == 'AR':
        return AR        
    elif ret == 'BR':
        return BR
    elif ret == 'ARBR':
        return ARBR    


#--------------------------------------------------------- 
#type4 - caculate factor with industry category 
        
def PSIndu(param = None):
    default_param = {'indu':'sw1'}
    if not param:
        param = default_param
        
    indu = param['indu']
                
    dv.add_field(indu,ds)
    dv.add_field('ps',ds)
    
    _indu = dv.get_ts(indu).stack()
    ps = dv.get_ts('PS').stack()
    
    ps_indu = pd.concat([_indu,ps],axis=1,keys=[indu,'ps'])

    Indu_mean = ps_indu.groupby([indu]).mean().ps.to_dict()
    Indu_std = ps_indu.groupby([indu]).std().ps.to_dict()
    
    ps_indu['PSIndu_Mean'] = [Indu_mean[c] for c in ps_indu[indu].values]
    ps_indu['PSIndu_Std'] = [Indu_std[c] for c in ps_indu[indu].values]
    
    ps_indu['PSIndu'] = (ps_indu['ps']-ps_indu['PSIndu_Mean'])/ps_indu['PSIndu_Std']
    
    PSIndu = ps_indu.PSIndu.unstack()
    
    dv.append_df(PSIndu, 'PSIndu')
    return dv.get_ts('PSIndu')






factor_list = ['EPSTTM','CETOP','BBI','ChaikinVolatility','EarnMom','RVI','ARBR','PSIndu']

#--------------------------------------------------------- 
#test output
def test(factor,data):
    if not isinstance(data, pd.core.frame.DataFrame):
        raise TypeError('On factor {} ,output must be a pandas.DataFrame!'.format(factor))
    else:
        try:
            index_name = data.index.names[0]
            columns_name = data.index.names[0]
        except:
            if not (index_name in ['trade_date','report_date'] and columns_name == 'symbol'):
                raise NameError('''Error index name,index name must in ["trade_date","report_date"],columns name must be "symbol" ''')
                
        index_dtype = data.index.dtype_str
        columns_dtype = data.columns.dtype_str
        
        if columns_dtype not in ['object','str']:
            raise TypeError('error columns type')
            
        if index_dtype not in ['int32','int64','int']:
            raise TypeError('error index type')


test_factor = False

if test_factor:   
    for factor in factor_list[5:]:
        data = globals()[factor]()
        test(factor,data)
