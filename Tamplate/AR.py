#type3  -  the intermediate variable of the factor is also a factor


def run_formula(dv, param = None):
    defult_param = {'t':26}
    if not param:
        param = defult_param
        
    t = param['t']
    
    AR = dv.add_formula("AR_J","Ts_Sum(high-open,%s)/Ts_Sum(open-low,%s)"%(t,t),
           is_quarterly=False,
           add_data=False)
    return AR        

