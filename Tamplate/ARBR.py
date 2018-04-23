#type3  -  the intermediate variable of the factor is also a factor

def run_formula(dv, param = None):
    defult_param = {'t':26}
    if not param:
        param = defult_param
        
    t = param['t']
    
    AR = dv.add_formula("AR_J","Ts_Sum(high-open,%s)/Ts_Sum(open-low,%s)"%(t,t),
           is_quarterly=False,
           add_data=True)
    
    BR = dv.add_formula("BR_J","Ts_Sum(Abs(high-Delay(close,1)),26)/Ts_Sum(Abs(Delay(close,1)-low),26)",
               is_quarterly=False,
               add_data=True)
    
    ARBR = dv.add_formula("ARBR_J","AR_J-BR_J",
           is_quarterly=False)

    return ARBR

