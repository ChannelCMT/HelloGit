#--------------------------------------------------------- 
#type4 - caculate factor with industry category 


def run_formula(dv, param = None):
    import pandas as pd
    default_param = {'indu':'sw1'}
    if not param:
        param = default_param
        
    indu = param['indu']
                
    dv.add_field(indu)
    dv.add_field('ps')
    
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
