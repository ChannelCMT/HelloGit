#------------------------------------------------------------------------------    
#type1  -  simplest type,only use add_formula function without parameter

def run_formula(dv):
    EPSTTM = dv.add_formula('EPSTTM', 
               "np_parent_comp_ttm/total_share"
               , is_quarterly=False)
    return EPSTTM    
