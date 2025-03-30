import pandas as pd

df = pd.read_csv('data/rams_batch_cur_20250325.csv')
attrs = df[['cu_account_nbr', 'ca_avg_utilz_lst_3_mnths', 'rb_crd_gr_new_crd_gr', 'ca_nsf_count_lst_12_months', 'ca_mob', 'cu_line_incr_excl_flag', 'cu_cur_nbr_due', 'cu_nbr_days_dlq', 'cu_nbr_of_plastics']]

# ca_avg_utilz_lst_3_mnths 
# rb_crd_gr_new_crd_gr (credit grade)
# ca_nsf_count_lst_12_months 
# ca_mob 
# cu_line_incr_excl_flag 
# cu_cur_nbr_due (current cyles delinquent)
# cu_nbr_days_dlq (number of days delinquent)
# cu_nbr_of_plastics


df = pd.read_csv('data/statement_fact_20250325.csv').rename(columns={'current_account_nbr' : 'cu_account_nbr'})
attrs = attrs.merge(df[['cu_account_nbr', 'payment_hist_1_12_mths', 'billing_cycle_date']], on='cu_account_nbr')
attrs['billing_cycle_date'] = pd.to_datetime(attrs['billing_cycle_date'])


df = pd.read_csv('data/account_dim_20250325.csv').rename(columns={'current_account_nbr' : 'cu_account_nbr'})
attrs = attrs.merge(df[['cu_account_nbr', 'card_activation_flag']], on='cu_account_nbr')
# #DONT NEED payment_hist_1_12_mths bc no absolutes

df = pd.read_csv('data/syf_id_20250325.csv').rename(columns={'account_nbr_pty' : 'cu_account_nbr'})
attrs = attrs.merge(df[['cu_account_nbr', 'confidence_level']], on='cu_account_nbr')

df = pd.read_csv('data/fraud_claim_case_20250325.csv').rename(columns={'current_account_nbr' : 'cu_account_nbr'})
attrs = attrs.merge(df[['cu_account_nbr', 'net_fraud_amt']], on='cu_account_nbr')

attrs = attrs.drop_duplicates()

acc = attrs[attrs.cu_account_nbr == f"FqHTEAbjd4z65FWv"]

threshold = acc.ca_mob.max() - (5 - 1) * 12
history = acc[acc.ca_mob >= threshold].payment_hist_1_12_mths

grade = ['A', 'C', 'F', 'G', 'I', 'K', 'L', 'M', 'N', 'P', 'Q', 'R']

def evaluateRisk(acc_n, nsf_count_lst_12_months=3, months_on_book=3):
    # get most recent attr
    acc = attrs[attrs.cu_account_nbr == f"{acc_n}"]
    
    attr = acc.nlargest(1, 'ca_mob')
    
    # absolute reject
    if (attr["cu_cur_nbr_due"].item() == 'LOW' or 
        attr['cu_cur_nbr_due'].item() or 
        attr['confidence_level'].item() == 'LOW' or 
        attr['card_activation_flag'].item() in {7, 8, 9} or
        attr['ca_nsf_count_lst_12_months'].item() >= nsf_count_lst_12_months or
        attr['ca_mob'].item() <= months_on_book or
        attr['rb_crd_gr_new_crd_gr'].item() in grade[len(grade) // 3:]
    ):
        return 'Bad'
    
    # get 5 year payment history (5 yrs - 12 month history)
    threshold = acc.ca_mob.max() - (5 - 1) * 12
    history = acc[acc.ca_mob >= threshold].payment_hist_1_12_mths
    stati = set(([c for h in history for c in h]))
    if 'B' in stati:
        return 'Bad'
    
    score = 1
    score -= attr['cu_nbr_of_plastics'].item() * 0.01
    score -= attr['cu_nbr_days_dlq'].item() * 0.1
    
    
    return 'Good' if score < 0.9 else 'Potential Risk'

def getEvaluationDf():
    accs = attrs.cu_account_nbr.drop_duplicates()
    risks = accs.map(evaluateRisk)
    return pd.DataFrame({'id' : accs, 'risk' : risks})