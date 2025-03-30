import pandas as pd

df = pd.read_csv('data/rams_batch_cur_20250325.csv')
attrs = df[['cu_account_nbr', 'ca_avg_utilz_lst_3_mnths', 'rb_crd_gr_new_crd_gr', 'ca_nsf_count_lst_12_months', 'ca_mob', 'cu_line_incr_excl_flag', 'cu_cur_nbr_due', 'cu_nbr_days_dlq', 'cu_nbr_of_plastics']]
print(attrs.cu_account_nbr.nunique())
# ca_avg_utilz_lst_3_mnths 
# rb_crd_gr_new_crd_gr (credit grade)
# ca_nsf_count_lst_12_months 
# ca_mob 
# cu_line_incr_excl_flag 
# cu_cur_nbr_due (current cyles delinquent)
# cu_nbr_days_dlq (number of days delinquent)
# cu_nbr_of_plastics


df = pd.read_csv('data/statement_fact_20250325.csv').rename(columns={'current_account_nbr' : 'cu_account_nbr'})
attrs = attrs.merge(df[['cu_account_nbr', 'payment_hist_1_12_mths', 'billing_cycle_date']], on='cu_account_nbr', how='outer')
attrs['billing_cycle_date'] = pd.to_datetime(attrs['billing_cycle_date'])
print(attrs.cu_account_nbr.nunique())


df = pd.read_csv('data/account_dim_20250325.csv').rename(columns={'current_account_nbr' : 'cu_account_nbr'})
attrs = attrs.merge(df[['cu_account_nbr', 'card_activation_flag']], on='cu_account_nbr', how='outer')
print(attrs.cu_account_nbr.nunique())
# #DONT NEED payment_hist_1_12_mths bc no absolutes

df = pd.read_csv('data/syf_id_20250325.csv').rename(columns={'account_nbr_pty' : 'cu_account_nbr'})
attrs = attrs.merge(df[['cu_account_nbr', 'confidence_level']], on='cu_account_nbr', how='outer')
print(attrs.cu_account_nbr.nunique())

df = pd.read_csv('data/fraud_claim_case_20250325.csv').rename(columns={'current_account_nbr' : 'cu_account_nbr'})
attrs = attrs.merge(df[['cu_account_nbr', 'net_fraud_amt']], on='cu_account_nbr', how='outer')
print(attrs.cu_account_nbr.nunique())

attrs = attrs.drop_duplicates()
attrs = attrs.dropna(subset=['ca_mob'])
print(attrs.cu_account_nbr.nunique())
attrs

# grade = ['A', 'C', 'F', 'G', 'I', 'K', 'L', 'M', 'N', 'P', 'Q', 'R']
grade = sorted(list(set(attrs.rb_crd_gr_new_crd_gr)))

# accs = attrs.cu_account_nbr
# risks = accs.map(evaluateRisk)
# pd.DataFrame({'id' : accs, 'risk' : risks})

def evaluateRiskVectorized(nsf_count_lst_12_months=6, months_on_book_bad=3, years_on_book_risky=3):
    latest_attrs = attrs.loc[attrs.groupby('cu_account_nbr')['ca_mob'].idxmax()]

    # absolute reject conditions
    bad_condition = (
        (latest_attrs["cu_cur_nbr_due"] == 'LOW') |
        (latest_attrs['confidence_level'] == 'LOW') |
        (latest_attrs['card_activation_flag'].isin({7, 8, 9})) |
        (latest_attrs['ca_nsf_count_lst_12_months'] >= nsf_count_lst_12_months) |
        (latest_attrs['ca_mob'] <= months_on_book_bad) |
        (~latest_attrs['rb_crd_gr_new_crd_gr'].isin(grade[len(grade)//3:])) |
        (latest_attrs['cu_nbr_days_dlq'] > 30)
    )
    
    risky_condition = (
        (latest_attrs['rb_crd_gr_new_crd_gr'].isin(grade[len(grade) // 6: 2*len(grade)//3])) |
        (latest_attrs['cu_nbr_days_dlq'] > 30) | 
        (latest_attrs['ca_nsf_count_lst_12_months'] >= nsf_count_lst_12_months // 2) |
        (latest_attrs['ca_mob'] / 12 <= years_on_book_risky)
    )
    # 5 yr history check
    threshold = attrs['ca_mob'].max() - (5 - 1) * 12
    payment_history_flags = attrs[attrs['ca_mob'] >= threshold].groupby('cu_account_nbr')['payment_hist_1_12_mths'].apply(lambda x: 'B' in set(''.join(x)))
    
    latest_attrs['risk'] = 'Good'
    latest_attrs.loc[bad_condition, 'risk'] = 'Bad'
    latest_attrs.loc[(latest_attrs['risk'] != 'Bad') & risky_condition, 'risk'] = 'Risky'
    
    
    # mask = latest_attrs.index.map(lambda x: payment_history_flags.get(x, False))
    # latest_attrs.loc[mask, 'risk'] = 'Bad'

    # # score = 1 - latest_attrs['cu_nbr_of_plastics'] * 0.01 - latest_attrs['cu_nbr_days_dlq'] * 0.1/30

    # latest_attrs['score'] = score
    # latest_attrs.loc[(latest_attrs['risk'] != 'Bad') & (score <= score_threshold), 'risk'] = 'Potential Risk'

    # return latest_attrs[['cu_account_nbr', 'risk', 'score', 'cu_nbr_of_plastics', 'cu_nbr_days_dlq']]
    return latest_attrs[['cu_account_nbr', 'risk']]

# def evaluateRisk(acc_n, nsf_count_lst_12_months=3, months_on_book=3):
#     # get most recent attr
#     acc = attrs[attrs.cu_account_nbr == f"{acc_n}"]
    
#     attr = acc.nlargest(1, 'ca_mob')
    
#     # absolute reject
#     if (attr["cu_cur_nbr_due"].item() == 'LOW' or 
#         attr['cu_cur_nbr_due'].item() or 
#         attr['confidence_level'].item() == 'LOW' or 
#         attr['card_activation_flag'].item() in {7, 8, 9} or
#         attr['ca_nsf_count_lst_12_months'].item() >= nsf_count_lst_12_months or
#         attr['ca_mob'].item() <= months_on_book or
#         attr['rb_crd_gr_new_crd_gr'].item() in grade[len(grade) // 3:]
#     ):
#         return 'Bad'
    
#     # get 5 year payment history (5 yrs - 12 month history)
#     threshold = acc.ca_mob.max() - (5 - 1) * 12
#     history = acc[acc.ca_mob >= threshold].payment_hist_1_12_mths
#     stati = set(([c for h in history for c in h]))
#     if 'B' in stati:
#         return 'Bad'
    
#     score = 1
#     score -= attr['cu_nbr_of_plastics'].item() * 0.01
#     score -= attr['cu_nbr_days_dlq'].item() * 0.1
    
    
#     return 'Good' if score < 0.9 else 'Potential Risk'

# def getEvaluationDf():
#     accs = attrs.cu_account_nbr.drop_duplicates()
#     risks = accs.map(evaluateRisk)
#     return pd.DataFrame({'id' : accs, 'risk' : risks})