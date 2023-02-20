def feature_code_generation(table_name, feature_list_char, feature_list_num, lookback_days=60):            
    var_name_char='\n,'.join(["'"+s+"'" for s in feature_list_char.split('\n')[1:-1]])
    max_list_char='\n,'.join(["max(case when var_name='"+s+"' then var_value else '' end ) as "+s for s in feature_list_char.split('\n')[1:-1]])

    var_name_num='\n,'.join(["'"+s+"'" for s in feature_list_num.split('\n')[1:-1]])
    max_list_num='\n,'.join(["max(case when var_name='"+s+"' then case when var_value SIMILAR TO '[0-9 .]*' then var_value::float else null end else 0 end) as "+s for s in feature_list_num.split('\n')[1:-1]])


    feature_code=f"""
    drop table if exists {table_name};
    create table {table_name} as (
    with 
    order_dedup_base AS (
    select
    (TIMESTAMP 'epoch' + event_info_event_time / 1000*INTERVAL '1 second') as checkout_time
    , event_info_event_time
    , par_region
    , order_token
    , request_consumer_uuid as consumer_id
    , merchant_id
    , row_number() over (partition by order_token order by event_info_event_time desc) as order_dedup
    from curated_feature_science_red.raw_c_e_fc_decision_record
    where
    checkpoint = 'CHECKOUT_CONFIRM'
    and date_trunc('day',checkout_time) >=  current_date-{lookback_days}
    )
    ,order_base as
    (select * from order_dedup_base where order_dedup = 1)

    ,sumo as 

    (select 
    order_id
    ,consumer_uuid
    , rule_ids
    , control_group
    from
    curated_feature_science_red.tbl_raw_r_e_sumo_logic_rules
    where checkpoint in ( 'CHECKOUT_CONFIRM')
    and left(asctime,10)>=current_date-{lookback_days}

    )
    ,ato_ticket as
    (

    SELECT

    distinct order_token

    FROM sandbox_analytics_us.ac_ato_zendesk_final as l
    LEFT JOIN curated_risk_bi_green.dwm_order_loss_tagging as r
    ON l.consumer_id = r.consumer_id
    AND l.country_code = r.consumer_country_code
    AND r.order_date between dateadd(day,-7,l.zticket_created_at) and dateadd(day,1,l.zticket_created_at)

    WHERE 1=1
    and zticket_created_at > current_date-30


    )

    ,loss as
    (select
    a.order_token,
    order_amount_local,
    order_amount_aud,
    case when cb_seg = '02.Fraud Chargeback' and b.cb_tagging in ('3.ATO','1.SF+ATO') then chargeback_amount_local else 0 end as chargeback_utd_local,
    case when cb_seg = '02.Fraud Chargeback' and b.cb_tagging in ('3.ATO','1.SF+ATO') then overdue_utd_local else 0 end as overdue_utd_local,
    case when cb_seg = '02.Fraud Chargeback' and b.cb_tagging in ('3.ATO','1.SF+ATO') then late_fee_collected_utd_local else 0 end as late_fee_collected_utd_local,
    refund_amt_aud gw_refund_amt_aud,
    waived_amt_aud gw_waived_amt_aud,
    failed_refund_amt_aud gw_faild_refund_amt_aud
    ,case when b.cb_tagging in ('3.ATO','1.SF+ATO') then 1 else 0 end as ato_ind
    ,case when b.cb_tagging in ('2.SF','1.SF+ATO') then 1 else 0 end as sf_ind
    ,case when order_refund_amount+order_waived_amt-failed_refund_amt>0 then 1 else 0 end as gwr_ind
    ,case when order_refund_amount+order_waived_amt-failed_refund_amt>0 or t.cb_amount_total>0 or ato_ticket.order_token is not null then 1 else 0 end as loss_ind
    from curated_risk_bi_green.dwm_order_loss_tagging a

    left join curated_fraud_risk_green.yy_combine_cb_table_order_temp t
    on a.order_token = t.order_token
    and t.cb_type not like '%Merchandise%'
    and t.order_source = 'Online'

    left join sandbox_analytics_au.ys_cb_tagging_order_lvl b
    on a.order_id = b.order_id
    left join sandbox_analytics_au.mw_ato_goodwill_refund_3 r
    on a.order_id = r.order_id
    and r.order_dispute_reason = 'UNAUTHORIZED_TRANSACTION'
    left join 
        ato_ticket 
    on a.order_token=ato_ticket.order_token

    where a.order_date>=current_date-{lookback_days}
    )
    ,feature_base as (
    select
        f.order_token
        ,f.checkpoint
        ,(TIMESTAMP 'epoch' + event_info_event_time / 1000*INTERVAL '1 second') as checkout_time
        ,consumer_id
        ,par_process_date
        ,par_region
        ,merchant_id
        ,{max_list_char},{max_list_num}

    from curated_feature_science_red.raw_c_e_fc_decision_record_rule_vars f

    where
        f.checkpoint in ( 'CHECKOUT_CONFIRM')
        and par_process_date >=current_date-{lookback_days}
        and var_name in ({var_name_char},{var_name_num})
    group by 1,2,3,4,5,6,7
    )

    select 
        distinct
        fb.*
        , s.rule_ids
        , s.control_group
        ,order_amount_local as GMV_local
        ,order_amount_aud GMV_aud
        ,gwr_ind
        ,case when ato_ind=1 or gwr_ind=1 then 1 else 0 end as ato_ind
        ,sf_ind
        ,loss_ind

    from order_base f
    join feature_base fb
    on f.consumer_id = fb.consumer_id
    and f.order_token = fb.order_token
    join sumo s
    on f.consumer_id = s.consumer_uuid
    and f.order_token = s.order_id

    left join loss
    on f.order_token=loss.order_token


    )
    ;"""
    return feature_code

def vega_table_generation(okta_username, query):
    import logging
    import pandas as pd
    import numpy as np
    from afterpay_gdp_interfaces import RedshiftHook

    import datetime
    import pytz
    CST = pytz.timezone('Asia/Shanghai')

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 100)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    vega = RedshiftHook(cluster='vega', okta_username=okta_username) # for vega connection
    def vega_execute(query):
        import datetime
        """
            vega execute SQL wrapper to commit everytime
        """
        t_start = datetime.datetime.now()
        with vega.get_conn() as vega_conn:
            with vega_conn.cursor() as cur:
                cur.execute(query)
            vega_conn.commit()
            t_end = datetime.datetime.now()
            logging.info("Vega Query Finished. Time used: {}".format(str(t_end - t_start)))
            # vega_conn.close()
    print(datetime.datetime.now(CST))
    vega_execute(query)
    
feature_list_char="""
consumer_state
in_flight_order_shipping_address_state
in_flight_order_shipping_address_postcode
consumer_contact_address_postcode
in_flight_order_shipping_address_name
in_flight_order_shipping_address_address_1
in_flight_order_shipping_address_address_2
consumer_name
in_flight_order_consumer_name
in_flight_order_consumer_email
consumer_email
in_flight_order_recipient_email
in_flight_order_recipient_message
in_flight_order_recipient_name
in_flight_order_shipping_address_mobile
bp_c_trusted_merch_side_email_yn
bp_c_trusted_shipping_address_hash
in_flight_order_shipping_address_hash
session_user_agent
consumer_is_first_order
region
region_code 
c_latest_login_2fa_success_method
city
sp_payment_method_last_card_scan_is_successful
attempt_payment_method
consumer_mobile
consumer_email_domain
consumer_given_names
consumer_address_1
consumer_address_2
consumer_city
consumer_postcode
in_flight_order_shipping_address_city
session_client_ip
bp_m_mcc_code
bp_m_mcc_desc
in_flight_order_billing_address_name
in_flight_order_billing_address_address_1
in_flight_order_billing_address_address_2
in_flight_order_billing_address_city
in_flight_order_billing_address_state
in_flight_order_billing_address_postcode
in_flight_order_billing_address_mobile
in_flight_order_billing_address_country_code
in_flight_order_item_names
consumer_birth_date_epoch_millis
"""
feature_list_num="""
in_flight_order_amount
sp_c_fraud_decline_attempt_d7_0
sp_c_fraud_decline_attempt_d3_0
sp_c_fraud_decline_attempt_h12_0
sp_c_fraud_decline_attempt_h1_0
bp_c_seed_based_frozen_linking_merchant_side_email
bp_c_seed_based_frozen_linking_all
bp_c_seed_based_frozen_linking_device_id
bp_c_seed_based_frozen_linking_gift_card_email
bp_c_seed_based_frozen_linking_payment_card
bp_c_seed_based_frozen_linking_profile
bp_c_seed_based_frozen_linking_shipping_address
bp_c_seed_based_linking_all
bp_c_seed_based_linking_device_id
bp_c_seed_based_linking_gift_card_email
bp_c_seed_based_linking_merchant_side_email
bp_c_seed_based_linking_payment_card
bp_c_seed_based_linking_profile
bp_c_seed_based_linking_shipping_address
sp_entity_linking_hop0_tot_order_cnt_by_merch_side_email_h72_0
sp_c_longest_time_spent_2fa_pass_sms_d7
sp_c_order_amt_same_merchant_as_current_h12_0
sp_c_order_amt_same_merchant_as_current_h1_0
sp_c_order_amt_same_merchant_as_current_h24_0
sp_c_order_amt_same_merchant_as_current_h6_0
sp_c_order_cnt_same_merchant_as_current_h12_0
sp_c_order_cnt_same_merchant_as_current_h1_0
sp_c_order_cnt_same_merchant_as_current_h24_0
sp_c_order_cnt_same_merchant_as_current_h6_0
sp_c_online_order_amount_h12_0
sp_c_online_order_amount_h1_0
sp_c_online_order_amount_h24_0
sp_c_online_order_amount_h6_0
sp_c_online_order_amount_h168_0
sp_c_online_order_count_h12_0
sp_c_online_order_count_h1_0
sp_c_online_order_count_h24_0
sp_c_online_order_count_h6_0
sp_c_online_order_count_h168_0
days_since_first_order_date
device_age_in_days
bp_c_merch_side_email_age_days
whitepages_identity_check_score
model_gibberish_merchant_side_email_august_2022_score
model_gibberish_consumer_profile_email_august_2022_score
bp_entity_linking_hop0_cnt_by_merch_side_email_d7
bp_entity_linking_hop0_tot_order_cnt_by_merch_side_email_d7
bp_entity_linking_hop0_tot_order_amt_by_merch_side_email_d1
bp_entity_linking_hop0_tot_order_amt_by_merch_side_email_d3
sp_c_online_ordr_attmpt_credit_card_cnt_h12_0
sp_c_pymt_attmpt_cnt_h1_0
sp_c_failed_pymt_cnt_h12_0
sp_c_manual_pymt_attmpt_cnt_h12_0
pymt_method_1st_attempt_timestamp
ip_to_profile_address_postcode_geo_distance_miles
consumer_postcode_to_shipping_address_postcode_geo_distance_miles_v2
ip_to_shipping_address_postcode_geo_distance_miles
model_online_cb_global_july_2022_score
model_online_ato_global_july_2022_score
model_online_ato_global_september_2022_score
model_online_cb_all_global_september_2022_score
num_days_to_booking
in_flight_card_name_vs_profile_name
bp_c_latest_approved_credit_limit
bp_c_latest_approved_credit_limit_v2
c_latest_login_2fa_success_timestamp
c_latest_pwd_reset_success_timestamp
c_latest_sms_login_2fa_success_timestamp
c_latest_email_login_2fa_success_timestamp
c_latest_email_pwd_reset_success_timestamp
sp_c_latest_profile_address_change_timestamp
c_latest_sms_pwd_reset_success_timestamp
c_latest_login_2fa_required_timestamp
sp_c_latest_profile_email_change_timestamp
sp_c_latest_profile_phone_number_change_timestamp
"""