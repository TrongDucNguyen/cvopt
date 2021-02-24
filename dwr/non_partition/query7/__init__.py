# ---------------------
# MASG - query 7 TPC_DS
# ---------------------

RESULT_TABLE = 'query7'

STORE_SALES = 'store_sales'

# language=HQL
query7_create_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        i_item_id string,
        agg1 double,
        agg2 double,
        agg3 double,
        agg4 double
    )
"""

# language=HQL
query7_raw = """
    -- start query 1 in stream 0 using template query7.tpl and seed 1930872976
    select  i_item_id, 
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4 
    from store_sales, customer_demographics, date_dim, item, promotion
    where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_cdemo_sk = cd_demo_sk and
       ss_promo_sk = p_promo_sk and
       cd_gender = 'F' and 
       cd_marital_status = 'W' and
       cd_education_status = 'Primary' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 1998 
    group by i_item_id
    order by i_item_id
    limit 100;
"""

# language=HQL
query7 = """    
    INSERT OVERWRITE TABLE {table_name}
    SELECT i_item_id,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
    FROM {store_sales},
        customer_demographics,
        date_dim,
        item,
        promotion
    WHERE ss_sold_date_sk = d_date_sk
        AND ss_item_sk = i_item_sk
        AND ss_cdemo_sk = cd_demo_sk
        AND ss_promo_sk = p_promo_sk
        AND cd_gender = 'F'
        AND cd_marital_status = 'W'
        AND cd_education_status = 'Primary'
        AND (p_channel_email = 'N' OR p_channel_event = 'N')
        AND d_year = 1998
    GROUP BY i_item_id
"""
