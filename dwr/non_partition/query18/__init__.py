# ---------------
# query 18 - MAMG
# ---------------

RESULT_TABLE = 'query18'

CATALOG_SALES = 'catalog_sales'

# language=HQL
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        i_item_id string,
        ca_country string,
        ca_state string,
        ca_county string,
        agg1 double,
        agg2 double,
        agg3 double,
        agg4 double,
        agg5 double,
        agg6 double,
        agg7 double
    )
"""

# language=HQL
query18_raw = """
    -- start query 1 in stream 0 using template query18.tpl and seed 1978355063
    select  i_item_id,
            ca_country,
            ca_state, 
            ca_county,
            avg( cast(cs_quantity as numeric(12,2))) agg1,
            avg( cast(cs_list_price as numeric(12,2))) agg2,
            avg( cast(cs_coupon_amt as numeric(12,2))) agg3,
            avg( cast(cs_sales_price as numeric(12,2))) agg4,
            avg( cast(cs_net_profit as numeric(12,2))) agg5,
            avg( cast(c_birth_year as numeric(12,2))) agg6,
            avg( cast(cd1.cd_dep_count as numeric(12,2))) agg7
     from catalog_sales, customer_demographics cd1, 
          customer_demographics cd2, customer, customer_address, date_dim, item
     where cs_sold_date_sk = d_date_sk and
           cs_item_sk = i_item_sk and
           cs_bill_cdemo_sk = cd1.cd_demo_sk and
           cs_bill_customer_sk = c_customer_sk and
           cd1.cd_gender = 'M' and 
           cd1.cd_education_status = 'College' and
           c_current_cdemo_sk = cd2.cd_demo_sk and
           c_current_addr_sk = ca_address_sk and
           c_birth_month in (9,5,12,4,1,10) and
           d_year = 2001 and
           ca_state in ('ND','WI','AL'
                       ,'NC','OK','MS','TN')
     group by rollup (i_item_id, ca_country, ca_state, ca_county)
     order by ca_country,
            ca_state, 
            ca_county,
        i_item_id
     limit 100;
    
    -- end query 1 in stream 0 using template query18.tpl
"""

# language=HQL
query18 = """
    INSERT OVERWRITE TABLE {table_name}
    SELECT i_item_id,
        ca_country,
        ca_state,
        ca_county,
        avg(cs_quantity) agg1,
        avg(cs_list_price) agg2,
        avg(cs_coupon_amt) agg3,
        avg(cs_sales_price) agg4,
        avg(cs_net_profit) agg5,
        avg(c_birth_year) agg6,
        avg(cd1.cd_dep_count) agg7
    FROM {catalog_sales},
        customer_demographics cd1,
        customer_demographics cd2,
        customer,
        customer_address,
        date_dim,
        item
    WHERE cs_sold_date_sk = d_date_sk
        AND cs_item_sk = i_item_sk
        AND cs_bill_cdemo_sk = cd1.cd_demo_sk
        AND cs_bill_customer_sk = c_customer_sk
        AND cd1.cd_gender = 'M'
        AND cd1.cd_education_status = 'College'
        AND c_current_cdemo_sk = cd2.cd_demo_sk
        AND c_current_addr_sk = ca_address_sk
        AND c_birth_month IN (9, 5, 12, 4, 1, 10)
        AND d_year = 2001
        AND ca_state IN ('ND', 'WI', 'AL', 'NC', 'OK', 'MS', 'TN')
    GROUP BY rollup(i_item_id, ca_country, ca_state, ca_county)
"""
