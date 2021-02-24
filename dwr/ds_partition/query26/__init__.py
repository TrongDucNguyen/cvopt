# ds list and sample rate parameters
#ds_list = ["2018-08-{:02}".format(i) for i in range(1,2)]
ds_list = ['10gb']
#ds_list = ['2018-08-01']

sample_rates = [0.1,0.15,0.25,0.3]

# constant names
CATALOG_SALES = 'catalog_sales'

# language=HQL
query26_create_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        i_item_id string,
        agg1 double,
        agg2 double,
        agg3 double,
        agg4 double
    )
    PARTITIONED BY (
        ds string
    )
"""

# language=HQL
query26_sampled = query26 = """  
 INSERT OVERWRITE TABLE {query26_table} PARTITION (ds = '{ds}')
 SELECT i_item_id, 
               Avg(cs_quantity)    agg1,
               Avg(cs_list_price)  agg2, 
               Avg(cs_coupon_amt)  agg3, 
               Avg(cs_sales_price) agg4 

FROM   {catalog_sales_table}, 
       customer_demographics, 
       date_dim, 
       item, 
       promotion 
WHERE  {catalog_sales_table}.ds = '{ds}'
        AND item.ds ='{ds}'
	   AND cs_sold_date_sk = d_date_sk 
       AND cs_item_sk = i_item_sk 
       AND cs_bill_cdemo_sk = cd_demo_sk 
       AND cs_promo_sk = p_promo_sk 
       AND cd_gender = 'F' 
       AND cd_marital_status = 'W' 
       AND cd_education_status = 'Secondary' 
       AND ( p_channel_email = 'N' 
              OR p_channel_event = 'N' ) 
       AND d_year = 2000 
GROUP  BY i_item_id 
"""