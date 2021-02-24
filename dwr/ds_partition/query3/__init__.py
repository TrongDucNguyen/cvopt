# language=HQL
query3_create_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        d_year int, 
        brand_id string, 
        brand string, 
        sum_agg double
    )
    PARTITIONED BY (
        ds string
    )
"""

# language=HQL
query3_raw = """
    SELECT dt.d_year, 
           item.i_brand_id          brand_id, 
           item.i_brand             brand, 
           Sum(ss_ext_discount_amt) sum_agg 
    FROM   date_dim dt, 
           store_sales, 
           item 
    WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk 
           AND store_sales.ss_item_sk = item.i_item_sk 
           AND item.i_manufact_id = 427 
           AND dt.d_moy = 11 
    GROUP BY dt.d_year, 
              item.i_brand, 
              item.i_brand_id 
    ORDER BY dt.d_year, 
              sum_agg DESC, 
              brand_id
    LIMIT 100;
"""

# language=HQL
query3 = """
    INSERT OVERWRITE TABLE {table_name} PARTITION (ds = '{ds}')
    SELECT dt.d_year, 
           item.i_brand_id          brand_id, 
           item.i_brand             brand, 
           Sum(ss_ext_discount_amt) sum_agg 
    FROM   date_dim dt, 
           store_sales, 
           item 
    WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk 
           AND store_sales.ss_item_sk = item.i_item_sk 
           AND item.i_manufact_id = 427 
           AND dt.d_moy = 11
           AND store_sales.ds = '{ds}'
           AND item.ds = '{ds}'            
    GROUP BY dt.d_year, 
             item.i_brand, 
             item.i_brand_id     
"""

# language=HQL
query3_sampled = """
    INSERT OVERWRITE TABLE {table_name} PARTITION (ds = '{ds}')
    SELECT dt.d_year, 
           item.i_brand_id          brand_id, 
           item.i_brand             brand, 
           Sum(ss_ext_discount_amt / sample_rate) sum_agg 
    FROM   date_dim dt, 
           {store_sales}, 
           item 
    WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk 
           AND store_sales.ss_item_sk = item.i_item_sk 
           AND item.i_manufact_id = 427 
           AND dt.d_moy = 11
           AND store_sales.ds = '{ds}'
           AND item.ds = '{ds}'            
    GROUP BY dt.d_year, 
             item.i_brand, 
             item.i_brand_id     
"""
