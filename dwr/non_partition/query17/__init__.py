# ---------------
# query 17 - MASG
# ---------------

RESULT_TABLE = 'query17'

STORE_SALES = 'store_sales'

# language=HQL
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        i_item_id string,
        i_item_desc string,
        s_state string,
        store_sales_quantitycount int,
        store_sales_quantityave double,
        store_sales_quantitystdev double,
        store_sales_quantitycov double,
        store_returns_quantitycount int,
        store_returns_quantityave double,
        store_returns_quantitystdev double,
        store_returns_quantitycov double,
        catalog_sales_quantitycount int,
        catalog_sales_quantityave double,
        catalog_sales_quantitystdev double,
        catalog_sales_quantitycov double
    )
"""

# language=HQL
query17_raw = """
-- start query 1 in stream 0 using template query17.tpl and seed 1819994127
select  i_item_id
       ,i_item_desc
       ,s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       ,count(sr_return_quantity) as_store_returns_quantitycount
       ,avg(sr_return_quantity) as_store_returns_quantityave
       ,stddev_samp(sr_return_quantity) as_store_returns_quantitystdev
       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitystdev
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
 from store_sales
     ,store_returns
     ,catalog_sales
     ,date_dim d1
     ,date_dim d2
     ,date_dim d3
     ,store
     ,item
 where d1.d_quarter_name = '2000Q1'
   and d1.d_date_sk = ss_sold_date_sk
   and i_item_sk = ss_item_sk
   and s_store_sk = ss_store_sk
   and ss_customer_sk = sr_customer_sk
   and ss_item_sk = sr_item_sk
   and ss_ticket_number = sr_ticket_number
   and sr_returned_date_sk = d2.d_date_sk
   and d2.d_quarter_name in ('2000Q1','2000Q2','2000Q3')
   and sr_customer_sk = cs_bill_customer_sk
   and sr_item_sk = cs_item_sk
   and cs_sold_date_sk = d3.d_date_sk
   and d3.d_quarter_name in ('2000Q1','2000Q2','2000Q3')
 group by i_item_id
         ,i_item_desc
         ,s_state
 order by i_item_id
         ,i_item_desc
         ,s_state
limit 100;

-- end query 1 in stream 0 using template query17.tpl
"""

# language=HQL
query17 = """
    INSERT OVERWRITE TABLE {table_name}
    SELECT i_item_id,
        i_item_desc,
        s_state,
        
        count(ss_quantity)                          AS store_sales_quantitycount,
        avg(ss_quantity)                            AS store_sales_quantityave,
        stddev_samp(ss_quantity)                    AS store_sales_quantitystdev,
        stddev_samp(ss_quantity) / avg(ss_quantity) AS store_sales_quantitycov,
        
        count(sr_return_quantity)                                   AS store_returns_quantitycount,
        avg(sr_return_quantity)                                     AS store_returns_quantityave,
        stddev_samp(sr_return_quantity)                             AS store_returns_quantitystdev,
        stddev_samp(sr_return_quantity) / avg(sr_return_quantity)   AS store_returns_quantitycov,
        
        count(cs_quantity)                          AS catalog_sales_quantitycount,
        avg(cs_quantity)                            AS catalog_sales_quantityave,
        stddev_samp(cs_quantity) / avg(cs_quantity) AS catalog_sales_quantitystdev,
        stddev_samp(cs_quantity) / avg(cs_quantity) AS catalog_sales_quantitycov
    FROM store_sales,
        store_returns,
        catalog_sales,
        date_dim d1,
        date_dim d2,
        date_dim d3,
        store,
        item
    WHERE d1.d_quarter_name = '2000Q1'
        AND d1.d_date_sk = ss_sold_date_sk
        AND i_item_sk = ss_item_sk
        AND s_store_sk = ss_store_sk
        AND ss_customer_sk = sr_customer_sk
        AND ss_item_sk = sr_item_sk
        AND ss_ticket_number = sr_ticket_number
        AND sr_returned_date_sk = d2.d_date_sk
        AND d2.d_quarter_name IN ('2000Q1', '2000Q2', '2000Q3')
        AND sr_customer_sk = cs_bill_customer_sk
        AND sr_item_sk = cs_item_sk
        AND cs_sold_date_sk = d3.d_date_sk
        AND d3.d_quarter_name IN ('2000Q1', '2000Q2', '2000Q3')
    GROUP BY i_item_id,
        i_item_desc,
        s_state
"""

# language=HQL
query17_sampled = """
    INSERT OVERWRITE TABLE {table_name}
    SELECT i_item_id,
        i_item_desc,
        s_state,

        CAST(sum(IF(ss_quantity IS NULL, 0.0, 1.0 / sample_rate)) AS int) AS store_sales_quantitycount,
        avg(ss_quantity)                                                     AS store_sales_quantityave,
        stddev_samp(ss_quantity)                                             AS store_sales_quantitystdev,
        stddev_samp(ss_quantity) / avg(ss_quantity)                          AS store_sales_quantitycov,

        count(sr_return_quantity)                                   AS store_returns_quantitycount,
        avg(sr_return_quantity)                                     AS store_returns_quantityave,
        stddev_samp(sr_return_quantity)                             AS store_returns_quantitystdev,
        stddev_samp(sr_return_quantity) / avg(sr_return_quantity)   AS store_returns_quantitycov,

        count(cs_quantity)                          AS catalog_sales_quantitycount,
        avg(cs_quantity)                            AS catalog_sales_quantityave,
        stddev_samp(cs_quantity) / avg(cs_quantity) AS catalog_sales_quantitystdev,
        stddev_samp(cs_quantity) / avg(cs_quantity) AS catalog_sales_quantitycov
    FROM {store_sales},
        store_returns,
        catalog_sales,
        date_dim d1,
        date_dim d2,
        date_dim d3,
        store,
        item
    WHERE d1.d_quarter_name = '2000Q1'
        AND d1.d_date_sk = ss_sold_date_sk
        AND i_item_sk = ss_item_sk
        AND s_store_sk = ss_store_sk
        AND ss_customer_sk = sr_customer_sk
        AND ss_item_sk = sr_item_sk
        AND ss_ticket_number = sr_ticket_number
        AND sr_returned_date_sk = d2.d_date_sk
        AND d2.d_quarter_name IN ('2000Q1', '2000Q2', '2000Q3')
        AND sr_customer_sk = cs_bill_customer_sk
        AND sr_item_sk = cs_item_sk
        AND cs_sold_date_sk = d3.d_date_sk
        AND d3.d_quarter_name IN ('2000Q1', '2000Q2', '2000Q3')
    GROUP BY i_item_id,
        i_item_desc,
        s_state
"""
