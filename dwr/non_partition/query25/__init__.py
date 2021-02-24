# ---------------
# query 25 - MASG
# ---------------

RESULT_TABLE = 'query25'

# language=HQL
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        i_item_id string,
        i_item_desc string,
        s_store_id string,
        s_store_name string,
        store_sales_profit double,
        store_returns_loss double,
        catalog_sales_profit double
    )
"""

# language=HQL
query25_raw = """
    -- start query 1 in stream 0 using template query25.tpl and seed 1819994127
SELECT i_item_id,
    i_item_desc,
    s_store_id,
    s_store_name,
    sum(ss_net_profit) AS store_sales_profit,
    sum(sr_net_loss) AS store_returns_loss,
    sum(cs_net_profit) AS catalog_sales_profit
FROM store_sales,
    store_returns,
    catalog_sales,
    date_dim d1,
    date_dim d2,
    date_dim d3,
    store,
    item
WHERE d1.d_moy = 4
    AND d1.d_year = 2000
    AND d1.d_date_sk = ss_sold_date_sk
    AND i_item_sk = ss_item_sk
    AND s_store_sk = ss_store_sk
    AND ss_customer_sk = sr_customer_sk
    AND ss_item_sk = sr_item_sk
    AND ss_ticket_number = sr_ticket_number
    AND sr_returned_date_sk = d2.d_date_sk
    AND d2.d_moy BETWEEN 4
        AND 10
    AND d2.d_year = 2000
    AND sr_customer_sk = cs_bill_customer_sk
    AND sr_item_sk = cs_item_sk
    AND cs_sold_date_sk = d3.d_date_sk
    AND d3.d_moy BETWEEN 4
        AND 10
    AND d3.d_year = 2000
GROUP BY i_item_id,
    i_item_desc,
    s_store_id,
    s_store_name
ORDER BY i_item_id,
    i_item_desc,
    s_store_id,
    s_store_name limit 100;
    -- end query 1 in stream 0 using template query25.tpl
"""

# language=HQL
query25 = """
    INSERT OVERWRITE TABLE {table_name}
    SELECT i_item_id,
        i_item_desc,
        s_store_id,
        s_store_name,
        sum(ss_net_profit) AS store_sales_profit,
        sum(sr_net_loss) AS store_returns_loss,
        sum(cs_net_profit) AS catalog_sales_profit
    FROM store_sales,
        store_returns,
        catalog_sales,
        date_dim d1,
        date_dim d2,
        date_dim d3,
        store,
        item
    WHERE d1.d_moy = 4
        AND d1.d_year = 2000
        AND d1.d_date_sk = ss_sold_date_sk
        AND i_item_sk = ss_item_sk
        AND s_store_sk = ss_store_sk
        AND ss_customer_sk = sr_customer_sk
        AND ss_item_sk = sr_item_sk
        AND ss_ticket_number = sr_ticket_number
        AND sr_returned_date_sk = d2.d_date_sk
        AND d2.d_moy BETWEEN 4 AND 10
        AND d2.d_year = 2000
        AND sr_customer_sk = cs_bill_customer_sk
        AND sr_item_sk = cs_item_sk
        AND cs_sold_date_sk = d3.d_date_sk
        AND d3.d_moy BETWEEN 4 AND 10
        AND d3.d_year = 2000
    GROUP BY i_item_id,
        i_item_desc,
        s_store_id,
        s_store_name
"""

# language=HQL
query25_sampled = """
    INSERT OVERWRITE TABLE {table_name}
    SELECT i_item_id,
        i_item_desc,
        s_store_id,
        s_store_name,
        sum(ss_net_profit / sample_rate) AS store_sales_profit,
        sum(sr_net_loss) AS store_returns_loss,
        sum(cs_net_profit) AS catalog_sales_profit
    FROM store_sales,
        store_returns,
        catalog_sales,
        date_dim d1,
        date_dim d2,
        date_dim d3,
        store,
        item
    WHERE d1.d_moy = 4
        AND d1.d_year = 2000
        AND d1.d_date_sk = ss_sold_date_sk
        AND i_item_sk = ss_item_sk
        AND s_store_sk = ss_store_sk
        AND ss_customer_sk = sr_customer_sk
        AND ss_item_sk = sr_item_sk
        AND ss_ticket_number = sr_ticket_number
        AND sr_returned_date_sk = d2.d_date_sk
        AND d2.d_moy BETWEEN 4 AND 10
        AND d2.d_year = 2000
        AND sr_customer_sk = cs_bill_customer_sk
        AND sr_item_sk = cs_item_sk
        AND cs_sold_date_sk = d3.d_date_sk
        AND d3.d_moy BETWEEN 4 AND 10
        AND d3.d_year = 2000
    GROUP BY i_item_id,
        i_item_desc,
        s_store_id,
        s_store_name
"""
