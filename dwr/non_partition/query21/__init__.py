# ---------------
# query 21 - SASG
# ---------------

RESULT_TABLE = 'query21'

INVENTORY = 'inventory'

# language=HQL
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        w_warehouse_name string,
        i_item_id string,
        inv_before double,
        inv_after double
    )
"""

# language=HQL
query21_raw = """
-- start query 1 in stream 0 using template query21.tpl and seed 1819994127
select  *
 from(select w_warehouse_name
            ,i_item_id
            ,sum(case when (cast(d_date as date) < cast ('1998-04-08' as date))
	                then inv_quantity_on_hand 
                      else 0 end) as inv_before
            ,sum(case when (cast(d_date as date) >= cast ('1998-04-08' as date))
                      then inv_quantity_on_hand 
                      else 0 end) as inv_after
   from inventory
       ,warehouse
       ,item
       ,date_dim
   where i_current_price between 0.99 and 1.49
     and i_item_sk          = inv_item_sk
     and inv_warehouse_sk   = w_warehouse_sk
     and inv_date_sk    = d_date_sk
     and d_date between (cast ('1998-04-08' as date) - 30 days)
                    and (cast ('1998-04-08' as date) + 30 days)
   group by w_warehouse_name, i_item_id) x
 where (case when inv_before > 0 
             then inv_after / inv_before 
             else null
             end) between 2.0/3.0 and 3.0/2.0
 order by w_warehouse_name
         ,i_item_id
 limit 100;

-- end query 1 in stream 0 using template query21.tpl
"""

# language=HQL
query21 = """
    INSERT OVERWRITE TABLE {table_name}
    SELECT *
    FROM (
        SELECT w_warehouse_name,
            i_item_id,
            sum(CASE 
                    WHEN (cast(d_date AS DATE) < cast('1998-04-08' AS DATE))
                        THEN inv_quantity_on_hand
                    ELSE 0
                    END) AS inv_before,
            sum(CASE 
                    WHEN (cast(d_date AS DATE) >= cast('1998-04-08' AS DATE))
                        THEN inv_quantity_on_hand
                    ELSE 0
                    END) AS inv_after
        FROM inventory,
            warehouse,
            item,
            date_dim
        WHERE i_current_price BETWEEN 0.99
                AND 1.49
            AND i_item_sk = inv_item_sk
            AND inv_warehouse_sk = w_warehouse_sk
            AND inv_date_sk = d_date_sk
            AND d_date BETWEEN (cast('1998-04-08' AS DATE) - 30 days)
                AND (cast('1998-04-08' AS DATE) + 30 days)
        GROUP BY w_warehouse_name,
            i_item_id
        ) x
    WHERE (
            CASE 
                WHEN inv_before > 0
                    THEN inv_after / inv_before
                ELSE NULL
                END
            ) BETWEEN 2.0 / 3.0
            AND 3.0 / 2.0
"""

# language=HQL
query21_sampled = """
    INSERT OVERWRITE TABLE {table_name}
    SELECT *
    FROM (
        SELECT w_warehouse_name,
            i_item_id,
            sum(CASE 
                    WHEN (cast(d_date AS DATE) < cast('1998-04-08' AS DATE))
                        THEN inv_quantity_on_hand / sample_rate
                    ELSE 0
                    END) AS inv_before,
            sum(CASE 
                    WHEN (cast(d_date AS DATE) >= cast('1998-04-08' AS DATE))
                        THEN inv_quantity_on_hand / sample_rate
                    ELSE 0
                    END) AS inv_after
        FROM {inventory},
            warehouse,
            item,
            date_dim
        WHERE i_current_price BETWEEN 0.99
                AND 1.49
            AND i_item_sk = inv_item_sk
            AND inv_warehouse_sk = w_warehouse_sk
            AND inv_date_sk = d_date_sk
            AND d_date BETWEEN (cast('1998-04-08' AS DATE) - 30 days)
                AND (cast('1998-04-08' AS DATE) + 30 days)
        GROUP BY w_warehouse_name,
            i_item_id
        ) x
    WHERE (
            CASE 
                WHEN inv_before > 0
                    THEN inv_after / inv_before
                ELSE NULL
                END
            ) BETWEEN 2.0 / 3.0
            AND 3.0 / 2.0
"""
