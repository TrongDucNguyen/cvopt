# -------------------------------------
# Multiple aggregates - Single group by
# -------------------------------------

# constant
RESULT_TABLE = 'query5'
STORE_SALES = 'store_sales'
CATALOG_SALES = 'catalog_sales'
WEB_SALES = 'web_sales'

# language=HQL
query5_create_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        channel string,
        id string, 
        sales double,
        returns double, 
        profit double
    )
    PARTITIONED BY (
        ds string
    )
"""

# language=HQL
query5_raw = """
    -- start query 1 in stream 0 using template query5.tpl and seed 1819994127
    with ssr as
     (select s_store_id,
            sum(sales_price) as sales,
            sum(profit) as profit,
            sum(return_amt) as returns,
            sum(net_loss) as profit_loss
     from
      ( select  ss_store_sk as store_sk,
                ss_sold_date_sk  as date_sk,
                ss_ext_sales_price as sales_price,
                ss_net_profit as profit,
                cast(0 as decimal(7,2)) as return_amt,
                cast(0 as decimal(7,2)) as net_loss
        from store_sales
        union all
        select sr_store_sk as store_sk,
               sr_returned_date_sk as date_sk,
               cast(0 as decimal(7,2)) as sales_price,
               cast(0 as decimal(7,2)) as profit,
               sr_return_amt as return_amt,
               sr_net_loss as net_loss
        from store_returns
       ) salesreturns,
         date_dim,
         store
     where date_sk = d_date_sk
           and d_date between cast('1998-08-04' as date) 
                      and (cast('1998-08-04' as date) +  14 days)
           and store_sk = s_store_sk
     group by s_store_id)
     ,
     csr as
     (select cp_catalog_page_id,
            sum(sales_price) as sales,
            sum(profit) as profit,
            sum(return_amt) as returns,
            sum(net_loss) as profit_loss
     from
      ( select  cs_catalog_page_sk as page_sk,
                cs_sold_date_sk  as date_sk,
                cs_ext_sales_price as sales_price,
                cs_net_profit as profit,
                cast(0 as decimal(7,2)) as return_amt,
                cast(0 as decimal(7,2)) as net_loss
        from catalog_sales
        union all
        select cr_catalog_page_sk as page_sk,
               cr_returned_date_sk as date_sk,
               cast(0 as decimal(7,2)) as sales_price,
               cast(0 as decimal(7,2)) as profit,
               cr_return_amount as return_amt,
               cr_net_loss as net_loss
        from catalog_returns
       ) salesreturns,
         date_dim,
         catalog_page
     where date_sk = d_date_sk
           and d_date between cast('1998-08-04' as date)
                      and (cast('1998-08-04' as date) +  14 days)
           and page_sk = cp_catalog_page_sk
     group by cp_catalog_page_id)
     ,
     wsr as
     (select web_site_id,
            sum(sales_price) as sales,
            sum(profit) as profit,
            sum(return_amt) as returns,
            sum(net_loss) as profit_loss
     from
      ( select  ws_web_site_sk as wsr_web_site_sk,
                ws_sold_date_sk  as date_sk,
                ws_ext_sales_price as sales_price,
                ws_net_profit as profit,
                cast(0 as decimal(7,2)) as return_amt,
                cast(0 as decimal(7,2)) as net_loss
        from web_sales
        union all
        select ws_web_site_sk as wsr_web_site_sk,
               wr_returned_date_sk as date_sk,
               cast(0 as decimal(7,2)) as sales_price,
               cast(0 as decimal(7,2)) as profit,
               wr_return_amt as return_amt,
               wr_net_loss as net_loss
        from web_returns left outer join web_sales on
             ( wr_item_sk = ws_item_sk
               and wr_order_number = ws_order_number)
       ) salesreturns,
         date_dim,
         web_site
     where date_sk = d_date_sk
           and d_date between cast('1998-08-04' as date)
                      and (cast('1998-08-04' as date) +  14 days)
           and wsr_web_site_sk = web_site_sk
     group by web_site_id)
      select  channel
            , id
            , sum(sales) as sales
            , sum(returns) as returns
            , sum(profit) as profit
     from 
     (select 'store channel' as channel
            , 'store' || s_store_id as id
            , sales
            , returns
            , (profit - profit_loss) as profit
     from   ssr
     union all
     select 'catalog channel' as channel
            , 'catalog_page' || cp_catalog_page_id as id
            , sales
            , returns
            , (profit - profit_loss) as profit
     from  csr
     union all
     select 'web channel' as channel
            , 'web_site' || web_site_id as id
            , sales
            , returns
            , (profit - profit_loss) as profit
     from   wsr
     ) x
     group by rollup (channel, id)
     order by channel
             ,id
     limit 100;
    -- end query 1 in stream 0 using template query5.tpl
"""

# language=HQL
query5 = """
    WITH ssr AS (
        SELECT s_store_id,
            sum(sales_price) AS sales,
            sum(profit) AS profit,
            sum(return_amt) AS returns,
            sum(net_loss) AS profit_loss
        FROM (
            SELECT ss_store_sk AS store_sk,
                ss_sold_date_sk AS date_sk,
                ss_ext_sales_price AS sales_price,
                ss_net_profit AS profit,
                cast(0 AS DECIMAL(7, 2)) AS return_amt,
                cast(0 AS DECIMAL(7, 2)) AS net_loss
            FROM store_sales
            WHERE ds = '{ds}'
            
            UNION ALL
            
            SELECT sr_store_sk AS store_sk,
                sr_returned_date_sk AS date_sk,
                cast(0 AS DECIMAL(7, 2)) AS sales_price,
                cast(0 AS DECIMAL(7, 2)) AS profit,
                sr_return_amt AS return_amt,
                sr_net_loss AS net_loss
            FROM store_returns
            WHERE ds = '{ds}'
            ) salesreturns,
            date_dim,
            store
        WHERE date_sk = d_date_sk
            AND d_date BETWEEN cast('1998-08-04' AS DATE) AND (cast('1998-08-04' AS DATE) + 14 days)
            AND store_sk = s_store_sk
            AND store.ds = '{ds}'
        GROUP BY s_store_id
    ),
    csr AS (
        SELECT cp_catalog_page_id,
            sum(sales_price) AS sales,
            sum(profit) AS profit,
            sum(return_amt) AS returns,
            sum(net_loss) AS profit_loss
        FROM (
            SELECT cs_catalog_page_sk AS page_sk,
                cs_sold_date_sk AS date_sk,
                cs_ext_sales_price AS sales_price,
                cs_net_profit AS profit,
                cast(0 AS DECIMAL(7, 2)) AS return_amt,
                cast(0 AS DECIMAL(7, 2)) AS net_loss
            FROM catalog_sales
            WHERE ds = '{ds}'
            
            UNION ALL
            
            SELECT cr_catalog_page_sk AS page_sk,
                cr_returned_date_sk AS date_sk,
                cast(0 AS DECIMAL(7, 2)) AS sales_price,
                cast(0 AS DECIMAL(7, 2)) AS profit,
                cr_return_amount AS return_amt,
                cr_net_loss AS net_loss
            FROM catalog_returns
            WHERE ds = '{ds}'
            ) salesreturns,
            date_dim,
            catalog_page
        WHERE date_sk = d_date_sk
            AND d_date BETWEEN cast('1998-08-04' AS DATE) AND (cast('1998-08-04' AS DATE) + 14 days)
            AND page_sk = cp_catalog_page_sk
            AND catalog_page.ds = '{ds}'
        GROUP BY cp_catalog_page_id
    ),
    wsr AS (
        SELECT web_site_id,
            sum(sales_price) AS sales,
            sum(profit) AS profit,
            sum(return_amt) AS returns,
            sum(net_loss) AS profit_loss
        FROM (
            SELECT ws_web_site_sk AS wsr_web_site_sk,
                ws_sold_date_sk AS date_sk,
                ws_ext_sales_price AS sales_price,
                ws_net_profit AS profit,
                cast(0 AS DECIMAL(7, 2)) AS return_amt,
                cast(0 AS DECIMAL(7, 2)) AS net_loss
            FROM web_sales
            WHERE ds = '{ds}'
            
            UNION ALL
            
            SELECT ws_web_site_sk AS wsr_web_site_sk,
                wr_returned_date_sk AS date_sk,
                cast(0 AS DECIMAL(7, 2)) AS sales_price,
                cast(0 AS DECIMAL(7, 2)) AS profit,
                wr_return_amt AS return_amt,
                wr_net_loss AS net_loss
            FROM web_returns
            LEFT OUTER JOIN web_sales 
            ON wr_item_sk = ws_item_sk
                AND wr_order_number = ws_order_number
                AND web_sales.ds = '{ds}'
            WHERE web_returns.ds = '{ds}'
        ) salesreturns,
        date_dim,
        web_site
        WHERE date_sk = d_date_sk
            AND d_date BETWEEN cast('1998-08-04' AS DATE) AND (cast('1998-08-04' AS DATE) + 14 days)
            AND wsr_web_site_sk = web_site_sk
            AND web_site.ds = '{ds}'
        GROUP BY web_site_id
    )
    SELECT channel,
        id,
        sum(sales) AS sales,
        sum(returns) AS returns,
        sum(profit) AS profit
    FROM (
        SELECT 'store channel' AS channel,
            'store' || s_store_id AS id,
            sales,
            returns,
            (profit - profit_loss) AS profit
        FROM ssr
        
        UNION ALL
        
        SELECT 'catalog channel' AS channel,
            'catalog_page' || cp_catalog_page_id AS id,
            sales,
            returns,
            (profit - profit_loss) AS profit
        FROM csr
        
        UNION ALL
        
        SELECT 'web channel' AS channel,
            'web_site' || web_site_id AS id,
            sales,
            returns,
            (profit - profit_loss) AS profit
        FROM wsr
        ) x
    GROUP BY rollup(channel, id)    
"""

# language=HQL
query5_sampled = """
    WITH ssr AS (
        SELECT s_store_id,
            sum(sales_price / sample_rate) AS sales,
            sum(profit / sample_rate) AS profit,
            sum(return_amt / sample_rate) AS returns,
            sum(net_loss / sample_rate) AS profit_loss
        FROM (
            SELECT ss_store_sk AS store_sk,
                ss_sold_date_sk AS date_sk,
                ss_ext_sales_price AS sales_price,
                ss_net_profit AS profit,
                cast(0 AS DECIMAL(7, 2)) AS return_amt,
                cast(0 AS DECIMAL(7, 2)) AS net_loss,
                sample_rate
            FROM {store_sales}
            WHERE ds = '{ds}'

            UNION ALL

            SELECT sr_store_sk AS store_sk,
                sr_returned_date_sk AS date_sk,
                cast(0 AS DECIMAL(7, 2)) AS sales_price,
                cast(0 AS DECIMAL(7, 2)) AS profit,
                sr_return_amt AS return_amt,
                sr_net_loss AS net_loss,
                1.0 as sample_rate
            FROM store_returns
            WHERE ds = '{ds}'
        ) salesreturns,
        date_dim,
        store
        WHERE date_sk = d_date_sk
            AND d_date BETWEEN cast('1998-08-04' AS DATE) AND (cast('1998-08-04' AS DATE) + 14 days)
            AND store_sk = s_store_sk
            AND store.ds = '{ds}'
        GROUP BY s_store_id
    ),
    csr AS (
        SELECT cp_catalog_page_id,
            sum(sales_price / sample_rate) AS sales,
            sum(profit / sample_rate) AS profit,
            sum(return_amt / sample_rate) AS returns,
            sum(net_loss / sample_rate) AS profit_loss
        FROM (
            SELECT cs_catalog_page_sk AS page_sk,
                cs_sold_date_sk AS date_sk,
                cs_ext_sales_price AS sales_price,
                cs_net_profit AS profit,
                cast(0 AS DECIMAL(7, 2)) AS return_amt,
                cast(0 AS DECIMAL(7, 2)) AS net_loss,
                sample_rate
            FROM {catalog_sales}
            WHERE ds = '{ds}'

            UNION ALL

            SELECT cr_catalog_page_sk AS page_sk,
                cr_returned_date_sk AS date_sk,
                cast(0 AS DECIMAL(7, 2)) AS sales_price,
                cast(0 AS DECIMAL(7, 2)) AS profit,
                cr_return_amount AS return_amt,
                cr_net_loss AS net_loss,
                1.0 as sample_rate
            FROM catalog_returns
            WHERE ds = '{ds}'
        ) salesreturns,
        date_dim,
        catalog_page
        WHERE date_sk = d_date_sk
            AND d_date BETWEEN cast('1998-08-04' AS DATE) AND (cast('1998-08-04' AS DATE) + 14 days)
            AND page_sk = cp_catalog_page_sk
            AND catalog_page.ds = '{ds}'
        GROUP BY cp_catalog_page_id
    ),
    wsr AS (
        SELECT web_site_id,
            sum(sales_price / sample_rate) AS sales,
            sum(profit / sample_rate) AS profit,
            sum(return_amt / sample_rate) AS returns,
            sum(net_loss / sample_rate) AS profit_loss
        FROM (
            SELECT ws_web_site_sk AS wsr_web_site_sk,
                ws_sold_date_sk AS date_sk,
                ws_ext_sales_price AS sales_price,
                ws_net_profit AS profit,
                cast(0 AS DECIMAL(7, 2)) AS return_amt,
                cast(0 AS DECIMAL(7, 2)) AS net_loss,
                sample_rate
            FROM {web_sales}
            WHERE ds = '{ds}'

            UNION ALL

            SELECT ws_web_site_sk AS wsr_web_site_sk,
                wr_returned_date_sk AS date_sk,
                cast(0 AS DECIMAL(7, 2)) AS sales_price,
                cast(0 AS DECIMAL(7, 2)) AS profit,
                wr_return_amt AS return_amt,
                wr_net_loss AS net_loss,
                1.0 sample_rate
            FROM web_returns
            LEFT OUTER JOIN web_sales 
            ON wr_item_sk = ws_item_sk
                AND wr_order_number = ws_order_number
                AND web_sales.ds = '{ds}'
            WHERE web_returns.ds = '{ds}'
        ) salesreturns,
        date_dim,
        web_site
        WHERE date_sk = d_date_sk
            AND d_date BETWEEN cast('1998-08-04' AS DATE) AND (cast('1998-08-04' AS DATE) + 14 days)
            AND wsr_web_site_sk = web_site_sk
            AND web_site.ds = '{ds}'
        GROUP BY web_site_id
    )
    SELECT channel,
        id,
        sum(sales) AS sales,
        sum(returns) AS returns,
        sum(profit) AS profit
    FROM (
        SELECT 'store channel' AS channel,
            'store' || s_store_id AS id,
            sales,
            returns,
            (profit - profit_loss) AS profit
        FROM ssr

        UNION ALL

        SELECT 'catalog channel' AS channel,
            'catalog_page' || cp_catalog_page_id AS id,
            sales,
            returns,
            (profit - profit_loss) AS profit
        FROM csr

        UNION ALL

        SELECT 'web channel' AS channel,
            'web_site' || web_site_id AS id,
            sales,
            returns,
            (profit - profit_loss) AS profit
        FROM wsr
        ) x
    GROUP BY rollup(channel, id)    
"""
