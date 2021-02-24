from dwr import *
from dwr.ds_partition.query7 import *

SAMPLE_TYPE = VOILA

for DS in ds_list:
    logging.info("DS: {0}".format(DS))

    logging.info("Get statistic from data")
    # language=HQL
    hiveql.execute("""
        SELECT
            i_item_id,
            COUNT(*),
            ABS(STDDEV(ss_quantity) / AVG(ss_quantity)),                    
            ABS(STDDEV(ss_list_price) / AVG(ss_list_price)),                    
            ABS(STDDEV(ss_coupon_amt) / AVG(ss_coupon_amt)),                    
            ABS(STDDEV(ss_sales_price) / AVG(ss_sales_price))   
        FROM store_sales
        JOIN item
            ON ss_item_sk = i_item_sk       
            AND item.ds = '{ds}'
        WHERE store_sales.ds = '{ds}'
        GROUP BY i_item_id
    """.format(ds=DS))
    result = hiveql.fetchall()
    frequency = {r[0]: r[1] for r in result}
    coefficient = {r[0]: r[2:6] for r in result}

    for sample_rate in sample_rates:
        logging.info("DS: {0}        sample rate: {1}".format(DS, sample_rate))

        # senate sample for web sales
        store_sales_sampled = masg_sample(
            sample_rate=sample_rate,
            table_name=STORE_SALES,
            frequency=frequency,
            coefficient=coefficient,
            partition={'ds': DS},
            # language=HQL
            select="""
                SELECT *
                FROM {table_name}
                JOIN (
                    SELECT *
                    FROM item
                    WHERE ds = '{ds}'
                ) x
                ON ss_item_sk = i_item_sk
                LEFT JOIN (
                    SELECT stratum,
                           sample_rate
                    FROM   voila_allocation
                    WHERE  table_name = '{table_name}'
                       AND ds = '{ds}'                
                       AND default_rate = {sample_rate}                                     
                ) z
                ON i_item_id = stratum 
                WHERE {table_name}.ds = '{ds}'
                AND   Rand() <= COALESCE(sample_rate, '{sample_rate}')                 
            """.format(
                table_name=STORE_SALES,
                sample_rate=sample_rate,
                ds=DS,
            ),
            overwrite=True,
        )

        # create result table
        sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate, 'ds')
        hiveql.execute(query7_create_table.format(sample_result_table))

        # run query over sample table
        hiveql.execute(query7.format(
            ds=DS,
            table_name=sample_result_table,
            store_sales=store_sales_sampled,
        ))

        sample_evaluate(
            table_name=RESULT_TABLE,
            sample_type=SAMPLE_TYPE,
            sample_rate=sample_rate,
            group_by_columns='i_item_id',
            aggregation_columns=['agg1', 'agg2', 'agg3', 'agg4'],
            partition={'ds': DS},
        )
