from dwr import *
from dwr.ds_partition.query7 import *

SAMPLE_TYPE = SENATE

for DS in ds_list:
    # language=HQL - get statistic from data
    hiveql.execute("""
        SELECT
            i_item_id,
            COUNT(*)
        FROM store_sales
        JOIN item
            ON ss_item_sk = i_item_sk  
            AND item.ds = '{ds}'
        WHERE store_sales.ds = '{ds}'  
        GROUP BY i_item_id
    """.format(ds=DS))
    frequency = {r[0]: r[1] for r in hiveql.fetchall()}

    for sample_rate in sample_rates:
        logging.info("DS: {0}        sample rate: {1}".format(DS, sample_rate))

        # senate sample
        sample_table = senate_sample(
            sample_rate=sample_rate,
            table_name=STORE_SALES,
            frequency=frequency,
            partition={'ds': DS},
            # language=HQL
            select="""
                SELECT *
                FROM {table_name}
                JOIN item
                    ON ss_item_sk = i_item_sk
                LEFT JOIN senate_allocation
                    ON  senate_allocation.table_name = '{table_name}'
                    AND senate_allocation.ds = '{ds}'                
                    AND senate_allocation.default_rate = {sample_rate}
                    AND i_item_id = stratum                                
                WHERE Rand() <= COALESCE(sample_rate, '{sample_rate}')                 
            """.format(
                table_name=STORE_SALES,
                sample_rate=sample_rate,
                ds=DS,
            ),
            overwrite=True,
        )

        logging.info('create result table')
        sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate, 'ds')
        hiveql.execute(query7_create_table.format(sample_result_table))

        logging.info('run query over sample table')
        hiveql.execute(query7.format(
            ds=DS,
            table_name=sample_result_table,
            store_sales=sample_table,
        ))

        sample_evaluate(
            table_name=RESULT_TABLE,
            sample_type=SAMPLE_TYPE,
            sample_rate=sample_rate,
            group_by_columns='i_item_id',
            aggregation_columns=['agg1', 'agg2', 'agg3', 'agg4'],
            partition={'ds': DS},
        )
