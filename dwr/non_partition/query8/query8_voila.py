from dwr import *
from dwr.non_partition.query8 import *

SAMPLE_TYPE = VOILA

logging.info("Get statistic")

# language=HQL - get statistic from data
hiveql.execute("""
    SELECT
        s_store_name,
        COUNT(*),
        ABS(STDDEV(ss_net_profit) / AVG(ss_net_profit))
    FROM store_sales, store 
    WHERE ss_store_sk = s_store_sk
    GROUP BY s_store_name
""")
result = hiveql.fetchall()
frequency = {r[0]: r[1] for r in result}
coefficient = {r[0]: r[2] for r in result}

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # senate sample for web sales
    store_sales_sampled = sasg_sample(
        sample_rate=sample_rate,
        table_name=STORE_SALES,
        group_by='s_store_name',
        aggregate_column='ss_net_profit',
        frequency=frequency,
        coefficient=coefficient,
        # language=HQL
        select="""
            SELECT *
            FROM store_sales, store  
            LEFT JOIN voila_allocation
                ON voila_allocation.table_name = '{table_name}'
                AND voila_allocation.default_rate = {sample_rate}
                AND voila_allocation.stratum = s_store_name
            WHERE ss_store_sk = s_store_sk                                                                        
                AND rand() <= COALESCE(sample_rate, '{sample_rate}')                 
        """.format(
            table_name=STORE_SALES,
            sample_rate=sample_rate,
        ),
        overwrite=True,
    )

    logging.info('Create result table')
    sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
    hiveql.execute(create_result_table.format(sample_result_table))

    logging.info('Run query over sample table')
    hiveql.execute(query8_sampled.format(
        table_name=sample_result_table,
        store_sales=store_sales_sampled,
    ))

    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['s_store_name'],
        aggregation_columns=['agg'],
    )
