from dwr import *
from dwr.non_partition.query17 import *

SAMPLE_TYPE = VOILA

logging.info("Get statistic")

# language=HQL - get statistic from data
hiveql.execute("""
    SELECT
        i_item_id || '_' || s_state,
        COUNT(*),
        ABS(stddev(ss_quantity) / avg(ss_quantity))
    FROM store_sales, 
         store, 
         item
    WHERE i_item_sk = ss_item_sk
        AND s_store_sk = ss_store_sk
    GROUP BY i_item_id,
             s_state
""")
result = hiveql.fetchall()
frequency = {r[0]: r[1] for r in result}
coefficient = {r[0]: r[2:] for r in result}

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # senate sample for web sales
    store_sales_sampled = masg_sample(
        sample_rate=sample_rate,
        table_name=STORE_SALES,
        frequency=frequency,
        coefficient=coefficient,
        # language=HQL
        select="""
            SELECT *
            FROM {table_name} full_table,
                 store, 
                 item
            LEFT JOIN voila_allocation allocation
                ON  allocation.table_name = '{table_name}'
                AND allocation.ds = '~'
                AND allocation.default_rate = {sample_rate}
                AND allocation.stratum = i_item_id || '_' || s_state                                                        
            WHERE i_item_sk = ss_item_sk
                AND s_store_sk = ss_store_sk 
                ANd RAND() <= COALESCE(sample_rate, '{sample_rate}')                
        """.format(
            table_name=STORE_SALES,
            sample_rate=sample_rate,
        ),
        overwrite=True,
    )

    logging.info('Create result table')
    sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
    hiveql.execute(create_result_table.format(sample_result_table))

    logging.info('Run query over sampled tables')
    hiveql.execute(query17_sampled.format(
        table_name=sample_result_table,
        store_sales=store_sales_sampled,
    ))

    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['i_item_id', 's_state'],
        aggregation_columns=[
            'store_sales_quantitycount',
            'store_sales_quantityave',
            'store_sales_quantitystdev',
            'store_sales_quantitycov',
        ],
    )
