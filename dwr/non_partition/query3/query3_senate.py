from dwr import *
from dwr.non_partition.query3 import *

SAMPLE_TYPE = SENATE

# language=HQL - get statistic from data
hiveql.execute("""
    SELECT 
        d_year || '_' || i_brand || '_' || i_brand_id,
        COUNT(*)
    FROM date_dim dt, store_sales, item
        WHERE dt.d_date_sk = store_sales.ss_sold_date_sk 
           AND store_sales.ss_item_sk = item.i_item_sk 
    GROUP BY d_year, i_brand, i_brand_id
""")
web_sales_count = {r[0]: r[1] for r in hiveql.fetchall()}

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # senate sample for web sales
    store_sales_sampled = senate_sample(
        sample_rate=sample_rate,
        table_name=STORE_SALES,
        frequency=web_sales_count,
        # language=HQL
        select="""
            SELECT *
            FROM {table_name}, 
                 date_dim dt,              
                 item 
            LEFT JOIN senate_allocation
                ON senate_allocation.table_name = '{table_name}'
                AND senate_allocation.default_rate = {sample_rate}
                AND senate_allocation.stratum = d_year || '_' || i_brand || '_' || i_brand_id                                                         
            WHERE dt.d_date_sk = store_sales.ss_sold_date_sk 
                AND store_sales.ss_item_sk = item.i_item_sk 
                AND RAND() <= COALESCE(sample_rate, '{sample_rate}')                 
        """.format(
            table_name=STORE_SALES,
            sample_rate=sample_rate,
        ),
        overwrite=True
    )

    logging.info('Create result table')
    sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
    hiveql.execute(create_result_table.format(sample_result_table))

    logging.info('Run query over sample table')
    hiveql.execute(query3_sampled.format(
        table_name=sample_result_table,
        store_sales=store_sales_sampled,
    ))

    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['d_year', 'brand_id', 'brand'],
        aggregation_columns=['sum_agg'],
    )
