from dwr import *
from dwr.non_partition.query3 import *

SAMPLE_TYPE = VOILA

logging.info("Get statistic")

# language=HQL - get statistic from data
hiveql.execute("""
    SELECT
        dt.d_year || '_' || item.i_brand || '_' || item.i_brand_id, 
        COUNT(*),
        ABS(STDDEV(ss_ext_discount_amt) / AVG(ss_ext_discount_amt))
    FROM   date_dim dt, 
           store_sales, 
           item 
    WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk 
           AND store_sales.ss_item_sk = item.i_item_sk 
    GROUP BY dt.d_year, 
          item.i_brand, 
          item.i_brand_id 
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
        frequency=frequency,
        coefficient=coefficient,
        # language=HQL
        select="""
            SELECT *
            FROM   date_dim dt, 
                   store_sales, 
                   item  
            LEFT JOIN voila_allocation
                ON voila_allocation.table_name = '{table_name}'
                AND voila_allocation.default_rate = {sample_rate}
                AND voila_allocation.stratum = dt.d_year || '_' || item.i_brand || '_' || item.i_brand_id
            WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk 
                AND store_sales.ss_item_sk = item.i_item_sk                                                                         
                AND rand() <= COALESCE(sample_rate, '{sample_rate}')                 
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
        table_name=sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate),
        store_sales=store_sales_sampled,
    ))

    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['d_year', 'brand_id', 'brand'],
        aggregation_columns=['sum_agg'],
    )
