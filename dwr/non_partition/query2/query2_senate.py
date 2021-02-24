from dwr import *
from dwr.non_partition.query2 import *

SAMPLE_TYPE = SENATE

# language=HQL - get statistic from data
hiveql.execute("""
    SELECT
        d_week_seq,
        COUNT(*)
    FROM web_sales
    JOIN date_dim
    ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
    GROUP BY d_week_seq
""")
web_sales_count = {r[0]: r[1] for r in hiveql.fetchall()}

# language=HQL
hiveql.execute("""
    SELECT
        d_week_seq,
        COUNT(*)
    FROM catalog_sales
    JOIN date_dim
    ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk            
    GROUP BY d_week_seq
""")
catalog_sales_count = {r[0]: r[1] for r in hiveql.fetchall()}

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # senate sample for web sales
    web_sales_sampled = senate_sample(
        sample_rate=sample_rate,
        table_name=WEB_SALES,
        group_by=['d_week_seq'],
        frequency=web_sales_count,
        # language=HQL
        select="""
            SELECT *
            FROM {table_name} full_table
            JOIN date_dim
                ON full_table.ws_sold_date_sk = date_dim.d_date_sk
            LEFT JOIN senate_allocation
                ON senate_allocation.table_name = '{table_name}'
                AND senate_allocation.default_rate = {sample_rate}
                AND date_dim.d_week_seq = senate_allocation.stratum                                                         
            WHERE RAND() <= COALESCE(sample_rate, '{sample_rate}')                 
        """.format(
            table_name=WEB_SALES,
            sample_rate=sample_rate,
        ),
    )

    # senate sample for catalog sale
    catalog_sales_sampled = senate_sample(
        sample_rate=sample_rate,
        table_name=CATALOG_SALES,
        group_by=['d_week_seq'],
        frequency=web_sales_count,
        # language=HQL
        select="""
            SELECT *
            FROM {table_name} full_table
            JOIN date_dim
                ON full_table.cs_sold_date_sk = date_dim.d_date_sk
            LEFT JOIN senate_allocation
                ON senate_allocation.table_name = '{table_name}'
                AND senate_allocation.default_rate = {sample_rate}
                AND date_dim.d_week_seq = senate_allocation.stratum                                                         
            WHERE RAND() <= COALESCE(sample_rate, '{sample_rate}')                 
        """.format(
            table_name=CATALOG_SALES,
            sample_rate=sample_rate,
        ),
    )

    logging.info('Create result table')
    sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
    hiveql.execute(create_result_table.format(sample_result_table))

    logging.info('Run query over sampled tables')
    hiveql.execute(query2_sampled.format(
        query2_table=sample_result_table,
        web_sales_table=web_sales_sampled,
        catalog_sales_table=catalog_sales_sampled,
    ))

    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['d_week_seq1'],
        aggregation_columns=['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'],
    )
