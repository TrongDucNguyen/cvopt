from dwr import *
from dwr.ds_partition.query2 import *

SAMPLE_TYPE = SENATE

for DS in ds_list:
    web_sales_count = None
    catalog_sales_count = None
    for sample_rate in sample_rates:
        logging.info("DS: {0}        sample rate: {1}".format(DS, sample_rate))

        if web_sales_count is None:
            # language=HQL - get statistic from data
            hiveql.execute("""
                SELECT
                    d_week_seq,
                    COUNT(*)
                FROM web_sales
                JOIN date_dim
                ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
                WHERE web_sales.ds = '{0}'
                GROUP BY d_week_seq
            """.format(DS))
            web_sales_count = {r[0]: r[1] for r in hiveql.fetchall()}

        if catalog_sales_count is None:
            # language=HQL
            hiveql.execute("""
                SELECT
                    d_week_seq,
                    COUNT(*)
                FROM catalog_sales
                JOIN date_dim
                ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
                WHERE catalog_sales.ds = '{0}'
                GROUP BY d_week_seq
            """.format(DS))
            catalog_sales_count = {r[0]: r[1] for r in hiveql.fetchall()}

        # senate sample for web sales
        web_sales_sampled = senate_sample(
            sample_rate=sample_rate,
            table_name=WEB_SALES,
            group_by=['d_week_seq'],
            partition={'ds': DS},
            frequency=web_sales_count,
            # language=HQL
            select="""
                SELECT *
                FROM {table_name} full_table
                JOIN date_dim
                    ON full_table.ws_sold_date_sk = date_dim.d_date_sk
                LEFT JOIN senate_allocation
                    ON senate_allocation.ds = '{ds}'
                    AND senate_allocation.table_name = '{table_name}'
                    AND senate_allocation.default_rate = {sample_rate}
                    AND date_dim.d_week_seq = senate_allocation.stratum                          
                WHERE full_table.ds = '{ds}'                               
                AND RAND() <= COALESCE(sample_rate, '{sample_rate}')                 
            """.format(
                ds=DS,
                table_name=WEB_SALES,
                sample_rate=sample_rate,
            ),
            overwrite=True,
        )

        # senate sample for catalog sale
        catalog_sales_sampled = senate_sample(
            sample_rate=sample_rate,
            table_name=CATALOG_SALES,
            group_by=['d_week_seq'],
            partition={'ds': DS},
            frequency=web_sales_count,
            # language=HQL
            select="""
                SELECT *
                FROM {table_name} full_table
                JOIN date_dim
                    ON full_table.cs_sold_date_sk = date_dim.d_date_sk
                LEFT JOIN senate_allocation
                    ON senate_allocation.ds = '{ds}'
                    AND senate_allocation.table_name = '{table_name}'
                    AND senate_allocation.default_rate = {sample_rate}
                    AND date_dim.d_week_seq = senate_allocation.stratum                          
                WHERE full_table.ds = '{ds}'                               
                AND RAND() <= COALESCE(sample_rate, '{sample_rate}')                 
            """.format(
                ds=DS,
                table_name=CATALOG_SALES,
                sample_rate=sample_rate,
            ),
            overwrite=True,
        )

        sample_table = sample_table_name('query2', SAMPLE_TYPE, sample_rate, 'ds')
        if not exists(sample_table, {'ds': DS}):
            # create result table
            hiveql.execute(query2_create_table.format(sample_table))

            # run query over sample table
            hiveql.execute(query2_sampled.format(
                ds=DS,
                query2_table=sample_table,
                web_sales_table=web_sales_sampled,
                catalog_sales_table=catalog_sales_sampled,
            ))

        sample_evaluate(
            table_name='query2',
            sample_type=SAMPLE_TYPE,
            sample_rate=sample_rate,
            group_by_columns=['d_week_seq1'],
            aggregation_columns=['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'],
            partition={'ds': DS},
        )
