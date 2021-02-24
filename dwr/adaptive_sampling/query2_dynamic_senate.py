from dwr.adaptive_sampling import *

# Expect to have 90% of the result less than 10% error
EPSILON = 0.1
SAMPLE_TYPE = SENATE

for DS in ds_list:
    logging.info("DS: {0} - Get statistic".format(DS))

    # get old dynamic sample rate
    old_sample_rate = get_dynamic_allocation(yesterday(DS), 'query2', SAMPLE_TYPE)

    # compute new dynamic sample rate
    if old_sample_rate:
        # language=HQL - get statistic from sample_error
        hiveql.execute("""
            SELECT AVG(error)
            FROM sample_error
            WHERE ds = '{ds}'
            AND table_name = 'query2'
            AND sample_rate = '{sample_rate}' 
            AND sample_type = '{sample_type}'
        """.format(
            ds=yesterday(DS),
            sample_type=SAMPLE_TYPE,
            sample_rate=old_sample_rate,
        ))
        avg_error = hiveql.fetchall()[0][0]
        try:
            float(avg_error)
            logging.info("Average error: {0}".format(avg_error))
            sample_rate = max(0.01, min(1.0, round((old_sample_rate + avg_error - EPSILON) * 100) / 100.0))
        except ValueError:
            sample_rate = 0.1
    else:
        sample_rate = 0.1

    # set new dynamic sample rate
    set_dynamic_allocation(DS, 'query2', SAMPLE_TYPE, sample_rate)

    # draw senate sample
    web_sales_sampled = sample_table_name(WEB_SALES, SAMPLE_TYPE, 'dynamic')
    senate_sample(
        sample_rate=sample_rate,
        table_name=WEB_SALES,
        group_by=['d_week_seq'],
        partition={'ds': DS},
        # language=HQL - get statistic from web_sales
        frequency="""
            SELECT
                d_week_seq,
                COUNT(*)
            FROM web_sales
            JOIN date_dim
            ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
            WHERE web_sales.ds = '{0}'
            GROUP BY d_week_seq
        """.format(DS),
        # language=HQL - draw sample
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
        sample_table=web_sales_sampled,
    )

    catalog_sales_sampled = sample_table_name(CATALOG_SALES, SAMPLE_TYPE, 'dynamic')
    senate_sample(
        sample_rate=sample_rate,
        table_name=CATALOG_SALES,
        group_by=['d_week_seq'],
        partition={'ds': DS},
        # language=HQL - get statistic from web_sales
        frequency="""
            SELECT
                d_week_seq,
                COUNT(*)
            FROM catalog_sales
            JOIN date_dim
            ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
            WHERE catalog_sales.ds = '{0}'
            GROUP BY d_week_seq
        """.format(DS),
        # language=HQL - draw sample
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
        sample_table=catalog_sales_sampled,
    )

    # create result table
    sample_result_table = sample_table_name('query2', SAMPLE_TYPE, 'dynamic')
    hiveql.execute(query2_create_table.format(sample_result_table))

    # run query over sample table
    hiveql.execute(query2_sampled.format(
        ds=DS,
        web_sales_table=web_sales_sampled,
        catalog_sales_table=catalog_sales_sampled,
        query2_table=sample_result_table,
    ))

    # evaluate sample error
    sample_evaluate(
        table_name='query2',
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        sample_table=sample_result_table,
        group_by_columns=['d_week_seq1'],
        aggregation_columns=['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'],
        partition={'ds': DS},
    )
