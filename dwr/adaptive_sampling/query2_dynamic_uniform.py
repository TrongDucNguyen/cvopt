from dwr.adaptive_sampling import *

# Expect to have average of the error less than 10% error
EPSILON = 0.2
SAMPLE_TYPE = UNIFORM

for DS in ds_list:
    logging.info("DS: {0}".format(DS))

    # get old dynamic sample rate
    old_sample_rate = get_dynamic_allocation(yesterday(DS), 'query2', SAMPLE_TYPE)

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

    # Create uniform sample tables
    web_sales_sampled = sample_table_name(WEB_SALES, SAMPLE_TYPE, 'dynamic')
    uniform_sample(
        sample_rate=sample_rate,
        table_name=WEB_SALES,
        sample_table=web_sales_sampled,
        partition={'ds': DS},
    )
    catalog_sales_sampled = sample_table_name(CATALOG_SALES, SAMPLE_TYPE, 'dynamic')
    uniform_sample(
        sample_rate=sample_rate,
        table_name=CATALOG_SALES,
        sample_table=catalog_sales_sampled,
        partition={'ds': DS},
    )

    # create result table
    logging.info('Create sampled result table')
    sample_result_table = sample_table_name('query2', SAMPLE_TYPE, 'dynamic')
    hiveql.execute(query2_create_table.format(sample_result_table))

    # run query over sample table
    logging.info('run query over sample table')
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
