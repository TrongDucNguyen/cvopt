from dwr.adaptive_sampling import *

# Expect to have average of the error less than 10% error
EPSILON = 0.1
SAMPLE_TYPE = 'dynamic_uniform'
sample_table = sample_table_name(INPUT_TABLE, SAMPLE_TYPE)
sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE)

# create result table
logging.info('create result table')
hiveql.execute(create_result_table.format(sample_result_table))

for DS in ds_list:
    logging.info("DS: {0}".format(DS))

    logging.info('Get old dynamic sample rate')
    old_sample_rate = get_dynamic_allocation(yesterday(DS), RESULT_TABLE, SAMPLE_TYPE)

    if old_sample_rate:
        logging.info('Get average error from sample_error')
        # language=HQL
        hiveql.execute("""
            SELECT AVG(error)
            FROM sample_error
            WHERE ds = '{ds}'
            AND table_name = '{table_name}'
            AND sample_type = '{sample_type}'
            AND sample_rate = '{sample_rate}'             
        """.format(
            ds=yesterday(DS),
            table_name=RESULT_TABLE,
            sample_type=SAMPLE_TYPE,
            sample_rate=old_sample_rate,
        ))
        avg_error = hiveql.fetchall()[0][0]
        logging.info("Average error: {0}".format(avg_error))

        # dynamic sample size
        sample_rate = max(0.01, min(1.0, round((old_sample_rate + avg_error - EPSILON) * 100) / 100.0))
        # sample_rate = max(0.01, min(1.0, round(old_sample_rate * (1.0 + avg_error - EPSILON) * 100) / 100.0))
    else:
        # default sample rate
        sample_rate = 0.1

    # store dynamic sample rate
    set_dynamic_allocation(DS, RESULT_TABLE, SAMPLE_TYPE, sample_rate)

    # create sampled table
    sample_table = uniform_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        partition={'ds': DS},
        sample_table=sample_table,
        overwrite=True,
    )

    # run query over sampled table
    logging.info('run query over sampled table')
    hiveql.execute(average_by_parameter_sql.format(
        ds=DS,
        input_table=sample_table,
        result_table=sample_result_table,
    ))

    # evaluate sample error
    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['parameter', 'unit'],
        aggregation_columns=['average'],
        partition={'ds': DS},
        sample_table=sample_result_table,
    )
