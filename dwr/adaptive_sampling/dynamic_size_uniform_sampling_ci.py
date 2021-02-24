from dwr.adaptive_sampling import *

z_score = {
    0.80: 1.282,
    0.85: 1.440,
    0.90: 1.645,
    0.95: 1.960,
    0.99: 2.576,
    0.995: 2.807,
    0.999: 3.291,
}

# Expect to have average of the error less than 10% error
EPSILON = 0.1
CONFIDENT = 0.9

SAMPLE_TYPE = 'uniform_dynamic_ci'
sample_table = sample_table_name(INPUT_TABLE, SAMPLE_TYPE)
sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE)

# create result table
logging.info('create result table')
hiveql.execute(create_result_table_with_ci.format(sample_result_table))

for ds in ds_list:
    logging.info("DS: {0}".format(ds))

    logging.info('Get old dynamic sample rate')
    old_sample_rate = get_dynamic_allocation(yesterday(ds), RESULT_TABLE, SAMPLE_TYPE)

    if old_sample_rate:
        logging.info('Compute CI from previous sample')
        # language=HQL
        hiveql.execute("""
            SELECT                
                avg(abs(ci / average))            
            FROM {table_name}
            WHERE ds = '{ds}'
        """.format(
            table_name=sample_result_table,
            ds=yesterday(ds),
        ))
        ci = hiveql.fetchall()[0][0]
        logging.info("CI: {0}".format(ci))

        # dynamic sample size
        sample_rate = max(0.01, min(1.0, round(old_sample_rate * (1.0 + ci - EPSILON) * 100) / 100.0))
    else:
        # default sample rate
        sample_rate = 0.1

    # store dynamic sample rate
    set_dynamic_allocation(ds, RESULT_TABLE, SAMPLE_TYPE, sample_rate)

    # create sampled table
    sample_table = uniform_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        partition={'ds': ds},
        sample_table=sample_table,
        overwrite=True,
    )

    # run query over sampled table
    logging.info('run query over sampled table')
    hiveql.execute(average_by_parameter_sql_with_ci.format(
        ds=ds,
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
        partition={'ds': ds},
        sample_table=sample_result_table,
    )
