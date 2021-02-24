from dwr import *
from dwr.non_partition.openaq_avg_by_parameter import *

SAMPLE_TYPE = VOILA

logging.info('get statistic')
# language=HQL
hiveql.execute("""
    SELECT     
        parameter || '_' || unit,
        COUNT(*),
        ABS(STDDEV(value) / AVG(value))        
    FROM {0}
    GROUP BY parameter, unit
""".format(INPUT_TABLE))
result = hiveql.fetchall()
frequency = {r[0]: r[1] for r in result}
coefficient = {r[0]: r[2] for r in result}

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # create sampled table
    sample_table = sasg_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        group_by=['parameter', 'unit'],
        aggregate_column='value',
        frequency=frequency,
        coefficient=coefficient,
        overwrite=True,
    )

    # create result table
    logging.info('create result table')
    hiveql.execute(create_result_table.format(sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)))

    # run query over sampled table
    logging.info('run query over sampled table')
    hiveql.execute(average_by_parameter_sql.format(
        input_table=sample_table,
        result_table=sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate),
    ))

    # evaluate sample error
    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['parameter', 'unit'],
        aggregation_columns=['average'],
    )
