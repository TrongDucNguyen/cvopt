from dwr import *
from dwr.non_partition.openaq_masg import *

SAMPLE_TYPE = UNIFORM

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # create sampled table
    sample_table = uniform_sample(sample_rate, INPUT_TABLE)

    # create result table
    logging.info('create result table')
    hiveql.execute(create_result_table.format(sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)))

    # run query over sampled table
    logging.info('run query over sampled table')
    hiveql.execute(masg_sampled.format(
        input_table=sample_table,
        result_table=sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate),
    ))

    # evaluate sample error
    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['country', 'parameter', 'unit'],
        aggregation_columns=['count', 'sum', 'avg', 'stddev'],
    )
