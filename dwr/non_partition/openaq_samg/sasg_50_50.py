from dwr import *
from dwr.non_partition.openaq_samg import *

SAMPLE_TYPE = VOILA

for r in sample_rates:
    sample_rate = r / 2
    logging.info("Sample rate: {0}".format(sample_rate))

    # create sampled table
    sample_input_table1 = sasg_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        group_by='unit',
        aggregate_column='value',
        overwrite=True,
    )

    sample_input_table2 = sasg_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        group_by='parameter',
        aggregate_column='value',
        overwrite=True,
    )

    # create result table
    logging.info('create result table')
    sample_result_table1 = sample_table_name(RESULT_TABLE1, SAMPLE_TYPE, sample_rate)
    sample_result_table2 = sample_table_name(RESULT_TABLE2, SAMPLE_TYPE, sample_rate)

    hiveql.execute('DROP TABLE {0}'.format(sample_result_table1))
    hiveql.execute('DROP TABLE {0}'.format(sample_result_table2))

    hiveql.execute(create_result_table.format(sample_result_table1, "unit"))
    hiveql.execute(create_result_table.format(sample_result_table2, "parameter"))

    # run query over sampled table
    logging.info('run query over sampled table')
    hiveql.execute(samg_q1.format(
        input_table=sample_input_table1,
        result_table=sample_result_table1,
    ))
    hiveql.execute(samg_q2.format(
        input_table=sample_input_table2,
        result_table=sample_result_table2,
    ))

    # evaluate sample error
    sample_evaluate(
        table_name=RESULT_TABLE1,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['unit'],
        aggregation_columns=['agg_value'],
    )
    sample_evaluate(
        table_name=RESULT_TABLE2,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['parameter'],
        aggregation_columns=['agg_value'],
    )
