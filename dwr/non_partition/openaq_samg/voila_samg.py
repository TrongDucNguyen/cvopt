from dwr import *
from dwr.non_partition.openaq_samg import *

SAMPLE_TYPE = VOILA

group_by = [{'unit'}, {'parameter'}]

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # create sampled table
    sample_input_table = samg_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        group_by=group_by,
        aggregate_column='value',
        overwrite=True,
    )

    # ----------------------------------------------------------
    # The following section applies to all three sampling methods

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
        input_table=sample_input_table,
        result_table=sample_result_table1,
    ))
    hiveql.execute(samg_q2.format(
        input_table=sample_input_table,
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

