from dwr import *
from dwr.non_partition.query21 import *

SAMPLE_TYPE = UNIFORM

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # create sample tables
    sample_input_table = uniform_sample(sample_rate, INVENTORY)

    logging.info('Create result table')
    sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
    hiveql.execute(create_result_table.format(sample_result_table))

    logging.info('Run query over sampled tables')
    hiveql.execute(query21_sampled.format(
        table_name=sample_result_table,
        inventory=sample_input_table,
    ))

    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['w_warehouse_name', 'i_item_id'],
        aggregation_columns=['inv_before', 'inv_after'],
    )
