from dwr import *
from dwr.non_partition.query7 import *

SAMPLE_TYPE = UNIFORM


for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # create sample tables
    sample_table = uniform_sample(sample_rate, STORE_SALES, overwrite=True)

    logging.info('Create result table')
    sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
    hiveql.execute(query7_create_table.format(sample_result_table))

    logging.info('Run query over sampled tables')
    hiveql.execute(query7.format(
        table_name=sample_result_table,
        store_sales=sample_table,
    ))

    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns='i_item_id',
        aggregation_columns=['agg1', 'agg2', 'agg3', 'agg4'],
    )
