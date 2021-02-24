from dwr import *
from dwr.non_partition.query8 import *

SAMPLE_TYPE = UNIFORM

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # create sample tables
    store_sales_sampled = uniform_sample(sample_rate, STORE_SALES)

    logging.info('Create result table')
    sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
    hiveql.execute(create_result_table.format(sample_result_table))

    logging.info('Run query over sample table')
    hiveql.execute(query8_sampled.format(
        table_name=sample_result_table,
        store_sales=store_sales_sampled,
    ))

    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['s_store_name'],
        aggregation_columns=['agg'],
    )
