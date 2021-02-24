from dwr import *
from dwr.non_partition.query2 import *

SAMPLE_TYPE = UNIFORM

for sample_rate in sample_rates:
    logging.info("Sample rate: {0}".format(sample_rate))

    # create sample tables
    web_sales_sampled = uniform_sample(sample_rate, WEB_SALES)
    catalog_sales_sampled = uniform_sample(sample_rate, CATALOG_SALES)

    logging.info('Create result table')
    sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
    hiveql.execute(create_result_table.format(sample_result_table))

    logging.info('Run query over sampled tables')
    hiveql.execute(query2_sampled.format(
        query2_table=sample_result_table,
        web_sales_table=web_sales_sampled,
        catalog_sales_table=catalog_sales_sampled,
    ))

    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['d_week_seq1'],
        aggregation_columns=['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'],
    )
