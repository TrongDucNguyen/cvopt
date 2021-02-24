from dwr import *
from dwr.ds_partition.query3 import *

SAMPLE_TYPE = UNIFORM
OVERWRITE = False

for DS in ds_list:
    for sample_rate in sample_rates:
        logging.info("DS: {0}        sample rate: {1}".format(DS, sample_rate))

        # create sample tables
        store_sales_sampled = uniform_sample(sample_rate, STORE_SALES, {'ds': DS}, OVERWRITE)

        logging.info('Create result table')
        hiveql.execute(query3_create_table.format(sample_table_name('query3', SAMPLE_TYPE, sample_rate, 'ds')))

        logging.info('Run query over sampled tables')
        hiveql.execute(query3_sampled.format(
            ds=DS,
            query3_table=sample_table_name('query3', SAMPLE_TYPE, sample_rate, 'ds'),
            store_sales_table=store_sales_sampled,
        ))

        sample_evaluate(
            table_name='query3',
            sample_type=SAMPLE_TYPE,
            sample_rate=sample_rate,
            group_by_columns=['d_year','i_brand_id','i_brand'],
            aggregation_columns=['sum_agg'],
            partition={'ds': DS},
        )
