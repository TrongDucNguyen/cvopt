from dwr import *
from dwr.ds_partition.query26 import *

SAMPLE_TYPE = UNIFORM

for DS in ds_list:
    for sample_rate in sample_rates:
        logging.info("DS: {0}        sample rate: {1}".format(DS, sample_rate))

        # create sample tables
        catalog_sales_sampled = uniform_sample(sample_rate, CATALOG_SALES, {'ds': DS}, overwrite=True)

        logging.info('Create result table')
        hiveql.execute(query26_create_table.format(sample_table_name('query26', SAMPLE_TYPE, sample_rate, 'ds')))

        logging.info('Run query over sampled tables')
        hiveql.execute(query26_sampled.format(
            ds=DS,
            query26_table=sample_table_name('query26', SAMPLE_TYPE, sample_rate, 'ds'),
            catalog_sales_table=catalog_sales_sampled,
        ))

        sample_evaluate(
            table_name='query26',
            sample_type=SAMPLE_TYPE,
            sample_rate=sample_rate,
            group_by_columns=['i_item_id'],
            aggregation_columns=['agg1','agg2','agg3','agg4'],
            partition={'ds': DS},
        )
