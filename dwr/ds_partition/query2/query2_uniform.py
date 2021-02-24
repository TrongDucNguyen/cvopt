from dwr import *
from dwr.ds_partition.query2 import *

SAMPLE_TYPE = UNIFORM

for DS in ds_list:
    for sample_rate in sample_rates:
        logging.info("DS: {0}        sample rate: {1}".format(DS, sample_rate))

        # create sample tables
        web_sales_sampled = uniform_sample(
            sample_rate=sample_rate,
            table_name=WEB_SALES,
            partition={'ds': DS},
            overwrite=True,
        )
        catalog_sales_sampled = uniform_sample(
            sample_rate=sample_rate,
            table_name=CATALOG_SALES,
            partition={'ds': DS},
            overwrite=True,
        )

        sample_table = sample_table_name('query2', SAMPLE_TYPE, sample_rate, 'ds')
        if not exists(sample_table, {'ds': DS}):
            logging.info('Create result table')
            hiveql.execute(query2_create_table.format(sample_table))

            logging.info('Run query over sampled tables')
            hiveql.execute(query2_sampled.format(
                ds=DS,
                query2_table=sample_table,
                web_sales_table=web_sales_sampled,
                catalog_sales_table=catalog_sales_sampled,
            ))

        sample_evaluate(
            table_name='query2',
            sample_type=SAMPLE_TYPE,
            sample_rate=sample_rate,
            group_by_columns=['d_week_seq1'],
            aggregation_columns=['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'],
            partition={'ds': DS},
        )
