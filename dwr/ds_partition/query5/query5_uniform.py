from dwr import *
from dwr.ds_partition.query5 import *

SAMPLE_TYPE = UNIFORM

for ds in ds_list:
    for sample_rate in sample_rates:
        logging.info("DS: {0}        sample rate: {1}".format(ds, sample_rate))
        # check if result is available
        if exists(
                table_name='sample_error',
                partition={'ds': ds, 'table_name': RESULT_TABLE, 'sample_type': SAMPLE_TYPE,
                           'sample_rate': sample_rate},
        ):
            continue

        # create sample tables
        store_sales_sampled = uniform_sample(
            sample_rate=sample_rate,
            table_name=STORE_SALES,
            partition={'ds': ds},
        )
        catalog_sales_sampled = uniform_sample(
            sample_rate=sample_rate,
            table_name=CATALOG_SALES,
            partition={'ds': ds},
        )
        web_sales_sampled = uniform_sample(
            sample_rate=sample_rate,
            table_name=WEB_SALES,
            partition={'ds': ds},
        )

        sample_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate, 'ds')
        if not exists(sample_table, {'ds': ds}):
            logging.info('Create result table')
            hiveql.execute(query5_create_table.format(sample_table))

            logging.info('Run query over sampled tables')
            hiveql.execute(query5_sampled.format(
                ds=ds,
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
            partition={'ds': ds},
        )
