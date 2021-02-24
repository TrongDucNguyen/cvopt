from dwr import *
from dwr.ds_partition.query7 import *

SAMPLE_TYPE = UNIFORM

for DS in ds_list:
    for sample_rate in sample_rates:
        logging.info("DS: {0}        sample rate: {1}".format(DS, sample_rate))

        # create sample tables
        store_sales_sampled = uniform_sample(
            sample_rate=sample_rate,
            table_name=STORE_SALES,
            partition={'ds': DS},
            overwrite=True,
        )

        sample_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate, 'ds')
        if not exists(sample_table, {'ds': DS}):
            logging.info('Create result table')
            hiveql.execute(query7_create_table.format(sample_table))

            logging.info('Run query over sampled tables')
            hiveql.execute(query7.format(
                ds=DS,
                table_name=sample_table,
                store_sales=store_sales_sampled,
            ))

        sample_evaluate(
            table_name=RESULT_TABLE,
            sample_type=SAMPLE_TYPE,
            sample_rate=sample_rate,
            group_by_columns='i_item_id',
            aggregation_columns=['agg1', 'agg2', 'agg3', 'agg4'],
            partition={'ds': DS},
        )
