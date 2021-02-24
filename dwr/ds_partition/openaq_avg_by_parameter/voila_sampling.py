from dwr import *
from dwr.ds_partition.openaq_avg_by_parameter import *

SAMPLE_TYPE = VOILA

for DS in ds_list:
    for sample_rate in sample_rates:
        logging.info("DS: {0} - Sample rate: {1}".format(DS, sample_rate))

        # create sampled table
        sample_table = voila_sample(
            sample_rate=sample_rate,
            table_name=INPUT_TABLE,
            group_by=['parameter', 'unit'],
            aggregate_column='value',
            partition={'ds': DS},
        )

        # create result table
        logging.info('create result table')
        hiveql.execute(create_result_table.format(sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate, 'ds')))

        # run query over sampled table
        logging.info('run query over sampled table')
        hiveql.execute(average_by_parameter_sql.format(
            ds=DS,
            input_table=sample_table,
            result_table=sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate, 'ds'),
        ))

        # evaluate sample error
        sample_evaluate(
            table_name=RESULT_TABLE,
            sample_type=SAMPLE_TYPE,
            sample_rate=sample_rate,
            group_by_columns=['parameter', 'unit'],
            aggregation_columns=['average'],
            partition={'ds': DS}
        )
