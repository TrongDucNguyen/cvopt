from dwr import *
from dwr.ds_partition.openaq_sasg import *
from dwr.ds_partition.openaq_sasg.sasg import *

# Experiment parameters
aggr = "avg"    # single aggregation function (sum, avg, count)
col = "value"   # aggregated column name
grp = {'country': 'STRING', 'parameter': 'STRING', 'unit': 'STRING'}  # group by

# INPUT_TABLE & RESULT_TABLE
INPUT_TABLE = "openaq"
RESULT_TABLE = "openaq_ds_{0}_by_{1}".format(aggr, list(grp)[0])

# SASG
x = sasg(aggr, col, grp)

# Senate Sampling
SAMPLE_TYPE = SENATE

ds_list = ["2017-01-01"]
sample_rates = [0.01]

# Build sample and run aggregation
for DS in ds_list:
    for sample_rate in sample_rates:
        logging.info("DS: {0} - Sample rate: {1}".format(DS, sample_rate))

        # create sampled table
        sample_table = senate_sample(sample_rate, INPUT_TABLE, grp.keys(), {'ds': DS})

        # create result table
        logging.info('create result table')
        hiveql.execute(
            x.create_result_table_sql(sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate, 'ds'))
        )

        # run query over sampled table
        logging.info('run query over sampled table')
        hiveql.execute(
            x.sasg_sql(DS, sample_table, sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate, 'ds'))
        )

        # evaluate sample error
        sample_evaluate(
            table_name=RESULT_TABLE,
            sample_type=SAMPLE_TYPE,
            sample_rate=sample_rate,
            group_by_columns=grp,
            aggregation_columns="aggr",
            partition={'ds': DS}
        )

