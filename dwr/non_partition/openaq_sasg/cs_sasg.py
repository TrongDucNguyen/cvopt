from dwr import *
from dwr.CSRLSS import *
from dwr.non_partition.openaq_sasg import *

SAMPLE_TYPE = CS

logging.info("get frequency of strata")
# language=HQL
hiveql.execute("""
    SELECT
        country || '_' || parameter || '_' || unit,
        COUNT(*)
    FROM {0}
    GROUP BY country, parameter, unit
""".format(INPUT_TABLE))
frequency = {r[0]: r[1] for r in hiveql.fetchall()}


for sample_rate in sample_rates:

    #logging.info("Sample rate: {0}".format(sample_rate))

    # create sampled table
    sample_input_table = cs_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        group_by=['country', 'parameter', 'unit'],
        frequency=frequency,
        overwrite=True,
    )

    # ----------------------------------------------------------
    # The following section applies to all sampling methods

    # create result table
    logging.info('create result table')
    sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)

    logging.info(create_result_table.format(sample_result_table))
    # hiveql.execute(create_result_table.format(sample_result_table))

    # run query over sampled table
    logging.info('run query over sampled table')
    logging.info(sasg.format(
        input_table=sample_input_table,
        result_table=sample_result_table,
    ))

    # evaluate sample error
    # sample_evaluate(
    #     table_name=RESULT_TABLE,
    #     sample_type=SAMPLE_TYPE,
    #     sample_rate=sample_rate,
    #     group_by_columns=['country', 'parameter', 'unit'],
    #     aggregation_columns=['average'],
    # )
