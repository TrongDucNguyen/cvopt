from dwr import *
from dwr.non_partition.openaq_samg_csrl import *

SAMPLE_TYPE = SENATE

logging.info("get frequency of strata")
# language=HQL
hiveql.execute("""
    SELECT
        attribution || '_' || city,
        COUNT(*)
    FROM openaq_clean
    GROUP BY attribution, city
""")
frequency = {r[0]: r[1] for r in hiveql.fetchall()}

for sample_rate in sample_rates:

    logging.info("Sample rate: {0}".format(sample_rate))

    # create sampled table
    sample_input_table = senate_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        group_by=['attribution', 'city'],
        frequency=frequency,
        overwrite=True,
    )

    # ----------------------------------------------------------
    # The following section applies to all three sampling methods

    # create result table
    logging.info('create result table')
    sample_result_table1 = sample_table_name(RESULT_TABLE1, SAMPLE_TYPE, sample_rate)
    sample_result_table2 = sample_table_name(RESULT_TABLE2, SAMPLE_TYPE, sample_rate)

    hiveql.execute('DROP TABLE {0}'.format(sample_result_table1))
    hiveql.execute('DROP TABLE {0}'.format(sample_result_table2))

    hiveql.execute(create_result_table.format(sample_result_table1, "city"))
    hiveql.execute(create_result_table.format(sample_result_table2, "attribution"))

    # run query over sampled table
    logging.info('run query over sampled table')
    hiveql.execute(samg_q1_sampled.format(
        input_table=sample_input_table,
        result_table=sample_result_table1,
    ))
    hiveql.execute(samg_q2_sampled.format(
        input_table=sample_input_table,
        result_table=sample_result_table2,
    ))

    # evaluate sample error
    sample_evaluate(
        table_name=RESULT_TABLE1,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['city'],
        aggregation_columns=['agg_value'],
    )
    sample_evaluate(
        table_name=RESULT_TABLE2,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=['attribution'],
        aggregation_columns=['agg_value'],
    )
