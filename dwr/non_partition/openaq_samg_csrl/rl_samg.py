from dwr.CSRLSS import *
from dwr.non_partition.openaq_samg_csrl import *

SAMPLE_TYPE = RL

logging.info("get frequency of strata")
# language=HQL
hiveql.execute("""
    SELECT
        attribution || '_' || city,
        COUNT(*),
        ABS(AVG(value)),
        STDDEV(value)   
    FROM openaq_clean
    GROUP BY attribution, city
""")
result = hiveql.fetchall()
frequency = {r[0]: r[1] for r in result}
coefficient = {r[0]: r[3]/max(r[2], 1) for r in result}

hiveql.execute("""
    SELECT
        attribution,
        ABS(AVG(value)),
        STDDEV(value)
    FROM openaq_clean
    GROUP BY attribution
""")
coeff_a = {r[0]: r[2] / max(r[1], 1) for r in hiveql.fetchall()}

hiveql.execute("""
    SELECT
        city,
        ABS(AVG(value)),
        STDDEV(value)
    FROM openaq_clean
    GROUP BY city
""")
coeff_b = {r[0]: r[2] / max(r[1], 1) for r in hiveql.fetchall()}


for sample_rate in sample_rates:

    logging.info("Sample rate: {0}".format(sample_rate))

    # create sampled table
    sample_input_table = rl_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        group_by=['attribution', 'city'],
        frequency=frequency,
        coefficient=coefficient,
        coeff_a=coeff_a,
        coeff_b=coeff_b,
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
