from dwr.CSRLSS import *
from dwr.non_partition.trips_cube import *

SAMPLE_TYPE = RL

logging.info("get frequency of strata")
# language=HQL
hiveql.execute("""
    SELECT
        {g1} || '_' || {g2},
        COUNT(*),
        ABS(AVG({a})),
        STDDEV({a})   
    FROM {input}
    GROUP BY {g1}, {g2}
""".format(
    g1=GROUP_BY[0],
    g2=GROUP_BY[1],
    a=AGG,
    input=INPUT_TABLE,
))
result = hiveql.fetchall()
frequency = {r[0]: r[1] for r in result}
coefficient = {r[0]: r[3]/max(r[2], 1) for r in result}

hiveql.execute("""
    SELECT
        CAST({g1} AS string),
        ABS(AVG({a})),
        STDDEV({a})   
    FROM {input}
    GROUP BY {g1}
""".format(
    g1=GROUP_BY[0],
    a=AGG,
    input=INPUT_TABLE
))
coeff_a = {r[0]: r[2] / max(r[1], 1) for r in hiveql.fetchall()}

hiveql.execute("""
    SELECT
        CAST({g2} AS string),
        ABS(AVG({a})),
        STDDEV({a})   
    FROM {input}
    GROUP BY {g2}
""".format(
    g2=GROUP_BY[1],
    a=AGG,
    input=INPUT_TABLE
))
coeff_b = {r[0]: r[2] / max(r[1], 1) for r in hiveql.fetchall()}


for sample_rate in sample_rates:

    logging.info("Sample rate: {0}".format(sample_rate))

    # create sampled table
    sample_table = rl_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        group_by=GROUP_BY,
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
    sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
    drop_table(sample_result_table, DROP)
    hiveql.execute(create_result_table.format(sample_result_table))

    # run query over sampled table
    logging.info('run query over sampled table')
    hiveql.execute(cube_sample.format(
        input_table=sample_table,
        result_table=sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate),
    ))

    # evaluate sample error
    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=GROUP_BY,
        aggregation_columns=['sum_value'],
    )
