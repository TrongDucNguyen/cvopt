from dwr.CSRLSS import *
from dwr.non_partition.openaq_mamg import *

SAMPLE_TYPE = CS

logging.info("get frequency of strata")
# language=HQL
hiveql.execute("""
    SELECT
        {g1} || '_' || {g2},
        COUNT(*)
    FROM {input}
    GROUP BY {g1}, {g2}
""".format(
    g1=GROUP_BY[0],
    g2=GROUP_BY[1],
    input=INPUT_TABLE,
))
frequency = {r[0]: r[1] for r in hiveql.fetchall()}

hiveql.execute("""
    SELECT
        CAST({g1} AS string),
        COUNT(*)
    FROM {input}
    GROUP BY {g1}
""".format(
    g1=GROUP_BY[0],
    input=INPUT_TABLE
    ))
freq1 = {r[0]: r[1] for r in hiveql.fetchall()}

hiveql.execute("""
    SELECT
        CAST({g2} AS string),
        COUNT(*)
    FROM {input}
    GROUP BY {g2}
""".format(
    g2=GROUP_BY[1],
    input=INPUT_TABLE
    ))
freq2 = {r[0]: r[1] for r in hiveql.fetchall()}


for sample_rate in sample_rates:

    logging.info("Sample rate: {0}".format(sample_rate))

    # create sampled table
    sample_table = cs_sample(
        sample_rate=sample_rate,
        table_name=INPUT_TABLE,
        group_by=GROUP_BY,
        frequency=frequency,
        freq1=freq1,
        freq2=freq2,
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
    hiveql.execute(mamg_s.format(
        input_table=sample_table,
        result_table=sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate),
    ))

    # evaluate sample error
    sample_evaluate(
        table_name=RESULT_TABLE,
        sample_type=SAMPLE_TYPE,
        sample_rate=sample_rate,
        group_by_columns=GROUP_BY,
        aggregation_columns=['agg1', 'agg2'],
        weighted=True
    )
