from dwr.CSRLSS import *
from dwr.non_partition.openaq_masg_complex_time import *

SAMPLE_TYPE = SS


def run():

    logging.info("get statistics")

    t1 = time.time()  # ---------------------------------------
    # language=HQL
    hiveql.execute("""
        SELECT
            COUNT(*),
            SUM(value)
        FROM {input_table}
    """.format(input_table=INPUT_TABLE))
    result = hiveql.fetchone()
    total = result[0]
    sum_value = result[1]
    t2 = time.time()  # ---------------------------------------
    time_export("X", SAMPLE_TYPE, "Statistics", t2-t1)

    for sample_rate in sample_rates:

        logging.info("Sample rate: {0}".format(sample_rate))

        t1 = time.time()  # ---------------------------------------
        # create sampled table
        sample_input_table = ss_sample(
            sample_rate=sample_rate,
            table_name=INPUT_TABLE,
            aggregate_column='value',
            total=total,
            sum_value=sum_value,
            overwrite=True,
        )
        t2 = time.time()  # ---------------------------------------
        time_export(sample_rate, SAMPLE_TYPE, "Draw Sample", t2 - t1)

        # ----------------------------------------------------------
        # The following section applies to all three sampling methods

        # create result table
        logging.info('create result table')
        sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)
        drop_table(sample_result_table, DROP)
        hiveql.execute(create_result_table.format(sample_result_table))

        t1 = time.time()  # ---------------------------------------
        # run query over sampled table
        logging.info('run query over sampled table')
        hiveql.execute(masg_ss.format(
            input_table=sample_input_table,
            result_table=sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate),
        ))
        t2 = time.time()  # ---------------------------------------
        time_export(sample_rate, SAMPLE_TYPE, "Query", t2 - t1)

        # # evaluate sample error
        # sample_evaluate(
        #     table_name=RESULT_TABLE,
        #     sample_type=SAMPLE_TYPE,
        #     sample_rate=sample_rate,
        #     group_by_columns=['country'],
        #     aggregation_columns=['avg_incre', 'cnt_incre'],
        #     weighted=True
        # )
