from dwr import *
from dwr.non_partition.openaq_sasg_complex import *

SAMPLE_TYPE = VOILA


def run():

    t1 = time.time()  # ---------------------------------------

    logging.info('get statistic')
    # language=HQL
    hiveql.execute("""
        SELECT     
            country || '_' || MONTH(local_time) || '_' || YEAR(local_time),
            COUNT(*),
            ABS(STDDEV(value) / AVG(value))        
        FROM openaq_clean 
        GROUP BY country, MONTH(local_time), YEAR(local_time)
    """)
    result = hiveql.fetchall()
    frequency = {r[0]: r[1] for r in result}
    coefficient = {r[0]: r[2] for r in result}

    t2 = time.time()  # ---------------------------------------
    time_export("X", SAMPLE_TYPE, "Statistics", t2 - t1)

    for sample_rate in sample_rates:
        logging.info("Sample rate: {0}".format(sample_rate))

        t1 = time.time()

        # create sampled table
        sample_input_table = sasg_sample(
            sample_rate=sample_rate,
            table_name=INPUT_TABLE,
            group_by=['country', 'parameter', 'unit'],
            aggregate_column='value',
            frequency=frequency,
            coefficient=coefficient,
            overwrite=True,
        )

        t2 = time.time()  # ---------------------------------------
        time_export(sample_rate, SAMPLE_TYPE, "Draw Sample", t2 - t1)

        # ----------------------------------------------------------
        # The following section applies to all sampling methods

        # create result table
        logging.info('create result table')
        sample_result_table = sample_table_name(RESULT_TABLE, SAMPLE_TYPE, sample_rate)

        hiveql.execute(create_result_table.format(sample_result_table))

        t1 = time.time()  # ---------------------------------------

        # run query over sampled table
        logging.info('run query over sampled table')
        hiveql.execute(sasg.format(
            input_table=sample_input_table,
            result_table=sample_result_table,
        ))

        t2 = time.time()  # ---------------------------------------
        time_export(sample_rate, SAMPLE_TYPE, "Query", t2 - t1)

        # evaluate sample error
        sample_evaluate(
            table_name=RESULT_TABLE,
            sample_type=SAMPLE_TYPE,
            sample_rate=sample_rate,
            group_by_columns=['country', 'month', 'year'],
            aggregation_columns=['average'],
        )


run()