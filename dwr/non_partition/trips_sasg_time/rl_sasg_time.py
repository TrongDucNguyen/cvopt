from dwr.CSRLSS import *
from dwr.non_partition.trips_sasg_time import *

SAMPLE_TYPE = RL


def run():

    t1 = time.time()  # ---------------------------------------
    logging.info('get statistic')
    hiveql.execute("""
        SELECT     
            CAST(from_station_id AS string),
            COUNT(*),
            ABS(AVG(tripduration)),
            STDDEV(tripduration)        
        FROM {input_table}
        GROUP BY from_station_id
    """.format(input_table=INPUT_TABLE))
    result = hiveql.fetchall()
    frequency = {r[0]: r[1] for r in result}
    coefficient = {r[0]: r[3] / max(r[2], 1) for r in result}

    t2 = time.time()  # ---------------------------------------
    time_export("X", SAMPLE_TYPE, "Statistics", t2-t1)

    for sample_rate in sample_rates:

        logging.info("Sample rate: {0}".format(sample_rate))

        t1 = time.time()

        # create sampled table
        sample_input_table = rl_sample(
            sample_rate=sample_rate,
            table_name=INPUT_TABLE,
            aggregate_column='tripduration',
            group_by=['from_station_id'],
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
        drop_table(sample_result_table, DROP)
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
        # sample_evaluate(
        #     table_name=RESULT_TABLE,
        #     sample_type=SAMPLE_TYPE,
        #     sample_rate=sample_rate,
        #     group_by_columns=['country', 'parameter', 'unit'],
        #     aggregation_columns=['average'],
        # )
