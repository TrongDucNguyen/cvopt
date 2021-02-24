from dwr import *
from dwr.non_partition.openaq_sasg_complex import *


def run():

    # create result table
    hiveql.execute(create_result_table.format(RESULT_TABLE))

    t1 = time.time()

    logging.info("Full query")
    hiveql.execute(sasg.format(
        result_table=RESULT_TABLE,
        input_table=INPUT_TABLE,
    ))

    t2 = time.time()

    time_export("X", "Original", "Query", t2-t1)

