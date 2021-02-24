from dwr.CSRLSS import *
from dwr.non_partition.openaq_masg_lat import *

# create result table
hiveql.execute(create_result_table.format(RESULT_TABLE))

logging.info("Full query")
hiveql.execute(masg.format(
    result_table=RESULT_TABLE,
    input_table=INPUT_TABLE,
))
