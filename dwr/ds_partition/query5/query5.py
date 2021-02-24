from dwr import *
from dwr.ds_partition.query5 import *

# create result table
hiveql.execute(query5_create_table.format(RESULT_TABLE))

for ds in ds_list:
    logging.info("DS: {0} - Full query".format(ds))
    hiveql.execute(query5.format(
        query2_table=RESULT_TABLE,
        ds=ds,
    ))
