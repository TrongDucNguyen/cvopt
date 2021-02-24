from dwr import *
from dwr.ds_partition.query7 import *

# create result table
hiveql.execute(query7_create_table.format(RESULT_TABLE))

for ds in ds_list:
    logging.info("DS: {0} - Full query".format(ds))
    hiveql.execute(query7.format(
        ds=ds,
        table_name=RESULT_TABLE,
        store_sales=STORE_SALES,
    ))
