from dwr import *
from dwr.ds_partition.query3 import *

for ds in ds_list:
    logging.info("DS: {0} - Full query".format(ds))
    insert(
        table_name='query3',
        select=query3.format(
            ds=ds,
        ),
        partition={'ds': ds},
    )
