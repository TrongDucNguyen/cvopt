from dwr import *
from dwr.non_partition.trips_sasg_time import *
from dwr.non_partition.trips_sasg_time import sasg_time
from dwr.non_partition.trips_sasg_time import uniform_sasg_time
from dwr.non_partition.trips_sasg_time import ss_sasg_time
from dwr.non_partition.trips_sasg_time import cs_sasg_time
from dwr.non_partition.trips_sasg_time import rl_sasg_time
from dwr.non_partition.trips_sasg_time import voila_sasg_time

LOOP = 1

# Export header
time_export("sample rate", "sample type", "time type", "time")

for i in range(1, LOOP+1):

    if i != 1:
        time_export(" ", " ", " ", " ")

    sasg_time.run()
    uniform_sasg_time.run()
    ss_sasg_time.run()
    cs_sasg_time.run()
    rl_sasg_time.run()
    voila_sasg_time.run()
