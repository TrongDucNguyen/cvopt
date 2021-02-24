from dwr import *
from dwr.non_partition.openaq_masg_complex_time import *
from dwr.non_partition.openaq_masg_complex_time import masg_time
from dwr.non_partition.openaq_masg_complex_time import uniform_masg_time
from dwr.non_partition.openaq_masg_complex_time import ss_masg_time
from dwr.non_partition.openaq_masg_complex_time import cs_masg_time
from dwr.non_partition.openaq_masg_complex_time import rl_masg_time
from dwr.non_partition.openaq_masg_complex_time import voila_masg_time

LOOP = 1

# Export header
time_export("sample rate", "sample type", "time type", "time")

for i in range(1, LOOP+1):

    if i != 1:
        time_export(" ", " ", " ", " ")

    masg_time.run()
    uniform_masg_time.run()
    ss_masg_time.run()
    cs_masg_time.run()
    rl_masg_time.run()
    voila_masg_time.run()
