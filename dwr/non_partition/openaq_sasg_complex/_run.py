from dwr import *
from dwr.non_partition.openaq_sasg_complex import *
from dwr.non_partition.openaq_sasg_complex import sasg_complex
from dwr.non_partition.openaq_sasg_complex import uniform_sasg_complex
from dwr.non_partition.openaq_sasg_complex import ss_sasg_complex
from dwr.non_partition.openaq_sasg_complex import cs_sasg_complex
from dwr.non_partition.openaq_sasg_complex import rl_sasg_complex
from dwr.non_partition.openaq_sasg_complex import voila_sasg_complex

LOOP = 1

# Export header
time_export("sample rate", "sample type", "time type", "time")

for i in range(1, LOOP+1):

    if i != 1:
        time_export(" ", " ", " ", " ")

    sasg_complex.run()
    uniform_sasg_complex.run()
    ss_sasg_complex.run()
    cs_sasg_complex.run()
    rl_sasg_complex.run()
    voila_sasg_complex.run()
