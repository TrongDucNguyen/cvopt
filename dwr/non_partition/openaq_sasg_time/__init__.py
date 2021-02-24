import time
import os
import csv

INPUT_TABLE = 'openaq_10x'
RESULT_TABLE = 'openaq_sasg_time'

DROP = True

OUTPUT_FILE = 'openaq_sasg_time.csv'
OUTPUT_PATH = 'C:/Users/mshih/Downloads/'
OUTPUT = os.path.join(OUTPUT_PATH, OUTPUT_FILE)


# Time Export Function
# ------------ Usage -------------
# t1 = time.time()  # ---------------------------------------
# t2 = time.time()  # ---------------------------------------
# time_export(sample_rate, SAMPLE_TYPE, "Event", t2-t1)
# --------------------------------
def time_export(sample_rate, sample_type, time_type, elapsed_time):

    row = [sample_rate, sample_type, time_type, elapsed_time]

    with open(OUTPUT, 'a', newline='') as csvFile:
        w = csv.writer(csvFile)
        w.writerow(row)

    csvFile.close()



# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        country STRING,
        parameter STRING,
        unit STRING,
        average DOUBLE
    )
"""

# language=HQL - average by country parameter, unit
sasg = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        parameter, 
        unit, 
        AVG(value) average
    FROM {input_table}
    GROUP BY country, parameter, unit
"""

sasg_ss = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        parameter, 
        unit, 
        SUM(value/sample_rate) / SUM(1/sample_rate) average
    FROM {input_table}
    GROUP BY country, parameter, unit
"""
