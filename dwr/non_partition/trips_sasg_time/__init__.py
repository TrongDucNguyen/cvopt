import time
import os
import csv

INPUT_TABLE = 'trips_3_year_subscribers'
RESULT_TABLE = 'trips_sasg_time'

DROP = True

OUTPUT_FILE = 'trips_sasg_time.csv'
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
        from_station_id INT,
        average DOUBLE
    )
"""

# language=HQL - average by parameter, unit
sasg = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        from_station_id,
        AVG(tripduration) average
    FROM {input_table}
    GROUP BY from_station_id
"""

sasg_ss = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        from_station_id,
        SUM(tripduration/sample_rate) / SUM(1/sample_rate) average
    FROM {input_table}
    GROUP BY from_station_id
"""