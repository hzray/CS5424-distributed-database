import csv
from decimal import Decimal
import pandas as pd
import numpy as np
import os

PERFORMANCE_DIR = "Y:/ProgramProject/CS5424_DD/CS5424/performance/Workload_A"
CLIENT_CSV_OUTPUT_DIR = ""
THROUGHPUT_CSV_OUTPUT_DIR = ""
DBSTATE_CSV_OUTPUT_DIR = ""
DBSTATE_TXT_INPUT_DIR = ""

files = os.listdir(PERFORMANCE_DIR)

# output_1 = pd.DataFrame()

throughputs = []

# Task 4.1
with open(CLIENT_CSV_OUTPUT_DIR + "clients.csv", "w", newline='') as out_csv:
    writer = csv.writer(out_csv)

    for fid in range(len(files)):
        filename = str(fid) + ".csv"
        print("Extracting:", filename)
        crt_csv = pd.read_csv(PERFORMANCE_DIR + "/" + filename)
        data_list = []
        raw_data_list = list(crt_csv.columns)
        # Saved for Task 4.2
        throughputs.append(float(raw_data_list[3]))
        # Check if is float - round to 2 decimal places
        for data in raw_data_list:
            if type(eval(data)) == float:
                data_list.append(float(Decimal(data).quantize(Decimal("0.00"))))
            else:
                data_list.append(int(data))
        writer.writerow(data_list)

# Task 4.2
with open(THROUGHPUT_CSV_OUTPUT_DIR + "throughput.csv", "w", newline='') as out_csv:
    writer = csv.writer(out_csv)

    min_throughput = float(Decimal(min(throughputs)).quantize(Decimal("0.00")))
    max_throughput = float(Decimal(max(throughputs)).quantize(Decimal("0.00")))
    avg_throughput = float(Decimal(np.mean(throughputs)).quantize(Decimal("0.00")))
    writer.writerow([min_throughput, max_throughput, avg_throughput])


# Task 4.3
with open(DBSTATE_CSV_OUTPUT_DIR + "dbstate.csv", "w", newline='') as out_csv, open(DBSTATE_TXT_INPUT_DIR + "dbstate.txt", "r") as f_txt:
    writer = csv.writer(out_csv)
    lines = f_txt.readlines()
    for line in lines:
        data = line.split(" = ")
        writer.writerow(data)



print("Completed.")