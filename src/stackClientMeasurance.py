import csv
from decimal import Decimal
import pandas as pd
import numpy as np
import os

path = "Y:/ProgramProject/CS5424_DD/CS5424/performance/Workload_A"
files = os.listdir(path)

# output_1 = pd.DataFrame()

throughputs = []

# Task 4.1
with open("clients.csv", "w") as out_csv:
    writer = csv.writer(out_csv)

    for fid in range(len(files)):
        filename = str(fid) + ".csv"
        print("Extracting:", filename)
        crt_csv = pd.read_csv(path + "/" + filename)
        data_list = []
        raw_data_list = list(crt_csv.columns)
        # Saved for Task 4.2
        throughputs.append(raw_data_list[3])
        # Check if is float - round to 2 decimal places
        for data in raw_data_list:
            if type(eval(data)) == float:
                data_list.append(float(Decimal(data).quantize(Decimal("0.00"))))
            else:
                data_list.append(data)
        writer.writerow(data_list)

# Task 4.2
with open("throughput..csv", "w") as out_csv:
    writer = csv.writer(out_csv)
    min_throughput = float(Decimal(min(throughputs)).quantize(Decimal("0.00")))
    max_throughput = float(Decimal(max(throughputs)).quantize(Decimal("0.00")))
    avg_throughput = float(Decimal(np.mean(throughputs)).quantize(Decimal("0.00")))
    writer.writerow([min_throughput, max_throughput, avg_throughput])








print(files)