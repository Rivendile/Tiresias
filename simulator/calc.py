import csv
import sys

log_path = sys.argv[1]

csv_reader = csv.reader(open(log_path+"/job.csv"))
jct_sum = 0
makespan = 0
cnt = 0
for line_id,line in enumerate(csv_reader):
    if line_id > 0:
        jct_sum += float(line[-5])
        makespan = max(makespan, float(line[5]))
        cnt += 1


print("Total jobs: %d, avg JCT: %.6f, makespan: %d" % (cnt, 1.0*jct_sum/cnt, makespan))