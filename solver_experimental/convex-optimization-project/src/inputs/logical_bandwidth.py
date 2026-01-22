import csv


def load_logical_bandwidth(filepath):
    logical_bw = {}
    with open(filepath, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) != 3:
                continue
            t1, t2, bw = row
            logical_bw[(t1, t2)] = float(bw)
    return logical_bw
