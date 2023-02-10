import os

num_warehouses = [5, 10, 20, 30, 40, 50, 60, 70, 80, 90]
commands = []

for num in num_warehouses:
    commands.append("./tpcc %d 0 >> records.txt" % num)

for command in commands:
    print("Running command: %s" % command)
    os.system(command)