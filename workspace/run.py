import os

num_warehouses = [150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 999]
commands = []

for num in num_warehouses:
    commands.append("./tpcc_blitz %d 0 >> records_blitz_0214.txt" % num)

for command in commands:
    print("Running command: %s" % command)
    os.system(command)
