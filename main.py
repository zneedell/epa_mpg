import vin_processing as vp
from datetime import datetime
import pandas as pd
reload(vp)
file_path = 'data/txsafe18.txt'
counts_with_vins = vp.get_counts(file_path=file_path, put_in_db=False)
vin_list = counts_with_vins.VIN.tolist()
vin_list = [v for v in vin_list if len(v) >= 17]
len(vin_list)
other_file_path = 'data/forybvin.txt'
other_counts_with_vins = vp.get_counts(file_path=other_file_path, put_in_db=False)
other_vin_list = other_counts_with_vins.VIN.tolist()
other_vin_list = [v for v in other_vin_list if len(v) >= 17]
len(other_vin_list)
vin_list = list(set(vin_list + other_vin_list))
len(vin_list)
reload(vp)
file_name = r'out_parallel_'
if_exists = 'replace'

chunck_size = 50000
counter = 0
missing_vins = []
for vins in [vin_list[i:i + chunck_size] for i in xrange(0, len(vin_list), chunck_size)]:
    counter += len(vins)
    vin_file_path = r'data/{}.csv'.format(file_name + str(counter / chunck_size))
    print 'writing {}/{} in {}'.format(counter, len(vin_list), vin_file_path)

    missing_vins += vp.get_data_stream(vins, vin_file_path)

    #print '\tFixing column names'
    #vp.take_out_results_string(vin_file_path)
    #print '\tAdd vtyp and concat dataframes'
    #vin_df = pd.concat([vp.add_vtyp(vin_file_path, if_exists=if_exists), vin_df], axis=0)
    #missing_vins += list(set(vins) - set(vin_df.VIN))
    #print '\tWe\'re still missing {}'.format(len(missing_vins))

with open('data/missings.txt', 'w') as f:
    for item in missing_vins:
        print >> f, item