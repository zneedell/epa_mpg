import vin_processing as vp
import sys
from pymongo import MongoClient
from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count
import csv
from datetime import datetime
import re

def download_vin():
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
    missing_file_name = r'missing_'

    chunck_size = 10
    counter = 0
    n = len(vin_list)
    client = MongoClient('localhost:27017')
    for vins in [vin_list[i:i + chunck_size] for i in xrange(0, len(vin_list), chunck_size)]:
        sys.stderr.write('\rDone {}/{}={:.3%}'.format(counter, n, float(counter) / n))
        vp.download_and_store_in_mongdb(vins, client)
        counter += len(vins)


def merge_vins(collections):
    client = MongoClient('localhost:27017')
    for col in collections:
        bulk_has_operation = False
        count = 0
        #vin_list_db = list(client['epampg']['vin_merged'].find({}, {'Results.VIN': 1}))
        vin_list_db = list(client['epampg']['vin_merged'].aggregate([{"$unwind": "$Results"},
                                                                {"$group": {"_id": None, "vins": {"$push":"$Results.VIN"}}},
                                                                {"$project": {"vins": True, "_id": False}}]))
        cursor = client['epampg'][col].find({'Results.VIN': {'$nin': vin_list_db[0]["vins"]}})
        bulk = client['epampg']['vin_merged'].initialize_ordered_bulk_op()
        for vin_doc in cursor:
            if vin_doc['Message'] == 'Results returned successfully':
                bulk.insert(vin_doc)
                bulk_has_operation = True
            else:
                count = count + 1

        if bulk_has_operation:
            result = bulk.execute()
            print(result)
        print("count is {}".format(count))


def download_missing_vins():
    reload(vp)
    file_path = 'data/txsafe18.txt'
    counts_with_vins = vp.get_counts(file_path=file_path, put_in_db=False)
    vin_list = counts_with_vins.VIN.tolist()
    vin_list = [v for v in vin_list if len(v) >= 17]
    other_file_path = 'data/forybvin.txt'
    other_counts_with_vins = vp.get_counts(file_path=other_file_path, put_in_db=False)
    other_vin_list = other_counts_with_vins.VIN.tolist()
    other_vin_list = [v for v in other_vin_list if len(v) >= 17]
    vin_list = list(set(vin_list + other_vin_list))

    client = MongoClient('localhost:27017')
    vin_list_db = list(client['epampg']['vin_merged'].aggregate([{"$unwind": "$Results"},
                                                                 {"$group": {"_id": None,
                                                                             "vins": {"$push": "$Results.VIN"}}},
                                                                 {"$project": {"vins": True, "_id": False}}]))

    vin_list_db_encoded = [x.encode('ascii','ignore') for x in vin_list_db[0]["vins"]]
    vin_list_missing = list(set(vin_list)-set(vin_list_db_encoded))
    print("total: {}, database: {}, missing: {}".format(len(vin_list), len(vin_list_db_encoded), len(vin_list_missing)))

    chunk_size = 1
    counter = len(vin_list_db_encoded)
    n = len(vin_list)
    threadPool = ThreadPool(cpu_count())
    for vins in [vin_list_missing[i:i + chunk_size] for i in xrange(0, len(vin_list_missing), chunk_size)]:
        sys.stderr.write('\r[{}] {:.3%} Done => {}/{}'.format(datetime.now(), float(counter) / n, counter, n))
        vp.download_and_store_in_mongdb(vins, threadPool, client['epampg']['vin_merged'])
        counter += len(vins)


def export_vehicles():
    mongo_col = MongoClient('localhost:27017')['epampg']['vin_merged_filtered1']
    keys = [x.encode('ascii', 'ignore') for x in mongo_col.find_one().keys()]
    keys.remove('_id')
    with open('vehicles.csv', mode='w') as employee_file:
        wr = csv.writer(employee_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        wr.writerow(keys)
        counter = 0
        for veh in mongo_col.find():
            row = []
            for key in keys:
                s = veh[key]
                s_encoded = re.sub(' +', ' ', ("" if s is None else str(s) if type(s) == int else s).replace(u'\u2013', u'-').encode("utf-8"))
                row = [s_encoded] + row
            wr.writerow(row)
            counter = counter + 1
            sys.stderr.write('\r[{}] {:.3%} Done => {}'.format(datetime.now(), float(counter) / 370920, counter))


#merge_vins(['vehicles_1', 'vehicles_2'])
#download_missing_vins()
export_vehicles()