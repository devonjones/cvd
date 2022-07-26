import sqlite3
import gzip
import csv
import sys
import os.path
from pprint import pprint
import importlib.resources as pkg_resources

def create_db(dbname):
    if os.path.exists(dbname):
        raise Exception("DB already exists")
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    cur.execute("CREATE TABLE registered_voters"
        " (voter_id INT PRIMARY KEY, county_code TEXT, county TEXT,"
        "  last_name TEXT, first_name TEXT, middle_name TEXT, name_suffix TEXT,"
        "  voter_name TEXT, status_code TEXT, precinct_name TEXT,"
        "  address_library_id INT, house_num INT, house_suffix TEXT,"
        "  pre_dir TEXT, street_name TEXT, street_type TEXT, post_dir TEXT,"
        "  unit_type TEXT, unit_num TEXT, residential_address TEXT,"
        "  residential_city TEXT, residential_state TEXT,"
        "  residential_zip_code TEXT, residential_zip_plus TEXT,"
        "  effective_date TEXT, registration_date TEXT, status TEXT,"
        "  status_reason TEXT, birth_year TEXT, gender TEXT, precinct TEXT,"
        "  split TEXT, voter_status_id TEXT, party TEXT, preference TEXT,"
        "  party_affiliation_date TEXT, phone_num TEXT, mail_addr1 TEXT,"
        "  mail_addr2 TEXT, mail_addr3 TEXT, mailing_city TEXT,"
        "  mailing_state TEXT, mailing_zip_code TEXT, mailing_zip_plus TEXT,"
        "  mailing_country TEXT, spl_id TEXT, permanent_mail_in_voter INT,"
        "  congressional TEXT, state_senate TEXT, state_house TEXT,"
        "  id_required INT)")
    cur.execute("CREATE INDEX idx_last_name ON registered_voters(last_name)")
    cur.execute("CREATE INDEX idx_first_name ON registered_voters(first_name)")
    cur.execute("CREATE INDEX idx_voter_name ON registered_voters(voter_name)")
    cur.execute("CREATE INDEX idx_house_num ON registered_voters(house_num)")
    cur.execute("CREATE INDEX idx_street_name"
        " ON registered_voters(street_name)")
    cur.execute("CREATE INDEX idx_residential_address"
        " ON registered_voters(residential_address)")
    cur.execute("CREATE INDEX idx_residential_city"
        " ON registered_voters(residential_city)")
    cur.execute("CREATE INDEX idx_residential_zip_code"
        " ON registered_voters(residential_zip_code)")
    conn.commit()
    cur.close()
    return conn

def find_data():
    retarr = []
    for file in pkg_resources.files("cvd.data").iterdir():
        if file.name.startswith("Registered_Voters") and file.name.endswith(".gz"):
            retarr.append(file)
    retarr.sort()
    return retarr

def load_file(conn, fn):
    print(fn.name)
    data = gzip.decompress(fn.read_bytes()).decode('latin-1').split("\r\n")
    csvreader = csv.reader(data)
    header = None
    curs = conn.cursor()
    count = 0
    for row in csvreader:
        if len(row) == 0:
            continue
        if not header:
            header = row
        else:
            count += 1
            curs.execute("INSERT INTO registered_voters VALUES (?, ?, ?, ?, ?,"
                " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                " ?, ?, ?, ?, ?, ?)", tuple(row))
        if count % 10000 == 0:
            print(count)
            conn.commit()
    curs.close()
    conn.commit()

def load_db(dbname):
    filenames = find_data()
    conn = create_db(dbname)
    for fn in filenames:
        load_file(conn, fn)
