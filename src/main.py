from os import environ
import argparse
import sys
import requests
from requests.auth import HTTPBasicAuth
from sodapy import Socrata

from elasticsearch import Elasticsearch, RequestsHttpConnection,helpers


parser = argparse.ArgumentParser(description='Process data from OPCV.')
parser.add_argument('--page_size', type=int,
                    help='how many rows to get per page', required=True)
parser.add_argument('--num_pages',
                    type=int, help='how many pages to get in total')
args = parser.parse_args(sys.argv[1:])
if args.page_size==0:
     raise Exception("Error: page_size cannot be 0")

DATASET_ID = environ["DATASET_ID"]
APP_TOKEN = environ["APP_TOKEN"]
ES_HOST = environ["ES_HOST"]
ES_USERNAME = environ["ES_USERNAME"]
ES_PASSWORD = environ["ES_PASSWORD"]


if __name__ == '__main__':
    # create elasticsearch index
    try:
        resp = requests.put(
            f"https://{ES_HOST}/project01-opcv",
            auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            # these are the "columns" of this database/table
            json={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                 },
                "mappings": {
                    "properties": {
                        "plate": {"type": "text"},
                        "state": {"type": "keyword"},
                        "license_type": {"type": "keyword"},
                        "summons_number": {"type": "text"},
                        "issue_date": {"type": "date","format":"MM/dd/yyyy"},
                        "violation_time": {"type": "text"},
                        "violation": {"type": "keyword"},
                        "fine_amount": {"type": "float"},
                        "penalty_amount": {"type": "float"},
                        "interest_amount": {"type": "float"},
                        "reduction_amount": {"type": "float"},
                        "payment_amount": {"type": "float"},
                        "amount_due": {"type": "float"},
                        "precinct": {"type": "keyword"},
                        "county": {"type": "keyword"},
                        "issuing_agency": {"type": "keyword"},
                    }
                },
            })
        resp.raise_for_status()
    except Exception:
        print("Index already exists! Skipping")
    
    
    client = Socrata(
        "data.cityofnewyork.us",
        APP_TOKEN,
    )
    client.timeout = 1000
    
    if args.num_pages==None:
        number_of_rows=client.get(DATASET_ID,select='COUNT(*)')[0]['COUNT']
    else:
        number_of_rows=args.page_size*args.num_pages

    try:
        es = Elasticsearch(
            hosts=[{'host':ES_HOST[8:] , 'port': 443}],
            http_auth=(ES_USERNAME, ES_PASSWORD),
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection)
        print("Successfully connect to ElasticSearch")
        # print(es.info())
    except Exception as E:
        print("Unable to connect to {0}".format(ES_HOST))
        print(E)

    start = 0 
    chunk_size = args.page_size
    while True:
         # If we have fetched all of the records, bail out
         if start >= int(number_of_rows):
            break
         rows =[]
         # Fetch the set of records starting at 'start'
         rows.extend( client.get(DATASET_ID,limit=chunk_size , offset=start))
         #upload to elasticsearch one by one
         ACTIONS = []
         for row in rows:
            try:
                # convert
                row["fine_amount"] = float(row["fine_amount"])
                row["penalty_amount"] = float(row["penalty_amount"])
                row["interest_amount"] = float(row["interest_amount"])
                row["reduction_amount"] = float(row["reduction_amount"])
                row["payment_amount"] = float(row["payment_amount"])
                row["amount_due"] = float(row["amount_due"])
            except Exception as e:
                #print(f"Error!: {e}, skipping row: {row}")
                continue
            
            try:
                action = {
                    "_index": "project01-opcv",
                    "_type": "_doc",
                    "_source": {
                        "plate": row["plate"],
                        "state": row["state"],
                        "license_type": row["license_type"],
                        "summons_number": row["summons_number"],
                        "issue_date": row["issue_date"],
                        "violation_time": row["violation_time"],
                        "violation": row["violation"],
                        "fine_amount": row["fine_amount"],
                        "penalty_amount": row["penalty_amount"],
                        "interest_amount": row["interest_amount"],
                        "reduction_amount": row["reduction_amount"],
                        "payment_amount": row["payment_amount"],
                        "amount_due": row["amount_due"],
                        "precinct": row["precinct"],
                        "county": row["county"],
                        "issuing_agency": row["issuing_agency"],
                        }
                }
                ACTIONS.append(action)
            except Exception as e:
                #print(f"Error!: {e}, skipping row: {row}")
                continue
                
         
         try:
             resp = helpers.bulk(es, ACTIONS)
             print(resp)
         except Exception as e:
             continue
         
         start=start+chunk_size

    # resp = requests.get(ES_HOST, auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD))
    # print(resp.json())