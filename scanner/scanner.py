## TODO: make sure we're handling removed files
## remove all files at the project level before writing anything new

# export GOOGLE_APPLICATION_CREDENTIALS=path/to/creds.json
# pip3 install -r requirements.txt 
# python3 scanner.py --platform postgresql

import argparse
import json
import logging
import os
import re
import sys

from google.cloud import bigquery
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from config.config import CONFIG

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

root_dir = "schemas"

def traverse_postgresql():
    project = 'chorus_analytics'

    folder_project = '%s/postgresql/%s' % (root_dir, project)
    if os.path.exists(folder_project):
        for root, dirs, files in os.walk(folder_project, topdown=False):
            for name in files:
                logging.info("Clearing file: %s" % name)
                os.remove(os.path.join(root, name))
            for name in dirs:
                logging.info("Clearing folder: %s" % name)
                os.rmdir(os.path.join(root, name))
        logging.info("Clearing folder: %s" % folder_project)
        os.rmdir(folder_project)
    os.makedirs(folder_project)

    write_file('list', '%s/postgresql' % root_dir, [project])

    logging.info("Project: %s" % project)

    # TODO: work on a better way to distinguish parent
    # from child tables - this is an incomplete solution
    LIST_TABLES = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type != 'VIEW'
    AND table_name NOT LIKE 'fl_mt_%'
    ORDER BY table_name;
    """

    DESCRIBE_TABLE = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{}'
    """

    ENV = os.environ.get('ENV', 'staging')
    pgdb = CONFIG[ENV]['postgres_db']
    engine = create_engine(pgdb, client_encoding='utf8', echo=False, pool_size=20, max_overflow=100)
    Session = sessionmaker(bind=engine)
    Session.configure(bind=engine)
    session = Session()

    result = engine.execute(text(LIST_TABLES))
    tables = result.fetchall()

    project_obj = {
        "dataset": project,
        "tables": []
    }
    table_list = []
    for t in tables:
        table = t[0]
        table_list.append(table)
        logging.info("\tTable: %s" % table)

        q = DESCRIBE_TABLE.format(table)
        r = engine.execute(text(q))

        # file body
        table_obj = {
            "table": table,
            "schema": [],
        }
        schema = r.fetchall()
        for s in schema:
            table_obj['schema'].append({
                'name': s[0],
                'type': s[1],
            })
        project_obj['tables'].append(table_obj)

    write_file('table', folder_project, project_obj)
    write_file('list', folder_project, table_list)

def traverse_bigquery():
    client = bigquery.Client()
    datasets = list(client.list_datasets())
    project = client.project

    folder_project = '%s/bigquery/%s' % (root_dir, project)
    if os.path.exists(folder_project):
        for root, dirs, files in os.walk(folder_project, topdown=False):
            for name in files:
                logging.info("Clearing file: %s" % name)
                os.remove(os.path.join(root, name))
            for name in dirs:
                logging.info("Clearing folder: %s" % name)
                os.rmdir(os.path.join(root, name))
        logging.info("Clearing folder: %s" % folder_project)
        os.rmdir(folder_project)
    os.makedirs(folder_project)

    # if the list file exists, open and read
    # if project name is in it, close and continue
    # if project name is not in it, append and close
    filename = "%s/bigquery/list.txt" % root_dir
    if os.path.exists(filename):
        f = open(filename, 'r', encoding='utf-8')
        for line in f:
            if line == project:
                pass
            else:
                with open(filename, 'w+') as f:
                    f.write(project + "\n")
    else:
        # if the list file doesn't exist, create it
        f = open(filename, 'w+')
        f.write(project + "\n")

    dataset_list = []
    if datasets:
        logging.info("Project: %s" % project)
        for d in datasets:  # API request(s)
            dataset_id = d.dataset_id

            dataset_ref = client.dataset(dataset_id, project=project)
            dataset = client.get_dataset(dataset_ref)
            logging.info("Dataset: %s" % dataset_id)

            tables = list(client.list_tables(dataset))  # API request(s)

            # strip out partitions
            if tables:
                table_objs = []
                table_names = []
                for t in tables:
                    match = re.search(r'(_\d{8})$', t.table_id)
                    if match:
                        if not t.table_id[:-9] in table_names:
                            table_names.append(t.table_id[:-9])
                            table_objs.append((t.table_id[:-9], t))
                    else:
                        table_names.append(t.table_id)
                        table_objs.append((t.table_id, t))

                # only make the dataset folder if there are tables in it
                folder_dataset = folder_project + '/' + dataset_id
                if not os.path.exists(folder_dataset):
                    os.makedirs(folder_dataset)
                dataset_list.append(dataset_id)

                dataset_obj = {
                    "project": project,
                    "dataset": dataset_id,
                    "tables": []
                }
                table_list = []
                for t in table_objs:
                    table_ref = dataset_ref.table(t[1].table_id)
                    table = client.get_table(table_ref)

                    logging.info("\tTable: %s" % t[0])
                    table_list.append(t[0])

                    table_obj = {
                        "table": t[0],
                        "schema": [],
                    }
                    schema = list(table.schema)
                    for s in schema:
                        table_obj['schema'].append({
                            'name': s.name,
                            'type': s.field_type,
                            'description': s.description,
                            'is_nullable': s.is_nullable,
                            'mode': s.mode
                        })
                    dataset_obj['tables'].append(table_obj)

                write_file('table', folder_dataset, dataset_obj)
                write_file('list', folder_dataset, table_list)

            else:
                logging.info("This dataset does not contain any tables.")
    else:
        logging.info("%s project does not contain any datasets." % project)

    write_file('list', folder_project, dataset_list)


def write_file(type, path, body):
    if type == 'list':
        filename = path + '/list.txt'
        file_str = ''
        for b in body:
            file_str += b+"\n"
        with open(filename, 'w+') as f:
            f.write(file_str)

    elif type == 'table':
        filename = path + '/' + body['dataset'] + '.json'
        with open(filename, 'w') as f:
            json.dump(body, f, indent=4)

    else:
        sys.exit()

    logging.info("\t\tFile: %s" % filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--platform', help='bigquery or postgresql', action="store")
    args = parser.parse_args()

    if args.platform:
        if args.platform == 'bigquery':
            # GOOGLE_APPLICATION_CREDENTIALS will determine the project to run
            # so there's no need to pass the project name
            # If that var is not set, none of the BigQuery projects can be inspected.
            if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
                traverse_bigquery()
            else:
                logging.warning("GOOGLE_APPLICATION_CREDENTIALS is not set")
                sys.exit(125)
        else:
            traverse_postgresql()
    else:
        logging.warning("Platform not defined")
        sys.exit(125)
