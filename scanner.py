## TODO: make sure we're handling removed files
## remove all files at the project level before writing anything new

# export GOOGLE_APPLICATION_CREDENTIALS=path/to/creds.json
# pip3 install -r requirements.txt 
# python3 scanner.py --platform postgresql

import argparse
import logging
import os
import sys

from google.cloud import bigquery
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from config.config import CONFIG

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

def traverse_postgresql():
    project = 'chorus_analytics'

    folder_project = 'schemas/postgresql/' + project
    if os.path.exists(folder_project):
        for root, dirs, files in os.walk(folder_project, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(folder_project)
    os.makedirs(folder_project)

    write_file('list', 'schemas/postgresql', [project])

    logging.info("Project: %s" % project)

    LIST_TABLES = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type != 'VIEW'
    ORDER BY table_name;
    """

    DESCRIBE_TABLE = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{}'
    """

    pgdb = CONFIG['postgres_db']
    engine = create_engine(pgdb, client_encoding='utf8', echo=False, pool_size=20, max_overflow=100)
    Session = sessionmaker(bind=engine)
    Session.configure(bind=engine)
    session = Session()

    result = engine.execute(text(LIST_TABLES))
    tables = result.fetchall()

    table_list = []
    for t in tables:
        table = t[0]
        table_list.append(table)
        logging.info("\tTable: %s" % table)

        folder_table = folder_project + '/' + table  # path
        if not os.path.exists(folder_table):
            os.makedirs(folder_table)

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
        write_file('table', folder_table, table_obj)

    write_file('list', folder_project, table_list)


def traverse_bigquery():
    client = bigquery.Client()
    datasets = list(client.list_datasets())
    project = client.project

    # if the list file exists, open and read
    # if project name is in it, close and continue
    # if project name is not in it, append and close
    filename = "schemas/bigquery/list.txt"
    if os.path.exists(filename):
        f = open(filename, 'r', encoding='utf-8')
        for line in f:
            if line == project:
                pass
            else:
                with open(filename, 'w+') as f:
                    f.write(project + "\n")
    else:
        with open(filename, 'w+') as f:
            f.write(project + "\n")

    folder_project = 'schemas/bigquery/' + project
    if os.path.exists(folder_project):
        for root, dirs, files in os.walk(folder_project, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(folder_project)
    os.makedirs(folder_project)

    dataset_list = []
    if datasets:
        logging.info("Project: %s" % project)
        for d in datasets:  # API request(s)
            dataset_id = d.dataset_id

            dataset_ref = client.dataset(dataset_id, project=project)
            dataset = client.get_dataset(dataset_ref)
            logging.info("Dataset: %s" % dataset_id)

            tables = list(client.list_tables(dataset))  # API request(s)
            if tables:
                # only make the dataset folder if there are tables in it
                folder_dataset = folder_project + '/' + dataset_id
                if not os.path.exists(folder_dataset):
                    os.makedirs(folder_dataset)
                dataset_list.append(dataset_id)

                table_list = []
                for t in tables:
                    table_ref = dataset_ref.table(t.table_id)
                    table = client.get_table(table_ref)
                    logging.info("\tTable: %s (%s rows)" % (t.table_id, table.num_rows))
                    table_list.append(t.table_id)

                    folder_table = folder_dataset + '/' + t.table_id
                    if not os.path.exists(folder_table):
                        os.makedirs(folder_table)

                    table_obj = {
                        "table": t.table_id,
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
                    write_file('table', folder_table, table_obj)
                write_file('list', folder_dataset, table_list)
            else:
                logging.info("This dataset does not contain any tables.")
    else:
        logging.info("%s project does not contain any datasets." % project)

    write_file('list', folder_project, dataset_list)


def write_file(type, path, body):
    if type == 'list':
        """
        postgresql - page that lists: chorus_analytics
            chorus_analytics: page that lists all tables
        bigquery - page that lists: voxmedia-chorus-analytics, voxmedia.com:phrasal-alpha-839
            in each project: page that lists all datasets
                in each dataset: page that lists all tables
        """
        filename = path + '/list.txt'

        file_str = ''
        for b in body:
            file_str += b+"\n"

    elif type == 'table':
        filename = path + '/' + body['table'] + '.md'

        table_label = 'Table: ' + path.split('/')[-1]
        table_underline = '=' * len(table_label)

        file_str = table_label + "\n"
        file_str += table_underline + "\n\n"
        for b in body['schema']:
            file_str += "Name: " + b['name'] + "\n"
            file_str += "Type: " + b['type'] + "\n\n"

    else:
        sys.exit()

    # always create the file as a new file
    with open(filename, 'w+') as f:
        f.write(file_str)
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
        # if platform is not defined, run chorus_analytics db by default
        traverse_postgresql()
