# export GOOGLE_APPLICATION_CREDENTIALS=path/to/creds.json
# pip3 install -r requirements.txt 
# python3 scanner.py --datastore postgresql

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

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
env = os.environ.get('ENV', 'staging')
root_dir = os.environ['WRITE_PATH'] if 'WRITE_PATH' in os.environ else 'schemas'

def traverse_postgresql():
    project = 'chorus_analytics'
    datasets = ['chorus_analytics']

    datastore_folder = '%s/postgresql' % root_dir
    project_folder = '%s/postgresql/%s' % (root_dir, project)
    clear_folders(project_folder)

    logging.info("Project: %s" % project)

    projects_file = "%s/projects.json" % datastore_folder
    if not os.path.isfile(projects_file):
        logging.info("Create new file: %s" % projects_file)
        with open(projects_file, 'w') as f:
            json.dump({"projects": [project]}, f, indent=4)
    else:
        pass
    logging.info("\tFile: %s" % projects_file)

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

    config_path = os.environ['CONFIG_PATH']
    config = json.loads(open(config_path).read())
    pgdb = config[env]['postgres_db']
    engine = create_engine(pgdb, client_encoding='utf8', echo=False, pool_size=20, max_overflow=100)
    Session = sessionmaker(bind=engine)
    Session.configure(bind=engine)
    session = Session()

    result = engine.execute(text(LIST_TABLES))
    tables = result.fetchall()

    datasets_obj = {
        "project": project,
        "datasets": [d for d in datasets]
    }

    if datasets:
        for d in datasets:
            dataset_id = d
            logging.info("Dataset: %s" % dataset_id)

            tables_obj = {
                "project": project,
                "dataset": dataset_id,
                "tables": []
            }

            if tables:

                # only make the dataset folder if there are tables in it
                dataset_folder = project_folder + '/' + dataset_id
                if not os.path.exists(dataset_folder):
                    os.makedirs(dataset_folder)

                for t in tables:
                    table = t[0]
                    logging.info("\tTable: %s" % table)

                    q = DESCRIBE_TABLE.format(table)
                    r = engine.execute(text(q))

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
                    tables_obj['tables'].append(table_obj)

                write_file(dataset_folder, tables_obj, 'tables')

            else:
                logging.info("This dataset does not contain any tables.")

    else:
        logging.info("%s project does not contain any datasets." % project)

    write_file(project_folder, datasets_obj, 'datasets')


def traverse_bigquery():
    client = bigquery.Client()
    project = client.project
    datasets = list(client.list_datasets())

    datastore_folder = '%s/bigquery' % root_dir
    project_folder = '%s/bigquery/%s' % (root_dir, project)
    clear_folders(project_folder)

    logging.info("Project: %s" % project)

    projects_file = "%s/projects.json" % datastore_folder
    if not os.path.isfile(projects_file):
        logging.info("Create new file: %s" % projects_file)
        with open(projects_file, 'w') as f:
            json.dump({"projects": [project]}, f, indent=4)
    else:
        try:
            with open(projects_file, 'r+') as f:
                data = json.load(f)
                f.seek(0)
                if project not in data['projects']:
                    data['projects'].append(project)
                json.dump(data, f, indent=4)
                f.truncate()
        except Exception as e:
            logging.info("Error, can't proceed: %s" % e)
            sys.exit(125)
    logging.info("\tFile: %s" % projects_file)

    datasets_obj = {
        "project": project,
        "datasets": [d.dataset_id for d in datasets]
    }

    if datasets:
        for d in datasets:  # API request(s)
            dataset_id = d.dataset_id

            dataset_ref = client.dataset(dataset_id, project=project)
            dataset = client.get_dataset(dataset_ref)
            logging.info("Dataset: %s" % dataset_id)

            tables = list(client.list_tables(dataset))  # API request(s)

            tables_obj = {
                "project": project,
                "dataset": dataset_id,
                "tables": []
            }

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
                dataset_folder = project_folder + '/' + dataset_id
                if not os.path.exists(dataset_folder):
                    os.makedirs(dataset_folder)

                for t in table_objs:
                    table_ref = dataset_ref.table(t[1].table_id)
                    table = client.get_table(table_ref)
                    logging.info("\tTable: %s" % t[0])

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
                    tables_obj['tables'].append(table_obj)

                write_file(dataset_folder, tables_obj, 'tables')

            else:
                logging.info("This dataset does not contain any tables.")

    else:
        logging.info("%s project does not contain any datasets." % project)

    write_file(project_folder, datasets_obj, 'datasets')


def write_file(path, body, name):
    filename = path + '/' + name + '.json'
    with open(filename, 'w') as f:
        json.dump(body, f, indent=4)
    logging.info("\tFile: %s" % filename)

def clear_folders(project_folder):
    if os.path.exists(project_folder):
        for root, dirs, files in os.walk(project_folder, topdown=False):
            for name in files:
                logging.info("Clearing file: %s" % name)
                os.remove(os.path.join(root, name))
            for name in dirs:
                logging.info("Clearing folder: %s" % name)
                os.rmdir(os.path.join(root, name))
        logging.info("Clearing folder: %s" % project_folder)
        os.rmdir(project_folder)
    os.makedirs(project_folder)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--datastore', help='bigquery or postgresql', action="store")
    args = parser.parse_args()

    if args.datastore:
        if args.datastore== 'bigquery':
            # GOOGLE_APPLICATION_CREDENTIALS will determine the project to run
            # so there's no need to pass the project name
            # If that var is not set, none of the BigQuery projects can be inspected.
            if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
                traverse_bigquery()
            else:
                logging.warning("GOOGLE_APPLICATION_CREDENTIALS is not set")
                sys.exit(125)
        else:
            if os.environ.get('CONFIG_PATH'):
                traverse_postgresql()
            else:
                logging.warning("CONFIG_PATH is not set")
                sys.exit(125)
    else:
        logging.warning("Platform not defined")
        sys.exit(125)
