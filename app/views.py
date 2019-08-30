import json
import markdown
import os
import sys

from app import app

from flask import render_template
from flask import Markup

root_dir = os.environ['WRITE_PATH'] if 'WRITE_PATH' in os.environ else 'schemas'

@app.route("/", methods=['GET'])
def index():
    return render_template('index.html')

@app.route("/search/<string:search_term>/", methods=['GET', 'POST'])
def search(search_term):
    pass
    return render_template('index.html')

@app.route("/db/<string:db_name>/", methods=['GET'])
def list_projects(db_name):
    """
    Postgresql - list the chorus_analytics database as a project
    Bigquery - list all the project names
    """
    links = []
    list_file = '%s/%s/projects.json' % (root_dir, db_name)
    f = open(list_file, 'r', encoding='utf-8')
    data = json.load(f)
    for d in data['projects']:
        links.append(d) 
    return render_template('projects.html', database=db_name, links=links)

@app.route("/db/<string:db_name>/project/<string:project_name>/", methods=['GET'])
def list_datasets(db_name, project_name):
    """
    Postgresql - list chorus_analytics as a dataset
    Bigquery - list all the datasets in the project
    """
    links = []
    list_file = '%s/%s/%s/datasets.json' % (root_dir, db_name, project_name)
    f = open(list_file, 'r', encoding='utf-8')
    data = json.load(f)
    for d in data['datasets']:
        links.append(d)                              
    return render_template('datasets.html', database=db_name, project=project_name, links=links)

@app.route("/db/<string:db_name>/project/<string:project_name>/dataset/<string:dataset_name>/", methods=['GET'])
def list_tables(db_name, project_name, dataset_name):
    """
    list all the tables in the dataset and link to them
    """
    links = []
    list_file = '%s/%s/%s/%s/tables.json' % (root_dir, db_name, project_name, dataset_name)
    f = open(list_file, 'r', encoding='utf-8')
    data = json.load(f)
    for d in data['tables']:
        links.append(d['table'])
    return render_template('tables.html', database=db_name, project=project_name, dataset=dataset_name, links=links)

@app.route("/db/<string:db_name>/project/<string:project_name>/dataset/<string:dataset_name>/table/<string:table_name>/", methods=['GET'])
def read_page(db_name, project_name, dataset_name, table_name):
    """
    """
    tables_file = '%s/%s/%s/%s/tables.json' % (root_dir, db_name, project_name, dataset_name)
    content = ''
    f = open(tables_file, 'r', encoding='utf-8')
    data = json.load(f)
    for d in data['tables']:
        if d['table'] == table_name:
            content = d
            for s in d['schema']:
                if "description" not in s:
                    s["description"] = ''
                if "is_nullable" not in s:
                    s["is_nullable"] = ''
                if "mode" not in s:
                    s["mode"] = ''
            print(d)
    return render_template('read.html', **locals())

@app.route("/db/<string:db_name>/project/<string:project_name>/dataset/<string:dataset_name>/table/<string:table_name>/edit", methods=['GET'])
def edit_page(db_name, project_name, dataset_name, table_name):
    """
    still TBD
    """
    return render_template('edit.html', **locals())
