import json
import markdown
import os
import sys

from app import app
from app.forms import SearchForm

from flask import Flask, render_template, request, flash, redirect

root_dir = os.environ['WRITE_PATH'] if 'WRITE_PATH' in os.environ else 'schemas'


def text_search(search_term):
    lterm = search_term.lower()
    results = []

    table_files = []
    for root, dirs, files in os.walk(root_dir, topdown=False):
        for name in files:
            file_path = os.path.join(root, name)
            if file_path.split('/')[-1] == 'tables.json':
                table_files.append(file_path)

    for file_path in table_files:
        f = open(file_path, 'r', encoding='utf-8')
        data = json.load(f)

        db = 'bigquery'
        if data['project'].lower() == 'chorus_analytics':
            db = 'postgresql'

        if lterm in data['project'].lower():
            result = {
                'type': 'project',
                'icon': 'server',
                'name': data['project'],
                'description': "Project in the '%s' database" % db,
                'url': '/db/%s/project/%s/' % (db, data['project'])
            }
            results.append(result)

        if lterm in data['dataset'].lower():
            result = {
                'type': 'dataset',
                'icon': 'database',
                'name': data['dataset'],
                'description': "Dataset in the '%s' project" % data['project'],
                'url': '/db/%s/project/%s/dataset/%s/' % (db, data['project'], data['dataset'])
            }
            results.append(result)

        for t in data['tables']:
            if lterm in t['table'].lower():
                result = {
                    'type': 'table',
                    'icon': 'table',
                    'name': t['table'],
                    'description': "Table in the '%s' dataset" % data['dataset'],
                    'url': '/db/%s/project/%s/dataset/%s/table/%s/' % (db, data['project'], data['dataset'], t['table'])
                }
                results.append(result)

        for t in data['tables']:
            for item in t['schema']:
                if lterm in item['name'].lower():
                    result = {
                        'type': 'column',
                        'icon': 'columns',
                        'name': '%s' % item['name'],
                        'description': "'%s' column in the '%s' table" % (item['type'], t['table']),
                        'url': '/db/%s/project/%s/dataset/%s/table/%s/' % (db, data['project'], data['dataset'], t['table'])
                    }
                    results.append(result)

    deduped_results = [dict(t) for t in {tuple(d.items()) for d in results}]
    return deduped_results


@app.route("/", methods=['GET'])
def index():
    form = SearchForm()
    return render_template('index.html', **locals())

@app.route("/search/", methods=['GET', 'POST'])
def search():
    form = SearchForm()
    if request.method == 'POST':
        if form.validate() == False:
            flash('Search term is required.')
            return redirect(request.referrer)
        else:
            search_term = form.data['term']
            results = text_search(search_term)
            return render_template('searchresults.html', **locals())

@app.route("/db/<string:db_name>/", methods=['GET'])
def list_projects(db_name):
    """
    Postgresql - list the chorus_analytics database as a project
    Bigquery - list all the project names
    """
    form = SearchForm()
    links = []
    list_file = '%s/%s/projects.json' % (root_dir, db_name)
    f = open(list_file, 'r', encoding='utf-8')
    data = json.load(f)
    for d in data['projects']:
        links.append(d) 
    return render_template('projects.html', **locals())

@app.route("/db/<string:db_name>/project/<string:project_name>/", methods=['GET'])
def list_datasets(db_name, project_name):
    """
    Postgresql - list chorus_analytics as a dataset
    Bigquery - list all the datasets in the project
    """
    form = SearchForm()
    links = []
    list_file = '%s/%s/%s/datasets.json' % (root_dir, db_name, project_name)
    f = open(list_file, 'r', encoding='utf-8')
    data = json.load(f)
    for d in data['datasets']:
        links.append(d)                              
    return render_template('datasets.html', **locals())

@app.route("/db/<string:db_name>/project/<string:project_name>/dataset/<string:dataset_name>/", methods=['GET'])
def list_tables(db_name, project_name, dataset_name):
    """
    list all the tables in the dataset and link to them
    """
    form = SearchForm()
    links = []
    list_file = '%s/%s/%s/%s/tables.json' % (root_dir, db_name, project_name, dataset_name)
    f = open(list_file, 'r', encoding='utf-8')
    data = json.load(f)
    for d in data['tables']:
        links.append(d['table'])
    return render_template('tables.html', **locals())

@app.route("/db/<string:db_name>/project/<string:project_name>/dataset/<string:dataset_name>/table/<string:table_name>/", methods=['GET'])
def read_page(db_name, project_name, dataset_name, table_name):
    """
    """
    form = SearchForm()
    tables_file = '%s/%s/%s/%s/tables.json' % (root_dir, db_name, project_name, dataset_name)
    content = ''
    f = open(tables_file, 'r', encoding='utf-8')
    data = json.load(f)
    for d in data['tables']:
        if d['table'] == table_name:
            content = d
            # add bigquery metadata to postgres
            for s in d['schema']:
                if "description" not in s:
                    s["description"] = ''
                if "is_nullable" not in s:
                    s["is_nullable"] = ''
                if "mode" not in s:
                    s["mode"] = ''
    return render_template('read.html', **locals())

@app.route("/db/<string:db_name>/project/<string:project_name>/dataset/<string:dataset_name>/table/<string:table_name>/edit", methods=['GET'])
def edit_page(db_name, project_name, dataset_name, table_name):
    """
    still TBD
    """
    return render_template('edit.html', **locals())
