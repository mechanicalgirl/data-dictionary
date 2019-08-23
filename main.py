# export FLASK_APP=main.py
# python3 -m flask run

import markdown
import os
import sys

from flask import Flask, request, redirect, render_template
from flask import Markup
from google.cloud import bigquery, storage
from google.cloud.bigquery import SchemaField

app = Flask(__name__)

def walk(top, maxdepth):
    dirs, nondirs = [], []
    for name in os.listdir(top):
        (dirs if os.path.isdir(os.path.join(top, name)) else nondirs).append(name)
    yield top, dirs, nondirs
    if maxdepth > 1:
        for name in dirs:
            for x, y, z in walk(os.path.join(top, name), maxdepth-1):
                yield x

@app.route("/", methods=['GET'])
def index():
    """
    This is a hardcoded index page that just
    links to the Postgresql and BigQuery sections.
    """
    return render_template('index.html')

@app.route("/database/<string:db_name>/")
def list_databases(db_name):
    """
    Postgresql - list the chorus_analytics database
    Bigquery - list all the project names
    """
    links = []
    list_file = 'schemas/%s/list.txt' % db_name
    f = open(list_file, 'r', encoding='utf-8')
    for line in f:
        links.append({"link": line, "label": line}) 
    return render_template('databases.html', database=db_name, links=links)

@app.route("/<string:db_name>/project/<string:project_name>/")
def list_datasets(db_name, project_name):
    """
    Postgresql - list the tables in the chorus_analytics database
    Bigquery - list all the datasets in the project
    """
    if db_name == 'postgresql':
        # generate 'page' links with the table names
        template = 'tables.html'
    else:
        # generate another set of links listing the datasets
        template = 'datasets.html'

    links = []
    list_file = 'schemas/%s/%s/list.txt' % (db_name, project_name)
    f = open(list_file, 'r', encoding='utf-8')
    for line in f:
        links.append({"link": line, "label": line})        
    return render_template(template, database=db_name, project=project_name, links=links)

@app.route("/<string:db_name>/project/<string:project_name>/dataset/<string:dataset_name>/")
def list_tables_in_dataset(db_name, project_name, dataset_name):
    """
    Bigquery - list all the tables in the dataset and link to them
    """
    if db_name == 'bigquery':
        # generate 'page' links with the table names
        template = 'tables.html'

        links = []
        list_file = 'schemas/%s/%s/%s/list.txt' % (db_name, project_name, dataset_name)
        f = open(list_file, 'r', encoding='utf-8')
        for line in f:
            links.append({"link": line, "label": line})
        return render_template(template, database=db_name, project=project_name, dataset=dataset_name, links=links)

@app.route("/<string:db_name>/project/<string:project_name>/dataset/<string:dataset_name>/table/<string:table_name>/")
@app.route("/<string:db_name>/project/<string:project_name>/table/<string:table_name>/")
def read_page(db_name, project_name, table_name, dataset_name=None):
    """
    - use the incoming page name to locate and open a markdown file
    - use the body of that file as 'content'
    - port that out to the read.html template
    """
    if db_name == 'postgresql':
        table_file = 'schemas/%s/%s/%s/%s.md' % (db_name, project_name, table_name, table_name)
    elif db_name == 'bigquery':
        table_file = 'schemas/%s/%s/%s/%s/%s.md' % (db_name, project_name, dataset_name, table_name, table_name)

    content = ''
    with open(table_file, 'r') as content_file:
        content = content_file.read()
    content = Markup(markdown.markdown(content))
    return render_template('read.md', **locals())

@app.route("/<string:db_name>/project/<string:project_name>/dataset/<string:dataset_name>/table/<string:table_name>/edit")
def edit_page(db_name, project_name, dataset_name, table_name):
    """
    - use the incoming page name to locate and open a markdown file
    - use the body of that file as 'content'
    - port that out to the edit.html template
    """
    if db_name == 'postgresql':
        table_file = 'schemas/%s/%s/%s.md' % (db_name, project_name, table_name)
    elif db_name == 'bigquery':
        table_file = 'schemas/%s/%s/%s/%s.md' % (db_name, project_name, dataset_name, table_name)

    content = ''
    with open(table_file, 'r') as content_file:
        content = content_file.read()
    content = Markup(markdown.markdown(content))
    return render_template('edit.html', **locals())

if __name__ == '__main__':
    app.run(debug=True)
