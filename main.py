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

@app.route("/")
def index():
    links = []
    # rootDir = '.'
    rootDir = 'schemas'
    for dirName, subdirList, fileList in os.walk(rootDir):
        page_link = '.'.join(dirName.split('/')[1:])
        for fname in fileList:
            href_label = fname
            links.append({"link": page_link, "label": fname})

    return render_template('index.html', links=links)

@app.route("/pages/<string:page_name>/")
def read_page(page_name):
    """
    - use the incoming page name to locate and open a markdown file
    - use the body of that file as 'content'
    - port that out to the read.html template
    """
    table_file = 'schemas/' + ('/').join(page_name.split('.')) + '/' + page_name.split('.')[-1] + '.md'
    content = ''
    with open(table_file, 'r') as content_file:
        content = content_file.read()
    content = Markup(markdown.markdown(content))
    return render_template('read.html', **locals())

@app.route("/pages/<string:page_name>/edit")
def edit_page(page_name):
    """
    - use the incoming page name to locate and open a markdown file
    - use the body of that file as 'content'
    - port that out to the edit.html template
    """
    table_file = 'schemas/' + ('/').join(page_name.split('.')) + '/' + page_name.split('.')[-1] + '.md'
    content = ''
    with open(table_file, 'r') as content_file:
        content = content_file.read()
    content = Markup(markdown.markdown(content))
    return render_template('edit.html', **locals())

if __name__ == '__main__':
    app.run(debug=True)
