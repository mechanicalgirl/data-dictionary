from flask_wtf import Form
from wtforms import TextField, SubmitField
from wtforms import validators, ValidationError

class SearchForm(Form):
    term = TextField("Search Term",[validators.Required("Please enter a search term.")])
