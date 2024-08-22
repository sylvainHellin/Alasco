# Alasco

## Introduction  

This package is an SDK for `python` to facilitate interaction with the Alasco API.

The official documentation from [Alasco](https://www.alasco.com/) can be found [here](https://developer.alasco.de/).

## How to install it

Run the following command in your terminal:
```bash
pip install alasco
```

## Setup in python

### Import alasco

And import the module with the following command in your python script:
```python
from alasco import Alasco
```

### Instantiate client

Then instantiate the alasco client like this:
```python
from alasco import Alasco
from dotenv import load_dotenv, find_dotenv

_ = load_dotenv(find_dotenv(raise_error_if_not_found=True))

token = os.environ["token"] # your alasco token
key = os.environ["key"] # your alasco key

alasco = Alasco(token=token, key=key, verbose=True)
```

## Example: fetch all DataFrames

```python
dfs = alasco.data_fetcher.get_all_df()
dfs["properties"].head(2)
```
**Output:**

|    | id                                   | type     | name                     | description   | address        |   zip_code | city         | country   | date_created                     | relationships.projects.links.related                                               |
|---:|:-------------------------------------|:---------|:-------------------------|:--------------|:---------------|-----------:|:-------------|:----------|:---------------------------------|:-----------------------------------------------------------------------------------|
|  0 | 97bad92e-0fd8-4987-9f8e-aafcd4eafcd7 | PROPERTY | Wohnpark Unteraching     |               | Am Sportpark 4 |      82008 | Unterhaching |           | 2022-09-19T12:15:55.539340+00:00 | https://api.alasco.de/v1/properties/97bad92e-0fd8-4987-9f8e-aafcd4eafcd7/projects/ |
|  1 | 6ee2f67d-6911-4085-a7b3-4dbc370f494f | PROPERTY | Grundstück Leopoldstraße |               | Leopoldstr. 21 |      80802 | München      |           | 2021-07-14T09:50:47.760738+00:00 | https://api.alasco.de/v1/properties/6ee2f67d-6911-4085-a7b3-4dbc370f494f/projects/ |
