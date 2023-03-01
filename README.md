# NYC TLC - Yellow Taxi data modelling

A project to store all the notes and codes for Yellow Taxi data modelling.

Data source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page. Dataset from December 2019 was used. Data Dictionaries and MetaData can be found by following the same link.

# Using virtual environment

Create virtual environment
```
python3 -m venv dbt-env-taxi
```

Activate virtual environment
```
source dbt-env-taxi/bin/activate
```

Upgrade pip
```
pip install --upgrade pip wheel setuptools
```

Install packages from the **requirements.txt** to the virtual environment
```
pip install -r requirements.txt
```