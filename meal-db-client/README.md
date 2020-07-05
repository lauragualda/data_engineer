# meal_db: a Python client to access The Meals DB API
https://www.themealdb.com/api.php

The Meal DB is a free and simple JSON API with food meals recipes. The `meal_db` is an Python client to pull data from this API and export it as a csv or as a pandas dataframe.

I had two reasons to choose it for this challenge:

1. Defining and delimiting the scope of a project like this was hard. I got excited about exploring all the popular public APIs that are very well designed and started thinking about all the data I could get and explore from it. But in the end, I recognized it would be better to focus on one API with a limited numer of data exploration possibilities, to focus on the engineering part of the problem.

2. During these last months of home office I had to start planning what I was going to eat for a week or more and it was often hard to have ideas or to decide what I should cook. So when I saw this API I got very enthusiastic and even added to my weekly plan one of the randomly selected recipes.

## Pulling data with the MealDBClient

My initial idea fot this project was that `meal_db_client` could be used to pull data via command line through a scheduled process or to pull data and use it within a Python framework, such as jupyter notebook. Therefore, the user can either clone the repository and activate a virtual environment to use it as a command line tool or import it as a Python package.


### 1. Installing it

1.1. Make sure you have Python3 installed on your local machine.  
1.2 Clone the repository into a local dir and cd to it
1.3 If you want to use it as a **command line tool**, install and spawn the repository virtual environment using `pipenv` or `virtualenv`:
* pipenv (https://pypi.org/project/pipenv/):
```
pipenv install
pipenv shell (should be active for every usage)
```

* virtualenv (https://pypi.org/project/virtualenv/)

```
virtualenv venv
source venv/bin/activate (should be active for every usage)
pip install -r requirements.txt
```

If you want to use it as a Python library, just make sure to import it from the correct dir, eg: 

```python
import sys
sys.path.append('local_repo_dir')
from api_client import meal_db
```


### 2. Using it

#### 2.1. As a command-line tool

The `meal_db_client.py` is a cli with three commands to pull meals data. You can execute them to pull data from inside the `pipenv shell` (or by calling it directly with `pipenv run python`) or `venv`.

Make sure you have a json file containing:

```
{"api_key" : "YOUR_API_KEY"}
```

(Only for testing purposes, I left my `config.json` on the repository.)

* To get see the cli functionalities:
```python
python meal_db_client.py --help
```

* To pull meals basic info filtering by category, area or ingredient value, eg:

```python
python ./meal_db_client.py filter -k ./config.json -ft category -fv Breakfast>> ./meal_db_client.logs 2>&1
```

* To pull meals recipes data based on a meal name or meal id search value.

```python
python ./meal_db_client.py search -k ./config.json -st meal_name -sv Carbonara>> ./meal_db_client.logs 2>&1
```
    
* To pull recipes data of n random meals.

```python
python ./meal_db_client.py random -k ./config.json -n 50>> ./meal_db_client.logs 2>&1
```

**Note: if you're calling `pipenv run` from outside the dir where you local repository lives, don't forget to specify the relative path to ./meal_db_client.py**


#### 2.2. As a Python library


The `meal_db` consists of one class: `MealDBClient`and it has three methods to pull meals data.

You can initialize this class by calling:

```python
import sys
sys.path.append('local_repo_dir')
from api_client import meal_db

client = meal_db.MealDBCLient(key=YOUR_API_KEY)
```

The three methods within this class are analogous to the command line tool functionalities:

* To pull basic info about n meals according to filters on category, area or ingredient:

```python
df_breakfast = client.get_meals_filter_by(filter_type='category', filter_value='Breakfast', n_meals=10)
```

* To pull recipes of available meals according to search criteria maches od meal_name or meal_id:

```python
df_carbonara_recipes = get_meals_search_by(search_type='meal_name', search_value='Carbonara')
```

* To pull recipes of n random meals:

```python
df_random_recipes = get_meals_random(n_meals=50)
```

* Extra: To query all the available categories, areas and ingredientd to filter by:

```python
categories = client.list_filter_values(filter_type='category')
```

(Only for testing purposes, you can use the api_key available on `config.json`.)

## Development

### 1. Requirements
* Python (3.6 or later)
* pipenv or virtualenv

### 2. Executing tests
1. Clone this repository on our local machine
2. Spawn the repository virtual environment by executing:
    - pipenv install
    - pipenv shell
    - python -m unittest

3. Reporting bugs: please send an email to laurasgualda@gmail.com  

### 3. Next steps
1. Use a more generic framework to wrap the application, such as Docker.
2. Enable installation of the python library via `pip install`
3. Based on the possible use cases for this API client, add more configuration parameters to existing methods.

