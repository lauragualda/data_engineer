import click
import logging
import sys
import json

from api_client import meal_db

@click.group()
def cli():
    """
    Command-line tool to to pull data from The Meal DB public API.
    \b
    Includes: \n
        1. Pull meals basic info filtering by category, area or ingredient value. \n
        2. Pull meals recipes data based on a meal name or meal id search value. \n
        3. Pull recipes data of n random meals. \n
    \b
    Commands: \n
        1. python meal_db_client.py filter -k ./config.json -ft category -fv Breakfast -o csv \n
        2. python meal_db_client.py search -k ./config.json -st meal_name -sv Carbonara \n
        3. python meal_db_client.py random -k ./config.json -n 50 \n
    \b
    Help: python meal_db_client.py ---help
    """
@cli.command()
@click.option('--key_config', '-k', prompt='Enter the config containing your API key.', default='./config.json')
@click.option('--filter_type', '-ft', prompt='Which filter would you like to apply: category, area or ingredient?', required=True)
@click.option('--filter_value', '-fv', prompt='Which value should be used for filtering?', required=True)
@click.option('--n_meals', '-n', help='Number of meals to limit the filter.', default=None)
@click.option('--csv_path', '-p', help='File path if you selected csv as output type.', default='./output.csv')
@click.option('--debug/--no-debug', default=False)

def filter(key_config, filter_type, filter_value, n_meals, csv_path, debug):
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    key = _load_config(json_path=key_config)['api_key']
    client = meal_db.MealDBClient(key)
    try:
        if n_meals: 
            client.get_meals_filter_by(filter_type=filter_type, filter_value=filter_value, n_meals=int(n_meals), output_type='csv', csv_path=csv_path)
        else:
            client.get_meals_filter_by(filter_type=filter_type, filter_value=filter_value, output_type='csv', csv_path=csv_path)
        logging.info('Successfully pulled data from API.')
    except Exception as err:
        logging.error('Pulling data process failed.', extra={'error': err})
        sys.exit(err)

@cli.command()
@click.option('--key_config', '-k', prompt='Enter the config containing your API key.', default='./config.json')
@click.option('--search_type', '-st', prompt='Which search would you like to apply: meal_id or meal_name?', required=True)
@click.option('--search_value', '-sv', prompt='Which value should be used for searching?', required=True)
@click.option('--csv_path', '-p', help='File path if you selected csv as output type.', default='./output.csv')
@click.option('--debug/--no-debug', default=False)

def search(key_config, search_type, search_value, csv_path, debug):
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    key = _load_config(json_path=key_config)['api_key']
    client = meal_db.MealDBClient(key)
    try:
        client.get_meals_search_by(search_type=search_type, search_value=search_value, output_type='csv', csv_path=csv_path)
        logging.info('Successfully pulled data from API.')
    except Exception as err:
        logging.error('Pulling data process failed.', extra={'error': err})
        sys.exit(err)

@cli.command()
@click.option('--key_config', '-k', prompt='Enter the config containing your API key.', default='./config.json')
@click.option('--n_meals', '-n', help='Number of meals to limit the filter.', required=True)
@click.option('--csv_path', '-p', help='File path if you selected csv as output type.', default='./output.csv')
@click.option('--debug/--no-debug', default=False)

def random(key_config, n_meals, csv_path, debug):
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    key = _load_config(json_path=key_config)['api_key']
    client = meal_db.MealDBClient(key)
    try:
        client.get_meals_random(int(n_meals), output_type='csv', csv_path=csv_path)
        logging.info('Successfully pulled data from API.')
    except Exception as err:
        logging.error('Pulling data process failed.', extra={'error': err})
        sys.exit(err)

def _load_config(json_path):
    """
    A helper to read the API key from a config file. 
    Parameters
    ----------
        json_path (str): Absolute path of the config.

    Returns
    -------
        response (object): Server's response to the HTTP request.
    """
    try:
        with open(json_path, "r") as f:
            logging.info("Reading API key.")
            return json.load(f)
    except Exception:
        logging.error("Could not open config file. Do you have it on your directory?")
