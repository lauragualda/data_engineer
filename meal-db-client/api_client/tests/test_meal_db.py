import unittest
import api_client.meal_db as meal_db
import pandas as pd 
import requests
from requests.exceptions import ConnectionError
import responses

BASE_URL='https://www.themealdb.com/api/json/v1'
API_KEY='1'

class TestMealDBClient(unittest.TestCase):
    @responses.activate 
    def test_connect_api_fail(self):
        responses.add(responses.GET, 
                      'https://www.themealdb.com/api/json/v1/1/filter.php?c=Sea',
                       json={'error': 'not found'}, 
                       status=404)

        endpoint = '/filter.php?c=Sea'
        client = meal_db.MealDBClient(key=API_KEY)
        client_response = client._connect_api(endpoint)
        self.assertEqual(client_response.status_code, 404)

    def test_list_filter_values(self):
        list_expected = ['Beef', 'Breakfast', 'Chicken', 'Dessert', 'Goat', 'Lamb', 'Miscellaneous', 
                      'Pasta', 'Pork', 'Seafood', 'Side', 'Starter', 'Vegan', 'Vegetarian']
        client = meal_db.MealDBClient(key=API_KEY, api_base_url=BASE_URL)
        list_response = client.list_filter_values(filter_type='category')
        self.assertEqual(list_response, list_expected)

    def test_get_meals_filter_by(self):
        expected = 'Brown Stew Chicken' # Note that this is dangerous, test can start to fail if new meals are added or change positions
        client = meal_db.MealDBClient(key=API_KEY, api_base_url=BASE_URL)
        df_response = client.get_meals_filter_by(filter_type='ingredient', filter_value='chicken', n_meals=1, output_type='pandas')
        response = df_response.strMeal.loc[0]
        self.assertEqual(response, expected)

    def test_get_meals_search_by(self):
        expected = 'Chicken' # Note that this is dangerous, test can start to fail if new meals are added or change positions
        client = meal_db.MealDBClient(key=API_KEY, api_base_url=BASE_URL)
        df_response = client.get_meals_search_by(search_type='meal_name', search_value='chicken', output_type='pandas')
        response = df_response.strCategory.loc[0]
        self.assertEqual(response, expected)

    def test_connect_random_endpoint(self):
        expected = 5 
        client = meal_db.MealDBClient(key=API_KEY, api_base_url=BASE_URL)
        df_response = client.get_meals_random(n_meals=5, output_type='pandas')
        response = len(df_response)
        self.assertEqual(response, expected)