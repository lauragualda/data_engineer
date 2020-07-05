import requests
import pandas as pd
import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

"""A class to connect and get data from The Meal DB public API https://www.themealdb.com/api.php."""

class MealDBClient:
    """
    Parameters   
    ----------
        url_base (str): The Meal DB API base url, default 'https://www.themealdb.com/api/json/v1/'
                        The url might be updated for newer versions.

        key (str): API key for authentication, for app development phase use '1'
                   For a production API key, signup as Patreon supporter.

    """
    def __init__(self, key, api_base_url = 'https://www.themealdb.com/api/json/v1'):
        self.url_base = f'{api_base_url}/{key}'
    
    def _connect_api(self, endpoint):
        """
        Establishes connection to API using Python requests package.

        Parameters
        ----------
            endpoint (str): The endpoint to call in the API.

        Returns
        -------
            response (object): Server's response to the HTTP request.
        """
        url = self.url_base + endpoint
        try:
            response = requests.get(url)
            response.encoding = 'utf-8'
            logging.info('Successfully connected to API.', extra={'url': url})
            return response
        except requests.exceptions.RequestException as err:
            logging.error("Could not connect to the API. Can you verify the input parameters?",extra={'error': err})  
            raise SystemExit(err)
            
    def _output_type(self, df, output_type, csv_path):
        """
        Returns data according to output type.

        Parameters
        ----------
            df (pandas.DataFrame): Valid DataFrame returned from methods within MealDBClient.
            
            output_type (int) : Format of the data outuput: pandas.DataFrame or csv

            csv_path (str) : Absolute path of where the csv should be downloaded to.

        Returns
        -------
            pandas.DataFrame or csv
        """
        
        if output_type == 'pandas':
            logging.info(f'Pulling data as a pandas.DataFrame.')
            return df
        elif output_type == 'csv':
            logging.info(f'Exporting data as csv to {csv_path}.')
            return df.to_csv(csv_path)
        else:
            logging.error(f'Please specify a valid output type: pandas or csv.')
            return

    def list_filter_values(self, filter_type): 
        """
        Get the n first meals filtered according to filter_type = filter_value.

        Parameters
        ----------
            filter_type (str) : Filter types supported by the API: category, area, ingredient.

        Returns
        -------
            list_filter_values (list) : List with all possible values for a given filter_type.
        """

        if filter_type not in ['ingredient', 'category', 'area']:
            logging.error("Please insert a valid filter type: ingredient, category or area.") 
            return 

        list_endpoint = f'/list.php?{filter_type[0]}=list'  
        response = self._connect_api(list_endpoint)  

        dict_meals = response.json()['meals']
        list_filter_values = [d['strCategory'] for d in dict_meals] 

        return list_filter_values
            
    def get_meals_filter_by(self, filter_type, filter_value, n_meals=None, output_type='pandas', csv_path='./output.csv'):
        """
        Get the n first meals filtered according to filter_type = filter_value.

        Parameters
        ----------
            filter_type (str) : Filter types supported by the API: category, area, ingredient.

            filter_value (str): Value to apply filtering, eg: 
                                - filter_value = 'Seafood' when filter_type = 'category'
                                - filter_value = 'French' when  filter_type = 'area'
                                - filter_value = 'Tofu' when filter_type = 'ingredient'
                                To checkk all filter values options, call list_filter_values(filter_type).
            
            n_meals (int) : Maximum number of meals to get, default None (all meals).
            
            output (int) : Format of the data outuput: pandas.DataFrame or csv

            csv_path (str) : Absolute path of where the csv should be downloaded to.

        Returns
        -------
            df_meals : Meals basic info according to input filter criteria.
                       columns: [idMeal, strMeal, strMealThumb]
                        - can be returned as a pandas.DataFrame or a csv specified on the csv_path
        """

        if filter_type not in ['ingredient', 'category', 'area']:
            logging.error("Please insert a valid filter type: ingredient, category or area.") 
            return

        filter_endpoint = f'/filter.php?{filter_type[0]}={filter_value}'
        response = self._connect_api(filter_endpoint)

        dict_meals = response.json()['meals']
        if not dict_meals:
            logging.error(f'Could not find any meal with this filter, please retry with another value.')
            return

        df_meals = pd.DataFrame(dict_meals)

        if n_meals:
            df_meals = df_meals.head(n_meals) 
            if len(df_meals) < n_meals: 
                logging.info(f'Could not find {n_meals} meals with this filter, returning data for only {len(df_meals)}.')
            else:
                logging.info(f'Getting data for {n_meals} meals with the selected filter.')
        else:
            logging.info(f'Getting data for {len(df_meals)} meals with the selected filter.')
            
        return self._output_type(df_meals, output_type, csv_path)

    def get_meals_search_by(self, search_type, search_value, output_type='pandas', csv_path='./output.csv'):
        """
        Get all the meals that match a search criteria.

        Parameters
        ----------
            search_type (str) : Search types supported by the API: category, area, ingredient.

            search_value (str): Value to search for, eg: 
                    - search_value = 'Arrabiata' when search_type = 'meal_name'
                    - search_value = '52772' when  search_type = 'meal_id'
            
            n_meals (int) : Maximum number of meals to get, optional.
        Returns
        -------
            df_meals (pandas.DataFrame) : DataFrame containing meals recipes that match input search criteria.
                Columns: [dateModified, idMeal, strArea, strCategory, strDrinkAlternate, strIngredient1 - strIngredient20,
                          strMeal, strMealThumb, strMeasure1 - strMeasure20, strSource, strTags, strYoutube]
        """

        if search_type == 'meal_name':
            search_endpoint = f'/search.php?s={search_value}'
        elif search_type == 'meal_id':
            search_endpoint = f'/lookup.php?i={search_value}'
        else:
            logging.error("Please insert a valid search type: meal_name or meal_id.") 
            return
        
        response = self._connect_api(search_endpoint)
        dict_meals = response.json()['meals']
        if not dict_meals:
            logging.error(f'Could not find any meal with this search, please retry with another value.')
            return

        df_meals = pd.DataFrame(dict_meals)
        logging.info(f'Getting data for {len(df_meals)} meals with this search.')

        return self._output_type(df_meals, output_type, csv_path)

    def get_meals_random(self, n_meals=1, output_type='pandas', csv_path='./output.csv'):
        """
        Get n random meals recipes.

        Parameters
        ----------
            n_meals (int) : Number of random meals to get, default 1.
        Returns
        -------
            df_meals (pandas.DataFrame) : DataFrame containing n random meals recipes.
                columns: [dateModified, idMeal, strArea, strCategory, strDrinkAlternate, strIngredient1 - strIngredient20,
                          strMeal, strMealThumb, strMeasure1 - strMeasure20, strSource, strTags, strYoutube]
        """
        random_endpoint = f'/random.php'
        
        append_meals = []
        for _ in range(n_meals):
            response = self._connect_api(random_endpoint)
            dict_random = response.json()['meals']
            append_meals = append_meals + dict_random

        df_meals = pd.DataFrame(append_meals)
        logging.info(f'Getting data for {len(df_meals)} random meals.')
        
        return self._output_type(df_meals, output_type, csv_path)