import requests
import psycopg2
import argparse
import psycopg2.extras as extras
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from retry import retry


class Arguments:
    @staticmethod
    def create_args():
        parser = argparse.ArgumentParser()
        parser.add_argument('-y', '--year', type=str, required=True, help="Since 1999")
        parser.add_argument('-m', '--month', type=str, required=True, help="Must be MM format")
        parser.add_argument('-d', '--day', type=str, required=True, help="Must be DD format")
        args_func = parser.parse_args()
        return args_func


class Client:
    @staticmethod
    @retry(tries=5, delay=60)
    def get_data(year, month, day):
        date = f'{year}-{month}-{day}'
        api_url = f'https://openexchangerates.org/api/historical/{date}.json'
        params = {
            'app_id': '5523540ec5ca48eca314700ac6ef0cd4'
        }
        result = requests.get(api_url, params=params)
        data = result.json()
        print("All data has been successfully received.")
        return date, data["base"], data["rates"]


class Database:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(host="localhost",
                                               user="postgres",
                                               password="postgres",
                                               port="5433",
                                               dbname="postgres_db")
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        except (Exception, Error) as error:
            print("Error connecting to PostgreSQL database!", error)

    def create_table(self):
        with self.connection.cursor() as cur:
            cur.execute('''CREATE TABLE exchange_rates
                              (ID SMALLSERIAL PRIMARY KEY,
                              Date DATE NOT NULL,
                              Base CHAR(3) NOT NULL,
                              Rates CHAR(3) NOT NULL,
                              Value REAL NOT NULL); ''')

    def insert_data(self, data):
        date, base, rates = data
        values_list = []
        for key in rates:
            values = (date, base, key, rates[key])
            values_list.append(values)
        with self.connection.cursor() as cur:
            extras.execute_values(cur, """INSERT INTO exchange_rates (Date, Base, Rates, Value) VALUES %s""",
                                  values_list)
        print("All data has been successfully written to the database.")

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("PostgreSQL connection closed!")
