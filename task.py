import requests
import psycopg2
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def request_decorator(func):
    def wrapper():
        try:
            return func()
        except requests.exceptions.ConnectionError:
            raise Exception("Server openexchangerates.org don't ask!")
    return wrapper


class Database:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(host="localhost",
                                               user="postgres",
                                               password="postgres",
                                               port="5433",
                                               dbname="postgres_db")
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.connection.cursor()
        except (Exception, Error) as error:
            print("Error connecting to PostgreSQL database!", error)

    def create_table(self):
        self.cursor.execute("DROP TABLE IF EXISTS exchange_rates")
        self.cursor.execute('''CREATE TABLE exchange_rates
                              (ID SMALLSERIAL PRIMARY KEY,
                              Date DATE NOT NULL,
                              Base CHAR(3) NOT NULL,
                              Rates CHAR(3) NOT NULL,
                              Value REAL NOT NULL); ''')
        self.connection.commit()

    @staticmethod
    @request_decorator
    def get_data():
        date = input('Enter the date in format YYYY-MM-DD: ')
        api_url = f'https://openexchangerates.org/api/historical/{date}.json'
        params = {'app_id': '5523540ec5ca48eca314700ac6ef0cd4'}
        retry_strategy = Retry(total=10, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=0.1)
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=30, max_retries=retry_strategy)
        session = requests.Session()
        session.mount("https://", adapter)
        result = session.get(api_url, params=params)
        data = result.json()
        print("All data has been successfully received.")
        return date, data["base"], data["rates"]

    def insert_data(self, data):
        date, base, rates = data
        for key in rates:
            self.cursor.execute(f"""INSERT INTO exchange_rates (Date, Base, Rates, Value) 
                                    VALUES ('{date}', '{base}', '{key}', {rates[key]})""")
        self.connection.commit()
        print("All data has been successfully written to the database.")

    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("PostgreSQL connection closed!")


def main():
    database = Database()
    database.create_table()
    database.insert_data(database.get_data())
    database.close_connection()


if __name__ == "__main__":
    main()
