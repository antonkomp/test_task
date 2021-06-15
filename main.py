from classes import Database, Arguments, Client


def main():
    arguments = Arguments()
    arg = arguments.create_args()
    database = Database()
    database.create_table()
    client = Client()
    database.insert_data(client.get_data(arg.year, arg.month, arg.day))
    database.close_connection()


if __name__ == "__main__":
    main()
