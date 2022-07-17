import mysql.connector
import requests
from progressbar import ProgressBar
from pandas import DataFrame

# Set database connection parameters

connection_params = {
    'host': "localhost",
    'user': "root",
    'password': "root",
    'database': "dataengineer",
}

# Request address datas
request = "SELECT * FROM address  "

with mysql.connector.connect(**connection_params) as db:
    with db.cursor() as c:
        c.execute(request)
        address_list = c.fetchall()
        print(len(address_list))

# Get all coordinates

## With original API

api_endpoint_1 = "https://nominatim.openstreetmap.org/search"

pbar = ProgressBar()
errors = []
for address in pbar(address_list):

    address_query = ' '.join(list(address)[1:4])
    address_query = address_query.lower()
    address_id = address[0]
    params = {'q': address_query, 'format': 'json'}
    response = requests.get(url=api_endpoint_1, params=params)

    # Check if an address is found by the API, if not add the address to the errors list
    if len(eval(response.text)) < 1:
        errors.append(address)

print(len(errors))  # 100 errors with this API

## With the new API

api_endpoint_2 = "https://api-adresse.data.gouv.fr/search/"
pbar = ProgressBar()
errors = []
coordinates = []

for address in pbar(address_list):

    # Set up the queries to give to the API
    address_query = ' '.join(list(address)[1:4])
    address_query = address_query.lower()
    address_id = address[0]

    # Set up the API call
    params = {'q': address_query, 'format': 'json'}
    response = requests.get(url=api_endpoint_2, params=params)

    # Get API call result
    result = response.json()['features']

    # If results not found
    if len(result) < 1:
        errors.append(address_query)
        coordinates.append([address_id, 'NULL', 'NULL'])

    # If results found get the coordinates
    else:
        lat = result[0]['geometry']['coordinates'][0]
        long = result[0]['geometry']['coordinates'][1]
        coordinates.append([address_id, lat, long])

print(len(errors))  # Plus qu'une seule erreur avec cette API



# Put coordinates in database

## Create the columns
query_1 = "ALTER TABLE address ADD latitude DECIMAL(8,5);"
query_2 = "ALTER TABLE address ADD longitude DECIMAL(8,5);"

with mysql.connector.connect(**connection_params) as db:
    with db.cursor() as c:
        c.execute(query_1)
        c.execute(query_2)

# Fill the columns
with mysql.connector.connect(**connection_params) as db:
    with db.cursor() as c:
        for coord in coordinates:

            # If the address wasn't found by the API, set NULL value
            if coord[1] == 'NULL':
                query = f"UPDATE address SET latitude = NULL, longitude = NULL WHERE address_id = {int(coord[0])}"
                print(query)
                c.execute(query)

            else:
                query = f"UPDATE address SET latitude = {round(coord[1], 5)}, longitude = {round(coord[2], 5)}" \
                        f" WHERE address_id = {int(coord[0])}"
                print(query)
                c.execute(query)
        db.commit()


# Partie 4

query = "SELECT c.first_name, c.last_name, a.address_id, a.address, a.latitude, \
         a.longitude,  COUNT(r.rental_id) as count_rent	\
         FROM customer AS c \
         JOIN address as a USING(address_id) \
         JOIN rental as r USING(customer_id) \
         GROUP BY c.customer_id \
         ORDER BY count_rent DESC \
         LIMIT 1"

with mysql.connector.connect(**connection_params) as db:
    with db.cursor(buffered=True) as c:
        print(query)
        c.execute(query)
        df = DataFrame(c.fetchall())
        df.columns = c.column_names

df.head()

# La personne ayant fait le plus de location est Madame Eleanor Hunt avec 46 locations

