import psycopg
import csv
import pandas as pd
from psycopg.rows import dict_row
from sqlalchemy import create_engine

#connexion database
engine = create_engine('postgresql://postgres:admin@localhost:5432/demo')

con = psycopg.connect("dbname=demo user=postgres password=admin")

#cursor
cur = con.cursor(row_factory=dict_row)

cur.execute('DROP TABLE IF EXISTS star.fact_Customer_flight_data')
cur.execute('DROP TABLE IF EXISTS star.dim_passenger')
cur.execute('DROP TABLE IF EXISTS star.dim_airports')
cur.execute('DROP TABLE IF EXISTS star.dim_flight')
cur.execute('DROP TABLE IF EXISTS star.dim_ticket')

cur.execute('''
    CREATE TABLE IF NOT EXISTS star.dim_passenger (
        passenger_id varchar(20) PRIMARY KEY, 
        book_ref char(6) NOT NULL,
        passenger_name TEXT NOT NULL,
        contact_data jsonb 
    );
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS star.dim_airports (
        airport_code varchar(20) PRIMARY KEY, 
        airport_name jsonb NOT NULL,
        city jsonb NOT NULL,
        coordinates point NOT NULL,
        timezone TEXT NOT NULL
    );
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS star.dim_flight (
        flight_no char(6) PRIMARY KEY, 
        scheduled_departure timestamptz NOT NULL,
        scheduled_arrival timestamptz NOT NULL,
        departure_airport char(3) NOT NULL,
        arrival_airport char(3) NOT NULL,
        status varchar(20) NOT NULL,
        aircraft_code char(3) NOT NULL,
        actual_departure timestamptz,
        actual_arrival timestamptz
    );
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS star.dim_ticket (
        ticket_no char(13) PRIMARY KEY, 
        flight_id INTEGER NOT NULL,
        boarding_no INTEGER NOT NULL
    );
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS star.fact_Customer_flight_data (
        ID serial PRIMARY KEY,
        passenger_id varchar(20) NOT NULL, 
        ticket_no char(13) NOT NULL,
        flight_no char(6) NOT NULL,
        airport_code VARCHAR(20) NOT NULL,
        FOREIGN KEY(flight_no) REFERENCES star.dim_flight(flight_no) ON DELETE CASCADE,
        FOREIGN KEY(passenger_id) REFERENCES star.dim_passenger(passenger_id) ON DELETE CASCADE,
        FOREIGN KEY(ticket_no) REFERENCES star.dim_ticket(ticket_no) ON DELETE CASCADE,
        FOREIGN KEY(airport_code) REFERENCES star.dim_airports(airport_code) ON DELETE CASCADE
    );
''')

con.commit()

# generate csv file 
cur.execute('''
    SELECT * 
    FROM bookings.tickets t 
    INNER JOIN bookings.ticket_flights tf ON t.ticket_no = tf.ticket_no
    INNER JOIN bookings.boarding_passes b ON tf.ticket_no = b.ticket_no
    LIMIT 10
''')
tickets = cur.fetchall()

cur.execute('''
    SELECT * 
    FROM bookings.bookings b
    INNER JOIN bookings.tickets t ON b.book_ref = t.book_ref
    LIMIT 10
''')
passagers = cur.fetchall()

cur.execute('SELECT * FROM bookings.flights LIMIT 10')
flights = cur.fetchall()

cur.execute('SELECT * FROM bookings.airports_data LIMIT 10')
airports = cur.fetchall()

#souci
Customer_flights = cur.execute('''
    SELECT *
    FROM bookings.tickets t
    INNER JOIN bookings.ticket_flights tf ON t.ticket_no = tf.ticket_no
    INNER JOIN bookings.flights f ON tf.flight_id = f.flight_id
    LIMIT 10
''')

with open('dim_ticket.csv', 'w', encoding='utf-8') as file:
    writer = csv.writer(file)
    field = ["ticket_no", "flight_id", "boarding_no"]
    writer.writerow(field)
    for ticket in tickets:
        writer.writerow([ticket['ticket_no'], ticket['flight_id'], ticket['boarding_no']])

with open('dim_passenger.csv', 'w', encoding='utf-8') as file:
    writer = csv.writer(file)
    field = ["passenger_id", "book_ref", "passenger_name", "contact_data"]
    writer.writerow(field)
    for passager in passagers:
        writer.writerow([passager['passenger_id'], passager['book_ref'], passager['passenger_name'], passager['contact_data']])

with open('dim_flight.csv', 'w', encoding='utf-8') as file:
    writer = csv.writer(file)
    field = ["flight_no", "scheduled_departure", "scheduled_arrival", "departure_airport", "arrival_airport", "status", "aircraft_code", "actual_departure", "actual_arrival"]
    writer.writerow(field)
    for flight in flights:
        writer.writerow([flight['flight_no'], flight['scheduled_departure'], flight['scheduled_arrival'], flight['departure_airport'], flight['arrival_airport'], flight['status'], flight['aircraft_code'],
                        flight['actual_departure'], flight['actual_arrival']])

with open('dim_airport.csv', 'w', encoding='utf-8') as file:
    writer = csv.writer(file)
    field = ["airport_code", "airport_name", "city", "coordinates", "timezone"]
    writer.writerow(field)
    for airport in airports:
        writer.writerow([airport['airport_code'], airport['airport_name'], airport['city'], airport['coordinates'], airport['timezone']])

# insert dimension data
dim_airport = pd.read_csv('dim_airport.csv')
dim_airport.to_sql(name='dim_airport',con=engine,schema='star',if_exists='append',index=False)

dim_passenger = pd.read_csv('dim_passenger.csv')
dim_passenger.to_sql(name='dim_passenger',con=engine,schema='star',if_exists='append',index=False)

dim_flight = pd.read_csv('dim_flight.csv')
dim_flight.to_sql(name='dim_flight',con=engine,schema='star',if_exists='append',index=False)

dim_ticket = pd.read_csv('dim_ticket.csv')
dim_ticket.to_sql(name='dim_ticket',con=engine,schema='star',if_exists='append',index=False)

con.commit()