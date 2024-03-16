import psycopg2
from config import load_config
import pandas as pd

def insert_data_into_cities(conn, cursor):
    cities_file = "cities.csv"
    cities_df = pd.read_csv(cities_file)
    for index, row in cities_df.iterrows():
        id = row["id"]
        print(id)
        City = row["City"]
        print(City)
        Country = row["Country"]
        print(Country)
        
        insert_query = """
        INSERT INTO Cities (id, City, Country)
        VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (id, City, Country))
        # if index == 1:
        #     break
    
    conn.commit()
    print("Cities data inserted")

def insert_data_into_hotels(conn, cursor):
    hotels_file = "hotels.csv"
    hotels_df = pd.read_csv(hotels_file)
    for index, row in hotels_df.iterrows():
        id = row["id"]
        print(id)
        Name = row["Name"]
        print(Name)
        City = row["City"]
        print(City)

        cursor.execute("SELECT id FROM Cities WHERE City = %s", (City,))
        City_id = cursor.fetchone()
         
        if City_id:
            City_id = City_id[0]
            print(f"City Id for {City}: {City_id}")

            insert_query = """
            INSERT INTO Hotels (id, Name, City_id)
            VALUES (%s, %s, %s)
            """
            cursor.execute(insert_query, (id, Name, City_id))
        else:
            print(f"No city found for {City}")

        # if index == 1:
        #     break
    
    conn.commit()
    print("Hotels data inserted")

def insert_data_into_hotel_bookings(conn, cursor):
    hotel_bookings_file = "hotel_bookings_filtered.csv"
    hotel_bookings_df = pd.read_csv(hotel_bookings_file)

    for index, row in hotel_bookings_df.iterrows():
        hotel = row["hotel"]
        print(hotel)
        is_canceled = row["is_canceled"]
        print(is_canceled)
        reservation_status_date = row["reservation_status_date"]
        print(reservation_status_date)
        country = row["country"]
        print(country)
        deposit_type = row["deposit_type"]
        print(deposit_type)
        reserved_room_type = row["reserved_room_type"]
        print(reserved_room_type)
        assigned_room_type = row["assigned_room_type"]
        print(assigned_room_type)
        adults = row["adults"]
        print(adults)
        children = row["children"]
        print(children)
        stays_in_week_nights = row["stays_in_week_nights"]
        print(stays_in_week_nights)
        stays_in_weekend_nights = row["stays_in_weekend_nights"]
        print(stays_in_weekend_nights)
        booking_changes = row["booking_changes"]
        print(booking_changes)
        adr = row["adr"]
        print(adr)
        reservation_status = row["reservation_status"]
        print(reservation_status)
        market_segment = row["market_segment"]
        print(market_segment)
        insert_query = """
        INSERT INTO Hotel_Bookings (hotel, is_canceled, reservation_status_date, country, deposit_type, reserved_room_type, assigned_room_type, adults, children, stays_in_week_nights, stays_in_weekend_nights, booking_changes, adr, reservation_status, market_segment)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (hotel, is_canceled, reservation_status_date, country, deposit_type, reserved_room_type, assigned_room_type, adults, children, stays_in_week_nights, stays_in_weekend_nights, booking_changes, adr, reservation_status, market_segment))
        if index == 1000:
            break
    
    conn.commit()
    print("Hotel bookings data inserted")

def insert_data_into_hotel_reviews(conn, cursor, csv_file):
    hotel_reviews_df = pd.read_csv(csv_file)
    for index, row in hotel_reviews_df.iterrows():
        if pd.notnull(row["Score"]):
            Score = row["Score"]
            print(Score)

            if 'Cleanliness' in hotel_reviews_df.columns:
                Cleanliness = row["Cleanliness"]
            else:
                Cleanliness = -1
            print(Cleanliness)

            if 'Comfort' in hotel_reviews_df.columns:
                Comfort = row["Comfort"]
            else:
                Comfort = -1
            print(Comfort)

            hotel = row["hotel"]
            print(hotel)
            cursor.execute("SELECT id FROM Hotels WHERE Name = %s", (hotel,))
            Hotel_id = cursor.fetchone()
            if Hotel_id:
                Hotel_id = Hotel_id[0]
                print(f"Hotel Id for {hotel}: {Hotel_id}")

                insert_query = """
                INSERT INTO Hotel_Reviews (Hotel_id, Score, Cleanliness, Comfort)
                VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_query, (Hotel_id, Score, Cleanliness, Comfort))
            else:
                print(f"No hotel found for {hotel}")
        else:
            print("Skipping row with no score")

        # if index == 1:
        #     break
    
    conn.commit()
    print("Hotel reviews data inserted")

def insert_data():
    try:
        config = load_config()
        conn = psycopg2.connect(**config)
        print(f"Connected to database {conn}")
        cursor = conn.cursor()

        insert_data_into_cities(conn, cursor)

        insert_data_into_hotels(conn, cursor)

        insert_data_into_hotel_reviews(conn, cursor,"nyc_all_filtered.csv")
        insert_data_into_hotel_reviews(conn, cursor, "la_all_filtered.csv")
        insert_data_into_hotel_reviews(conn, cursor, "ol_all_filtered.csv")
        insert_data_into_hotel_reviews(conn, cursor, "ind_all_filtered.csv")

        insert_data_into_hotel_bookings(conn, cursor)

        cursor.close()
        conn.close()
        print("Data insertion successful")
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
