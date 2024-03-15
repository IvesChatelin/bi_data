import psycopg2
from config import load_config
import pandas as pd

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
        if index == 5:
            break
    
    conn.commit()
    print("Hotel bookings data inserted")

def insert_data_into_hotel_reviews(conn, cursor, table_name, csv_file, columns):
    hotel_reviews_df = pd.read_csv(csv_file)
    for index, row in hotel_reviews_df.iterrows():
        hotel = row["hotel"]
        print(hotel)
        zip_code = row["zip_code"]
        print(zip_code)
        numRev = row["numRev"]
        print(numRev)
        Score = row["Score"]
        print(Score)
        Cleanliness = row["Cleanliness"]
        print(Cleanliness)
        Comfort = row["Comfort"]
        print(Comfort)
        insert_query = """
        INSERT INTO Hotel_Reviews (hotel, zip_code,
          numRev, Score, Cleanliness, Comfort)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (hotel, zip_code, numRev, Score, Cleanliness, Comfort))
        if index == 1:
            break
    
    conn.commit()
    print("Hotel reviews data inserted")

def insert_data():
    try:
        config = load_config()
        conn = psycopg2.connect(**config)
        print(f"Connected to database {conn}")
        cursor = conn.cursor()
        
        insert_data_into_hotel_bookings(conn, cursor)

        columns = ["hotel", "zip_code", "numRev", "Score", "Cleanliness", "Comfort"]
        
        insert_data_into_hotel_reviews(conn, cursor, "hotel_reviews", "nyc_all_filtered.csv", columns)
        insert_data_into_hotel_reviews(conn, cursor, "Hotel_Reviews", "la_all_filtered.csv", columns)
        insert_data_into_hotel_reviews(conn, cursor, "Hotel_Reviews", "ol_all_filtered.csv", columns)

        cursor.close()
        conn.close()
        print("Data insertion successful")
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
