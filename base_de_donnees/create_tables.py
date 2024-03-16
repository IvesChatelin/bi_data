import psycopg2
from config import load_config

def createTables():
    try:
        config = load_config()
        conn = psycopg2.connect(**config)
        print(f"Connected to database {conn}")
        cursor = conn.cursor()
        
        drop_table_hotel_bookings = """
        DROP TABLE IF EXISTS Hotel_Bookings;
        """
        cursor.execute(drop_table_hotel_bookings)

        drop_table_hotel_reviews = """
        DROP TABLE IF EXISTS Hotel_Reviews;
        """
        cursor.execute(drop_table_hotel_reviews)

        drop_table_hotels = """
        DROP TABLE IF EXISTS Hotels;
        """
        cursor.execute(drop_table_hotels)
        
        drop_table_cities = """
        DROP TABLE IF EXISTS Cities;
        """
        cursor.execute(drop_table_cities)

        create_table_cities = """
        CREATE TABLE IF NOT EXISTS Cities (
            id SERIAL PRIMARY KEY,
            City VARCHAR(255),
            Country VARCHAR(255)
        );
        """
        cursor.execute(create_table_cities)

        create_table_hotels = """
        CREATE TABLE IF NOT EXISTS Hotels (
            id SERIAL PRIMARY KEY,
            Name VARCHAR(255),
            City_id SERIAL,
            FOREIGN KEY (City_id) REFERENCES Cities(id) ON DELETE CASCADE
        );
        """
        cursor.execute(create_table_hotels)

        create_table_hotel_reviews = """
        CREATE TABLE IF NOT EXISTS Hotel_Reviews (
            id SERIAL PRIMARY KEY,
            Hotel_id SERIAL,
            Score FLOAT NULL,
            Cleanliness FLOAT NULL,
            Comfort FLOAT,
            FOREIGN KEY (Hotel_id) REFERENCES Hotels(id) ON DELETE CASCADE
        );
        """
        cursor.execute(create_table_hotel_reviews)

        create_table_hotel_bookings = """
        CREATE TABLE IF NOT EXISTS Hotel_Bookings (
            id SERIAL PRIMARY KEY,
            hotel VARCHAR(255),
            is_canceled INT,
            reservation_status_date DATE,
            country VARCHAR(255),
            deposit_type VARCHAR(255),
            reserved_room_type VARCHAR(255),
            assigned_room_type VARCHAR(255),
            adults INT,
            children INT,
            stays_in_week_nights INT,
            stays_in_weekend_nights INT,
            booking_changes INT,
            adr FLOAT,
            reservation_status VARCHAR(255),
            market_segment VARCHAR(255)
        );
        """
        cursor.execute(create_table_hotel_bookings)

        print(f"Tables dropped and recreated")

        conn.commit()
        cursor.close()
        conn.close()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
