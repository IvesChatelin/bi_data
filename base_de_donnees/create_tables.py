import psycopg2
from config import load_config

def createTables():
    try:
        config = load_config()
        conn = psycopg2.connect(**config)
        print(f"Connected to database {conn}")
        cursor = conn.cursor()
        
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

        create_table_hotel_reviews = """
        CREATE TABLE IF NOT EXISTS Hotel_Reviews (
            id SERIAL PRIMARY KEY,
            hotel VARCHAR(255),
            zip_code VARCHAR(255),
            numRev INT,
            Score FLOAT,
            Cleanliness FLOAT,
            Comfort FLOAT
        );
        """
        cursor.execute(create_table_hotel_reviews)
        
        print(f"Tables created")

        conn.commit()
        cursor.close()
        conn.close()
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
