import pandas as pd

def filter_csvs():
    hotel_bookings_columns = ["hotel", "is_canceled", "reservation_status_date", "country", "deposit_type", "reserved_room_type", "adults", "children", "stays_in_week_nights","stays_in_weekend_nights", "adr", "reservation_status", "market_segment"]
    hotel_reviews_columns = ["hotel", "zip_code", "numRev", "Score", "Cleanliness", "Comfort"]

    hotel_bookings_file = "hotel_bookings.csv"
    nyc_all_file = "nyc_all.csv"
    la_all_file = "la_all.csv"
    ol_all_file = "ol_all.csv"

    hotel_bookings_df = pd.read_csv(hotel_bookings_file)
    nyc_all_df = pd.read_csv(nyc_all_file)
    la_all_df = pd.read_csv(la_all_file)
    ol_all_df = pd.read_csv(ol_all_file)

    hotel_bookings_filtered = hotel_bookings_df[hotel_bookings_columns]
    nyc_all_filtered = nyc_all_df[hotel_reviews_columns]
    la_all_filtered = la_all_df[hotel_reviews_columns]
    ol_all_filtered = ol_all_df[hotel_reviews_columns]

    hotel_bookings_filtered.to_csv("hotel_bookings_filtered.csv", index=False)
    nyc_all_filtered.to_csv("nyc_all_filtered.csv", index=False)
    la_all_filtered.to_csv("la_all_filtered.csv", index=False)
    ol_all_filtered.to_csv("ol_all_filtered.csv", index=False)

    print("filtered csvs")
