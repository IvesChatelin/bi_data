import pandas as pd

def filter_csvs_create_hotels():
    # hotel_bookings_columns = ["hotel", "is_canceled", "reservation_status_date", "country", "deposit_type", "reserved_room_type", "assigned_room_type", "adults", "children", "stays_in_week_nights","stays_in_weekend_nights", "booking_changes", "adr", "reservation_status", "market_segment"]
    # hotel_reviews_columns = ["hotel", "zip_code", "numRev", "Score", "Cleanliness", "Comfort"]

    # hotel_bookings_file = "hotel_bookings.csv"
    ind_all_file = "ind_all.csv"
    nyc_all_file = "nyc_all.csv"
    la_all_file = "la_all.csv"
    ol_all_file = "ol_all.csv"

    # hotel_bookings_df = pd.read_csv(hotel_bookings_file)
    ind_all_df = pd.read_csv(ind_all_file)
    nyc_all_df = pd.read_csv(nyc_all_file)
    la_all_df = pd.read_csv(la_all_file)
    ol_all_df = pd.read_csv(ol_all_file)

    ind_hotels_df = ind_all_df[['Name', 'Area']].drop_duplicates()
    ind_hotels_df['Country'] = 'India'
    ind_hotels_df.columns = ['Name', 'City', 'Country']

    nyc_hotels_df = nyc_all_df[['hotel', 'zip_code']].drop_duplicates()
    nyc_hotels_df['City'] = nyc_hotels_df['zip_code'].astype(str) + ', New York'
    nyc_hotels_df['Country'] = 'United States of America'
    nyc_hotels_df.drop(columns=['zip_code'], inplace=True)
    nyc_hotels_df.columns = ['Name', 'City', 'Country']

    la_hotels_df = la_all_df[['hotel', 'zip_code']].drop_duplicates()
    la_hotels_df['City'] = la_all_df['zip_code'].astype(str) + ', Los Angeles'
    la_hotels_df['Country'] = 'United States of America'
    la_hotels_df.drop(columns=['zip_code'], inplace=True)
    la_hotels_df.columns = ['Name', 'City', 'Country']

    ol_hotels_df = ol_all_df[['hotel', 'zip_code']].drop_duplicates()
    ol_hotels_df['City'] = ol_all_df['zip_code'].astype(str) + ', Orlando'
    ol_hotels_df['Country'] = 'United States of America'
    ol_hotels_df.drop(columns=['zip_code'], inplace=True)
    ol_hotels_df.columns = ['Name', 'City', 'Country']


    hotels_df = pd.concat([la_hotels_df, nyc_hotels_df, ol_hotels_df, ind_hotels_df], ignore_index=True)
    hotels_df['id'] = range(1, len(hotels_df) + 1)
    # hotel_bookings_filtered = hotel_bookings_df[hotel_bookings_columns]
    # nyc_all_filtered = nyc_all_df[hotel_reviews_columns]
    # la_all_filtered = la_all_df[hotel_reviews_columns]
    # ol_all_filtered = ol_all_df[hotel_reviews_columns]

    # hotel_bookings_filtered.to_csv("hotel_bookings_filtered.csv", index=False)
    hotels_df.to_csv("hotels.csv", index=False)
    # nyc_all_filtered.to_csv("nyc_all_filtered.csv", index=False)
    # la_all_filtered.to_csv("la_all_filtered.csv", index=False)
    # ol_all_filtered.to_csv("ol_all_filtered.csv", index=False)

    print("created hotels.csv")

def filter_csvs_create_countries_cities():
    hotels_df = pd.read_csv("hotels.csv")

    cities_df = hotels_df[['City', 'Country']].drop_duplicates()
    cities_df['id'] = range(1, len(cities_df) + 1)

    hotels_df.drop(columns=['Country'], inplace=True)

    hotels_df.to_csv("hotels.csv", index=False)

    cities_df.to_csv("cities.csv", index=False)

    print("created cities.csv and updated hotels.csv")

def filter_csvs_hotel_reviews():
    hotel_reviews_columns = ["hotel", "Score", "Cleanliness", "Comfort"]

    ind_all_file = "ind_all.csv"
    nyc_all_file = "nyc_all.csv"
    la_all_file = "la_all.csv"
    ol_all_file = "ol_all.csv"

    ind_all_df = pd.read_csv(ind_all_file)
    nyc_all_df = pd.read_csv(nyc_all_file)
    la_all_df = pd.read_csv(la_all_file)
    ol_all_df = pd.read_csv(ol_all_file)

    ind_all_filtered_df = ind_all_df.groupby('Name')['Rating(Out of 10)'].mean().reset_index()
    ind_all_filtered_df.columns = ['hotel', 'Score']

    nyc_all_filtered = nyc_all_df[hotel_reviews_columns]
    la_all_filtered = la_all_df[hotel_reviews_columns]
    ol_all_filtered = ol_all_df[hotel_reviews_columns]

    ind_all_filtered_df.to_csv("ind_all_filtered.csv", index=False)
    nyc_all_filtered.to_csv("nyc_all_filtered.csv", index=False)
    la_all_filtered.to_csv("la_all_filtered.csv", index=False)
    ol_all_filtered.to_csv("ol_all_filtered.csv", index=False)

    print("filtered reviews")

def filter_csv_hotel_bookings():
    hotel_bookings_columns = ["hotel", "is_canceled", "reservation_status_date", "country", "deposit_type", "reserved_room_type", "assigned_room_type", "adults", "children", "stays_in_week_nights","stays_in_weekend_nights", "booking_changes", "adr", "reservation_status", "market_segment"]

    hotel_bookings_file = "hotel_bookings.csv"

    hotel_bookings_df = pd.read_csv(hotel_bookings_file)

    hotel_bookings_filtered = hotel_bookings_df[hotel_bookings_columns]

    hotel_bookings_filtered.to_csv("hotel_bookings_filtered.csv", index=False)

    print("filtered hotel bookings.csv")