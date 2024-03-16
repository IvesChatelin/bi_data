from create_tables import createTables
import filter_csv
import insert_data_from_csv

def main():
    createTables()
    filter_csv.filter_csvs_create_hotels()
    filter_csv.filter_csvs_create_countries_cities()
    filter_csv.filter_csvs_hotel_reviews()
    filter_csv.filter_csv_hotel_bookings()
    insert_data_from_csv.insert_data()
    return "done"

if __name__ == '__main__':
    mesg = main()
    print(mesg)