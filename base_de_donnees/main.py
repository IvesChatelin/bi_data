from create_tables import createTables
import filter_csv
import insert_data_from_csv

def main():
    createTables()
    filter_csv.filter_csvs()
    insert_data_from_csv.insert_data()
    return "done"

if __name__ == '__main__':
    mesg = main()
    print(mesg)