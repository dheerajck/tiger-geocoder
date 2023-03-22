from geocoder import Database

if __name__ == "__main__":
    db = Database()
    address_list = ["115 Cass Avenue in Woonsocket, RI", "60 TEMPLE PL, BOSTON, MA"]
    for address in address_list:
        print(db.get_geocoded_data(address))
