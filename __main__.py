from geocoder import Database

if __name__ == "__main__":
    db = Database()

    address_list = ["115 Cass Avenue in Woonsocket, RI", "60 TEMPLE PL, BOSTON, MA"]

    address_list += ["115 Cass Avenue, RI 02895", "1279 Wampanoag Trl, Riverside, RI 02915"]

    for address in address_list:
        print(db.get_geocoded_data(address))

    print()
    print()

    for address in address_list:
        print(db.get_geocoded_data(address, pagc_normalize_address=True))
