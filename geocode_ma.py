from geocoder import Database

if __name__ == "__main__":
    db = Database()

    # Geocoding address
    address_list = [
        "60 TEMPLE PL, BOSTON, MA",
        "643 Summer St, Boston, Suffolk, MA, 02210",
    ]

    for address in address_list:
        print(address)
        print(db.get_geocoded_data(address))
        print()

    print()

    for address in address_list:
        print(address)
        print(db.get_geocoded_data(address, pagc_normalize_address=True))
        print()
