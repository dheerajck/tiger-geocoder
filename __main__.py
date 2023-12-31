from geocoder import Database

if __name__ == "__main__":
    db = Database()

    # Geocoding address
    address_list = [
        "115 Cass Avenue in Woonsocket, RI",
        "60 TEMPLE PL, BOSTON, MA",
        "115 Cass Avenue, RI 02895",
        "1279 Wampanoag Trl, Riverside, RI 02915",
        "1428 West AVE APT 405 Miami FL 33139",
        "1428 West AVE Miami FL 33139",
        # Address from AAA Test SOV-PINGv2-DEV (5).xlsm shared via slack pm
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

    print()

    # Reverse geocoding address
    # 115 Cass Avenue in Woonsocket, RI
    latitude = 42.00520268824846
    longitude = -71.49633130645371
    print(db.reverse_geocode(latitude, longitude))

    # 1428 West Ave APT 405, Miami Beach, FL 33139, USA and 1428 West Ave, Miami Beach, FL 33139, USA
    latitude = 25.78629822167768
    longitude = -80.14252463174698
    print(db.reverse_geocode(latitude, longitude))

    latitude = 25.786397605531484
    longitude = -80.14256448941912
    print(db.reverse_geocode(latitude, longitude))
