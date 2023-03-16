from geocoder import Database

if __name__ == "__main__":
    db = Database()
    print(db.get_geocoded_data("60 TEMPLE PL, BOSTON, MA"))
