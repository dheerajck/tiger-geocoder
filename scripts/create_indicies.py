from geocoder import Database


def create_indicies():
    db = Database()

    db.execute("SELECT install_missing_indexes();")
    db.execute("vacuum (analyze, verbose) tiger.addr;")
    db.execute("vacuum (analyze, verbose) tiger.edges;")
    db.execute("vacuum (analyze, verbose) tiger.faces;")
    db.execute("vacuum (analyze, verbose) tiger.featnames;")
    db.execute("vacuum (analyze, verbose) tiger.place;")
    db.execute("vacuum (analyze, verbose) tiger.cousub;")
    db.execute("vacuum (analyze, verbose) tiger.county;")
    db.execute("vacuum (analyze, verbose) tiger.state;")
    db.execute("vacuum (analyze, verbose) tiger.zip_lookup_base;")
    db.execute("vacuum (analyze, verbose) tiger.zip_state;")
    db.execute("vacuum (analyze, verbose) tiger.zip_state_loc;")
