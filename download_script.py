from scripts import (
    create_extensions,
    create_indicies,
    load_national_data_caller,
    load_states_data_caller,
    create_folders,
)


if __name__ == "__main__":
    print("Creating Postgis extensions")
    create_extensions()

    print("Creating Folders")
    create_folders()

    print("Adding US national data and states data of specified states in env file")
    load_national_data_caller()

    load_states_data_caller()

    print("Creating indicies")
    create_indicies()
