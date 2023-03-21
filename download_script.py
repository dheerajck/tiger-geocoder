import os
from scripts import create_extensions, create_indicies, load_national_data_caller, load_states_data_caller


if __name__ == "__main__":
    current_working_directory = os.getcwd()
    print("Creating Postgis extensions")
    create_extensions()

    print("Adding US national data")
    load_national_data_caller()
    os.chdir(current_working_directory)

    load_states_data_caller()
    os.chdir(current_working_directory)

    print("Creating indicies")
    create_indicies()
