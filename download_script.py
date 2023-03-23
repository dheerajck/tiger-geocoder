from scripts import (
    create_extensions,
    create_indicies,
    load_national_data_caller,
    load_states_data_caller,
    create_folders,
)


if __name__ == "__main__":
    create_extensions()

    create_folders()

    load_national_data_caller()

    load_states_data_caller()

    create_indicies()
