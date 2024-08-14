import argparse
import gc
from pathlib import Path

import polars as pl
from faker import Factory
from tqdm.auto import tqdm


def create_fake_row(tqdm_bar=None):

    if tqdm_bar is not None:
        tqdm_bar.update(1)

    return {
        "id": faker.numerify("####"),
        "name": faker.first_name(),
        "birth_date": faker.date_between(),
        "hair_color": faker.color_name(),
        "is_MU_fan": faker.boolean(),
        "iq": int(faker.numerify("###")),
        "accession_month": int(faker.month()),
        "balance": float(faker.pydecimal(min_value=0, max_value=1_000_000_000)),
        "debt": float(faker.pydecimal(min_value=0, max_value=10_000_000_000)),
        "country": faker.country(),
        "city": faker.city(),
        "street_name": faker.street_name(),
        "building_number": int(faker.building_number()),
        "coordinate": faker.coordinate(),
        "license_plate": faker.license_plate(),
        "vin": faker.vin(),
        "swift": faker.swift(length=8),
        "company_name": faker.company(),
        "company_suffix": faker.company_suffix(),
        "job_name": faker.job(),
        "credit_card_expire": faker.credit_card_expire(),
        "credit_card_number": int(faker.credit_card_number()),
        "credit_card_security_code": int(faker.credit_card_security_code()),
        "currency": faker.currency_code(),
    }


def initialize_the_file(data_path):
    pl.DataFrame(create_fake_row()).write_csv(data_path)


def fill_the_file(n_rows, n_rows_per_iter, data_path):
    pbar = tqdm(total=(n_rows // n_rows_per_iter) * n_rows_per_iter)
    for _ in range(n_rows // n_rows_per_iter):
        temp_df = pl.DataFrame([create_fake_row(pbar) for _ in range(n_rows_per_iter)])

        with open(data_path, mode="a") as f:
            temp_df.write_csv(f, include_header=False)

        del temp_df
        gc.collect()
    pbar.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Make artificial data")
    parser.add_argument(
        "file_name",
        type=str,
        help="Name of the file where the data will be saved",
    )
    parser.add_argument(
        "--n_rows",
        type=int,
        default=5_000_000,
        help="Total number of rows in the generated dataset",
    )
    parser.add_argument(
        "--n_rows_per_iter",
        type=int,
        default=200_000,
        help="Number of rows of the dataset stored at RAM",
    )
    args = parser.parse_args()

    faker = Factory.create()

    # create the data directory
    data_path = Path(f"data/{args.file_name}")
    data_path.parent.mkdir(exist_ok=True)

    initialize_the_file(data_path)
    fill_the_file(args.n_rows, args.n_rows_per_iter, data_path)
