from pathlib import Path

import polars as pl

if __name__ == "__main__":
    data_dir = Path("data/")

    # list the data files
    file_paths = [i for i in data_dir.iterdir()]
    default_file = file_paths[0]

    for data_path in file_paths[1:]:
        temp_df = pl.read_csv(data_path)
        with open(default_file, mode="a") as f:
            temp_df.write_csv(f, include_header=False)
