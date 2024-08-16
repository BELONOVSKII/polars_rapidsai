import polars as pl
import polars.selectors as cs

df = pl.scan_csv("data/dummy_dataset.csv")

df.describe()

cols_for_groupping = [
    "name",
    "hair_color",
    "accession_month",
    "credit_card_expire",
    "currency",
    "city",
]
cols_for_aggregation_num = [
    "balance",
    "debt",
    "building_number",
    "credit_card_number",
    "credit_card_security_code",
    "coordinate",
]
cols_for_aggregation_cat = ["country", "street_name", "credit_card_expire"]

for col in cols_for_groupping:
    temp = df.group_by(col).agg(
        *[pl.mean(num).alias(f"{num}_avg") for num in cols_for_aggregation_num],
        *[pl.std(num).alias(f"{num}_std") for num in cols_for_aggregation_num],
        *[pl.median(num).alias(f"{num}_median") for num in cols_for_aggregation_num],
        *[
            pl.col(cat).mode().first().alias(f"{cat}_mode")
            for cat in cols_for_aggregation_cat
        ],
    )
    temp.collect()
    del temp


cols_for_groupping = [
    "name",
    "hair_color",
    "city",
    "credit_card_expire",
    "currency",
    "accession_month",
]
cols_for_aggregation_num = [
    "balance",
    "debt",
    "building_number",
    "credit_card_number",
    "credit_card_security_code",
    "coordinate",
]
cols_for_aggregation_cat = ["country", "street_name", "credit_card_expire"]

arr = [
    df.group_by(cols_for_groupping[i]).agg(
        *[pl.mean(num).alias(f"{num}_avg_{i}") for num in cols_for_aggregation_num],
        *[pl.std(num).alias(f"{num}_std_{i}") for num in cols_for_aggregation_num],
        *[
            pl.median(num).alias(f"{num}_median_{i}")
            for num in cols_for_aggregation_num
        ],
        *[
            pl.col(cat).mode().first().alias(f"{cat}_mode_{i}")
            for cat in cols_for_aggregation_cat
        ],
    )
    for i in range(3)
]

for i in range(3):
    df = df.join(arr[i], on=cols_for_groupping[i], how="left")

num_cols = df.select(cs.numeric()).collect_schema().names()
cat_cols = df.select(cs.string()).collect_schema().names()


df.with_columns(
    [pl.col(col).sqrt().alias(col + "_sqrt") for col in num_cols]
    + [pl.col(col).sin().alias(col + "_sin") for col in num_cols]
    + [(pl.col(col) + 5).alias(col + "_add") for col in num_cols]
    + [pl.col(col).str.tail(3).alias(col + "_tail") for col in cat_cols]
)

df.collect()
