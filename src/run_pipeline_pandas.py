import pandas as pd
import numpy as np
import random

df_pd = pd.read_csv("data/dummy_dataset.csv")

df_pd.describe()

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
    aggs = {num: ["mean", "median", "std"] for num in cols_for_aggregation_num}
    aggs.update({cat: pd.Series.mode for cat in cols_for_aggregation_cat})
    temp = df_pd.groupby(by=col).agg(aggs)
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


arr = []
for i in range(3):
    aggs = {num: ["mean", "median", "std"] for num in cols_for_aggregation_num}
    aggs.update({cat: pd.Series.mode for cat in cols_for_aggregation_cat})
    arr.append(
        df_pd.groupby(by=cols_for_groupping[i], as_index=False)
        .agg(aggs)
        .droplevel(level=0, axis=1)
        .rename(
            columns=lambda x: (
                x + f"_{i}_{random.random()}" if x != "" else cols_for_groupping[i]
            )
        )
    )

num_cols = df_pd.select_dtypes(include="number").columns
cat_cols = df_pd.select_dtypes(include="object").columns

for i in range(3):
    df_pd = df_pd.merge(arr[i], on=cols_for_groupping[i], how="left")


df_pd[[col + "_sqrt" for col in num_cols]] = np.sqrt(df_pd[num_cols])
df_pd[[col + "_sin" for col in num_cols]] = np.sin(df_pd[num_cols])
df_pd[[col + "_add" for col in num_cols]] = df_pd[num_cols] + 5
df_pd.assign(**{col + "_tail": df_pd[col].str[-3:] for col in cat_cols})
