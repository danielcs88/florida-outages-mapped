import json

import pandas as pd


def load_json_data(filename: str) -> dict:
    with open(filename) as f:
        data = json.load(f)
    return data


def duke_data(filename: str) -> pd.DataFrame:
    duke_json: dict = load_json_data(filename)

    return pd.DataFrame(duke_json["data"])


def duke_data_prep(duke_df: pd.DataFrame) -> pd.DataFrame:
    return (
        pd.concat([duke_df.iloc[:, :-1], pd.json_normalize(duke_df.iloc[:, -1])])
        .pipe(
            lambda d: d.assign(
                **{
                    col: pd.to_datetime(d[col], errors="coerce")
                    for col in [
                        col
                        for col in d.columns
                        if ("Date" in col) or ("Updated" in col)
                    ]
                }
            )
        )
        .pipe(lambda e: e.loc[:, ~e.columns.duplicated()])
    )


def teco_data(filename: str) -> pd.DataFrame:
    teco_json = load_json_data(filename)
    return pd.DataFrame([record["_source"] for record in teco_json["hits"]["hits"]])


def teco_data_prep(teco_df) -> pd.DataFrame:
    return pd.concat(
        [
            teco_df.drop(columns=["polygonCenter"]),
            teco_df["polygonCenter"]
            .apply(pd.Series)
            .set_axis(["lon", "lat"], axis="columns"),
        ],
        axis=1,
    ).pipe(lambda d: d.assign(updateTime=pd.to_datetime(d["updateTime"])))


def fpl_data(filename: str) -> pd.DataFrame:
    return pd.json_normalize(pd.read_json(filename)["outages"])


def fpl_data_prep(fpl_df: pd.DataFrame) -> pd.DataFrame:
    return fpl_df.assign(
        **{
            col: pd.to_datetime(fpl_df[col], format="mixed", errors="coerce")
            for col in ["lastUpdated", "etr", "dateReported"]
        }
    )

def main():
    duke = duke_data("duke-energy.app/counties.json").pipe(duke_data_prep)
    duke.to_parquet("duke.parquet")

    teco = teco_data("outage.tecoenergy.com/outage.json").pipe(teco_data_prep)
    teco.to_parquet("teco.parquet")

    fpl = fpl_data("fplmaps.com/StormFeedRestoration.json").pipe(fpl_data_prep)
    fpl.to_parquet("fpl.parquet")


if __name__ == "__main__":
    main()