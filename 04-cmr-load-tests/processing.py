# Utilities for processing locust load test results
import pandas as pd


def calc_individual_response_times(df, query):
    """Calculate individual response times based on the moving average stored by locust"""
    subset = df[df["Name"] == query]
    subset = subset.drop_duplicates(subset="Total Request Count", keep="last")
    subset = subset.set_index("Total Request Count")
    for ind, row in subset.iterrows():
        if ind == 1:
            subset.loc[ind, "Response Time"] = subset.loc[
                ind, "Total Average Response Time"
            ]
        elif ind == 2 and 1 not in subset.index.unique():
            subset.loc[ind, "Response Time"] = subset.loc[
                ind, "Total Max Response Time"
            ]
            subset.loc[1] = subset.loc[ind]
            subset.loc[1, "Total Request Count"] = 1
            subset.loc[1, "Response Time"] = subset.loc[ind, "Total Min Response Time"]
        else:
            subset.loc[ind, "Response Time"] = (
                ind * subset.loc[ind, "Total Average Response Time"]
                - (ind - 1) * subset.loc[ind - 1, "Total Average Response Time"]
            )
    subset = subset[["Name", "Response Time"]]
    return subset


def split_aggregated_results(df, full_df):
    """Transform aggregated results into individual response times"""
    queries = df[df["Request Count"] > 1]["Name"].to_list()
    df = df[df["Request Count"] == 1]
    df = df.rename(columns={"Average Response Time": "Response Time"})
    df = df[["Name", "Request Count", "Response Time", "Min Response Time"]]
    for query in queries:
        split_data = calc_individual_response_times(full_df, query)
        df = pd.concat([df, split_data], axis=0)
    return df


def process_locust_results(
    results_location, run_id: str = "0", split_aggregated: bool = True
):
    """Load locust results and extract relevant information"""
    df = pd.read_csv(f"{results_location}_stats.csv")
    full_results = pd.read_csv(f"{results_location}_stats_history.csv")
    df = df[df["Type"] == "GET"]
    if split_aggregated:
        df = split_aggregated_results(df, full_results)
    df["zoom"] = df.apply(lambda x: int(x["Name"].split("?")[0].split("/")[3]), axis=1)
    df["tile"] = df.apply(
        lambda x: x["Name"].split("?")[0].split("/")[3:6], axis=1
    ).astype("str")
    df = df.rename(columns={"Name": "url"})
    df["method"] = "AWS Lambda"
    df["run_id"] = run_id
    return df
