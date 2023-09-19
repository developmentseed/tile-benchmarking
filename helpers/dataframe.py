import boto3
import pandas as pd

def csv_to_pandas(file_path):
    df = pd.read_csv(file_path)
    return df

def load_all_into_dataframe(credentials: dict, s3files: list[str]):
    boto3_session = boto3.Session(**credentials)
    s3_client = boto3_session.client('s3')
    dfs = []

    for s3url in s3files:
        df = pd.read_json(s3url, orient='index').T
        dfs.append(df)
 
    merged_df = pd.concat(dfs)
    #merged_df.set_index('dataset_id', inplace=True)
    return merged_df

def expand_timings(df: pd.DataFrame):
    df_expanded = df.explode('timings')
    df_expanded[['time', 'tile']] = pd.DataFrame(df_expanded['timings'].tolist(), index=df_expanded.index)
    df_expanded['zoom'] = df_expanded['tile'].apply(lambda x: x[2])
    return df_expanded
