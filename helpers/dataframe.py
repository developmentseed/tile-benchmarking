import boto3
import pandas as pd

def csv_to_pandas(file_path):
    df = pd.read_csv(file_path)
    return df

def load_all_into_dataframe(credentials: dict, s3files: list[str], data_format: str = 'json'):
    boto3_session = boto3.Session(**credentials)
    s3_client = boto3_session.client('s3')
    dfs = []

    for s3url in s3files:
        if data_format == 'json':
            df = pd.read_json(s3url, orient='index').T
        else:
            df = pd.read_csv(s3url)
        df['s3_url'] = s3url
        dfs.append(df)
 
    merged_df = pd.concat(dfs)
    return merged_df

def expand_timings(df: pd.DataFrame):
    df_expanded = df.explode('timings')
    df_expanded[['time', 'tile']] = pd.DataFrame(df_expanded['timings'].tolist(), index=df_expanded.index)
    df_expanded['zoom'] = df_expanded['tile'].apply(lambda x: x[2])
    return df_expanded
