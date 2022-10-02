import json
import pathlib
import datetime as dt
import pandas as pd


def load(df: pd.DataFrame, q_dt: dt.datetime) -> None:
    """
    Upload transformed data to AWS S3 Bucket.
    """
    secrets_path = pathlib.Path.cwd() / ".secret" / "aws_creds.json"
    with open(secrets_path, 'r') as f:
        aws_creds = json.load(f)
        df.to_csv(
            f'{aws_creds["bucket_url"]}/flight_data_{q_dt.strftime("%Y-%m-%d_%H-%M-%S")}.csv',
            index=False,
            storage_options={
                "key": aws_creds["aws_access_key_id"],
                "secret": aws_creds['aws_secret_access_key'],
            }
        )
