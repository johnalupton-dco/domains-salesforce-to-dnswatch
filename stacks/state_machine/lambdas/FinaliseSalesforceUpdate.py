import os
import io
import pandas as pd
import boto3
import sqlalchemy
from cddo.utils.constants import (
    ENV_UPDATE_FROM_SALESFORCE_BUCKET,
    FLD_SALESFORCE_CHANGE_FILES,
)
from cddo.utils.postgres import get_db_engine

OUTPUT_BUCKET = os.environ[ENV_UPDATE_FROM_SALESFORCE_BUCKET]
s3_client = boto3.client("s3")


def lambda_handler(event, _context):
    # s3 = boto3.resource("s3")
    # input_files = event[FLD_SALESFORCE_CHANGE_FILES]

    # for query_entity, files in input_files.items():
    #     print(f"Processing {query_entity}")
    #     for file in files:
    #         s3.Object(OUTPUT_BUCKET, f"archive/{file}").copy_from(
    #             CopySource=f"{OUTPUT_BUCKET}/{file}"
    #         )
    #         s3.Object(OUTPUT_BUCKET, file).delete()

    df_lookup: pd.DataFrame = pd.read_csv(
        io.StringIO(
            s3_client.get_object(
                Bucket=OUTPUT_BUCKET, Key="salesforce_salesforceobject.csv"
            )["Body"]
            .read()
            .decode("utf-8")
        ),
        sep=",",
    )

    df_lookup = df_lookup.loc[df_lookup["model"].isin(["domain", "organisation"]), :]
    df_lookup = df_lookup.dropna(subset=["object_id"])
    df_lookup["content_type_id"] = 0
    df_lookup["id"] = 0
    df_lookup["batch"] = pd.NA

    engine = get_db_engine(rds_secret_name=os.environ["RDS_SECRET_NAME"])

    with engine.connect() as db_conn:
        rows = df_lookup.to_sql(
            name="temp_table",
            con=db_conn,
            if_exists="replace",
            index=False,
            schema="public",
            dtype={
                "content_type_id": sqlalchemy.types.INTEGER,
                "id": sqlalchemy.types.BIGINT,
                "model": sqlalchemy.types.VARCHAR(100),
                "object_id": sqlalchemy.types.VARCHAR(255),
                "salesforce_id": sqlalchemy.types.VARCHAR(255),
                "batch": sqlalchemy.types.UUID,
            },
        )

        res = db_conn.execute(
            sqlalchemy.sql.text(
                "UPDATE temp_table tt SET content_type_id = dct.id  FROM django_content_type dct WHERE dct.model = tt.model"
            )
        )
        print(res.rowcount)
        db_conn.execute(
            sqlalchemy.sql.text(
                "UPDATE temp_table tt SET id = sso.id  FROM salesforce_salesforceobject sso WHERE sso.object_id = tt.object_id AND sso.content_type_id = tt.content_type_id"
            )
        )
        print(res.rowcount)

    print(rows)
    print(len(df_lookup))

    # data = (
    #     {"id": 1, "title": "The Hobbit", "primary_author": "Tolkien"},
    #     {"id": 2, "title": "The Silmarillion", "primary_author": "Tolkien"},
    # )

    # statement = text(
    #     """INSERT INTO book(id, title, primary_author) VALUES(:id, :title, :primary_author)"""
    # )

    # for line in data:
    #     con.execute(statement, **line)
