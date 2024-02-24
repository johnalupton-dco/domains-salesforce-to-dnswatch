import os
import io
import json
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


def upsert_from_file(
    bucket_name: str, key: str, db_conn: sqlalchemy.Connection, upsert_object: str
):
    df_input: pd.DataFrame = pd.read_csv(
        io.StringIO(
            s3_client.get_object(Bucket=bucket_name, Key=key)["Body"]
            .read()
            .decode("utf-8")
        ),
        sep=",",
    )

    fields = df_input.columns.to_list()
    print(df_input)

    print("Creating table")
    res = df_input.to_sql(
        name="temp_table",
        con=db_conn,
        if_exists="replace",
        index=False,
    )
    db_conn.commit()

    res = db_conn.execute(
        sqlalchemy.sql.text(
            f"ALTER TABLE temp_table ADD CONSTRAINT fk_tt_object FOREIGN KEY (id) REFERENCES {upsert_object} (id)"
        )
    )
    db_conn.commit()

    res = db_conn.execute(sqlalchemy.sql.text("SELECT * FROM temp_table"))
    print(res.rowcount)
    db_conn.commit()

    print(f"{upsert_object} -> {fields}")
    # print("Dropping table")
    # res = db_conn.execute(sqlalchemy.sql.text("TRUNCATE TABLE temp_table"))
    # db_conn.commit()
    # res = db_conn.execute(sqlalchemy.sql.text("DROP TABLE temp_table"))
    # db_conn.commit()


def lambda_handler(event, _context):
    s3 = boto3.resource("s3")
    input_files = event[FLD_SALESFORCE_CHANGE_FILES]

    engine, _ = get_db_engine(rds_secret_name=os.environ["RDS_SECRET_NAME"])

    with engine.connect() as db_conn:
        for query_entity, files in input_files.items():
            print(f"Processing {query_entity}")
            for file in files:
                print(f"File {file}")
                upsert_from_file(
                    bucket_name=OUTPUT_BUCKET,
                    key=file,
                    db_conn=db_conn,
                    upsert_object=query_entity,
                )
        # #         s3.Object(OUTPUT_BUCKET, f"archive/{file}").copy_from(
        # #             CopySource=f"{OUTPUT_BUCKET}/{file}"
        # #         )
        # #         s3.Object(OUTPUT_BUCKET, file).delete()

        # return

        # df_lookup = pd.read_csv(
        #     io.StringIO(
        #         s3_client.get_object(
        #             Bucket=OUTPUT_BUCKET, Key="salesforce_salesforceobject.csv"
        #         )["Body"]
        #         .read()
        #         .decode("utf-8")
        #     ),
        #     sep=",",
        # )

        # df_lookup = df_lookup.loc[
        #     df_lookup["model"].isin(["domain", "organisation"]), :
        # ]
        # df_lookup = df_lookup.dropna(subset=["id"])
        # df_lookup["content_type_id"] = 0
        # df_lookup["sso_id"] = 0
        # df_lookup["batch"] = pd.NA

        # print("Creating table")
        # df_lookup.to_sql(
        #     name="temp_table",
        #     con=db_conn,
        #     if_exists="replace",
        #     index=False,
        #     schema="public",
        #     dtype={
        #         "content_type_id": sqlalchemy.types.INTEGER,
        #         "sso_id": sqlalchemy.types.BIGINT,
        #         "model": sqlalchemy.types.VARCHAR(100),
        #         "id": sqlalchemy.types.VARCHAR(255),
        #         "salesforce_id": sqlalchemy.types.VARCHAR(255),
        #         "batch": sqlalchemy.types.UUID,
        #     },
        # )

        # # how many rows to be added
        # total_rows = len(df_lookup)
        # print(f"Rows to upsert: <{total_rows}>")

        # # how many rows in temp_table
        # res = db_conn.execute(sqlalchemy.sql.text("SELECT * FROM temp_table"))

        # print(f"Rows in temp_table: <{res.rowcount}>")

        # # get content type id for each model
        # res = db_conn.execute(
        #     sqlalchemy.sql.text(
        #         "UPDATE temp_table tt SET content_type_id = dct.id  FROM django_content_type dct WHERE dct.model = tt.model"
        #     )
        # )

        # print(f"Content type updates: <{res.rowcount}>")
        # print(f"Content type updates should be: <{total_rows}>")

        # # update existing entries
        # res = db_conn.execute(
        #     sqlalchemy.sql.text(
        #         "UPDATE temp_table tt SET sso_id = sso.id  FROM salesforce_salesforceobject sso WHERE sso.object_id = tt.id AND sso.content_type_id = tt.content_type_id"
        #     )
        # )

        # existing_rows = res.rowcount
        # print(f"Existing rows in salesforce_salesforceobject: <{existing_rows}>")

        # print("Nulling not known")
        # res = db_conn.execute(
        #     sqlalchemy.sql.text("update temp_table set sso_id=null where sso_id=0")
        # )

        # print(f"Nulled: <{res.rowcount}>")

        # # update existing
        # res = db_conn.execute(
        #     sqlalchemy.sql.text(
        #         "UPDATE salesforce_salesforceobject sso SET salesforce_id = tt.salesforce_id FROM temp_table tt WHERE tt.sso_id = sso.id"
        #     )
        # )

        # print(f"Existing updates: <{res.rowcount}>")
        # print(f"Should be: <{existing_rows}>")

        # # insert new
        # res = db_conn.execute(
        #     sqlalchemy.sql.text(
        #         "select id, salesforce_id, content_type_id, batch from temp_table WHERE sso_id IS NULL"
        #     )
        # )

        # db_conn.commit()

        # print(f"New inserts: <{res.rowcount}>")
        # print(f"Should be: <{total_rows - existing_rows}>")

        # print("Dropping table")
        # res = db_conn.execute(sqlalchemy.sql.text("TRUNCATE TABLE temp_table"))
        # db_conn.commit()
        # res = db_conn.execute(sqlalchemy.sql.text("DROP TABLE temp_table"))
        # db_conn.commit()

        # df = pd.read_sql_query(
        #     sql=sqlalchemy.sql.text(
        #         "SELECT t.relname, l.locktype, page, virtualtransaction, pid, mode, granted FROM pg_locks l, pg_stat_all_tables t  WHERE l.relation = t.relid and relname not like 'pg_%' ORDER BY pid asc;"
        #     ),
        #     con=db_conn,
        # )
        # db_conn.commit()
        # print(df)

    # data = (
    #     {"id": 1, "title": "The Hobbit", "primary_author": "Tolkien"},
    #     {"id": 2, "title": "The Silmarillion", "primary_author": "Tolkien"},
    # )

    # statement = text(
    #     """INSERT INTO book(id, title, primary_author) VALUES(:id, :title, :primary_author)"""
    # )

    # for line in data:
    #     con.execute(statement, **line)
