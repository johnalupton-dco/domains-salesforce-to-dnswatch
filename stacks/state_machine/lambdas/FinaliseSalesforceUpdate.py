import os
import io
from typing import List, Optional
import pandas as pd
import boto3
import sqlalchemy
from cddo.utils.constants import (
    ENV_UPDATE_FROM_SALESFORCE_BUCKET,
    FLD_SALESFORCE_CHANGE_FILES,
    FLD_FIELDS_TO_UPDATE,
    FLD_FIELDS_TO_JOIN,
    FLD_FILES_WRITTEN,
)
from cddo.utils.postgres import get_db_engine

TEMP_TABLE = "zzz_temp_table"
FLD_FIELDS_TO_NULL = "fieldsToNull"

OUTPUT_BUCKET = os.environ[ENV_UPDATE_FROM_SALESFORCE_BUCKET]
s3_client = boto3.client("s3")


def _drop_temp_table(db_conn: sqlalchemy.Connection):
    print("Dropping table")
    _res = db_conn.execute(sqlalchemy.sql.text(f"TRUNCATE TABLE {TEMP_TABLE}"))
    db_conn.commit()
    _res = db_conn.execute(sqlalchemy.sql.text(f"DROP TABLE {TEMP_TABLE}"))
    db_conn.commit()


def _create_null_sql(
    upsert_object: str,
    fields_to_null: List[str],
) -> str:
    set_stmt = ",".join([f"{f}=null" for f in fields_to_null])

    query = f"UPDATE {upsert_object} uo SET {set_stmt}"

    return query


def _create_update_sql(
    upsert_object: str,
    fields_to_join: List[str],
    fields_to_update: List[str],
) -> str:
    set_stmt = ",".join([f"{f}=tt.{f}" for f in fields_to_update])
    where_stmt = " and ".join([f"uo.{f}=tt.{f}" for f in fields_to_join])

    query = f"UPDATE {upsert_object} uo SET {set_stmt} FROM {TEMP_TABLE} tt WHERE {where_stmt}"

    return query


def _create_insert_sql(
    upsert_object: str,
    fields_to_join: List[str],
    fields_to_update: List[str],
) -> str:
    flds_stmt = ",".join([f for f in (fields_to_join + fields_to_update)])
    values_stmt = ",".join([f"tt.{f}" for f in (fields_to_join + fields_to_update)])

    where_stmt = " and ".join([f"tt.{f} is null" for f in fields_to_join])

    query = f"INSERT INTO {upsert_object}({flds_stmt}) SELECT {values_stmt} FROM {TEMP_TABLE} tt WHERE {where_stmt}"

    return query


def upsert_from_file(
    bucket_name: str,
    key: str,
    db_conn: sqlalchemy.Connection,
    upsert_object: str,
    fields_to_join: List[str],
    fields_to_update: List[str],
    fields_to_null: List[str],
):
    df_input: pd.DataFrame = pd.read_csv(
        io.StringIO(
            s3_client.get_object(Bucket=bucket_name, Key=key)["Body"]
            .read()
            .decode("utf-8")
        ),
        sep=",",
    )

    print("Creating table")
    res = df_input.to_sql(
        name=TEMP_TABLE,
        con=db_conn,
        if_exists="replace",
        index=False,
    )
    db_conn.commit()

    if len(fields_to_null) != 0:
        # null_query = _create_null_sql(
        #     upsert_object=upsert_object, fields_to_null=fields_to_null
        # )
        # print(null_query)
        # res = db_conn.execute(sqlalchemy.sql.text(null_query))
        # db_conn.commit()
        # print(f"Nulling {res.rowcount} rows")
        pass

    update_query = _create_update_sql(
        upsert_object=upsert_object,
        fields_to_join=fields_to_join,
        fields_to_update=fields_to_update,
    )
    print(update_query)

    res = db_conn.execute(sqlalchemy.sql.text(update_query))
    db_conn.commit()
    print(f"Updated {res.rowcount} rows")

    insert_query = _create_insert_sql(
        upsert_object=upsert_object,
        fields_to_join=fields_to_join,
        fields_to_update=fields_to_update,
    )
    print(insert_query)

    # res = db_conn.execute(
    #     sqlalchemy.sql.text(
    #         f"ALTER TABLE {TEMP_TABLE} ADD CONSTRAINT fk_tt_object FOREIGN KEY (id) REFERENCES {upsert_object} (id)"
    #     )
    # )
    # db_conn.commit()

    _drop_temp_table(db_conn=db_conn)


def lambda_handler(event, _context):
    s3 = boto3.resource("s3")
    input_files = event[FLD_SALESFORCE_CHANGE_FILES]

    engine = get_db_engine(rds_secret_name=os.environ["RDS_SECRET_NAME"])

    with engine.connect() as db_conn:
        for query_entity, object_info in input_files.items():
            print(f"Processing {query_entity}")
            for file in object_info[FLD_FILES_WRITTEN]:
                print(f"File {file}")
                upsert_from_file(
                    bucket_name=OUTPUT_BUCKET,
                    key=file,
                    db_conn=db_conn,
                    upsert_object=query_entity,
                    fields_to_join=object_info[FLD_FIELDS_TO_JOIN],
                    fields_to_update=object_info[FLD_FIELDS_TO_UPDATE],
                    fields_to_null=object_info[FLD_FIELDS_TO_NULL],
                )
        # #         s3.Object(OUTPUT_BUCKET, f"archive/{file}").copy_from(
        # #             CopySource=f"{OUTPUT_BUCKET}/{file}"
        # #         )
        # #         s3.Object(OUTPUT_BUCKET, file).delete()

        df_lookup = pd.read_csv(
            io.StringIO(
                s3_client.get_object(
                    Bucket=OUTPUT_BUCKET, Key="salesforce_salesforceobject.csv"
                )["Body"]
                .read()
                .decode("utf-8")
            ),
            sep=",",
        )

        df_lookup = df_lookup.loc[
            df_lookup["model"].isin(["domain", "organisation"]), :
        ]
        df_lookup = df_lookup.dropna(subset=["id"])
        df_lookup["content_type_id"] = 0
        df_lookup["sso_id"] = 0
        df_lookup["batch"] = pd.NA

        print("Creating table")
        df_lookup.to_sql(
            name=TEMP_TABLE,
            con=db_conn,
            if_exists="replace",
            index=False,
            schema="public",
            dtype={
                "content_type_id": sqlalchemy.types.INTEGER,
                "sso_id": sqlalchemy.types.BIGINT,
                "model": sqlalchemy.types.VARCHAR(100),
                "id": sqlalchemy.types.VARCHAR(255),
                "salesforce_id": sqlalchemy.types.VARCHAR(255),
                "batch": sqlalchemy.types.UUID,
            },
        )

        # how many rows to be added
        total_rows = len(df_lookup)
        print(f"Rows to upsert: <{total_rows}>")

        # how many rows in {TEMP_TABLE}
        res = db_conn.execute(sqlalchemy.sql.text(f"SELECT * FROM {TEMP_TABLE}"))

        print(f"Rows in {TEMP_TABLE}: <{res.rowcount}>")

        # get content type id for each model
        res = db_conn.execute(
            sqlalchemy.sql.text(
                f"UPDATE {TEMP_TABLE} tt SET content_type_id = dct.id  FROM django_content_type dct WHERE dct.model = tt.model"
            )
        )

        print(f"Content type updates: <{res.rowcount}>")
        print(f"Content type updates should be: <{total_rows}>")

        # update existing entries
        res = db_conn.execute(
            sqlalchemy.sql.text(
                f"UPDATE {TEMP_TABLE} tt SET sso_id = sso.id  FROM salesforce_salesforceobject sso WHERE sso.object_id = tt.id AND sso.content_type_id = tt.content_type_id"
            )
        )

        existing_rows = res.rowcount
        print(f"Existing rows in salesforce_salesforceobject: <{existing_rows}>")

        print("Nulling ids which dont exist in DNSWatch")
        res = db_conn.execute(
            sqlalchemy.sql.text(f"update {TEMP_TABLE} set sso_id=null where sso_id=0")
        )

        print(f"Nulled: <{res.rowcount}>")

        # update existing
        res = db_conn.execute(
            sqlalchemy.sql.text(
                f"UPDATE salesforce_salesforceobject sso SET salesforce_id = tt.salesforce_id FROM {TEMP_TABLE} tt WHERE tt.sso_id = sso.id"
            )
        )

        print(f"Existing updates: <{res.rowcount}>")
        print(f"Should be: <{existing_rows}>")

        # insert new
        res = db_conn.execute(
            sqlalchemy.sql.text(
                f"INSERT INTO salesforce_salesforceobject(id, salesforce_id, content_type_id, batch) select id, salesforce_id, content_type_id, batch from {TEMP_TABLE} WHERE sso_id IS NULL"
            )
        )

        db_conn.commit()

        print(f"New inserts: <{res.rowcount}>")
        print(f"Should be: <{total_rows - existing_rows}>")

        _drop_temp_table(db_conn=db_conn)

        _df = pd.read_sql_query(
            sql=sqlalchemy.sql.text(
                "SELECT t.relname, pid, mode, granted FROM pg_locks l, pg_stat_all_tables t  WHERE l.relation = t.relid and relname not like 'pg_%' ORDER BY pid asc;"
            ),
            con=db_conn,
        )
        db_conn.commit()

    # data = (
    #     {"id": 1, "title": "The Hobbit", "primary_author": "Tolkien"},
    #     {"id": 2, "title": "The Silmarillion", "primary_author": "Tolkien"},
    # )

    # statement = text(
    #     """INSERT INTO book(id, title, primary_author) VALUES(:id, :title, :primary_author)"""
    # )

    # for line in data:
    #     con.execute(statement, **line)
