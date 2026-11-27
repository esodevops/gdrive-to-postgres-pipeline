from sqlalchemy import create_engine, text, inspect
from config import DB_URL


def load(df, table: str, schema: str = "staging", *, source_file=None):
    engine = create_engine(DB_URL)
    if source_file is None and not df.empty:
        source_file = df["source_file"].iloc[0]

    with engine.begin() as conn:
        quote = conn.dialect.identifier_preparer.quote_identifier
        qualified_table = f"{quote(schema)}.{quote(table)}"
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quote(schema)}"))
        # Make reruns safe: remove rows previously loaded from this same file
        if source_file is not None and inspect(conn).has_table(table, schema=schema):
            conn.execute(
                text(f"DELETE FROM {qualified_table} WHERE source_file = :f"),
                {"f": source_file},
            )

        df.to_sql(
            table,
            conn,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
    return len(df)
