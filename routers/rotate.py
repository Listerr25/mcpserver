# routers/rotate.py

from fastapi import APIRouter
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()

def add_circular_navigation_fields(df):
    # Previous story: shift down by 1; wrap last → first
    df["prevstorytitle"] = df["storytitle"].shift(1)
    df["prevstorylink"]  = df["canurl"].shift(1)
    df.loc[0, ["prevstorytitle", "prevstorylink"]] = [
        df.loc[df.index[-1], "storytitle"],
        df.loc[df.index[-1], "canurl"]
    ]

    # Next story: shift up by 1; wrap first → last
    df["nextstorytitle"]   = df["storytitle"].shift(-1)
    df["nextstorylink"]    = df["canurl"].shift(-1)
    df["nextstoryimage"]   = df["nextstoryimageurl"].shift(-1)
    df["nextstoryimagealt"]= df["s1alt1"].shift(-1)
    df["s11paragraph1"]    = df["storytitle"].shift(-1)
    df["s11btnlink"]       = df["canurl"].shift(-1)

    last = df.index[-1]
    df.loc[last, [
        "nextstorytitle", "nextstorylink",
        "nextstoryimage", "nextstoryimagealt",
        "s11paragraph1", "s11btnlink"
    ]] = [
        df.loc[0, "storytitle"],
        df.loc[0, "canurl"],
        df.loc[0, "nextstoryimageurl"],
        df.loc[0, "s1alt1"],
        df.loc[0, "storytitle"],
        df.loc[0, "canurl"]
    ]

    return df

@router.post("/")
def rotate_meta_data():
    try:
        # 1) Connect to DB
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        # 2) Load meta_data table
        df = pd.read_sql_query("SELECT * FROM meta_data;", conn)

        # 3) Rename the “…1” columns to their base names
        df.rename(columns={
            "potraightcoverurl1":          "potraightcoverurl",
            "landscapecoverurl1":          "landscapecoverurl",
            "squarecoverurl1":             "squarecoverurl",
            "socialthumbnailcoverurl1":     "socialthumbnailcoverur",
            "nextstoryimageurl1":          "nextstoryimageurl",
            "standardurl1":                "s1image1"
        }, inplace=True)

        # 4) Drop the internal id so we can re‐serialize below
        if "id" in df.columns:
            df.drop(columns=["id"], inplace=True)

        # 5) Clean ALT-text columns
        alt_cols = [f"s{i}alt1" for i in range(1, 10)]
        for col in alt_cols:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(r'^ALT text:\s*', "", regex=True)
                    .str.strip('"')
                )

        # 6) Add circular navigation fields (prev/next title, link, image, etc.)
        df = add_circular_navigation_fields(df)

        # 7) Drop & recreate pre_final_stage_data
        cur.execute("DROP TABLE IF EXISTS pre_final_stage_data;")
        create_sql = (
            "CREATE TABLE pre_final_stage_data ("
            + ", ".join(f'"{c}" TEXT' for c in df.columns)
            + ", id SERIAL PRIMARY KEY"
            + ");"
        )
        cur.execute(create_sql)

        # 8) Bulk-insert rotated data
        cols = list(df.columns)
        insert_sql = f"""
            INSERT INTO pre_final_stage_data (
              {', '.join(f'"{c}"' for c in cols)}
            ) VALUES (
              {', '.join(['%s'] * len(cols))}
            );
        """
        cur.executemany(insert_sql, df[cols].values.tolist())
        conn.commit()

        cur.close()
        conn.close()
        return {"status": "success", "records_rotated": len(df)}

    except Exception as e:
        return {"status": "error", "message": str(e)}
