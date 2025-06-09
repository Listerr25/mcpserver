from fastapi import APIRouter
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()

# Final required order with batch_custom_id + curly‐brace fields
final_columns = [
    "batch_custom_id",
    "{{storytitle}}", "{{pagetitle}}", "{{uuid}}","{{urlslug}}",
    "{{canurl}}", "{{canurl1}}", "{{publishedtime}}", "{{modifiedtime}}",
    "{{metakeywords}}", "{{metadescription}}", "{{s2paragraph1}}", "{{s3paragraph1}}",
    "{{s4paragraph1}}", "{{s5paragraph1}}", "{{s6paragraph1}}", "{{s7paragraph1}}",
    "{{s8paragraph1}}", "{{s9paragraph1}}", "{{s1alt1}}", "{{s2alt1}}", "{{s3alt1}}",
    "{{s4alt1}}", "{{s5alt1}}", "{{s6alt1}}", "{{s7alt1}}", "{{s8alt1}}", "{{s9alt1}}",
    "{{hookline}}", "{{potraightcoverurl}}", "{{landscapecoverurl}}", "{{squarecoverurl}}",
    "{{socialthumbnailcoverurl}}", "{{s1image1}}", "{{s2image1}}", "{{s3image1}}",
    "{{s4image1}}", "{{s5image1}}", "{{s6image1}}", "{{s7image1}}", "{{s8image1}}",
    "{{s9image1}}", "{{s11btntext}}", "{{lang}}", "{{user}}", "{{userprofileurl}}",
    "{{storygeneratorname}}", "{{contenttype}}", "{{storygeneratorversion}}", "{{sitename}}",
    "{{generatorplatform}}", "{{sitelogo96x96}}", "{{person}}", "{{sitelogo32x32}}",
    "{{sitelogo192x192}}", "{{sitelogo144x144}}", "{{sitelogo92x92}}", "{{sitelogo180x180}}",
    "{{publisher}}", "{{publisherlogosrc}}", "{{gtagid}}", "{{organization}}",
    "{{publisherlogoalt}}", "{{s10video1}}", "{{s10alt1}}", "{{videoscreenshot}}",
    "{{s10caption1}}", "{{s11paragraph1}}", "{{nextstoryimage}}", "{{nextstoryimagealt}}",
    "{{prevstorytitle}}", "{{prevstorylink}}", "{{nextstorytitle}}", "{{nextstorylink}}",
    "{{s11btnlink}}", "{{writername}}"
]

# Mapping from source‐column → final curly name
mapping = {
    "batch_custom_id":       "batch_custom_id",
    "storytitle":            "{{storytitle}}",
    "pagetitle":             "{{pagetitle}}",
    "uuid":                  "{{uuid}}",
    "urlslug":               "{{urlslug}}",
    "canurl":                "{{canurl}}",
    "canurl1":               "{{canurl1}}",
    "publishedtime":         "{{publishedtime}}",
    "modifiedtime":          "{{modifiedtime}}",
    "metakeywords":          "{{metakeywords}}",
    "metadescription":       "{{metadescription}}",
    "s2paragraph1":          "{{s2paragraph1}}",
    "s3paragraph1":          "{{s3paragraph1}}",
    "s4paragraph1":          "{{s4paragraph1}}",
    "s5paragraph1":          "{{s5paragraph1}}",
    "s6paragraph1":          "{{s6paragraph1}}",
    "s7paragraph1":          "{{s7paragraph1}}",
    "s8paragraph1":          "{{s8paragraph1}}",
    "s9paragraph1":          "{{s9paragraph1}}",
    "s1alt1":                "{{s1alt1}}",
    "s2alt1":                "{{s2alt1}}",
    "s3alt1":                "{{s3alt1}}",
    "s4alt1":                "{{s4alt1}}",
    "s5alt1":                "{{s5alt1}}",
    "s6alt1":                "{{s6alt1}}",
    "s7alt1":                "{{s7alt1}}",
    "s8alt1":                "{{s8alt1}}",
    "s9alt1":                "{{s9alt1}}",
    "hookline":              "{{hookline}}",
    "potraightcoverurl":     "{{potraightcoverurl}}",
    "landscapecoverurl":     "{{landscapecoverurl}}",
    "squarecoverurl":        "{{squarecoverurl}}",
    "socialthumbnailcoverur":"{{socialthumbnailcoverurl}}",
    "s1image1":              "{{s1image1}}",
    "s2imageurl1":           "{{s2image1}}",
    "s3imageurl1":           "{{s3image1}}",
    "s4imageurl1":           "{{s4image1}}",
    "s5imageurl1":           "{{s5image1}}",
    "s6imageurl1":           "{{s6image1}}",
    "s7imageurl1":           "{{s7image1}}",
    "s8imageurl1":           "{{s8image1}}",
    "s9imageurl1":           "{{s9image1}}",
    "s11btntext":            "{{s11btntext}}",
    "lang":                  "{{lang}}",
    "user":                  "{{user}}",
    "userprofileurl":        "{{userprofileurl}}",
    "storygeneratorname":    "{{storygeneratorname}}",
    "contenttype":           "{{contenttype}}",
    "storygeneratorversion": "{{storygeneratorversion}}",
    "sitename":              "{{sitename}}",
    "generatorplatform":     "{{generatorplatform}}",
    "sitelogo96x96":         "{{sitelogo96x96}}",
    "person":                "{{person}}",
    "sitelogo32x32":         "{{sitelogo32x32}}",
    "sitelogo192x192":       "{{sitelogo192x192}}",
    "sitelogo144x144":       "{{sitelogo144x144}}",
    "sitelogo92x92":         "{{sitelogo92x92}}",
    "sitelogo180x180":       "{{sitelogo180x180}}",
    "publisher":             "{{publisher}}",
    "publisherlogosrc":       "{{publisherlogosrc}}",
    "gtagid":                "{{gtagid}}",
    "organization":          "{{organization}}",
    "publisherlogoalt":      "{{publisherlogoalt}}",
    "s10video1":             "{{s10video1}}",
    "s10alt1":               "{{s10alt1}}",
    "videoscreenshot":       "{{videoscreenshot}}",
    "s10caption1":           "{{s10caption1}}",
    "s11paragraph1":         "{{s11paragraph1}}",
    "nextstoryimageurl":     "{{nextstoryimage}}",
    "nextstoryimagealt":     "{{nextstoryimagealt}}",
    "prevstorytitle":        "{{prevstorytitle}}",
    "prevstorylink":         "{{prevstorylink}}",
    "nextstorytitle":        "{{nextstorytitle}}",
    "nextstoryimage":        "{{nextstorylink}}",
    "s11btnlink":            "{{s11btnlink}}",
    "writername":            "{{writername}}",
}

# Invert mapping: final curly → source
curly_to_source = {v: k for k, v in mapping.items()}


@router.post("/reorder")
def reorder_and_clean_data():
    try:
        # 1) Connect to PostgreSQL
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        # 2) Read entire pre_final_stage_data
        df = pd.read_sql_query("SELECT * FROM pre_final_stage_data;", conn)

        # 3) Drop the `id` column if present
        if "id" in df.columns:
            df.drop(columns=["id"], inplace=True)

        # 4) Construct a new DataFrame with columns exactly in final_columns order
        new_df = pd.DataFrame()

        for col in final_columns:
            if col == "batch_custom_id":
                # Copy directly if source exists
                new_df["batch_custom_id"] = df["batch_custom_id"] if "batch_custom_id" in df.columns else ""
            else:
                # col is a curly field: find its source name
                src = curly_to_source.get(col, None)

                if src and src in df.columns:
                    # Copy from the source column
                    new_df[col] = df[src]
                elif col in df.columns:
                    # If the DataFrame already has a literal curly column, preserve it
                    new_df[col] = df[col]
                else:
                    # Otherwise, fill with empty strings
                    new_df[col] = ""

        # 5) Drop old final_quote_fancy_data if it exists, then recreate with new columns
        cur.execute("DROP TABLE IF EXISTS final_quote_fancy_data;")
        create_sql = (
            "CREATE TABLE final_quote_fancy_data (\n"
            + ",\n".join([f'"{c}" TEXT' for c in new_df.columns])
            + "\n);"
        )
        cur.execute(create_sql)

        # 6) Bulk‐insert all rows
        insert_cols = ", ".join([f'"{c}"' for c in new_df.columns])
        placeholders = ", ".join(["%s"] * len(new_df.columns))
        insert_sql = f"INSERT INTO final_quote_fancy_data ({insert_cols}) VALUES ({placeholders});"
        cur.executemany(insert_sql, new_df.values.tolist())
        conn.commit()

        # 7) Close connections
        cur.close()
        conn.close()

        return {
            "status": "success",
            "message": "Data saved to final_quote_fancy_data",
            "columns_saved": new_df.columns.tolist(),
            "records": len(new_df)
        }

    except Exception as e:
        return {"status": "error", "detail": str(e)}
