import os
import psycopg2
import pandas as pd
import random  # optional, if you prefer random start offsets
from dotenv import load_dotenv

load_dotenv()

def distribute_urls():
    # 1) Connect
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT")
    )
    cur = conn.cursor()

    # 2) Load story text & metadata
    paragraph_df = pd.read_sql_query("""
        SELECT batch_custom_id,
               s2paragraph1, s3paragraph1, s4paragraph1, s5paragraph1,
               s6paragraph1, s7paragraph1, s8paragraph1, s9paragraph1,
               author_name, storytitle, metadescription, metakeywords
        FROM textual_structured_data;
    """, conn)

    # 3) Load image URLs + alt text
    resize_df = pd.read_sql_query("""
        SELECT author, alttxt, potraightcoverurl, landscapecoverurl,
               squarecoverurl, socialthumbnailcoverurl,
               nextstoryimageurl, standardurl
        FROM resized_url_data;
    """, conn)

    # 4) Normalize author keys
    paragraph_df["author_key"] = (
        paragraph_df["author_name"].str.replace(" ", "_").str.strip()
    )
    resize_df["author"] = resize_df["author"].str.strip()

    # 5) Build combined rows
    output_rows = []
    for row_num, (_, prow) in enumerate(paragraph_df.iterrows()):
        author_key  = prow["author_key"]
        author_imgs = resize_df[resize_df["author"] == author_key].reset_index(drop=True)
        total_imgs  = len(author_imgs)
        if total_imgs == 0:
            continue

        # start with all paragraph/metadata fields
        combined = prow.drop("author_key").to_dict()

        # choose an offset (rotate start for each story)
        start_offset = row_num % total_imgs
        # or random:
        # start_offset = random.randrange(total_imgs)

        # distribute each URL + alt text into suffixes 1..9
        for i in range(1, 10):
            idx = (start_offset + i - 1) % total_imgs
            suf = str(i)
            combined[f"potraightcoverurl{suf}"]       = author_imgs.at[idx, "potraightcoverurl"]
            combined[f"landscapecoverurl{suf}"]       = author_imgs.at[idx, "landscapecoverurl"]
            combined[f"squarecoverurl{suf}"]          = author_imgs.at[idx, "squarecoverurl"]
            combined[f"socialthumbnailcoverurl{suf}"] = author_imgs.at[idx, "socialthumbnailcoverurl"]
            combined[f"nextstoryimageurl{suf}"]       = author_imgs.at[idx, "nextstoryimageurl"]
            combined[f"standardurl{suf}"]             = author_imgs.at[idx, "standardurl"]
            combined[f"s{suf}alt1"]                   = author_imgs.at[idx, "alttxt"]

        output_rows.append(combined)

    final_df = pd.DataFrame(output_rows)

    # 6) Create distribution_data table
    cols_defs = ",\n".join(f"\"{col}\" TEXT" for col in final_df.columns)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS distribution_data (
      id SERIAL PRIMARY KEY,
      {cols_defs}
    );
    """
    cur.execute(ddl)

    # 7) Bulk insert
    if not final_df.empty:
        cols_quoted  = [f"\"{c}\"" for c in final_df.columns]
        placeholders = ", ".join(["%s"] * len(final_df.columns))
        insert_sql  = f"""
          INSERT INTO distribution_data ({', '.join(cols_quoted)})
          VALUES ({placeholders});
        """
        cur.executemany(insert_sql, final_df.values.tolist())
        conn.commit()

    # 8) Cleanup
    cur.close()
    conn.close()

    print(f"✅ Distributed {len(final_df)} records")
    return {"status": "success", "records_distributed": len(final_df)}
