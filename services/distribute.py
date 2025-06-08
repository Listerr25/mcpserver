import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def distribute_urls():
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT")
    )
    cur = conn.cursor()

    # 1) Pull all the paragraph & metadata columns
    paragraph_df = pd.read_sql_query("""
        SELECT batch_custom_id,
               s2paragraph1, s3paragraph1, s4paragraph1, s5paragraph1,
               s6paragraph1, s7paragraph1, s8paragraph1, s9paragraph1,
               author_name, storytitle, metadescription, metakeywords
        FROM textual_structured_data;
    """, conn)

    # 2) Pull all the image URL columns
    resize_df = pd.read_sql_query("""
        SELECT author, alttxt, potraightcoverurl, landscapecoverurl,
               squarecoverurl, socialthumbnailcoverurl,
               nextstoryimageurl, standardurl
        FROM resized_url_data;
    """, conn)

    # normalize author keys
    paragraph_df["author_key"] = paragraph_df["author_name"]\
        .str.replace(" ", "_").str.strip()
    resize_df["author"] = resize_df["author"].str.strip()

    output_rows = []
    for _, prow in paragraph_df.iterrows():
        author_key = prow["author_key"]
        author_imgs = resize_df[resize_df["author"] == author_key].reset_index(drop=True)
        total_imgs = len(author_imgs)
        if total_imgs == 0:
            # no images → skip
            continue

        # start with all paragraph & metadata fields
        combined = prow.drop("author_key").to_dict()

        # distribute each URL column into X1 ... X10
        for i in range(1, 11):
            idx = (i - 1) % total_imgs
            suf = str(i)
            combined[f"potraightcoverurl{suf}"]       = author_imgs.at[idx, "potraightcoverurl"]
            combined[f"landscapecoverurl{suf}"]       = author_imgs.at[idx, "landscapecoverurl"]
            combined[f"squarecoverurl{suf}"]          = author_imgs.at[idx, "squarecoverurl"]
            combined[f"socialthumbnailcoverurl{suf}"] = author_imgs.at[idx, "socialthumbnailcoverurl"]
            combined[f"nextstoryimageurl{suf}"]       = author_imgs.at[idx, "nextstoryimageurl"]
            combined[f"standardurl{suf}"]             = author_imgs.at[idx, "standardurl"]

        output_rows.append(combined)

    final_df = pd.DataFrame(output_rows)

    # 3) Create distribution_data if it doesn't exist
    #    (auto-generating TEXT columns for every field in final_df)
    cols_defs = "\n".join(f"{col} TEXT" for col in final_df.columns)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS distribution_data (
          id SERIAL PRIMARY KEY,
          {cols_defs}
        );
    """)

    # 4) Bulk insert
    if not final_df.empty:
        cols = list(final_df.columns)
        placeholders = ", ".join(["%s"] * len(cols))
        insert_sql = f"""
          INSERT INTO distribution_data ({', '.join(cols)})
          VALUES ({placeholders});
        """
        cur.executemany(insert_sql, final_df[cols].values.tolist())
        conn.commit()

    cur.close()
    conn.close()

    print(f"✅ Distributed {len(final_df)} records")
    return {"status": "success", "records_distributed": len(final_df)}
