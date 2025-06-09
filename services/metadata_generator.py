import os
import psycopg2
import pandas as pd
import random
import re
import string
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

def generate_meta_data():
    try:
        # ✅ Connect to PostgreSQL
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        # ✅ Fetch rows needing metadata
        df = pd.read_sql_query("""
            SELECT * FROM cleaned_video_meta WHERE meta_data_added = FALSE;
        """, conn)

        if df.empty:
            return {"status": "no_data", "message": "No rows with meta_data_added = FALSE"}

        # ── Helpers ────────────────────────────────────────────────────────────────
        def generate_urls(title):
            slug = re.sub(r'[^a-z0-9-]', '',
                          re.sub(r'\s+', '-', title.lower())).strip('-')
            alphabet = string.ascii_letters + string.digits + "_-"
            nano_id = ''.join(random.choices(alphabet, k=10)) + "_G"
            slug_nano = f"{slug}_{nano_id}"
            return (
                nano_id,
                slug_nano,
                f"https://suvichaar.org/stories/{slug_nano}",
                f"https://stories.suvichaar.org/{slug_nano}.html"
            )

        def generate_iso_time():
            now = datetime.now(timezone.utc)
            return now.strftime('%Y-%m-%dT%H:%M:%S+00:00')

        static_metadata = {
            "lang": "en-US",
            "storygeneratorname": "Suvichaar Board",
            "contenttype": "Article",
            "storygeneratorversion": "1.0.0",
            "sitename": "Suvichaar",
            "generatorplatform": "Suvichaar",
            "sitelogo96x96": "https://media.suvichaar.org/filters:resize/96x96/media/brandasset/suvichaariconblack.png",
            "sitelogo32x32": "https://media.suvichaar.org/filters:resize/32x32/media/brandasset/suvichaariconblack.png",
            "sitelogo192x192": "https://media.suvichaar.org/filters:resize/192x192/media/brandasset/suvichaariconblack.png",
            "sitelogo144x144": "https://media.suvichaar.org/filters:resize/144x144/media/brandasset/suvichaariconblack.png",
            "sitelogo92x92": "https://media.suvichaar.org/filters:resize/92x92/media/brandasset/suvichaariconblack.png",
            "sitelogo180x180": "https://media.suvichaar.org/filters:resize/180x180/media/brandasset/suvichaariconblack.png",
            "publisher": "Suvichaar",
            "publisherlogosrc": "https://media.suvichaar.org/media/brandasset/suvichaariconblack.png",
            "gtagid": "G-2D5GXVRK1E",
            "organization": "Suvichaar",
            "publisherlogoalt": "Suvichaarlogo",
            "person": "person",
            "s11btntext": "Read More",
            "s10caption1": "Your daily dose of inspiration"
        }

        user_profiles = {
            "Mayank": "https://www.instagram.com/iamkrmayank?igsh=eW82NW1qbjh4OXY2&utm_source=qr",
            "Onip": "https://www.instagram.com/onip.mathur/profilecard/?igsh=MW5zMm5qMXhybGNmdA==",
            "Naman": "https://njnaman.in/"
        }

        # ── Enrich metadata ───────────────────────────────────────────────────────
        enriched_rows = []
        for _, row in df.iterrows():
            storytitle = str(row.get("storytitle", "")).strip()
            uuid, slug, canonical_url, amp_url = generate_urls(storytitle)
            published_time = generate_iso_time()
            modified_time = generate_iso_time()
            pagetitle = f"{storytitle} | Suvichaar"
            user = random.choice(list(user_profiles.keys()))
            profile = user_profiles[user]

            enriched = row.to_dict()
            enriched.update({
                "uuid": uuid,
                "urlslug": slug,
                "canurl": canonical_url,
                "canurl1": amp_url,
                "publishedtime": published_time,
                "modifiedtime": modified_time,
                "pagetitle": pagetitle,
                "user": user,
                "userprofileurl": profile,
                **static_metadata
            })
            enriched_rows.append(enriched)

        # ── Drop original id so SERIAL can take over ─────────────────────────────
        enriched_df = pd.DataFrame(enriched_rows).drop(columns=["id"])

        # ── Create meta_data table if missing ───────────────────────────────────
        column_defs = ",\n".join([
            f'"{col}" TEXT'
            for col in enriched_df.columns
        ])
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS meta_data (
                id SERIAL PRIMARY KEY,
                {column_defs}
            );
        """)

        # ── Insert enriched rows ─────────────────────────────────────────────────
        cols = list(enriched_df.columns)
        insert_query = f"""
            INSERT INTO meta_data ({', '.join(f'"{c}"' for c in cols)})
            VALUES ({', '.join(['%s'] * len(cols))});
        """
        cur.executemany(insert_query, enriched_df[cols].values.tolist())

        # ── Mark source rows as done ─────────────────────────────────────────────
        ids_to_update = tuple(df["id"].tolist())
        cur.execute("""
            UPDATE cleaned_video_meta
               SET meta_data_added = TRUE
             WHERE id IN %s;
        """, (ids_to_update,))

        conn.commit()
        cur.close()
        conn.close()

        return {"status": "success", "records_processed": len(enriched_df)}

    except Exception as e:
        return {"status": "error", "detail": str(e)}
