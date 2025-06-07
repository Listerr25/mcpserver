import os
import shutil
import boto3
import uuid
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from nanoid import generate as nanoid
from simple_image_download import simple_image_download as simp

load_dotenv()

def download_and_upload_author_images():
    # AWS + S3 setup
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_KEY")
    region_name    = "ap-south-1"
    bucket_name    = "suvichaarapp"
    s3_prefix      = "media/"
    cdn_base_url   = "https://cdn.suvichaar.org/"

    # DB connection
    conn = psycopg2.connect(
        host     = os.getenv("PG_HOST"),
        database = os.getenv("PG_DATABASE"),
        user     = os.getenv("PG_USER"),
        password = os.getenv("PG_PASSWORD"),
        port     = os.getenv("PG_PORT")
    )
    cur = conn.cursor()

    # 1️⃣ Pick next batch
    cur.execute("""
        SELECT scrape_id
        FROM quote_scraped_data
        WHERE author_image_check IS DISTINCT FROM 'checked'
        GROUP BY scrape_id
        ORDER BY MIN(timestamp)
        LIMIT 1;
    """)
    row = cur.fetchone()
    if not row:
        return {"status": "no_pending_scrape_id"}
    scrape_id = row[0]

    # 2️⃣ Get authors needing images
    cur.execute("""
        SELECT DISTINCT author_name
        FROM quote_scraped_data
        WHERE scrape_id = %s
          AND author_image_check IS DISTINCT FROM 'checked';
    """, (scrape_id,))
    authors = [r[0].strip() for r in cur.fetchall() if r[0]]
    if not authors:
        return {"status": "no_authors"}

    # 3️⃣ Download 25 images per author
    downloader = simp.simple_image_download()
    for author in authors:
        downloader.download(author, 25)

    # 4️⃣ Upload to S3 + collect metadata
    s3 = boto3.client("s3",
        aws_access_key_id     = aws_access_key,
        aws_secret_access_key = aws_secret_key,
        region_name           = region_name
    )

    results     = []
    batch_uuid  = uuid.uuid4().hex[:8]
    batch_task  = f"{batch_uuid}_i1"

    for folder, _, files in os.walk("simple_images"):
        raw_author = os.path.basename(folder).strip()
        author     = raw_author.replace(" ", "_")

        # generate a nanoid suffix for this author's folder
        author_folder_id = nanoid(alphabet="0123456789abcdefghijklmnopqrstuvwxyz", size=10)
        s3_folder       = f"{author}-{author_folder_id}"

        for file in files:
            if not file.lower().endswith((".jpg", ".jpeg", ".png")):
                continue

            local_path = os.path.join(folder, file)
            orig_name  = file.replace(" ", "_")
            file_hash  = uuid.uuid4().hex[:8]                  # per-file hash
            new_name   = f"{file_hash}_{orig_name}"            # hashed filename

            s3_key = f"{s3_prefix}{batch_uuid}/{s3_folder}/{new_name}"
            try:
                s3.upload_file(local_path, bucket_name, s3_key)
            except Exception:
                continue

            cdn_url       = f"{cdn_base_url}{s3_key}"
            batch_custom  = f"{batch_task}_{author}"
            timestamp_now = datetime.utcnow()

            results.append((
                author,
                new_name,
                cdn_url,
                batch_task,
                batch_custom,
                "Auto",
                False,
                timestamp_now
            ))

    # 5️⃣ Ensure metadata table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS image_fetched_data (
            id SERIAL PRIMARY KEY,
            author         TEXT,
            filename       TEXT,
            cdn_url        TEXT,
            batch_task_id  TEXT,
            batch_custom_id TEXT,
            batch_type     TEXT,
            batch_created  BOOLEAN DEFAULT FALSE,
            timestamp      TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    # 6️⃣ Insert metadata rows
    if results:
        cur.executemany("""
            INSERT INTO image_fetched_data (
                author, filename, cdn_url,
                batch_task_id, batch_custom_id,
                batch_type, batch_created, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, results)

    # 7️⃣ Mark authors as checked
    for author in authors:
        cur.execute("""
            UPDATE quote_scraped_data
               SET author_image_check = 'checked'
             WHERE author_name = %s
               AND scrape_id = %s;
        """, (author, scrape_id))

    conn.commit()
    cur.close()
    conn.close()

    return {
        "status": "success",
        "scrape_id": scrape_id,
        "authors_processed": authors,
        "image_count": len(results),
        "db_table": "image_fetched_data"
    }
