import psycopg2
import os
import json

PG_HOST = os.environ.get("PGHOST", "localhost")
PG_PORT = os.environ.get("PGPORT", "5433")
PG_DB = os.environ.get("PGDATABASE", "wiztric_logistics;")
PG_USER = os.environ.get("PGUSER", "postgres")
PG_PASS = os.environ.get("PGPASSWORD", "oggy")

def check_data():
    print(f"Connecting to {PG_DB} on {PG_HOST}:{PG_PORT}...")
    try:
        # Try without semicolon first if it fails
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
        except:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB.replace(";", ""), user=PG_USER, password=PG_PASS)
            
        cur = conn.cursor()
        
        # Check Parcels
        cur.execute("SELECT COUNT(*) FROM parcels")
        count = cur.fetchone()[0]
        print(f"\nTotal Parcels in DB: {count}")
        
        if count > 0:
            cur.execute("SELECT id, company_id, status, created_at FROM parcels ORDER BY created_at DESC LIMIT 5")
            print("\nRecent Parcels:")
            for row in cur.fetchall():
                print(row)
                
            # Check company distribution
            cur.execute("SELECT company_id, COUNT(*) FROM parcels GROUP BY company_id")
            print("\nParcels by Company:")
            for row in cur.fetchall():
                print(row)

        # Check Robots
        cur.execute("SELECT COUNT(*) FROM robots")
        rcount = cur.fetchone()[0]
        print(f"\nTotal Robots in DB: {rcount}")
        
        if rcount > 0:
            cur.execute("SELECT id, company_id, status FROM robots LIMIT 5")
            print("\nSample Robots:")
            for row in cur.fetchall():
                print(row)

        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_data()
