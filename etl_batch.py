import pandas as pd
import pyodbc
from datetime import datetime
import glob

print("\n" + "="*60)
print(" ETL: BATCH DATA PIPELINE ")
print("="*60 + "\n")

# اتصال SQL
def connect_sql():
    options = [
        ('localhost,14333', '{ODBC Driver 17 for SQL Server}', True),
        ('localhost,14333', '{ODBC Driver 18 for SQL Server}', True),
    ]
    last_error = None
    for server, driver, trust_cert in options:
        try:
            conn_str = (
                f"DRIVER={driver};"
                f"SERVER={server};"
                f"DATABASE=SensorWarehouse1;"
                f"UID=sa;PWD=YourStrong!Passw0rd;"
            )
            if trust_cert:
                conn_str += "TrustServerCertificate=Yes;"
            print(f"🔄 Trying: {server} with {driver} ...")
            conn = pyodbc.connect(conn_str, timeout=10)
            print(f"✅ Connected: {server}\n")
            return conn
        except Exception as e:
            last_error = str(e)
            print(f"⚠️  Failed: {server} with {driver} -> {e}")
            continue
    print(f"❌ Last error: {last_error}")
    return None


# الاتصال
conn = connect_sql()
if conn:
    cursor = conn.cursor()
    cursor.fast_executemany = True
    print("✅ SQL connection established\n")
else:
    print("⚠️  SQL connection failed - continuing with CSV export only\n")
    cursor = None

# قراءة CSV
CSV_PATH = "sensor_stream_from_kafka.csv/*.csv"
csv_files = glob.glob(CSV_PATH)

if not csv_files:
    CSV_PATH = "sensor_stream.csv"
    csv_files = [CSV_PATH]

print(f"📂 Reading CSV files: {csv_files}")

data_frames = []
for file in csv_files:
    df = pd.read_csv(file)
    data_frames.append(df)

data = pd.concat(data_frames, ignore_index=True)
print(f"✅ Loaded {len(data)} records\n")

# تحويلات
print("🔄 Starting transformations...")
data = data.dropna().copy()
data['timestamp'] = pd.to_datetime(data['timestamp'])
data = data.sort_values('timestamp')
data['sensor_id'] = data['sensor_id'].astype(int)
data['temperature'] = data['temperature'].astype(float)
data['humidity'] = data['humidity'].astype(float)

# dim_date
data['date_id'] = data['timestamp'].dt.strftime("%Y%m%d").astype(int)
tmp = data.drop_duplicates('date_id').copy()
dim_date = pd.DataFrame({
    'date_id': tmp['date_id'].values,
    'date_value': tmp['timestamp'].dt.date.values,
    'year': tmp['timestamp'].dt.year.values,
    'month': tmp['timestamp'].dt.month.values,
    'day': tmp['timestamp'].dt.day.values,
    'day_name': tmp['timestamp'].dt.day_name().values,
    'hour': tmp['timestamp'].dt.hour.values
})

# anomalies
data['temp_anomaly'] = ((data['temperature'] > 40) | (data['temperature'] < 0)).astype(int)
data['humidity_anomaly'] = ((data['humidity'] > 90) | (data['humidity'] < 10)).astype(int)
data['anomaly_flag'] = data.apply(
    lambda x: 'Anomaly' if x['temp_anomaly'] or x['humidity_anomaly'] else 'Normal', axis=1
)
data['data_quality_score'] = 100 - (data['temp_anomaly'] + data['humidity_anomaly']) * 50

print("✅ Transformations complete\n")

# تحميل SQL (لو متاح)
if conn and cursor:
    try:
        print("📤 Loading data to SQL Server...")
        cursor.execute("DELETE FROM dbo.dim_date")
        cursor.executemany(
            "INSERT INTO dbo.dim_date VALUES (?,?,?,?,?,?,?)",
            list(zip(dim_date['date_id'], dim_date['date_value'], dim_date['year'],
                     dim_date['month'], dim_date['day'], dim_date['day_name'], dim_date['hour']))
        )
        conn.commit()
        print("✅ dim_date loaded")

        cursor.execute("DELETE FROM dbo.fact_sensor_readings")
        cursor.executemany(
            """INSERT INTO dbo.fact_sensor_readings 
            (sensor_id, date_id, temperature, humidity,
             temp_anomaly, humidity_anomaly, anomaly_flag, data_quality_score, ingestion_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [(int(r.sensor_id), int(r.date_id),
              float(r.temperature), float(r.humidity),
              int(r.temp_anomaly), int(r.humidity_anomaly),
              r.anomaly_flag, int(r.data_quality_score), datetime.now())
             for r in data.itertuples(index=False)]
        )
        conn.commit()
        print("✅ fact_sensor_readings loaded")

        # ========= هنا الإضافة الاختيارية Snapshot من SQL إلى CSV =========
        try:
            conn2 = pyodbc.connect(
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=localhost,14333;"
                "DATABASE=SensorWarehouse1;"
                "UID=sa;PWD=YourStrong!Passw0rd;"
                "TrustServerCertificate=Yes;",
                timeout=10
            )
            df_fact = pd.read_sql_query(
                "SELECT reading_id, sensor_id, date_id, temperature, humidity, anomaly_flag, data_quality_score, ingestion_time FROM dbo.fact_sensor_readings",
                conn2
            )
            df_fact.to_csv("fact_sensor_readings_export.csv", index=False, encoding="utf-8")
            print("✅ Exported: fact_sensor_readings_export.csv")
            conn2.close()
        except Exception as e:
            print(f"⚠️  SQL export snapshot failed: {e}")
        # ================================================================

        conn.close()
    except Exception as e:
        print(f"⚠️  SQL load failed: {e}")
else:
    print("⏭️  Skipping SQL load (no connection)")


# تصدير CSV (دائماً)
OUTPUT_FILE = "final_warehouse_data.csv"
data[['timestamp','sensor_id','temperature','humidity','anomaly_flag','data_quality_score']].to_csv(
    OUTPUT_FILE, index=False
)
print(f"✅ Exported: {OUTPUT_FILE}")

print("\n" + "="*60)
print(" ETL COMPLETED ")
print("="*60 + "\n")
