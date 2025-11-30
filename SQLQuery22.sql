-- إنشاء قاعدة البيانات لو مش موجودة
IF DB_ID('SensorWarehouse1') IS NULL
BEGIN
    CREATE DATABASE SensorWarehouse1;
    PRINT '✅ Database SensorWarehouse1 created.';
END
ELSE
BEGIN
    PRINT 'ℹ️  Database SensorWarehouse1 already exists.';
END
GO

-- استخدام القاعدة
USE SensorWarehouse1;
GO

-- حذف الجداول لو موجودة لإعادة الإنشاء نظيف
IF OBJECT_ID('dbo.fact_sensor_readings','U') IS NOT NULL DROP TABLE dbo.fact_sensor_readings;
IF OBJECT_ID('dbo.dim_sensor','U') IS NOT NULL DROP TABLE dbo.dim_sensor;
IF OBJECT_ID('dbo.dim_date','U') IS NOT NULL DROP TABLE dbo.dim_date;
IF OBJECT_ID('dbo.dim_location','U') IS NOT NULL DROP TABLE dbo.dim_location;
GO

-- جداول الأبعاد
CREATE TABLE dbo.dim_sensor (
    sensor_id INT PRIMARY KEY,
    sensor_name VARCHAR(50),
    sensor_type VARCHAR(50),
    model VARCHAR(50),
    status VARCHAR(20),
    creation_date DATETIME DEFAULT GETDATE()
);

CREATE TABLE dbo.dim_date (
    date_id INT PRIMARY KEY,
    date_value DATE,
    year INT,
    month INT,
    day INT,
    day_name VARCHAR(20),
    hour INT
);

CREATE TABLE dbo.dim_location (
    location_id INT PRIMARY KEY,
    location_name VARCHAR(100),
    building VARCHAR(50),
    floor INT,
    room VARCHAR(50)
);

-- جدول الفاكت
CREATE TABLE dbo.fact_sensor_readings (
    reading_id INT PRIMARY KEY IDENTITY(1,1),
    sensor_id INT NOT NULL,
    date_id INT NOT NULL,
    location_id INT NULL,
    temperature FLOAT,
    humidity FLOAT,
    temp_anomaly INT,
    humidity_anomaly INT,
    anomaly_flag VARCHAR(20),
    data_quality_score INT,
    ingestion_time DATETIME,
    CONSTRAINT FK_fact_sensor FOREIGN KEY (sensor_id) REFERENCES dbo.dim_sensor(sensor_id),
    CONSTRAINT FK_fact_date FOREIGN KEY (date_id) REFERENCES dbo.dim_date(date_id),
    CONSTRAINT FK_fact_location FOREIGN KEY (location_id) REFERENCES dbo.dim_location(location_id)
);
GO

-- بيانات مبدئية للأبعاد
INSERT INTO dbo.dim_sensor(sensor_id, sensor_name, sensor_type, model, status)
VALUES
(1, 'Sensor 1', 'Temperature', 'DHT22', 'Active'),
(2, 'Sensor 2', 'Humidity', 'BME680', 'Active'),
(3, 'Sensor 3', 'Mixed', 'DHT22+BME680', 'Active');

INSERT INTO dbo.dim_location(location_id, location_name, building, floor, room)
VALUES
(1, 'Building A - Floor 3', 'Building A', 3, '301'),
(2, 'Building B - Floor 2', 'Building B', 2, '201'),
(3, 'Building C - Floor 1', 'Building C', 1, '101');

PRINT '✅ Star schema created successfully!';
GO
