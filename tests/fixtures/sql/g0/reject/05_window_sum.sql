-- Sparrow SQL v0 reject: window aggregate
SELECT SUM(temperature) OVER (PARTITION BY device_id) FROM sensor_readings;
