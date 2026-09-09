-- Sparrow SQL v0 reject: GROUP BY
SELECT device_id, SUM(temperature) FROM sensor_readings GROUP BY device_id;
