-- Sparrow SQL v0 reject: HAVING
SELECT device_id FROM sensor_readings GROUP BY device_id HAVING COUNT(*) > 1;
