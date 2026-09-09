-- Sparrow SQL v0 reject: unbounded JOIN
SELECT a.temperature FROM sensor_readings a JOIN devices b ON a.device_id = b.id;
