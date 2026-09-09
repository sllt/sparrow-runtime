-- Sparrow SQL v0 reject: FULL OUTER JOIN
SELECT * FROM sensor_readings a FULL OUTER JOIN devices b ON a.device_id = b.id;
