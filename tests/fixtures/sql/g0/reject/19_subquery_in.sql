-- Sparrow SQL v0 reject: IN subquery
SELECT * FROM sensor_readings WHERE device_id IN (SELECT id FROM devices);
