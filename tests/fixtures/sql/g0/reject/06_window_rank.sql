-- Sparrow SQL v0 reject: RANK window
SELECT RANK() OVER (ORDER BY temperature) FROM sensor_readings;
