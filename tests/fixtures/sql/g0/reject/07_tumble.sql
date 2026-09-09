-- Sparrow SQL v0 reject: TUMBLE
SELECT TUMBLE(ts, INTERVAL '1' MINUTE) FROM sensor_readings;
