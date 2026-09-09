-- Sparrow SQL v0 reject: HOP
SELECT HOP(ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE) FROM sensor_readings;
