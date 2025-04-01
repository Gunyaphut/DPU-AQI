-- หา AQI สูงสุดในแต่ละวัน และเลือกวันที่สูงสุด
      SELECT 
          DATE(timestamp) AS date,
          MAX(aqius) AS max_aqius
      FROM pollution_data
      GROUP BY DATE(timestamp)
      ORDER BY max_aqius DESC
      LIMIT 1