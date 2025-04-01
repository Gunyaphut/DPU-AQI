-- คำนวณค่า AQI เฉลี่ยต่อวัน
      SELECT 
          DATE(timestamp) AS date,
          ROUND(AVG(aqius), 2) AS avg_aqius
      FROM pollution_data
      GROUP BY DATE(timestamp)