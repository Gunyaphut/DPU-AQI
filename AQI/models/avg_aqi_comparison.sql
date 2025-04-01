      -- เปรียบเทียบค่า AQI เฉลี่ยระหว่างมาตรฐาน US และ China
      SELECT 
          ROUND(AVG(aqius), 2) AS avg_aqius_us,
          ROUND(AVG(aqicn), 2) AS avg_aqicn_china
      FROM pollution_data