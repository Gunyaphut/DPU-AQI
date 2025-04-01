      -- นับจำนวนวันที่ AQI เกิน 150
      SELECT 
          COUNT(DISTINCT DATE(timestamp)) AS days_exceeding_150
      FROM pollution_data
      WHERE aqius > 150