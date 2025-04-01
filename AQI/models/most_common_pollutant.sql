      -- ค้นหาสารมลพิษที่พบบ่อยที่สุดในสหรัฐฯ
      SELECT 
          mainus AS main_pollutant,
          COUNT(*) AS occurrences
      FROM pollution_data
      GROUP BY mainus
      ORDER BY occurrences DESC
      LIMIT 1