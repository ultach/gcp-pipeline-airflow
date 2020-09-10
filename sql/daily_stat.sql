SELECT 
    toDate(starttime) AS trip_date,
    count() AS number_of_trips,
    round(avg(tripduration) / 60, 2) AS `average_tripduration(min)`,
    round(countIf(gender = 0) / number_of_trips, 2) * 100 AS percent_of_female_cliens,
    round(countIf(gender = 1) / number_of_trips, 2) * 100 AS percent_of_male_cliens,
    round(countIf(gender = 2) / number_of_trips, 2) * 100 AS percent_of_other_cliens
FROM trips
GROUP BY trip_date
WHERE trip_date = {today}
INTO OUTFILE {outfile}
FORMAT CSV
