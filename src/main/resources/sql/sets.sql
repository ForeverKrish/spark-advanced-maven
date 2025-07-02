/*
Given table ranges(id, start_date, end_date):
Id | start_date | end_date
1  | 20240101   | 20240102
2  | 20240103   | 20240104
3  | 20240105   | 20240106
4  | 20240108   | 20240109
5  | 20240110   | 20240111

Write a SQL to calculate the number of “sets.”
A new set begins whenever the gap between a range’s end_date and the next start_date is more than 1 day.
*/
WITH ordered AS (
  SELECT
    id,
    start_date,
    end_date,
    LAG(end_date) OVER (ORDER BY start_date) AS prev_end
  FROM ranges
),
flags AS (
  SELECT
    id,
    start_date,
    end_date,
    CASE WHEN start_date - prev_end = 1 THEN 0 ELSE 1 END AS new_set_flag
  FROM ordered
),
groups AS (
  SELECT
    id,
    SUM(new_set_flag) OVER (ORDER BY start_date
                             ROWS UNBOUNDED PRECEDING) AS set_group
  FROM flags
)
SELECT COUNT(DISTINCT set_group) AS number_of_sets
FROM groups;
