-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE FUNCTION count_char(s varchar, c varchar) RETURNS int LANGUAGE rust AS $$
fn count_char(s: &str, c: &str) -> i32 {
  s.matches(c).count() as i32
}
$$;
CREATE SINK nexmark_q14 AS
SELECT auction,
       bidder,
       0.908 * price as price,
       CASE
           WHEN
                       extract(hour from date_time) >= 8 AND
                       extract(hour from date_time) <= 18
               THEN 'dayTime'
           WHEN
                       extract(hour from date_time) <= 6 OR
                       extract(hour from date_time) >= 20
               THEN 'nightTime'
           ELSE 'otherTime'
           END       AS bidTimeType,
       date_time,
       -- extra
       count_char(extra, 'c') AS c_counts
FROM bid
WHERE 0.908 * price > 1000000
  AND 0.908 * price < 50000000
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
