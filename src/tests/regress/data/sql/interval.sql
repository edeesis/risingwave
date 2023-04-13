--
-- INTERVAL
--

SET DATESTYLE = 'ISO';
SET IntervalStyle to postgres;

-- check acceptance of "time zone style"
SELECT INTERVAL '01:00' AS "One hour";
SELECT INTERVAL '+02:00' AS "Two hours";
SELECT INTERVAL '-08:00' AS "Eight hours";
--@ SELECT INTERVAL '-1 +02:03' AS "22 hours ago...";
SELECT INTERVAL '-1 days +02:03' AS "22 hours ago...";
--@ SELECT INTERVAL '1.5 weeks' AS "Ten days twelve hours";
--@ SELECT INTERVAL '1.5 months' AS "One month 15 days";
SELECT INTERVAL '10 years -11 month -12 days +13:14' AS "9 years...";

CREATE TABLE INTERVAL_TBL (f1 interval);

INSERT INTO INTERVAL_TBL (f1) VALUES ('1 minute');
INSERT INTO INTERVAL_TBL (f1) VALUES ('5 hour');
INSERT INTO INTERVAL_TBL (f1) VALUES ('10 day');
INSERT INTO INTERVAL_TBL (f1) VALUES ('34 year');
INSERT INTO INTERVAL_TBL (f1) VALUES ('3 months');
-- INSERT INTO INTERVAL_TBL (f1) VALUES ('14 seconds ago');
INSERT INTO INTERVAL_TBL (f1) VALUES ('-14 seconds');
INSERT INTO INTERVAL_TBL (f1) VALUES ('1 day 2 hours 3 minutes 4 seconds');
INSERT INTO INTERVAL_TBL (f1) VALUES ('6 years');
INSERT INTO INTERVAL_TBL (f1) VALUES ('5 months');
INSERT INTO INTERVAL_TBL (f1) VALUES ('5 months 12 hours');

-- badly formatted interval
INSERT INTO INTERVAL_TBL (f1) VALUES ('badly formatted interval');
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 30 eons ago');

-- test interval operators

SELECT * FROM INTERVAL_TBL;

SELECT * FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 <> interval '10 days';

SELECT * FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 <= interval '5 hours';

SELECT * FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 < interval '1 day';

SELECT * FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 = interval '34 years';

SELECT * FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 >= interval '1 month';

SELECT * FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 > interval '-3 seconds';

SELECT r1.*, r2.*
   FROM INTERVAL_TBL r1, INTERVAL_TBL r2
   WHERE r1.f1 > r2.f1
   ORDER BY r1.f1, r2.f1;

-- Test intervals that are large enough to overflow 64 bits in comparisons
CREATE TABLE INTERVAL_TBL_OF (f1 interval);
INSERT INTO INTERVAL_TBL_OF (f1) VALUES
  ('2147483647 days 2147483647 months'),
  ('2147483647 days -2147483648 months'),
  ('1 year'),
  ('-2147483648 days 2147483647 months'),
  ('-2147483648 days -2147483648 months');
-- these should fail as out-of-range
INSERT INTO INTERVAL_TBL_OF (f1) VALUES ('2147483648 days');
INSERT INTO INTERVAL_TBL_OF (f1) VALUES ('-2147483649 days');
INSERT INTO INTERVAL_TBL_OF (f1) VALUES ('2147483647 years');
INSERT INTO INTERVAL_TBL_OF (f1) VALUES ('-2147483648 years');

-- Test edge-case overflow detection in interval multiplication
--@ select extract(epoch from '256 microseconds'::interval * (2^55)::float8);

SELECT r1.*, r2.*
   FROM INTERVAL_TBL_OF r1, INTERVAL_TBL_OF r2
   WHERE r1.f1 > r2.f1
   ORDER BY r1.f1, r2.f1;

--@ CREATE INDEX ON INTERVAL_TBL_OF USING btree (f1);
--@ SET enable_seqscan TO false;
--@ EXPLAIN (COSTS OFF)
--@ SELECT f1 FROM INTERVAL_TBL_OF r1 ORDER BY f1;
SELECT f1 FROM INTERVAL_TBL_OF r1 ORDER BY f1;
--@ RESET enable_seqscan;

DROP TABLE INTERVAL_TBL_OF;

-- Test multiplication and division with intervals.
-- Floating point arithmetic rounding errors can lead to unexpected results,
-- though the code attempts to do the right thing and round up to days and
-- minutes to avoid results such as '3 days 24:00 hours' or '14:20:60'.
-- Note that it is expected for some day components to be greater than 29 and
-- some time components be greater than 23:59:59 due to how intervals are
-- stored internally.

CREATE TABLE INTERVAL_MULDIV_TBL (span interval);
-- COPY INTERVAL_MULDIV_TBL FROM STDIN;
INSERT INTO INTERVAL_MULDIV_TBL VALUES
('41 mon 12 days 360:00'),
('-41 mon -12 days +360:00'),
('-12 days'),
('9 mon -27 days 12:34:56'),
('-3 years 482 days 76:54:32.189'),
('4 mon'),
('14 mon'),
('999 mon 999 days');

SELECT span * 0.3 AS product
FROM INTERVAL_MULDIV_TBL;

SELECT span * 8.2 AS product
FROM INTERVAL_MULDIV_TBL;

SELECT span / 10 AS quotient
FROM INTERVAL_MULDIV_TBL;

SELECT span / 100 AS quotient
FROM INTERVAL_MULDIV_TBL;

DROP TABLE INTERVAL_MULDIV_TBL;

SELECT * FROM INTERVAL_TBL;

-- test avg(interval), which is somewhat fragile since people have been
-- known to change the allowed input syntax for type interval without
-- updating pg_aggregate.agginitval

select avg(f1) from interval_tbl;

-- test long interval input
--@ select '4 millenniums 5 centuries 4 decades 1 year 4 months 4 days 17 minutes 31 seconds'::interval;

-- test long interval output
-- Note: the actual maximum length of the interval output is longer,
-- but we need the test to work for both integer and floating-point
-- timestamps.
--@ select '100000000y 10mon -1000000000d -100000h -10min -10.000001s ago'::interval;

-- test justify_hours() and justify_days()

--@ SELECT justify_hours(interval '6 months 3 days 52 hours 3 minutes 2 seconds') as "6 mons 5 days 4 hours 3 mins 2 seconds";
--@ SELECT justify_days(interval '6 months 36 days 5 hours 4 minutes 3 seconds') as "7 mons 6 days 5 hours 4 mins 3 seconds";

-- test justify_interval()

--@ SELECT justify_interval(interval '1 month -1 hour') as "1 month -1 hour";

-- test fractional second input, and detection of duplicate units
--@ SELECT '1 millisecond'::interval, '1 microsecond'::interval,
--@        '500 seconds 99 milliseconds 51 microseconds'::interval;
--@ SELECT '3 days 5 milliseconds'::interval;

--@ SELECT '1 second 2 seconds'::interval;              -- error
SELECT '10 milliseconds 20 milliseconds'::interval; -- error
SELECT '5.5 seconds 3 milliseconds'::interval;      -- error
SELECT '1:20:05 5 microseconds'::interval;          -- error
--@ SELECT '1 day 1 day'::interval;                     -- error
--@ SELECT interval '1-2';  -- SQL year-month literal
SELECT interval '999' second;  -- oversize leading field is ok
SELECT interval '999' minute;
SELECT interval '999' hour;
SELECT interval '999' day;
SELECT interval '999' month;

-- test SQL-spec syntaxes for restricted field sets
SELECT interval '1' year;
SELECT interval '2' month;
SELECT interval '3' day;
SELECT interval '4' hour;
SELECT interval '5' minute;
SELECT interval '6' second;
--@ SELECT interval '1' year to month;
--@ SELECT interval '1-2' year to month;
--@ SELECT interval '1 2' day to hour;
--@ SELECT interval '1 2:03' day to hour;
--@ SELECT interval '1 2:03:04' day to hour;
--@ SELECT interval '1 2' day to minute;
--@ SELECT interval '1 2:03' day to minute;
--@ SELECT interval '1 2:03:04' day to minute;
--@ SELECT interval '1 2' day to second;
--@ SELECT interval '1 2:03' day to second;
--@ SELECT interval '1 2:03:04' day to second;
--@ SELECT interval '1 2' hour to minute;
--@ SELECT interval '1 2:03' hour to minute;
--@ SELECT interval '1 2:03:04' hour to minute;
--@ SELECT interval '1 2' hour to second;
--@ SELECT interval '1 2:03' hour to second;
--@ SELECT interval '1 2:03:04' hour to second;
--@ SELECT interval '1 2' minute to second;
--@ SELECT interval '1 2:03' minute to second;
--@ SELECT interval '1 2:03:04' minute to second;
--@ SELECT interval '1 +2:03' minute to second;
--@ SELECT interval '1 +2:03:04' minute to second;
--@ SELECT interval '1 -2:03' minute to second;
--@ SELECT interval '1 -2:03:04' minute to second;
--@ SELECT interval '123 11' day to hour; -- ok
SELECT interval '123 11' day; -- not ok
SELECT interval '123 11'; -- not ok, too ambiguous
SELECT interval '123 2:03 -2:04'; -- not ok, redundant hh:mm fields

-- test syntaxes for restricted precision
--@ SELECT interval(0) '1 day 01:23:45.6789';
--@ SELECT interval(2) '1 day 01:23:45.6789';
--@ SELECT interval '12:34.5678' minute to second(2);  -- per SQL spec
--@ SELECT interval '1.234' second;
--@ SELECT interval '1.234' second(2);
--@ SELECT interval '1 2.345' day to second(2);
--@ SELECT interval '1 2:03' day to second(2);
--@ SELECT interval '1 2:03.4567' day to second(2);
--@ SELECT interval '1 2:03:04.5678' day to second(2);
--@ SELECT interval '1 2.345' hour to second(2);
--@ SELECT interval '1 2:03.45678' hour to second(2);
--@ SELECT interval '1 2:03:04.5678' hour to second(2);
--@ SELECT interval '1 2.3456' minute to second(2);
--@ SELECT interval '1 2:03.5678' minute to second(2);
--@ SELECT interval '1 2:03:04.5678' minute to second(2);

-- test casting to restricted precision (bug #14479)
--@ SELECT f1, f1::INTERVAL DAY TO MINUTE AS "minutes",
--@   (f1 + INTERVAL '1 month')::INTERVAL MONTH::INTERVAL YEAR AS "years"
--@   FROM interval_tbl;

-- test inputting and outputting SQL standard interval literals
SET IntervalStyle TO sql_standard;
--@ SELECT  interval '0'                       AS "zero",
--@         interval '1-2' year to month       AS "year-month",
--@         interval '1 2:03:04' day to second AS "day-time",
--@         - interval '1-2'                   AS "negative year-month",
--@         - interval '1 2:03:04'             AS "negative day-time";

-- test input of some not-quite-standard interval values in the sql style
SET IntervalStyle TO postgres;
--@ SELECT  interval '+1 -1:00:00',
--@         interval '-1 +1:00:00',
--@         interval '+1-2 -3 +4:05:06.789',
--@         interval '-1-2 +3 -4:05:06.789';

-- test output of couple non-standard interval values in the sql style
SET IntervalStyle TO sql_standard;
--@ SELECT  interval '1 day -1 hours',
--@         interval '-1 days +1 hours',
--@         interval '1 years 2 months -3 days 4 hours 5 minutes 6.789 seconds',
--@         - interval '1 years 2 months -3 days 4 hours 5 minutes 6.789 seconds';

-- test outputting iso8601 intervals
SET IntervalStyle to iso_8601;
--@ select  interval '0'                                AS "zero",
--@         interval '1-2'                              AS "a year 2 months",
--@         interval '1 2:03:04'                        AS "a bit over a day",
--@         interval '2:03:04.45679'                    AS "a bit over 2 hours",
--@         (interval '1-2' + interval '3 4:05:06.7')   AS "all fields",
--@         (interval '1-2' - interval '3 4:05:06.7')   AS "mixed sign",
--@         (- interval '1-2' + interval '3 4:05:06.7') AS "negative";

-- test inputting ISO 8601 4.4.2.1 "Format With Time Unit Designators"
SET IntervalStyle to sql_standard;
--@ select  interval 'P0Y'                    AS "zero",
--@         interval 'P1Y2M'                  AS "a year 2 months",
--@         interval 'P1W'                    AS "a week",
--@         interval 'P1DT2H3M4S'             AS "a bit over a day",
--@         interval 'P1Y2M3DT4H5M6.7S'       AS "all fields",
--@         interval 'P-1Y-2M-3DT-4H-5M-6.7S' AS "negative",
--@         interval 'PT-0.1S'                AS "fractional second";

-- test inputting ISO 8601 4.4.2.2 "Alternative Format"
SET IntervalStyle to postgres;
--@ select  interval 'P00021015T103020'       AS "ISO8601 Basic Format",
--@         interval 'P0002-10-15T10:30:20'   AS "ISO8601 Extended Format";

-- Make sure optional ISO8601 alternative format fields are optional.
--@ select  interval 'P0002'                  AS "year only",
--@         interval 'P0002-10'               AS "year month",
--@         interval 'P0002-10-15'            AS "year month day",
--@         interval 'P0002T1S'               AS "year only plus time",
--@         interval 'P0002-10T1S'            AS "year month plus time",
--@         interval 'P0002-10-15T1S'         AS "year month day plus time",
--@         interval 'PT10'                   AS "hour only",
--@         interval 'PT10:30'                AS "hour minute";

-- test a couple rounding cases that changed since 8.3 w/ HAVE_INT64_TIMESTAMP.
select interval '-10 mons -3 days +03:55:06.70';
select interval '1 year 2 mons 3 days 04:05:06.699999';
--@ select interval '0:0:0.7', interval '@ 0.70 secs', interval '0.7 seconds';

-- check that '30 days' equals '1 month' according to the hash function
select '30 days'::interval = '1 month'::interval as t;
--@ select interval_hash('30 days'::interval) = interval_hash('1 month'::interval) as t;

-- numeric constructor
--@ select make_interval(years := 2);
--@ select make_interval(years := 1, months := 6);
--@ select make_interval(years := 1, months := -1, weeks := 5, days := -7, hours := 25, mins := -180);
--@ 
--@ select make_interval() = make_interval(years := 0, months := 0, weeks := 0, days := 0, mins := 0, secs := 0.0);
--@ select make_interval(hours := -2, mins := -10, secs := -25.3);
--@ 
--@ select make_interval(years := 'inf'::float::int);
--@ select make_interval(months := 'NaN'::float::int);
--@ select make_interval(secs := 'inf');
--@ select make_interval(secs := 'NaN');
--@ select make_interval(secs := 7e12);

--
-- test EXTRACT
--
SELECT f1,
    EXTRACT(MICROSECOND FROM f1) AS MICROSECOND,
    EXTRACT(MILLISECOND FROM f1) AS MILLISECOND,
    EXTRACT(SECOND FROM f1) AS SECOND,
    EXTRACT(MINUTE FROM f1) AS MINUTE,
    EXTRACT(HOUR FROM f1) AS HOUR,
    EXTRACT(DAY FROM f1) AS DAY,
    EXTRACT(MONTH FROM f1) AS MONTH,
    EXTRACT(QUARTER FROM f1) AS QUARTER,
    EXTRACT(YEAR FROM f1) AS YEAR,
    EXTRACT(DECADE FROM f1) AS DECADE,
    EXTRACT(CENTURY FROM f1) AS CENTURY,
    EXTRACT(MILLENNIUM FROM f1) AS MILLENNIUM,
    EXTRACT(EPOCH FROM f1) AS EPOCH
    FROM INTERVAL_TBL;

SELECT EXTRACT(FORTNIGHT FROM INTERVAL '2 days');  -- error
SELECT EXTRACT(TIMEZONE FROM INTERVAL '2 days');  -- error

SELECT EXTRACT(DECADE FROM INTERVAL '100 y');
SELECT EXTRACT(DECADE FROM INTERVAL '99 y');
SELECT EXTRACT(DECADE FROM INTERVAL '-99 y');
SELECT EXTRACT(DECADE FROM INTERVAL '-100 y');

SELECT EXTRACT(CENTURY FROM INTERVAL '100 y');
SELECT EXTRACT(CENTURY FROM INTERVAL '99 y');
SELECT EXTRACT(CENTURY FROM INTERVAL '-99 y');
SELECT EXTRACT(CENTURY FROM INTERVAL '-100 y');

-- date_part implementation is mostly the same as extract, so only
-- test a few cases for additional coverage.
SELECT f1,
    date_part('microsecond', f1) AS microsecond,
    date_part('millisecond', f1) AS millisecond,
    date_part('second', f1) AS second,
    date_part('epoch', f1) AS epoch
    FROM INTERVAL_TBL;

-- internal overflow test case
SELECT extract(epoch from interval '1000000000 days');

DROP TABLE INTERVAL_TBL;
