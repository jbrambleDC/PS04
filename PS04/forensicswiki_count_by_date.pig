--
-- part2 problem 4
-- Create a Pig program that reports the number of URLs served each day in 2012 by the forensicswiki.org website

-- Clear the output directory location
--
rmf count_by_date

--
-- Map locally defined functions to the Java functions in the piggybank
--
DEFINE EXTRACT       org.apache.pig.piggybank.evaluation.string.EXTRACT();

--
-- Uncomment the data source you wish to use:

-- This URL uses just one day
--raw_logs = load 's3://gu-anly502/ps03/forensicswiki.2012-01.unzipped/access.log.2012-01-01' as (line:chararray);
--
-- This URL uses another day:
--raw_logs = load 's3://gu-anly502/ps03/forensicswiki/access.log.2012-12-31.gz' as (line:chararray);

-- This URL reads a month:
-- raw_logs = load 's3://gu-anly502/ps03/forensicswiki.2012-01.unzipped/access.log.2012-01-??' as (line:chararray);
--
-- This URL reads all of 2012:
raw_logs = load 's3://gu-anly502/ps03/forensicswiki.2012.txt' as (line:chararray);

 
-- logs_base processes each of the lines 
-- FLATTEN takes the extracted values and flattens them into a single tupple
--
logs_base = 
  FOREACH
   raw_logs
  GENERATE
   FLATTEN ( EXTRACT( line,
     '^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] "(\\S+) (\\S+) \\S+" (\\S+) (\\S+) "([^"]*)" "([^"]*)"'
     ) ) AS (
     host: chararray, identity: chararray, user: chararray, datetime_str: chararray, verb: chararray, url: chararray, request: chararray, status: int,
     size: int, referrer: chararray, agent: chararray
     );

-- YOUR CODE GOES HERE

logs = FOREACH logs_base GENERATE ToDate(SUBSTRING(datetime_str,0,11),'dd/MMM/yyyy') AS date, host, url, size;
logs = FOREACH logs GENERATE SUBSTRING(ToString(date),0,10) AS date, host, url, size;
logs = FILTER logs BY SUBSTRING(date,0,4)=='2012';
logs = FOREACH logs GENERATE REGEX_EXTRACT(url,'(index.php\\?title=|/wiki/)([^ &]*)',2) AS url, date;
logs = FILTER logs BY url != '' and url != '-';
group_date = GROUP logs BY date;
dates = FOREACH group_date GENERATE group as date, COUNT(logs);
date_counts_sorted = ORDER dates BY date;
-- YOUR CODE SHOULD PUT THE RESULTS IN date_counts_sorted

store date_counts_sorted INTO 'count_by_date' USING PigStorage();


-- Get the results
--
fs -getmerge count_by_date forensicswiki_count_by_date.txt

