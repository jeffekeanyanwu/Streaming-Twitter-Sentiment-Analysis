#simple check to make sure data ingested (just two categories, id and text)
SELECT * FROM "twitter-project"."twitter_race_analysis" limit 10;

#this just tells me if it was a reply to someone else, or potentially a retweet (even though I filtered those out)
CREATE TABLE IF NOT EXISTS twitter_race_analysis WITH (format = 'JSON') AS
SELECT *,
	CASE
		WHEN text like '%@%' THEN 'yes' ELSE 'no'
	END AS is_tweet_reply
FROM "twitter-project"."twitter_race_2"

#rather lazy but quick way to add one table to my preliminary quicksight analysis
CREATE TABLE IF NOT EXISTS twitter_race_analysis1 WITH (format = 'JSON') AS
SELECT *, length(text) as tweet_length
FROM "twitter-project"."twitter_race_analysis"
---
CREATE TABLE twitter_race_analysis
AS SELECT * FROM twitter_race_analysis1
WITH DATA;
---
DROP TABLE IF EXISTS twitter_race_analysis1;