# TREC DD 2015 / Ebola Domain Data Preparation

This repository will be a pipeline for generating ~1M high recall tweets from
West Africa during the onset of the Ebola crisis: February - November, 2014.


The goal is to generate ~1M high recall Tweets from West Africa in the last
9 months (from February 2014 to the end of November), during the onset of the
Ebola crisis.

In general, our method is to use [mrjob]() to collect and aggregate statistics
from a number of tweets and twitter users in the corpus. From these stats, we 
select both
* users whose tweets are useful
* particular tweets that would be found in a high-recall search.

We then rebuild the pool retaining both the identified tweets and the tweets by
identified users. This is an iterative process, some of the steps of which include:

* Identify users who routinely mention portions of the afflicted regions.
* Identify user who routinely mention terms related to the outbreak.
* Identify users who are likely in the afflicted areas during the outbreak.