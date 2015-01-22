# TREC DD 2015 / Ebola Domain Data Preparation

## About

This repository will be a pipeline for generating ~1M high recall tweets from
West Africa during the onset of the Ebola crisis: February - November, 2014.

The goal is to generate ~1M high recall Tweets from West Africa in the last
9 months (from February 2014 to the end of November), during the onset of the
Ebola crisis. Our primary concern is the region from which the tweets come.
We might consider other elements, but that is the most important.

In general, our method is to use [mrjob](https://pythonhosted.org/mrjob/)
to collect and aggregate statistics from a number of tweets and twitter users
in the corpus. From these stats, we select...
1. tweets that might be found in a high-recall search
2. users who tend to produce tweets that would be found in a high-recall search

We then reprocess the tweets, this time pulling out...
1. The tweets that we identified in our first pass.
2. Additional tweets by the identified users.

To choose these additional tweets by users, we focus on whether they mentioned
(or were located within) West Africa at least *X* times in a period *P*. If so,
we keep all tweets made within *P*.
* Identify users who routinely mention portions of the afflicted regions.
* Identify user who routinely mention terms related to the outbreak.
* Identify users who are likely in the afflicted areas during the outbreak.

## Running

To run this job locally, you'll need...
* an uncompressed list of files containing tweets, with a name like `list_of_trec_files.txt`
* a key for decrypting this stuff, called something like `trec_decrypter.private`
* Everything in this repository

At the command line, type: `python westafricatwitter.py list_of_trec_files.txt > results.csv`
When the job finishes running, `results.csv` should contain a list of users and some summary stats.