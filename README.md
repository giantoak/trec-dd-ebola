# TREC DD 2015 / Ebola Domain Data Preparation

## About
This repository will be a pipeline for generating ~1M high recall tweets from
West Africa from the
[onset](http://en.wikipedia.org/wiki/Ebola_virus_epidemic_in_West_Africa_timeline)
of the Ebola crisis until the end of the archive; this
is roughly from February 1, 2014 through November 30, 2014.

The goal is to generate ~1M high recall Tweets from the data, all made by
people located with any of the three main west African countries afflicted by
the [epidemic](http://en.wikipedia.org/wiki/Ebola_virus_epidemic_in_West_Africa):
Guinea, Liberia, and Sierra Leone.

In general, our method is to use [mrjob](https://pythonhosted.org/mrjob/)
to collect and aggregate statistics from a number of tweets and twitter users
in the corpus. We've written a few programs to help with this:
* `MRTwitterWestAfricaUsers.py` collects usernames and counts of the times that they reference:
  * locations exclusively in West Africa
  * mentions of "ebola"
  * mentions of disaster-related terms
* `MRGetTweetGraph.py` to get the network of mentions for a list of users.
* `MRGetTweetsByUsers.py` to get all tweets by a list of users. (This is a MapReduce operation with no reducer...)


On our first pass, we select:
* tweets from locations within West Africa.
* tweets mentioning locations exclusively associated with West Africa

We use these tweets to build an index of twitter counts:
* those that tweet from within West Africa
* those 


From these stats, we select...
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
The process for executing programs that incorporate `mrjob` is laid out in
detail in the module's [documentation](https://pythonhosted.org/mrjob/); this
is a quick reference.

### Requirements
* an uncompressed list of files containing tweets, with a name like `list_of_trec_files.txt`
* a key for decrypting this stuff, called something like `trec_decrypter.private`
* Everything in this repository

### Running Locally
At the command line, type: `python westafricatwitter.py list_of_trec_files.txt > results.csv`
When the job finishes running, `results.csv` should contain a list of users
and some summary stats.

### Running on EC2
This is *way* too large a challenge to cover in a small blurb. To keep it overly brief:
1. Have an Amazon S3 bucket for results at [s3://my-bucket/](s3://my-bucket/).
2. Make sure you've got yourself configured to use EC2 as described in the [`mrjob` documentation](https://pythonhosted.org/mrjob/guides/emr-quickstart.html)
3. At the command line, type: `python westafricatwitter.py list_of_trec_files.txt -r emr -c conf_files/mrjob_wrapper.conf --output-dir=s3://my-bucket/wat_results --no-output `


## Run Times on EC2
EC2 run times vary a bit depending on settings. General notes:
* Bootstrapping takes roughly 1600 seconds. (26 minutes and 40 seconds)
* `MRGetTweetGraph.py`
  * The base run time for a file roughly 100 seconds. (1 minute and 40 seconds.) *This is assuming a linear scaling for `m1.large` boxes*, something that must be verified with additional runs.
  * 35,000 files can be processed with the *original* user list in roughly 3057,738 seconds (849.4 hours) of CPU time. An individual file takes roughly 88 seconds of CPU time (Run on 10 m1.xlarge core instances and 99 m1.xlarge task instances.)
* `MRTwitterWestAfricaUsers.py`
  * 35,000 files can be processed in roughly 4,675,028 seconds (246.7 hours) of CPU time. An individual file takes roughly 25.4 seconds of CPU time. (Run on 10(?) cc2.8xlarge instances) (This is ONLY cpu time, so is an undercount.)
  * 141,853 files can be processed in roughly  4,675,027,690 seconds (1,298 hours). An individual file takes roughly 33 seconds of CPU time. (Run on 10 m1.xlarge core instances and 70 m1.xlarge task instances.)