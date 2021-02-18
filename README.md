# lena_tweets

## Introduction

This is a package that can be set up to scrape data from twitter and output results into csv files for downstream data analysis.

The pipelines set up get the following information:
* `kick_off_study`: pulls in initial information about a set of twitter users who's handles are given in a txt file called study_input.txt.
    * It outputs to a csv. It collects:
        * twitter user id, name and description of the profile
        * these can be tweaked by amending function starting on lines 214 in lena_tweets/solids.py 
    * It also collects the ids of all the twitter users that this account follows and adds these - as well as the original account - to a tracking database
    * this pipeline needs to be kicked off manually. If it fails, it should be kicked of again - it will continue from where it left off.
* `tweet_history`: collects tweets for all users in the tracking database, going back as far as twitter holds (maximum most recent 3200 tweets) and puts these into a csv file.
*  `daily_user_scrape`: collects user ids that each participant of the study follows. Outputs these to a csv.
* `daily_tweet_scrape`: collects tweets of users continuously, since the latest tweet that was fetched. Outputs these to a csv.

When tweets are stored, the stored attributes are:
* user id
* id (of tweet)
* tweet text
* tweet creation time

These can be tweaked by amending function starting on lines 228 in lena_tweets/solids.py

## Instructions to install
1. Start up an AWS instance, a medium sized ubuntu should be OK. Change storage to something quite large to avoid running out of space - maybe around 128 GBs (hard disk storage is relatively cheap). This can be edited later too but it's a bit fiddly. Modify the Security Group to allow TCP connections to port 3003 and port 22 from the IP of the user - I recommend closing down all other ports since they are not needed.
1. Connect to instance with ssh. Install docker and docker-compose.
1. Get the lena_tweets repo on the instance, e.g. by cloning from git or by scp-ing it onto the instance.
1. Manually edit lena_tweets/config.py to include credentials.
    * this must be a list of credentials that have one of the following formats:
        ```
        {"API_KEY": "SOMETHING",
        "KEY_SECRET": "SOMETHING LONGER"},
        ```
        ```
        {"API_KEY": "SOMETHING",
        "KEY_SECRET": "SOMETHING LONGER",
        "ACCESS_TOKEN": "SOMETHING LONGER2",
        "ACCESS_TOKEN_SECRET": "SOMETHING LONGER3"}
        ```
    * these are twitter app credentials
    * the app is currently configured to work very nicely with 6 sets of credentials that belong to 3 apps all together. It will cycle over them.
1. Copy over a file of tweet handles to data/, and name this file study_input.txt
1. Build the image by running: `docker-compose build` in the root of the repo
1. Run the application by running `docker-compose up`
1. Head to port 3003 of the instance to check the application up and running


## Running the pipelines

The `kick_off_study` can be kicked off by selecting the pipeline in the left sidebar and clicking on the "playground" tab in the middle and then clicked "Launch Execution". This should open a new tab that shows the pipeline running. The tab can be closed - the pipeline will continue running. Runs can be seen in the "runs" tab later too - both currently executing and old ones, along with any logs.

The other pipelines are pipelines with schedules. They can be run manually too, however normally they would be run by turning on their schedule in the schedules tab. This will automatically run them at the schedule they specify - atm every 3 minutes.