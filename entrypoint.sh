#!/bin/sh
export DAGSTER_HOME=/app

# This block may be omitted if not packaging a repository with cron schedules:
####################################################################################################
# see: https://unix.stackexchange.com/a/453053 - fixes inflated hard link count
touch /etc/crontab /etc/cron.*/*

service cron start

# Add all schedules defined by the user
dagster schedule up
####################################################################################################

# Launch Dagit as a service
DAGSTER_HOME=/app dagit -h 0.0.0.0 -p 3003