Jessica Zhao, jmz2131

Big Data Analytics, Columbia University

#Final Project: Citi Bike Availability Predictor

## Overview
Citi Bike is a popular bike-share program with stations throughout NYC that a rider can pick up a bike from, or return a bike to. Previous projects on Citi Bike have analyzed popular stations and trip start/end locations, but have not addressed issues with availability and capacity of each station. Particularly at more popular locations, such as in Midtown Manhattan, there are sometimes no available bikes during popular time periods. For potential Citi Bike users that are considering getting a Citi Bike membership, as well as for current Citi Bike members who are looking to plan ahead, it would be extremely useful to be able to predict their chances of getting a bike from a given station at a particular day of week/time of day.

## Usage
In order to use the analytical tool, you need to first collect data using the scraper.

In order to use the scraper, you must first have a Postgres database setup with
a database named 'citibike' that the current user has access to.

### Using the scraper
In order to run the scraper: 
- Install ruby version 2.3.1.
- Install the `bundler` gem.
- Navigate to the scraper directory in this repo.
- Run `bundle install` to install the dependencies.
- Run `ruby scrape.rb`. Let this run "forever."

### Using the analytic tool
- Install maven and spark.
- Navigate to the root of this repository.
- Run `mvn install` to install the dependencies
- Modify the `build_and_run.sh` file to point to your copy of spark-submit and
  the driver class if you have a non-standard maven dependency directory.
- Run `build_and_run.sh` and follow the on screen prompts.
