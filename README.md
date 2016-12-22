Jessica Zhao, jmz2131
Big Data Analytics, Columbia University

#Final Project: Citi Bike Availability Predictor

## Overview

## Etc.

## Usage
In order to use the analysis, you need to first collect data using the scraper.

In order to use the scraper, you must first have a Postgres database setup with
a database named 'citibike' that the current user has access to.

### Using the scraper
In order to run the scraper, install ruby version 2.3.1.

- Next install the `bundler` gem.
- Next, navigate to the scraper directory in this repo.
- Run `bundle install` to install the dependencies.

Now, you should be able to run the scraper by just running `ruby scrape.rb`.

Let this run forever.

### Using the analytic tool
- Install maven and spark.
- Navigate to the root of this repository.
- Run `mvn install` to install the dependencies
- Modify the `build_and_run.sh` file to point to your copy of spark-submit and
  the driver class if you have a non-standard maven dependency directory.
- Run `build_and_run.sh` and follow the on screen prompts.
