## homework for week 2 workflow schéduler
Question 1:

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have? 

* 447,770
* 766,792
* 299,234
* 822,132

Response : 
447,770


Qestion 2:

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC.

* Response
bash off building yaml file

```bash

prefect deployment build etl_web_to_gcs.py:etl_web_to_gcs -n "Deployment_scheduled_every_mounth_first" --cron "0 5 1 * *"

```

apply deplyment

```bash

prefect deployment apply etl_web_to_gcs-deployment.yaml 

```
- `0 5 1 * *` : first of every mounth at 5 am utc
- `0 0 5 1 *` : At 12:00 AM on day 5 of the month, only in January
- `5 * 1 0 *` : Expression invalid
- `* * 5 1 0` : Every minute on Sunday or on day 5 of the month, only in January


## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. 
This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- 14,851,920
- 12,282,990
- 27,235,753
- 11,338,483


# response : 

- 14,851,920


## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,019
- 192,297
- 88,605
- 190,225