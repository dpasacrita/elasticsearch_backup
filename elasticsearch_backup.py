#!/usr/bin/python
from elasticsearch import Elasticsearch
import datetime
from dateutil.relativedelta import relativedelta
import os
__author__ = 'Daniel Pasacrita'
__date__ = '8/31/17'

# Global variables
# close_delta = how many months old before closing the index
close_delta = 6
# snapshot_delta = how many months old before snapshotting and deleting
snapshot_delta = 12
# es_url = the url of the elasticsearch instance
es_url = "elasticsearch1.domain.com:9200"
# snapshot_dir = the location of the snapshot
snapshot_dir = "/mnt/esbackup/snapshot"


def log_time():
    """
    Finds the current time, formatted for logging purposes

    :return: the current time
    """
    now = datetime.datetime.now()

    return now.strftime("%Y-%m-%d %H:%M")+": "


def calculate_date(months_prior):
    """
    Calculates and preps dates for later use, either 6 or 12 months ago.

    :param months_prior: how many months back we're looking for.
    :return: The date in this format: xxxx.xx(month).xx(day)
    """
    # Create the current date variable
    today = datetime.date.today()

    # If we're looking for 6 months prior, get that, otherwise, 12 months
    end_date = today - relativedelta(months=months_prior)

    # These next statements will make sure the day and month are correctly formatted
    current_day = '{:02d}'.format(end_date.day)
    current_month = '{:02d}'.format(end_date.month)
    current_year = end_date.year
    # Now let's put the whole thing together:
    date_formatted = str(current_year) + "." + str(current_month) + "." + str(current_day)
    return date_formatted


def flush_and_close(index, elastic):
    """
    Flushes an index and then closes it.

    :param index: The index that will be flushed and closed.
    :param elastic: The elasticsearch server.
    :return:
    """

    # First flush the index, to ensure there's no transactions left in the log
    print(log_time()+"Flushing index "+index)
    try:
        elastic.indices.flush(index=index)
    except:
        print(log_time()+"ERROR: Index is unavailable or closed!")
        return
    # Then close the index.
    print(log_time()+"Closing index "+index)
    elastic.indices.close(index=index)
    print(log_time()+"Closing complete.")


def snap_and_delete(indices, elastic, thedate, index1, index2):
    """
    Creates a snapshot of the indices passed, and then deletes them.

    :param indices: One or two indices (test and prod, etc) that will be part of the snapshot
    :param elastic: The elasticsearch server
    :param thedate: The date the snapshot will be named
    :param index1: The first index
    :param index2: The second index
    :return:
    """

    # Create the snapshot body
    index_body = {
        "indices": indices
    }
    print(log_time()+"Saving snapshot of indices " + indices)
    elastic.snapshot.create(repository="crown_backup", snapshot=thedate, body=index_body, wait_for_completion=True)
    print(log_time()+"Deleting indices " + indices)
    if index1:
        elastic.indices.delete(index1)
    if index2:
        elastic.indices.delete(index2)


if __name__ == "__main__":

    # Initialize elasticsearch variable
    try:
        es = Elasticsearch(es_url)
    except:
        print(log_time()+"Unable to connect to elasticsearch! Closing.")
        exit(1)
    # Also check to make sure the snapshot path exists
    if not os.path.exists(snapshot_dir):
        print(log_time()+"ERROR: snapshot directory: "+snapshot_dir+" is missing!")
        exit(1)

    # Calculate the date for 6 months ago
    date = calculate_date(close_delta)

    # flush and close all indices that are 6 months old
    # Start with Test
    # First check the make sure the indices in question exist
    # if it does, call flush and close
    print(log_time()+"Beginning closing of old indices.")
    current_index = "test-"+date
    if es.indices.exists(index=current_index):
        print(log_time()+"Found index: "+current_index)
        flush_and_close(current_index, es)
    else:
        print(log_time()+"Index "+current_index+" does not exist. Skipping")
    # Now try prod
    current_index = "prod-"+date
    if es.indices.exists(index=current_index):
        print(log_time()+"Found index: "+current_index)
        flush_and_close(current_index, es)
    else:
        print(log_time()+"Index "+current_index+" does not exist. Skipping")
    print(log_time()+"Completed closing.")

    # Calculate date for 12 months ago
    date = calculate_date(snapshot_delta)

    # Take a snapshot of all indices that are 12 months old
    # Delete all indices that are 12 months old after the snapshot is complete
    # First check the make sure the indices in question exist
    # if they do, call snap and delete
    # Initialize variables
    snap_indices = ""
    test_index = ""
    prod_index = ""

    print(log_time()+"Beginning snapshot of old indices.")
    # First test
    current_index = "test-"+date
    if es.indices.exists(index=current_index):
        print(log_time()+"Found index: " + current_index)
        # This variable is so we can create the snapshot body later
        snap_indices = current_index
        # This variable is so we can delete it later...
        test_index = current_index
    else:
        print(log_time()+"Index "+current_index+" does not exist. Skipping")
    # Now prod
    current_index = "prod-" + date
    if es.indices.exists(index=current_index):
        print(log_time()+"Found index: " + current_index)
        # If the snap_indices has something in it, add the current index to it.
        # If not just set it to current index.
        if snap_indices == "":
            snap_indices = current_index
        else:
            snap_indices = snap_indices+","+current_index
        prod_index = current_index
    else:
        print(log_time()+"Index " + current_index + " does not exist. Skipping")

    # Now call the snap and delete function if there is something to run it on
    if not test_index and not prod_index:
        print(log_time()+"Nothing to snapshot and delete! Skipping.")
    else:
        snap_and_delete(snap_indices, es, date, test_index, prod_index)
    print(log_time()+"Completed.")
