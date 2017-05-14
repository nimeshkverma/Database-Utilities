import datetime
from pymongo import MongoClient, ReadPreference
from bson.objectid import ObjectId
from bson.code import Code
from pymongo.errors import BulkWriteError


def get_mongo_cursor(db_name, collection_name):
    """
        Aim: Connects to provided mongodb' collection
        Input:
                db_name: database_name, type:string
                collection_name: collection_name, type:string
        Output:
                conn: database connection, type psycopg2 connection object
    """
    try:
        client = MongoClient(
            'mongodb://dsl_read:dsl@127.0.0.1:3338/' + db_name)
        db = client[db_name]
        collection = db[collection_name]
        return collection
    except Exception as e:

        msg = "Mongo Connection could not be established for collection {col}, Error: {error}".format(
            col=collection_name, error=str(e))
        raise Exception(msg)


def bulk_cursor_execute(bulk_cursor):
    """
        Executes the bulk_cursor and handles exception

        :param bulk_cursor: The cursor to perform bulk operations
        :type bulk_cursor: pymongo bulk cursor object

        :returns: cursor to perform bulk operations, pymongo bulk cursor object
    """
    try:
        result = bulk_cursor.execute()
    except BulkWriteError as bwe:
        msg = "bulk_cursor_execute: Exception in executing Bulk cursor to mongo with {error}".format(
            error=str(bwe))
        raise Exception(msg)


def get_bulk_cursor(collection_name):
    """
        Returns the Bulk operation(unordered) cursor using the configuration stored in the config file
    """
    try:
        cursor = get_mongo_cursor(collection_name)
        return cursor.initialize_unordered_bulk_op()
    except Exception as e:
        msg = "get_bulk_cursor: Mongo Bulk cursor could not be fetched for collection {col}, Error: {error}".format(
            col=collection_name, error=str(e))
        raise Exception(msg)


def object_id_day_range(start_time, end_time):
    """
        Aim: Converts time to Mongo's ObjectId
        Input:
                start_time: The starting time, type: datetime.datetime object
                end_time: The end time, type: datetime.datetime object
        Output:
                start_object_id: The starting ObjectId, pymongo ObjectId object
                end_object_id: The ending ObjectId, pymongo ObjectId object
    """

    start_object_id = ObjectId.from_datetime(start_time)
    end_object_id = ObjectId.from_datetime(end_time)
    return start_object_id, end_object_id
