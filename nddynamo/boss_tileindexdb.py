# Copyright 2014 NeuroData (http://neurodata.io)
# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
from __future__ import absolute_import
from ndingest.settings.settings import Settings
settings = Settings.load()
import botocore
import boto3
from boto3.dynamodb.conditions import Key, Attr
from operator import floordiv
from ndingest.util.bossutil import BossUtil
import time
from datetime import datetime, timedelta, timezone
from random import randrange

#try:
#    # Temp try-catch while developing on Windows.
#    from spdb.c_lib.ndlib import XYZMorton
#except Exception:
#    pass

# Expire tile entries after this many days.
DAYS_TO_LIVE = 21

# Name of global secondary index that indexes by appended_task_id.
TASK_INDEX = 'task_id_index'

# Make this more easily accessible by importers.
# Task id suffix must be >= 0 and < than this value.
MAX_TASK_ID_SUFFIX = settings.MAX_TASK_ID_SUFFIX

class BossTileIndexDB:

  def __init__(self, project_name, region_name=settings.REGION_NAME, endpoint_url=None):

    # creating the resource
    table_name = BossTileIndexDB.getTableName()
    dynamo = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

    self.table = dynamo.Table(table_name)
    self.project_name = project_name

  @staticmethod
  def generate_appended_task_id(task_id, n):
    """Generate an appended_task_id for the tile index table.
    
    Combine task_id with the given int to form a value suitable for the
    appended_task_id attribute of the tile index table.

    Args:
        task_id (int): Ingest job id.
        n (int): MAX_TASK_ID_SUFFIX > n >= 0

    Returns:
        (str)

    Raises:
        (ValueError): If n not in the proper range.
    """
    if n < 0 or n >= MAX_TASK_ID_SUFFIX:
        raise ValueError('Got n={}, must be >= 0 and < {}'.format(n, MAX_TASK_ID_SUFFIX))
    return '{}_{}'.format(task_id, n)

  @staticmethod
  def createTable(schema, region_name=settings.REGION_NAME, endpoint_url=None):
    """Create the tile index table in dynamodb.

    The table's name will be taken from settings.ini ([aws]tile_index_table).
    This method blocks until the table is created in DynamoDB.

    Args:
        schema (dict): Table's schema encoded in a dictionary.  If TableName is set, it will be overwritten by the name in settings.ini.
        region_name (optional[string]): AWS region queue lives in.  Extracted from settings.ini if not provided.
        endpoint_url (optional[string]): Provide if using a mock or fake Boto3 service.
    """

    # creating the resource
    table_name = BossTileIndexDB.getTableName()
    ttl_spec = None
    schema['TableName'] = table_name
    # TTL isn't supported in boto's create_table(), currently.
    if 'TimeToLiveSpecification' in schema:
        ttl_spec = schema['TimeToLiveSpecification']
        del schema['TimeToLiveSpecification']

    dynamo = boto3.client(
        'dynamodb', region_name=region_name, endpoint_url=endpoint_url,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

    try:
      table = dynamo.create_table(**schema)
    except Exception as e:
      print (e)
      raise

    BossTileIndexDB.wait_table_create(table_name, region_name, endpoint_url)
    if ttl_spec is not None:
        dynamo.update_time_to_live(
            TableName=table_name, TimeToLiveSpecification=ttl_spec)


  @staticmethod
  def wait_table_create(table_name, region_name=settings.REGION_NAME, endpoint_url=None):
      """Poll dynamodb at a 2s interval until the table creates."""
      client = boto3.client('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
      cnt = 0
      while True:
          time.sleep(2)
          cnt += 1
          if cnt > 50:
              # Give up waiting.
              return
          try:
              resp = client.describe_table(TableName=table_name)
              if resp['Table']['TableStatus'] == 'ACTIVE':
                  return
          except:
              # May get an exception if table doesn't currently exist.
              pass


  @staticmethod
  def deleteTable(region_name=settings.REGION_NAME, endpoint_url=None):
    """Delete the ingest database in dynamodb"""

    # creating the resource
    table_name = BossTileIndexDB.getTableName()
    dynamo = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

    try:
      table = dynamo.Table(table_name)
      table.delete()
    except Exception as e:
      print (e)
      raise

    BossTileIndexDB.wait_table_delete(table_name, region_name, endpoint_url)


  @staticmethod
  def wait_table_delete(table_name, region_name=settings.REGION_NAME, endpoint_url=None):
      """Poll dynamodb at a 2s interval until the table deletes."""
      client = boto3.client('dynamodb', region_name=region_name, endpoint_url=endpoint_url, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
      cnt = 0
      while True:
          time.sleep(2)
          cnt += 1
          if cnt > 50:
              # Give up waiting.
              return
          try:
              resp = client.describe_table(TableName=table_name)
          except:
              # Will get an exception once table is deleted.
              return

  @staticmethod
  def getTableName():
      return settings.DYNAMO_TILEINDEX_TABLE


  def createCuboidEntry(self, chunk_key, task_id):
    """Create the initial entry for tracking tiles uploaded for a cuboid.

    Call this before using markTileAsUploaded().

    The chunk_key represents the encodes the collection, experiment,
    channel/layer, and x, y, z, t indices of a cuboid.  In addition, it
    encodes the number of tiles that comprises the cuboid in the case where
    there are less tiles than the normal size of a cuboid in the z direction.

    Will raise if the chunk key already exists in the index.

    Args:
        chunk_key (string): Key used to store the entry for the cuboid.
        task_id (int): Task or job id that this cuboid belongs to.

    Raises:
        (botocore.exceptions.ClientError): ConditionalCheckFailedException if
        chunk_key already exists.
    """
    try:
        now = datetime.fromtimestamp(time.time(), timezone.utc)
        days = timedelta(days=DAYS_TO_LIVE)
        expires = int((now + days).timestamp())
        # Append random number to task_id to avoid a hot partition when
        # writing to the GSI.
        appended_task_id = BossTileIndexDB.generate_appended_task_id(
            task_id, randrange(MAX_TASK_ID_SUFFIX))
        response = self.table.put_item(
            Item = {
                'chunk_key': chunk_key,
                'tile_uploaded_map': {},
                'task_id': task_id,
                'appended_task_id': appended_task_id,
                'expires': expires
            },
            ConditionExpression=Attr('chunk_key').not_exists())
    except botocore.exceptions.ClientError as e:
        print (e)
        raise

  def markTileAsUploaded(self, chunk_key, tile_key, task_id):
    """Mark the tile as uploaded.

    Marks the tile belonging to the cuboid specified by the channel name,
    resolution, and coordinates as uploaded.  createCuboidEntry() must be
    called with the given chunk_key before tiles may be marked as uploaded.

    Args:
        chunk_key (string): Key used to store the entry for the cuboid.
        tile_key (string): Key to retrieve tile from S3 bucket.
        task_id (int): Task or job id that this cuboid belongs to.

    Returns:
        (dict): Map of uploaded tiles.
    """

    try:
      response = self.table.update_item(
          Key = {
            'chunk_key': chunk_key,
            'task_id': task_id
          },
          #UpdateExpression = 'ADD tile_uploaded_map.{} :uploaded'.format(tile_key),
          #ExpressionAttributeValues = {
          #    ':uploaded': 1
          #},
          UpdateExpression = 'ADD #tilemap.#tilekey :uploaded',
          ExpressionAttributeNames = {
              '#tilemap': 'tile_uploaded_map',
              '#tilekey': tile_key
          },
          ExpressionAttributeValues = {
              ':uploaded': 1
          },
          ReturnValues = 'ALL_NEW'
      )
      return self.cuboidReady(chunk_key, response['Attributes']['tile_uploaded_map'])
    except botocore.exceptions.ClientError as e:
      print (e)
      raise


  def cuboidReady(self, chunk_key, tile_uploaded_map):
    """Verify if we have all tiles for a given cuboid.

    Args:
        chunk_key (string): Key used to store the entry for the cuboid.
        tile_uploaded_map (dict): Dictionary with tile keys as the keys.  Presence of a tile indicates it's been uploaded.

    Returns:
        (bool)
    """

    key_parts = BossUtil.decode_chunk_key(chunk_key)
    num_tiles = key_parts['num_tiles']

    if num_tiles < settings.SUPER_CUBOID_SIZE[2]:
        return len(tile_uploaded_map) >= num_tiles

    return len(tile_uploaded_map) >= settings.SUPER_CUBOID_SIZE[2]


  def getCuboid(self, chunk_key, task_id):
    """Get the cuboid entry from the DynamoDB table.

    Args:
        chunk_key (string): Key used to store the entry for the cuboid.
        task_id (int): Id of upload task/job.

    Returns:
        (dict|None): Keys include 'tile_uploaded_map' and 'chunk_key'.
    """

    try:
      response = self.table.get_item(
          Key = {
            'chunk_key': chunk_key,
            'task_id': task_id
          },
          ConsistentRead = True,
          ReturnConsumedCapacity = 'INDEXES'
      )
      return response['Item'] if 'Item' in response else None
    except Exception as e:
      print (e)
      raise


  def getTaskItems(self, task_id, limit=200):
      """Get all the cuboid entries for a given task from the table.

      Args:
          task_id (int): Id of upload task/job.
          limit (optional[int]): Max number of items to read in a single query.

      Returns:
          (generator): Dictionary with keys: 'chunk_key', 'task_id', 'tile_uploaded_map'.
      """

      try:
          for i in range(MAX_TASK_ID_SUFFIX):
              appended_task_id = BossTileIndexDB.generate_appended_task_id(task_id, i)
              exclusive_start_key = None
              while True:
                  response = self._query(appended_task_id, exclusive_start_key, limit)
                  for item in response['Items']:
                      yield item
                  if 'LastEvaluatedKey' not in response:
                      break
                  exclusive_start_key = response['LastEvaluatedKey']
      except Exception as e:
          print(e)
          raise


  def _query(self, appended_task_id, exclusive_start_key, limit):
        """Query the GSI for chunks associated with the given task id.

        Args:
            appended_task_id (str): Id of upload task/job with a sequence number appended.
            exclusive_start_key (dict|None): Where to start the next query.
          limit (int): Max number of items to read in a single query.

        Returns:
            (dict): Response dictionary.
        """
        args = {
            'IndexName': TASK_INDEX,
            'KeyConditionExpression': 'appended_task_id = :task_id',
            'ExpressionAttributeValues': { ':task_id': appended_task_id },
            'Limit': limit
        }
        if exclusive_start_key is not None:
            args['ExclusiveStartKey'] = exclusive_start_key
        return self.table.query(**args)


  def deleteCuboid(self, chunk_key, task_id):
    """Delete cuboid from database.

    Args:
        chunk_key (string): Key used to store the entry for the cuboid.
        task_id (int): Id of upload task/job.
    """

    try:
      response = self.table.delete_item(
          Key = {
            'chunk_key': chunk_key,
            'task_id': task_id
          }
      )
      return response
    except botocore.exceptions.ClientError as e:
      print (e)
      raise
