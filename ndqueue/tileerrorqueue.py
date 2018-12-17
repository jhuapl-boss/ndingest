# Copyright 2014 NeuroData (http://neurodata.io)
# Copyright 2018 The Johns Hopkins University Applied Physics Laboratory
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

from __future__ import absolute_import
from __future__ import print_function
import boto3
import botocore
import random
from ndingest.settings.settings import Settings
from ndingest.ndqueue.ndqueue import NDQueue
settings = Settings.load()

"""
This class manages the tile errors queue. Messages will be put in this queue for each unreadable tile uploaded during 
and ingest job.  The messages can then be queried form the queue after the job is over so the uploader what tiles failed
"""


class TileErrorQueue(NDQueue):
    # Static variable to hold random number added to test queue names.
    test_queue_id = -1

    def __init__(self, nd_proj, region_name=settings.REGION_NAME, endpoint_url=None):
        """Create resources for the queue"""

        queue_name = TileErrorQueue.generateQueueName(nd_proj)
        super(TileErrorQueue, self).__init__(queue_name, region_name=region_name, endpoint_url=endpoint_url)

    @staticmethod
    def generateNeurodataQueueName(nd_proj):
        return '-'.join(nd_proj.generateProjectInfo() + ['INSERT']).replace('&', '-')

    @staticmethod
    def generateBossQueueName(nd_proj):
        if not settings.TEST_MODE and not NDQueue.test_mode:
            return '{}-ingest-{}'.format(settings.DOMAIN, nd_proj.job_id)

        if TileErrorQueue.test_queue_id == -1:
            TileErrorQueue.test_queue_id = random.randint(0, 999)

        return 'test{}-{}-ingest-{}'.format(TileErrorQueue.test_queue_id, settings.DOMAIN, nd_proj.job_id)

    @staticmethod
    def createQueue(nd_proj, region_name=settings.REGION_NAME, endpoint_url=None):
        """Create the upload queue"""

        # creating the resource
        queue_name = TileErrorQueue.generateQueueName(nd_proj)
        sqs = boto3.resource('sqs', region_name=region_name, endpoint_url=endpoint_url,
                             aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

        try:
            # creating the queue, if the queue already exists catch exception
            response = sqs.create_queue(
                QueueName=queue_name,
                Attributes={
                    'VisibilityTimeout': '120',
                    'DelaySeconds': '0',
                    'MaximumMessageSize': '262144',
                    'MessageRetentionPeriod': '1209600'  # 14 days.
                }
            )
            return queue_name
        except Exception as e:
            print(e)
            raise

    @staticmethod
    def deleteQueue(nd_proj, region_name=settings.REGION_NAME, endpoint_url=None, delete_deadletter_queue=False):
        """Delete the tile error queue.

        Also delete the dead letter queue if delete_deadletter_queue is true.

        Args:
            nd_proj (IngestProj): Project settings used to generate queue's name.
            region_name (optional[string]): AWS region queue lives in.  Extracted from settings.ini if not provided.
            endpoint_url (optional[string]): Provide if using a mock or fake Boto3 service.
            delete_deadletter_queue (optional[bool]): Also delete the dead letter queue.  Defaults to False.
        """

        # creating the resource
        queue_name = TileErrorQueue.generateQueueName(nd_proj)
        NDQueue.deleteQueueByName(queue_name, region_name, endpoint_url, delete_deadletter_queue)

    @staticmethod
    def generateQueueName(nd_proj):
        """Generate the queue name based on project information"""
        return TileErrorQueue.getNameGenerator()(nd_proj)

    def sendMessage(self, supercuboid_key):
        """Send a message to upload queue"""
        return super(TileErrorQueue, self).sendMessage(supercuboid_key)

    def sendBatchMessages(self, supercuboid_keys, delay_seconds=0):
        """Send up to 10 messages at once to the ingest queue.

        Returned dict contains two keys: 'Successful' and 'Failed'.  Each key is
        an array dicts with the common key: 'Id'.  The value associated with 'Id'
        is the index into the original list of messages passed in.  Use this to
        determine which messages were successfully enqueued vs failed.

        Args:
            supercuboid_keys (list): List of up to 10 message bodies.
            delay_seconds (optional[int]): Optional delay for processing of messages.

        Returns:
            (dict): Contains keys 'Successful' and 'Failed'.
        """
        return super(TileErrorQueue, self).sendBatchMessages(supercuboid_keys, delay_seconds)

    def receiveMessage(self, number_of_messages=1):
        """Receive a message from the ingest queue"""

        message_list = super(TileErrorQueue, self).receiveMessage(number_of_messages=number_of_messages)
        if message_list is None:
            raise StopIteration
        else:
            for message in message_list:
                yield message.message_id, message.receipt_handle, message.body

    def deleteMessage(self, message_id, receipt_handle, number_of_messages=1):
        """Delete a message from the ingest queue"""
        return super(TileErrorQueue, self).deleteMessage(message_id, receipt_handle, number_of_messages=1)
