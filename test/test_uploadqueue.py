# Copyright 2014 NeuroData (http://neurodata.io)
# Copyright 2020 The Johns Hopkins University Applied Physics Laboratory
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

import botocore
import hashlib
import json
from random import randint
from ndingest.settings.settings import Settings
settings = Settings.load()
from ndingest.ndqueue.serializer import Serializer
serializer = Serializer.load()
from ndingest.ndingestproj.ingestproj import IngestProj
ProjClass = IngestProj.load()

def generate_proj():
    """Generate project name based on Boss or Neurodata."""

    num = 100

    if settings.PROJECT_NAME == 'Boss':
        job_id = num
        nd_proj = ProjClass('testCol', 'kasthuri11', 'image', 0, job_id)
    else:
        channel = 'image{}'.format(num)
        nd_proj = ProjClass('kasthuri11', channel, '0')

    return nd_proj

def test_message(sqs):
    """Test put, get and delete Message"""

    x_tile = 0
    y_tile = 0

    proj = generate_proj()

    from ndingest.ndqueue.uploadqueue import UploadQueue
    UploadQueue.createQueue(proj)
    upload_queue = UploadQueue(proj)

    for z_tile in range(0, 2, 1):
        # encode the message
        message = serializer.encodeUploadMessage(proj.project_name, proj.channel_name, proj.resolution, x_tile, y_tile, z_tile)
        # send message to the queue
        upload_queue.sendMessage(message)

    # receive message from the queue
    for message_id, receipt_handle, message_body in upload_queue.receiveMessage(number_of_messages=3):
        # check if we get the tile_info back correctly
        assert(message_body['z_tile'] in [0, 1, 2])
        # delete message from the queue
        response = upload_queue.deleteMessage(message_id, receipt_handle)
        # check if the message was sucessfully deleted
        assert('Successful' in response)


def test_sendBatchMessages(sqs):
    fake_data0 = {'foo': 'bar'}
    fake_data1 = {'john': 'doe'}
    jsonized0 = json.dumps(fake_data0)
    jsonized1 = json.dumps(fake_data1)
    md5_0 = hashlib.md5(jsonized0.encode('utf-8')).hexdigest()
    md5_1 = hashlib.md5(jsonized1.encode('utf-8')).hexdigest()

    proj = generate_proj()

    from ndingest.ndqueue.uploadqueue import UploadQueue
    UploadQueue.createQueue(proj)
    upload_queue = UploadQueue(proj)

    try:
        response = upload_queue.sendBatchMessages([jsonized0, jsonized1], 0)
        assert('Successful' in response)
        success_ids = []
        for msg_result in response['Successful']:
            id = msg_result['Id']
            success_ids.append(id)
            if id == '0':
                assert(md5_0 == msg_result['MD5OfMessageBody'])
            elif id == '1':
                assert(md5_1 == msg_result['MD5OfMessageBody'])

        assert('0' in success_ids)
        assert('1' in success_ids)
    finally:
        for message_id, receipt_handle, _ in upload_queue.receiveMessage():
            upload_queue.deleteMessage(message_id, receipt_handle)

def test_createPolicy(sqs, iam):
    """Test policy creation"""

    proj = generate_proj()

    from ndingest.ndqueue.uploadqueue import UploadQueue
    UploadQueue.createQueue(proj)
    upload_queue = UploadQueue(proj)

    statements = [{
        'Sid': 'ReceiveAccessStatement',
        'Effect': 'Allow',
        'Action': ['sqs:ReceiveMessage'] 
    }]

    expName = upload_queue.generateQueueName(proj)
    expDesc = 'Test policy creation'

    actual = upload_queue.createPolicy(statements, description=expDesc)

    try:
        assert(expName == actual.policy_name)
        assert(expDesc == actual.description)
        assert(settings.IAM_POLICY_PATH == actual.path)

        # Confirm resource set correctly to the upload queue.
        statements = actual.default_version.document['Statement']
        arn = upload_queue.queue.attributes['QueueArn']
        for stmt in statements:
            assert(stmt['Resource'] == arn)

    finally:
        actual.delete()
