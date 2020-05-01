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
import json
from random import randint
import pytest
from ndingest.settings.settings import Settings
settings = Settings.load()
from ndingest.ndingestproj.ingestproj import IngestProj
ProjClass = IngestProj.load()

def generate_proj():
    """Add some randomness to the project.
   
    Queue names must be different between tests because a deleted queue
    cannot be recreated with the same name until 60s has elapsed.
    """

    num = 100

    if settings.PROJECT_NAME == 'Boss':
        job_id = num
        nd_proj = ProjClass('testCol', 'kasthuri11', 'image', 0, job_id)
    else:
        channel = 'image{}'.format(num)
        nd_proj = ProjClass('kasthuri11', channel, '0')

    return nd_proj

def test_create_queue_with_default_name(sqs):
    from ndingest.ndqueue.uploadqueue import UploadQueue

    proj = generate_proj()

    # Create upload queue.
    UploadQueue.createQueue(proj)
    upload_queue = UploadQueue(proj)

    # Create dead letter queue with default name.
    exp_max_receives = 4
    dl_queue = upload_queue.addDeadLetterQueue(exp_max_receives)

    exp_name = upload_queue.queue_name + '-dlq'
    exp_arn = dl_queue.attributes['QueueArn']

    try:
        policy = json.loads(
            upload_queue.queue.attributes['RedrivePolicy'])
        assert exp_max_receives == policy['maxReceiveCount']
        assert exp_arn == policy['deadLetterTargetArn']
        # Confirm dead letter queue named correctly by looking at the end 
        # of its ARN.
        assert dl_queue.attributes['QueueArn'].endswith(exp_name)
    finally:
        dl_queue.delete()

def test_add_existing_queue_as_dead_letter_queue(sqs):
    from ndingest.ndqueue.uploadqueue import UploadQueue

    proj = generate_proj()

    # Create existing queue for dead letter queue.
    queue_name = 'deadletter_test_{}'.format(randint(1000, 9999))
    existing_queue = sqs.create_queue(
        QueueName=queue_name,
        Attributes={
            'DelaySeconds': '0',
            'MaximumMessageSize': '262144'
        }
    )

    exp_arn = existing_queue.attributes['QueueArn']

    try:
        # Create upload queue.
        UploadQueue.createQueue(proj)
        upload_queue = UploadQueue(proj)

        # Attach the dead letter queue to it.
        exp_max_receives = 5
        dl_queue = upload_queue.addDeadLetterQueue(
            exp_max_receives, exp_arn)

        # Confirm policy settings.
        policy = json.loads(
            upload_queue.queue.attributes['RedrivePolicy'])
        assert exp_max_receives == policy['maxReceiveCount']
        assert exp_arn == policy['deadLetterTargetArn']

        # Confirm dead letter queue is the one created at the beginning 
        # of test.
        assert existing_queue.url == dl_queue.url
    finally:
        existing_queue.delete()

def test_delete_dead_letter_queue(sqs):
    from ndingest.ndqueue.uploadqueue import NDQueue
    from ndingest.ndqueue.uploadqueue import UploadQueue

    proj = generate_proj()

    # Create existing queue for dead letter queue.
    queue_name = 'deadletter_test_{}'.format(randint(1000, 9999))
    existing_queue = sqs.create_queue(
        QueueName=queue_name,
        Attributes={
            'DelaySeconds': '0',
            'MaximumMessageSize': '262144'
        }
    )

    # Create upload queue. 
    arn = existing_queue.attributes['QueueArn']
    UploadQueue.createQueue(proj)
    upload_queue = UploadQueue(proj)

    # Attach the dead letter queue to it.
    dl_queue = upload_queue.addDeadLetterQueue(2, arn)

    # Invoke the delete method.
    NDQueue.deleteDeadLetterQueue(sqs, upload_queue.queue)

    # Confirm deletion.
    with pytest.raises(botocore.exceptions.ClientError):
        sqs.get_queue_by_name(QueueName=queue_name)
