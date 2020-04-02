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

import hashlib
import json
from ndingest.settings.settings import Settings
settings = Settings.load()
import pytest
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
    """Testing the upload queue"""
    
    proj = generate_proj()

    from ndingest.ndqueue.cleanupqueue import CleanupQueue
    CleanupQueue.createQueue(proj)
    cleanup_queue = CleanupQueue(proj)

    supercuboid_key = 'kasthuri11&image&0&0'
    cleanup_queue.sendMessage(supercuboid_key)
    for message_id, receipt_handle, message_body in cleanup_queue.receiveMessage():
      assert(supercuboid_key == message_body)
      response = cleanup_queue.deleteMessage(message_id, receipt_handle)
      assert('Successful' in response)


def test_sendBatchMessages(sqs):
    fake_data0 = {'foo': 'bar'}
    fake_data1 = {'john': 'doe'}
    jsonized0 = json.dumps(fake_data0)
    jsonized1 = json.dumps(fake_data1)
    md5_0 = hashlib.md5(jsonized0.encode('utf-8')).hexdigest()
    md5_1 = hashlib.md5(jsonized1.encode('utf-8')).hexdigest()

    proj = generate_proj()

    from ndingest.ndqueue.cleanupqueue import CleanupQueue
    CleanupQueue.createQueue(proj)
    cleanup_queue = CleanupQueue(proj)

    try:
        response = cleanup_queue.sendBatchMessages([fake_data0, fake_data1], 0)
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
        for message_id, receipt_handle, _ in cleanup_queue.receiveMessage():
            cleanup_queue.deleteMessage(message_id, receipt_handle)


if __name__ == '__main__':
    pytest.main()
