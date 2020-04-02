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

"""
This file contains pytest fixtures that will automatically be discovered and
injected into test functions by pytest.
"""

import boto3
from moto import mock_s3, mock_iam, mock_sqs
import os
import pytest

from ndingest.settings.settings import Settings
settings = Settings.load()
from io import BytesIO
from ndingest.ndingestproj.ingestproj import IngestProj

ProjClass = IngestProj.load()
if settings.PROJECT_NAME == 'Boss':
    nd_proj = ProjClass('testCol', 'kasthuri11', 'image', 0, 124)
else:
    nd_proj = ProjClass('kasthuri11', 'image', '0')


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'

@pytest.fixture(scope='function')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')

@pytest.fixture(scope='function')
def iam(aws_credentials):
    with mock_iam():
        yield boto3.client('iam', region_name='us-east-1')

@pytest.fixture(scope='function')
def sqs(aws_credentials):
    with mock_sqs():
        #yield boto3.client('sqs', region_name='us-east-1')
        yield boto3.resource('sqs', region_name='us-east-1')

@pytest.fixture(scope='function')
def tile_bucket(s3, iam):
    from ndingest.ndbucket.tilebucket import TileBucket
    TileBucket.createBucket()
    yield TileBucket(nd_proj.project_name)
    TileBucket.deleteBucket()
