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

import pytest
import unittest
from ndingest.util.bossutil import BossUtil, TILE_INGEST, VOLUMETRIC_INGEST
from ndingest.ndingestproj.bossingestproj import BossIngestProj
from ndingest.settings.settings import Settings
settings = Settings.load()
import warnings


@pytest.fixture(scope='function')
def boss_util_fixtures(tile_bucket, sqs):
    job_id = 123
    nd_proj = BossIngestProj('testCol', 'kasthuri11', 'image', 0, job_id)

    from ndingest.ndqueue.uploadqueue import UploadQueue
    UploadQueue.createQueue(nd_proj)
    upload_queue = UploadQueue(nd_proj)

    from ndingest.ndqueue.tileindexqueue import TileIndexQueue
    TileIndexQueue.createQueue(nd_proj)
    tile_index_queue = TileIndexQueue(nd_proj)

    def get_test_data():
        return (nd_proj, upload_queue, tile_index_queue, tile_bucket)

    yield get_test_data

    UploadQueue.deleteQueue(nd_proj)
    TileIndexQueue.deleteQueue(nd_proj)
    

class TestBossUtil():
    def _setup(self, boss_util_fixtures):
        """
        Create all member variables.  This was originally derived from
        unittest.TestCase.  Put in every test method.
        """
        test_data = boss_util_fixtures()
        self.job_id = test_data[0].job_id
        self.upload_queue = test_data[1]
        self.tile_index_queue = test_data[2]
        self.tile_bucket = test_data[3]

    def test_create_ingest_policy_tile(self, boss_util_fixtures):
        self._setup(boss_util_fixtures)
        policy = BossUtil.generate_ingest_policy(
            self.job_id, self.upload_queue, self.tile_index_queue, self.tile_bucket.bucket.name, ingest_type=TILE_INGEST)
        from ndingest.ndbucket.tilebucket import TileBucket
        try:
            assert settings.IAM_POLICY_PATH == policy.path
            assert policy.default_version is not None
            statements = policy.default_version.document['Statement']
            assert 3 == len(statements)
            for stmt in statements:
                if stmt['Sid'] == 'ClientUploadQueuePolicy':
                    for perm in ["sqs:ReceiveMessage", "sqs:GetQueueAttributes", "sqs:DeleteMessage"]:
                        assert perm in stmt['Action']
                    assert len(stmt['Action']) == 3
                    assert self.upload_queue.arn == stmt['Resource']
                elif stmt['Sid'] == 'ClientTileBucketPolicy':
                    assert "s3:PutObject" in stmt['Action']
                    assert len(stmt['Action']) == 1
                    assert TileBucket.buildArn(self.tile_bucket.bucket.name) == stmt['Resource']
                elif stmt['Sid'] == 'ClientIndexQueuePolicy':
                    assert "sqs:SendMessage" in stmt['Action']
                    assert len(stmt['Action']) == 1
                    assert self.tile_index_queue.arn == stmt['Resource']
        finally:
            policy.delete()

    def test_create_ingest_policy_volumetric(self, boss_util_fixtures):
        self._setup(boss_util_fixtures)
        policy = BossUtil.generate_ingest_policy(
            self.job_id, self.upload_queue, self.tile_index_queue, self.tile_bucket.bucket.name, ingest_type=VOLUMETRIC_INGEST)
        from ndingest.ndbucket.tilebucket import TileBucket
        try:
            assert settings.IAM_POLICY_PATH == policy.path
            assert policy.default_version is not None
            statements = policy.default_version.document['Statement']
            assert 2 == len(statements)
            for stmt in statements:
                if stmt['Sid'] == 'ClientUploadQueuePolicy':
                    for perm in ["sqs:ReceiveMessage", "sqs:GetQueueAttributes", "sqs:DeleteMessage"]:
                        assert perm in stmt['Action']
                    assert 3 == len(stmt['Action'])
                    assert self.upload_queue.arn == stmt['Resource']
                elif stmt['Sid'] == 'ClientTileBucketPolicy':
                    assert "s3:PutObject" in stmt['Action']
                    assert len(stmt['Action']) == 1
                    assert TileBucket.buildArn(self.tile_bucket.bucket.name) == stmt['Resource']
        finally:
            policy.delete()

    def test_delete_ingest_policy(self, boss_util_fixtures):
        self._setup(boss_util_fixtures)
        BossUtil.generate_ingest_policy(
            self.job_id, self.upload_queue, self.tile_index_queue, self.tile_bucket.bucket.name)
        assert BossUtil.delete_ingest_policy(self.job_id)
