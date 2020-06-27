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
import sys
sys.path.append('..')
from ndingest.settings.settings import Settings
settings = Settings.load()
from ndingest.nddynamo.boss_tileindexdb import BossTileIndexDB, TILE_UPLOADED_MAP_KEY
from ndingest.ndingestproj.bossingestproj import BossIngestProj
job_id = '123'
nd_proj = BossIngestProj('testCol', 'kasthuri11', 'image', 0, job_id)
import json
import six
import unittest
import warnings
import time
import botocore



class Test_BossTileIndexDB(unittest.TestCase):
    """
    Note that the chunk keys used, for testing, do not have real hash keys.
    The rest of the chunk key is valid.
    """

    def setUp(self):
        # Suppress ResourceWarning messages about unclosed connections.
        warnings.simplefilter('ignore')

        with open('nddynamo/schemas/boss_tile_index.json') as fp:
            schema = json.load(fp)

        BossTileIndexDB.createTable(schema, endpoint_url=settings.DYNAMO_TEST_ENDPOINT)
        
        self.tileindex_db = BossTileIndexDB(
            nd_proj.project_name, endpoint_url=settings.DYNAMO_TEST_ENDPOINT)
        

    def tearDown(self):
        BossTileIndexDB.deleteTable(endpoint_url=settings.DYNAMO_TEST_ENDPOINT)


    def test_cuboidReady_false(self):
        fake_map = { 'o': 1 }
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        self.assertFalse(self.tileindex_db.cuboidReady(chunk_key, fake_map))


    def test_cuboidReady_true(self):
        fake_map = { 
            's1': 1, 's2': 1, 's3': 1, 's4': 1, 's5': 1, 's6': 1, 's7': 1, 's8': 1,
            's9': 1, 's10': 1, 's11': 1, 's12': 1, 's13': 1, 's14': 1, 's15': 1, 's16': 1
        }
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        self.assertTrue(self.tileindex_db.cuboidReady(chunk_key, fake_map))


    def test_cuboidReady_small_cuboid_true(self):
        """Test case where the number of tiles is smaller than a cuboid in the z direction."""
        fake_map = { 
            's1': 1, 's2': 1, 's3': 1, 's4': 1, 's5': 1, 's6': 1, 's7': 1, 's8': 1
        }

        num_tiles = 8
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        self.assertTrue(self.tileindex_db.cuboidReady(chunk_key, fake_map))


    def test_cuboidReady_small_cuboid_false(self):
        """Test case where the number of tiles is smaller than a cuboid in the z direction."""
        fake_map = { 
            's1': 1, 's2': 1, 's3': 1, 's4': 1, 's5': 1, 's6': 1, 's7': 1
        }

        num_tiles = 8
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        self.assertFalse(self.tileindex_db.cuboidReady(chunk_key, fake_map))

    def test_createCuboidEntry(self):
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        task_id = 21
        self.tileindex_db.createCuboidEntry(chunk_key, task_id)
        actual = self.tileindex_db.getCuboid(chunk_key, task_id)
        self.assertEqual(chunk_key, actual['chunk_key'])
        self.assertEqual({}, actual[TILE_UPLOADED_MAP_KEY])
        self.assertIn('expires', actual)
        self.assertEqual(task_id, actual['task_id'])
        self.assertTrue(actual['appended_task_id'].startswith('{}_'.format(task_id)))


    def test_markTileAsUploaded(self):
        # Cuboid must first have an entry before one of its tiles may be marked
        # as uploaded.
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        task_id = 231
        self.tileindex_db.createCuboidEntry(chunk_key, task_id)

        self.tileindex_db.markTileAsUploaded(chunk_key, 'fakekey&sss', task_id)

        expected = { 'fakekey&sss': 1 }
        resp = self.tileindex_db.getCuboid(chunk_key, task_id)
        self.assertEqual(expected, resp[TILE_UPLOADED_MAP_KEY])


    def test_markTileAsUploaded_multiple(self):
        # Cuboid must first have an entry before one of its tiles may be marked
        # as uploaded.
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        task_id = 231
        self.tileindex_db.createCuboidEntry(chunk_key, task_id)

        self.tileindex_db.markTileAsUploaded(chunk_key, 'fakekey&sss', task_id)

        expected_first = { 'fakekey&sss': 1 }
        resp = self.tileindex_db.getCuboid(chunk_key, task_id)
        self.assertEqual(expected_first, resp[TILE_UPLOADED_MAP_KEY])

        expected_second = { 'fakekey&sss': 1, 'fakekey&ttt': 1 }
        self.tileindex_db.markTileAsUploaded(chunk_key, 'fakekey&ttt', task_id)
        resp = self.tileindex_db.getCuboid(chunk_key, task_id)
        self.assertCountEqual(expected_second, resp[TILE_UPLOADED_MAP_KEY])


    def test_deleteItem(self):
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        chunk_key = '<hash>&{}&111&222&333&0&0&0&0&0'.format(num_tiles)
        task_id = 231
        self.tileindex_db.createCuboidEntry(chunk_key, task_id)
        preDelResp = self.tileindex_db.getCuboid(chunk_key, task_id)
        self.assertEqual(chunk_key, preDelResp['chunk_key'])
        self.tileindex_db.deleteCuboid(chunk_key, task_id)
        postDelResp = self.tileindex_db.getCuboid(chunk_key, task_id)
        self.assertIsNone(postDelResp)


    def test_getTaskItems(self):
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        chunk_key1 = '<hash>&{}&111&222&333&0&0&0&z&t'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key1, task_id=3)

        chunk_key2 = '<hash>&{}&111&222&333&0&1&0&z&t'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key2, task_id=3)

        chunk_key3 = '<hash>&{}&111&222&333&0&2&0&z&t'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key3, task_id=3)

        # Cuboid for a different upload job.
        chunk_key4 = '<hash>&{}&555&666&777&0&0&0&z&t'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key4, task_id=5)

        expected = [ 
            {'task_id': 3, TILE_UPLOADED_MAP_KEY: {}, 'chunk_key': chunk_key1},
            {'task_id': 3, TILE_UPLOADED_MAP_KEY: {}, 'chunk_key': chunk_key2},
            {'task_id': 3, TILE_UPLOADED_MAP_KEY: {}, 'chunk_key': chunk_key3}
        ]

        actual = list(self.tileindex_db.getTaskItems(3))
        filtered = [
            {
                'task_id': i['task_id'],
                TILE_UPLOADED_MAP_KEY: i[TILE_UPLOADED_MAP_KEY],
                'chunk_key': i['chunk_key']
            } for i in actual]

        six.assertCountEqual(self, expected, filtered)


    def test_getTaskItems_force_multiple_queries(self):
        num_tiles = settings.SUPER_CUBOID_SIZE[2]
        job=3
        chunk_key1 = '<hash>&{}&111&222&333&0&0&0&z&t'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key1, task_id=job)

        chunk_key2 = '<hash>&{}&111&222&333&0&1&0&z&t'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key2, task_id=job)

        chunk_key3 = '<hash>&{}&111&222&333&0&2&0&z&t'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key3, task_id=job)

        # Cuboid for a different upload job.
        chunk_key4 = '<hash>&{}&555&666&777&0&0&0&z&t'.format(num_tiles)
        self.tileindex_db.createCuboidEntry(chunk_key4, task_id=5)

        expected = [ 
            {'task_id': job, TILE_UPLOADED_MAP_KEY: {}, 'chunk_key': chunk_key1},
            {'task_id': job, TILE_UPLOADED_MAP_KEY: {}, 'chunk_key': chunk_key2},
            {'task_id': job, TILE_UPLOADED_MAP_KEY: {}, 'chunk_key': chunk_key3}
        ]

        # Limit only 1 read per query so multiple queries required.
        query_limit = 1
        actual = list(self.tileindex_db.getTaskItems(job, query_limit))
        filtered = [
            {
                'task_id': i['task_id'],
                TILE_UPLOADED_MAP_KEY: i[TILE_UPLOADED_MAP_KEY],
                'chunk_key': i['chunk_key']
            } for i in actual]

        six.assertCountEqual(self, expected, filtered)


    def test_createCuboidAlreadyExistsRaises(self):
        """Raise an error if the chunk key already exists in the index."""
        chunk_key = 'foo'
        task_id = 9999999
        self.tileindex_db.createCuboidEntry(chunk_key, task_id)

        with self.assertRaises(botocore.exceptions.ClientError) as err:
            self.tileindex_db.createCuboidEntry(chunk_key, task_id)
            error_code = err.response['Error'].get('Code', 'Unknown')
            self.assertEqual('ConditionalCheckFailedException', error_code)


if __name__ == '__main__':
    unittest.main()
