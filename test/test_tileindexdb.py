# Copyright 2014 NeuroData (http://neurodata.io)
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

import sys
import os
sys.path += [os.path.abspath('../../django')]
import ND.settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'ND.settings'
from django.conf import settings
from nddynamo.tileindexdb import TileIndexDB

project_name = 'kasthuri11'
channel_name = 'image'
resolution = 0

class Test_TileIndexDB():

  def setup_class(self):
    """Setup parameters"""
    TileIndexDB.createTable(endpoint_url='http://localhost:8000')
    self.tileindex_db = TileIndexDB(project_name, endpoint_url='http://localhost:8000')
    
  def teardown_class(self):
    """Teardown parameters"""
    TileIndexDB.deleteTable(endpoint_url='http://localhost:8000')
    
  def test_putItem(self):
    """Test data insertion"""
    
    x_tile = 0
    y_tile = 0
    # inserting three values for task 0
    for z_tile in range(0, 3, 1):
      self.tileindex_db.putItem(channel_name, resolution, x_tile, y_tile, z_tile, task_id=0)
    
    # inserting 2 values for task 1
    for z_tile in range(66, 68, 1):
      self.tileindex_db.putItem(channel_name, resolution, x_tile, y_tile, z_tile, task_id=1)

    # checking if the items were inserted
    z_tile = 0
    supercuboid_key = self.tileindex_db.generatePrimaryKey(channel_name, resolution, x_tile, y_tile, z_tile)
    item_value = self.tileindex_db.getItem(supercuboid_key)
    assert( item_value['zindex_list'] == set([0, 1, 2]) )
    
    z_tile = 65
    supercuboid_key = self.tileindex_db.generatePrimaryKey(channel_name, resolution, x_tile, y_tile, z_tile)
    item_value = self.tileindex_db.getItem(supercuboid_key)
    assert( item_value['zindex_list'] == set([66, 67]) )
  
  def test_queryTaskItems(self):
    """Test the query over SI"""
    
    for item in self.tileindex_db.getTaskItems(0):
      assert( item['zindex_list'] == set([0, 1, 2]) )

  def test_supercuboidReady(self):
    """Test if the supercuboid is ready"""
    
    x_tile = 0
    y_tile = 0
    for z_tile in range(129, 129+settings.SUPER_CUBOID_SIZE[2], 1):
      supercuboid_key, supercuboid_ready = self.tileindex_db.putItem(channel_name, resolution, x_tile, y_tile, z_tile, task_id=0)
      if z_tile < 129+settings.SUPER_CUBOID_SIZE[2]:
        assert(supercuboid_ready is False)
      else:
        assert(supercuboid_ready is True)


  def test_deleteItem(self):
    """Test item deletion"""
    
    x_tile = 0
    y_tile = 0
    # inserting three values for task 0
    for z_tile in range(0, 3, 1):
      supercuboid_key = self.tileindex_db.generatePrimaryKey(channel_name, resolution, x_tile, y_tile, z_tile)
      self.tileindex_db.deleteItem(supercuboid_key)
    
    # inserting 2 values for task 1
    for z_tile in range(66, 68, 1):
      supercuboid_key = self.tileindex_db.generatePrimaryKey(channel_name, resolution, x_tile, y_tile, z_tile)
      self.tileindex_db.deleteItem(supercuboid_key)
    
    # inserting three values for task 0
    for z_tile in range(0, 3, 1):
      supercuboid_key = self.tileindex_db.generatePrimaryKey(channel_name, resolution, x_tile, y_tile, z_tile)
      item = self.tileindex_db.getItem(supercuboid_key)
      assert(item == None)
    
    # inserting 2 values for task 1
    for z_tile in range(66, 68, 1):
      supercuboid_key = self.tileindex_db.generatePrimaryKey(channel_name, resolution, x_tile, y_tile, z_tile)
      item = self.tileindex_db.getItem(supercuboid_key)
      assert(item == None)
