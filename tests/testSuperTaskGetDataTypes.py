#!/usr/bin/env python
"""Unit test for SuperTask.get_data_types() method.
"""
#
# LSST Data Management System
# Copyright 2016 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#
import unittest

import lsst.utils.tests
import lsst.pex.config as pexConfig
from lsst.pipe import supertask

class LeafTaskBroken(supertask.SuperTask):
    """Does not implement get_data_types(), will throw exception"""
    _DefaultName = "leaf_task_broken"
    ConfigClass = pexConfig.Config

class LeafTaskIsr(supertask.SuperTask):
    _DefaultName = "leaf_task_isr"
    ConfigClass = pexConfig.Config

    def get_data_types(self, intermediates=False):
        return (['raw', 'raw_too'], ['isr'])

class LeafTaskCal(supertask.SuperTask):
    _DefaultName = "leaf_task_cal"
    ConfigClass = pexConfig.Config

    def get_data_types(self, intermediates=False):
        return (['isr', 'some_db'], ['calexp', 'calexp_db_extra'])

class IsrCalexpConfig(pexConfig.Config):
    isr = LeafTaskIsr.makeField("leaf task 1")
    cal = LeafTaskCal.makeField("leaf task 2")

class IsrCalexpTask(supertask.SuperTask):
    _DefaultName = "parent_subtask"
    ConfigClass = IsrCalexpConfig

    def __init__(self, **keyArgs):
        super(IsrCalexpTask, self).__init__(**keyArgs)
        self.makeSubtask("isr")
        self.makeSubtask("cal")


class GetDataTypesTestCase(unittest.TestCase):
    """A test case for SuperTask.get_data_types method
    """

    def testBrokenLeaf(self):
        """Testing that get_sub_tasks() without options returns all tasks
        """
        pipeline = LeafTaskBroken()
        with self.assertRaises(NotImplementedError):
            pipeline.get_data_types()
        with self.assertRaises(NotImplementedError):
            pipeline.get_data_types(True)

    def testIsr(self):
        """Testing that get_sub_tasks() without options returns all tasks
        """
        pipeline = LeafTaskIsr()
        input, output = pipeline.get_data_types()
        self.assertEqual(set(input), set(['raw', 'raw_too']))
        self.assertEqual(set(output), set(['isr']))
        input, output = pipeline.get_data_types(True)
        self.assertEqual(set(input), set(['raw', 'raw_too']))
        self.assertEqual(set(output), set(['isr']))

    def testCal(self):
        """Testing that get_sub_tasks() without options returns all tasks
        """
        pipeline = LeafTaskCal()
        input, output = pipeline.get_data_types()
        self.assertEqual(set(input), set(['isr', 'some_db']))
        self.assertEqual(set(output), set(['calexp', 'calexp_db_extra']))
        input, output = pipeline.get_data_types(True)
        self.assertEqual(set(input), set(['isr', 'some_db']))
        self.assertEqual(set(output), set(['calexp', 'calexp_db_extra']))

    def testIsrCal(self):
        """Testing that get_sub_tasks() without options returns all tasks
        """
        pipeline = IsrCalexpTask()
        input, output = pipeline.get_data_types()
        self.assertEqual(set(input), set(['raw', 'raw_too', 'some_db']))
        self.assertEqual(set(output), set(['isr', 'calexp', 'calexp_db_extra']))
        input, output = pipeline.get_data_types(True)
        self.assertEqual(set(input), set(['raw', 'raw_too', 'some_db', 'isr']))
        self.assertEqual(set(output), set(['isr', 'calexp', 'calexp_db_extra']))


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
