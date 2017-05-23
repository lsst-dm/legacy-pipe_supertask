"""Unit test for SuperTask.get_sub_tasks() method.
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


class LeafTask(supertask.SuperTask):
    _DefaultName = "leaf_task"
    ConfigClass = pexConfig.Config

class ParentSubConfig(pexConfig.Config):
    leaf1 = LeafTask.makeField("leaf task 1")
    leaf2 = LeafTask.makeField("leaf task 2")

class ParentSubTask(supertask.SuperTask):
    _DefaultName = "parent_subtask"
    ConfigClass = ParentSubConfig

    def __init__(self, **keyArgs):
        super(ParentSubTask, self).__init__(**keyArgs)
        self.makeSubtask("leaf1")
        self.makeSubtask("leaf2")

class ParentConfig(pexConfig.Config):
    subtask1 = ParentSubTask.makeField("sub task 1")
    subtask2 = ParentSubTask.makeField("sub task 2")

class ParentTask(supertask.SuperTask):
    _DefaultName = "parent_task"
    ConfigClass = ParentConfig

    def __init__(self, **keyArgs):
        super(ParentTask, self).__init__(**keyArgs)
        self.makeSubtask("subtask1")
        self.makeSubtask("subtask2")

class GetSubTasksTestCase(unittest.TestCase):
    """A test case for SuperTask.get_sub_tasks method
    """

    all_names = ['parent_task',
                 'parent_task.subtask1',
                 'parent_task.subtask2',
                 'parent_task.subtask1.leaf1',
                 'parent_task.subtask1.leaf2',
                 'parent_task.subtask2.leaf1',
                 'parent_task.subtask2.leaf2']
    all_leaves = ['parent_task.subtask1.leaf1',
                  'parent_task.subtask1.leaf2',
                  'parent_task.subtask2.leaf1',
                  'parent_task.subtask2.leaf2']

    def _check_names(self, tasks, names):
        task_names = [task.getFullName() for task in tasks]
        self.assertEqual(set(task_names), set(names))

    def testAllNames(self):
        """Testing that get_sub_tasks() without options returns all tasks
        """
        pipeline = ParentTask()
        for task in (pipeline, pipeline.subtask1, pipeline.subtask2,
                     pipeline.subtask1.leaf1):
            tasks = task.get_sub_tasks()
            self._check_names(tasks, self.all_names)

    def testLeafs(self):
        """Testing that get_sub_tasks() without options returns all tasks
        """
        pipeline = ParentTask()
        for task in (pipeline, pipeline.subtask1, pipeline.subtask2,
                     pipeline.subtask1.leaf1):
            tasks = task.get_sub_tasks(leaf_only=True)
            self._check_names(tasks, self.all_leaves)

    def testSubTasks(self):
        """Testing that get_sub_tasks() without options returns all tasks
        """
        pipeline = ParentTask()

        tasks = pipeline.get_sub_tasks(whole_pipeline=False)
        self._check_names(tasks, self.all_names)

        tasks = pipeline.subtask1.get_sub_tasks(whole_pipeline=False)
        self._check_names(tasks, ['parent_task.subtask1',
                                  'parent_task.subtask1.leaf1',
                                  'parent_task.subtask1.leaf2'])

        tasks = pipeline.subtask2.get_sub_tasks(whole_pipeline=False)
        self._check_names(tasks, ['parent_task.subtask2',
                                  'parent_task.subtask2.leaf1',
                                  'parent_task.subtask2.leaf2'])

        tasks = pipeline.subtask2.leaf1.get_sub_tasks(whole_pipeline=False)
        self._check_names(tasks, ['parent_task.subtask2.leaf1'])

        tasks = pipeline.subtask1.leaf2.get_sub_tasks(whole_pipeline=False)
        self._check_names(tasks, ['parent_task.subtask1.leaf2'])

    def testSubLeaves(self):
        """Testing that get_sub_tasks() without options returns all tasks
        """
        pipeline = ParentTask()

        tasks = pipeline.get_sub_tasks(leaf_only=True, whole_pipeline=False)
        self._check_names(tasks, self.all_leaves)

        tasks = pipeline.subtask1.get_sub_tasks(leaf_only=True, whole_pipeline=False)
        self._check_names(tasks, ['parent_task.subtask1.leaf1',
                                  'parent_task.subtask1.leaf2'])

        tasks = pipeline.subtask2.get_sub_tasks(leaf_only=True, whole_pipeline=False)
        self._check_names(tasks, ['parent_task.subtask2.leaf1',
                                  'parent_task.subtask2.leaf2'])

        tasks = pipeline.subtask2.leaf1.get_sub_tasks(leaf_only=True, whole_pipeline=False)
        self._check_names(tasks, ['parent_task.subtask2.leaf1'])

        tasks = pipeline.subtask1.leaf2.get_sub_tasks(leaf_only=True, whole_pipeline=False)
        self._check_names(tasks, ['parent_task.subtask1.leaf2'])


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
