#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
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

"""Simple unit test for Pipeline.
"""

import unittest
import pickle
from collections import namedtuple

import lsst.pex.config as pexConfig
from lsst.pipe.supertask import (Pipeline, SuperTask, TaskDef,
                                 isPipelineOrdered,
                                 SuperTaskConfig, InputDatasetConfig,
                                 OutputDatasetConfig)
import lsst.utils.tests

# mock for actual dataset type
DS = namedtuple("DS", "name units")
def makeDatasetType(dsConfig):
    return DS(name=dsConfig.name, units=dsConfig.units)


class AddConfig(SuperTaskConfig):
    input = pexConfig.ConfigField(dtype=InputDatasetConfig,
                                  doc="Input for this task")
    output = pexConfig.ConfigField(dtype=OutputDatasetConfig,
                                   doc="Output for this task")

    def setDefaults(self):
        self.input.name = "raw"
        self.input.units = ["Visit", "Sensor"]
        self.output.name = "addraw"
        self.output.units = ["Visit", "Sensor"]


class AddTask(SuperTask):
    ConfigClass = AddConfig

    @classmethod
    def getInputDatasetTypes(cls, config):
        return {"input": makeDatasetType(config.input)}

    @classmethod
    def getOutputDatasetTypes(cls, config):
        return {"output": makeDatasetType(config.output)}


class MultConfig(SuperTaskConfig):
    input = pexConfig.ConfigField(dtype=InputDatasetConfig,
                                  doc="Input for this task")
    output = pexConfig.ConfigField(dtype=OutputDatasetConfig,
                                   doc="Output for this task")

    def setDefaults(self):
        self.input.name = "addraw"
        self.input.units = ["Visit", "Sensor"]
        self.output.name = "mulraw"
        self.output.units = ["Visit", "Sensor"]


class MultTask(SuperTask):
    ConfigClass = MultConfig

    @classmethod
    def getInputDatasetTypes(cls, config):
        return {"input": makeDatasetType(config.input)}

    @classmethod
    def getOutputDatasetTypes(cls, config):
        return {"output": makeDatasetType(config.output)}


class PipelineToolsTestCase(unittest.TestCase):
    """A test case for pipelineTools
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testIsOrdered(self):
        """Tests for isPipelineOrdered method
        """
        pipeline = Pipeline([TaskDef("lsst.pipe.supertask.tests.AddTask", AddConfig(), AddTask),
                             TaskDef("lsst.pipe.supertask.tests.MultTask", MultConfig(), MultTask)])
        self.assertTrue(isPipelineOrdered(pipeline))

        pipeline = Pipeline([TaskDef("lsst.pipe.supertask.tests.MultTask", MultConfig(), MultTask),
                             TaskDef("lsst.pipe.supertask.tests.AddTask", AddConfig(), AddTask)])
        self.assertFalse(isPipelineOrdered(pipeline))

    def testIsOrderedExceptions(self):
        """Tests for isPipelineOrdered method exceptions
        """

        # two producers should throw ValueError
        pipeline = Pipeline([TaskDef("lsst.pipe.supertask.tests.AddTask", AddConfig(), AddTask),
                             TaskDef("lsst.pipe.supertask.tests.AddTask", AddConfig(), AddTask),
                             TaskDef("lsst.pipe.supertask.tests.MultTask", MultConfig(), MultTask)])
        with self.assertRaises(ValueError):
            isPipelineOrdered(pipeline)

        # missing factory should throw ValueError
        pipeline = Pipeline([TaskDef("lsst.pipe.supertask.tests.AddTask", AddConfig()),
                             TaskDef("lsst.pipe.supertask.tests.MultTask", MultConfig())])
        with self.assertRaises(ValueError):
            isPipelineOrdered(pipeline)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
