from __future__ import division, absolute_import
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#
import lsst.pipe.base as pipeBase
from lsst.pipe.supertask import SuperTask
from lsst.pipe.tasks.processCcd import ProcessCcdConfig


class SuperProcessCcdTask(SuperTask):
    """!A SuperTask version of ProcessCcdTask"""
    ConfigClass = ProcessCcdConfig
    _default_name = "processCcd"

    def __init__(self, *args, **kwargs):
        SuperTask.__init__(self, *args, **kwargs)
        self.makeSubtask("isr")
        self.makeSubtask("charImage")
        self.makeSubtask("calibrate", icSourceSchema=self.charImage.schema)

    @pipeBase.timeMethod
    def execute(self, dataRef):
        """!Apply common instrument signature correction algorithms to a raw frame

        @param dataRef: butler data reference
        @return a pipeBase Struct containing:
        - exposure 
        """
        self.log.info("Processing data ID %s" % (dataRef.dataId,))

        sensorRef = dataRef
        # Below is same as ProcessCcdTask.run()
        exposure = self.isr.runDataRef(sensorRef).exposure

        charRes = self.charImage.run(
            dataRef = sensorRef,
            exposure = exposure,
            doUnpersist = False,
        )
        exposure = charRes.exposure

        if self.config.doCalibrate:
            calibRes = self.calibrate.run(
                dataRef = sensorRef,
                exposure = charRes.exposure,
                background = charRes.background,
                doUnpersist = False,
                icSourceCat = charRes.sourceCat,
            )

        return pipeBase.Struct(
            charRes = charRes,
            calibRes = calibRes if self.config.doCalibrate else None,
            exposure = exposure,
            background = calibRes.background if self.config.doCalibrate else charRes.background,
        )

    def pre_run(self):
        print("Doing pre run at %s" % self.name)

    def run(self):
        print("Doing run at at %s" % self.name)

    def _getConfigName(self):
        """!Get the name prefix for the task config's dataset type, or None to prevent persisting the config

        This override returns None to avoid persisting metadata for this trivial task.
        """
        return None

    def _getMetadataName(self):
        """!Get the name prefix for the task metadata's dataset type, or None to prevent persisting metadata

        This override returns None to avoid persisting metadata for this trivial task.
        """
        return None
