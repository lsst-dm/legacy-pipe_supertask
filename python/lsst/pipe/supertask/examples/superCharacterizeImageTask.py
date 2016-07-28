from __future__ import division, absolute_import
#
# LSST Data Management System
#
# Copyright 2016  AURA/LSST.
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
# see <https://www.lsstcorp.org/LegalNotices/>.
#
import lsst.daf.base as dafBase
import lsst.pipe.base as pipeBase
from lsst.afw.table import SourceTable, SourceCatalog
from lsstDebug import getDebugFrame
from lsst.pipe.supertask import SuperTask
from lsst.pipe.tasks.characterizeImage import CharacterizeImageConfig, CharacterizeImageTask


class SuperCharacterizeImageTask(SuperTask):
    """!A SuperTask version of CharacterizeImageTask"""
    ConfigClass = CharacterizeImageConfig
    _default_name = "characterizeImage"
    # Ouch!
    #RunnerClass = pipeBase.ButlerInitializedTaskRunner

    def __init__(self, butler=None, refObjLoader=None, schema=None, **kwargs):
        """!Construct a CharacterizeImageTask

        @param[in] butler  A butler object is passed to the refObjLoader constructor in case
            it is needed to load catalogs.  May be None if a catalog-based star selector is
            not used, if the reference object loader constructor does not require a butler,
            or if a reference object loader is passed directly via the refObjLoader argument.
        @param[in] refObjLoader  An instance of LoadReferenceObjectsTasks that supplies an
            external reference catalog to a catalog-based star selector.  May be None if a
            catalog star selector is not used or the loader can be constructed from the
            butler argument.
        @param[in,out] schema  initial schema (an lsst.afw.table.SourceTable), or None
        @param[in,out] kwargs  other keyword arguments

        All copied from pipe.tasks.characterizeImage
        """
        super(SuperCharacterizeImageTask, self).__init__(**kwargs)
        if schema is None:
            schema = SourceTable.makeMinimalSchema()
        self.schema = schema
        self.makeSubtask("background")
        self.makeSubtask("installSimplePsf")
        self.makeSubtask("repair")
        self.makeSubtask("measurePsf", schema=self.schema)
        if self.config.doMeasurePsf and self.measurePsf.usesMatches:
            if not refObjLoader:
                self.makeSubtask('refObjLoader', butler=butler)
                refObjLoader = self.refObjLoader
            self.makeSubtask("astrometry", refObjLoader=refObjLoader)
        self.algMetadata = dafBase.PropertyList()
        self.makeSubtask('detection', schema=self.schema)
        if self.config.doDeblend:
            self.makeSubtask("deblend", schema=self.schema)
        self.makeSubtask('measurement', schema=self.schema, algMetadata=self.algMetadata)
        if self.config.doApCorr:
            self.makeSubtask('measureApCorr', schema=self.schema)
            self.makeSubtask('applyApCorr', schema=self.schema)
        self.makeSubtask('afterburners', schema=self.schema)
        self._initialFrame = getDebugFrame(self._display, "frame") or 1
        self._frame = self._initialFrame
        self.schema.checkUnits(parse_strict=self.config.checkUnitsParseStrict)

    def getInputData(self):
        """Return a list of dataset types of all possibly needed inputs"""
        return ["postISRCCD", "expIdInfo", "background"]

    def getOutputData(self):
        """Return a list of dataset types of all possible outputs"""
        return ["icSrc", "icExp", "icExpBackground", "icSrc_schema"]

    def getLevel(self):
        """Return the Butler level that this task operates on

           not sure if this is the way to go...
        """
        return "sensor"

    def readInputData(self, dataRef):
        """Read needed data thru Butler in a pipeBase.Struct based on config"""
        exposure = dataRef.get("postISRCCD", immediate=True)
        exposureIdInfo = dataRef.get("expIdInfo", immediate=True)
        background = None  # unsure about the actual butler type of background
        return pipeBase.Struct(exposure=exposure,
                               exposureIdInfo=exposureIdInfo,
                               background=background)

    def writeOutputData(self, dataRef, charRes):
        """Write output data thru Butler based on config"""
        if self.config.doWrite:
            dataRef.put(charRes.sourceCat, "icSrc")
            if self.config.doWriteExposure:
                dataRef.put(charRes.exposure, "icExp")
                dataRef.put(charRes.background, "icExpBackground")

    @pipeBase.timeMethod
    def execute(self, dataRef):
        """!Characterize a science image

        @param dataRef: butler data reference
        @return a pipeBase Struct containing the results
        """
        self.log.info("Performing Super CharacterizeImage on sensor data ID %s" % (dataRef.dataId,))

        self.log.info("Reading input data using dataRef")
        inputData = self.readInputData(dataRef)

        self.log.info("Running operations. The run() method should not take anything Butler")
        # this is likely wrong, e.g. schema?
        result = CharacterizeImageTask.characterize(CharacterizeImageTask(config=self.config, log=self.log, schema=SourceTable.makeMinimalSchema()),
                                                    **inputData.getDict())

        self.log.info("Writing output data using dataRef")
        self.writeOutputData(dataRef, result)

        return result

    def getSchemaCatalogs(self):
        """Return a dict of empty catalogs for each catalog dataset produced by this task.
        """
        sourceCat = SourceCatalog(self.schema)
        sourceCat.getTable().setMetadata(self.algMetadata)
        return {"icSrc": sourceCat}

    @classmethod
    def _makeArgumentParser(cls):
        """!Create and return an argument parser

        @param[in] cls      the class object
        @return the argument parser for this task.

        This override is used to delay making the data ref list until the daset type is known;
        this is done in @ref parseAndRun.
        """
        # Allow either _default_name or _DefaultName
        if cls._default_name is not None:
            task_name = cls._default_name
        elif cls._DefaultName is not None:
            task_name = cls._DefaultName
        else:
            raise RuntimeError("_default_name or _DefaultName is required for a task")
        parser = pipeBase.ArgumentParser(name=task_name)
        parser.add_id_argument(name="--id",
            datasetType="postISRCCD",
            help="data IDs, e.g. --id visit=12345 ccd=1,2^0,3")
        return parser