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
import lsst.afw.table as afwTable
import lsst.daf.base as dafBase
import lsst.pipe.base as pipeBase
from lsst.meas.astrom import createMatchMetadata
from lsst.pipe.supertask import SuperTask
from lsst.pipe.tasks.calibrate import CalibrateConfig, CalibrateTask


class SuperCalibrateTask(SuperTask):
    """!A SuperTask version of CalibrateTask"""
    ConfigClass = CalibrateConfig
    _default_name = "calibrate"
    # Ouch!
    #RunnerClass = pipeBase.ButlerInitializedTaskRunner

    def __init__(self, butler=None, refObjLoader=None, icSourceSchema=None, **kwargs):
        """!Construct a CalibrateTask

        @param[in] butler  The butler is passed to the refObjLoader constructor in case it is
            needed.  Ignored if the refObjLoader argument provides a loader directly.
        @param[in] refObjLoader  An instance of LoadReferenceObjectsTasks that supplies an
            external reference catalog.  May be None if the desired loader can be constructed
            from the butler argument or all steps requiring a reference catalog are disabled.
        @param[in] icSourceSchema  schema for icSource catalog, or None.
                   Schema values specified in config.icSourceFieldsToCopy will be taken from this schema.
                   If set to None, no values will be propagated from the icSourceCatalog
        @param[in,out] kwargs  other keyword arguments

        All copied from pipe.tasks.calibrate
        """
        super(SuperCalibrateTask, self).__init__(**kwargs)

        if icSourceSchema is None and butler is not None:
            # Use butler to read icSourceSchema from disk.
            icSourceSchema = butler.get("icSrc_schema", immediate=True).schema
            self.icSourceSchema = icSourceSchema  # only so a CalibrateTask of the same schema can be constructed, used in two places

        if icSourceSchema is not None:
            # use a schema mapper to avoid copying each field separately
            self.schemaMapper = afwTable.SchemaMapper(icSourceSchema)
            self.schemaMapper.addMinimalSchema(afwTable.SourceTable.makeMinimalSchema(), False)

            # Add fields to copy from an icSource catalog
            # and a field to indicate that the source matched a source in that catalog
            # If any fields are missing then raise an exception, but first find all missing fields
            # in order to make the error message more useful.
            self.calibSourceKey = self.schemaMapper.addOutputField(
                afwTable.Field["Flag"]("calib_detected", "Source was detected as an icSource"))
            missingFieldNames = []
            for fieldName in self.config.icSourceFieldsToCopy:
                try:
                    schemaItem = icSourceSchema.find(fieldName)
                except Exception:
                    missingFieldNames.append(fieldName)
                else:
                    # field found; if addMapping fails then raise an exception
                    self.schemaMapper.addMapping(schemaItem.getKey())

            if missingFieldNames:
                raise RuntimeError("isSourceCat is missing fields {} specified in icSourceFieldsToCopy"
                                   .format(missingFieldNames))

            # produce a temporary schema to pass to the subtasks; finalize it later
            self.schema = self.schemaMapper.editOutputSchema()
        else:
            self.schemaMapper = None
            self.schema = afwTable.SourceTable.makeMinimalSchema()
        self.makeSubtask('detection', schema=self.schema)

        self.algMetadata = dafBase.PropertyList()

        if self.config.doDeblend:
            self.makeSubtask("deblend", schema=self.schema)
        self.makeSubtask('measurement', schema=self.schema, algMetadata=self.algMetadata)
        if self.config.doApCorr:
            self.makeSubtask('applyApCorr', schema=self.schema)
        self.makeSubtask('afterburners', schema=self.schema)

        if self.config.doAstrometry or self.config.doPhotoCal:
            if refObjLoader is None:
                self.makeSubtask('refObjLoader', butler=butler)
                refObjLoader = self.refObjLoader
            self.pixelMargin = refObjLoader.config.pixelMargin
            self.makeSubtask("astrometry", refObjLoader=refObjLoader, schema=self.schema)
        if self.config.doPhotoCal:
            self.makeSubtask("photoCal", schema=self.schema)

        if self.schemaMapper is not None:
            # finalize the schema
            self.schema = self.schemaMapper.getOutputSchema()
        self.schema.checkUnits(parse_strict=self.config.checkUnitsParseStrict)

    def getInputData(self):
        """Return a list of dataset types of all possibly needed inputs"""
        return ["expIdInfo", "icExp", "icExpBackground", "icSrc", "icSrc_schema", "refObjLoader"]

    def getOutputData(self):
        """Return a list of dataset types of all possible outputs"""
        return ["src", "src_schema", "srcMatch", "calexp", "calexpBackground"]

    def getLevel(self):
        """Return the Butler level that this task operates on

           not sure if this is the way to go...
        """
        return "sensor"

    def readInputData(self, dataRef):
        """Read needed data thru Butler in a pipeBase.Struct based on config"""
        # This is needed at ctor
        #icSourceSchema = dataRef.get("icSrc_schema", immediate=True).schema

        exposure = dataRef.get("icExp", immediate=True)
        background = dataRef.get("icExpBackground", immediate=True)
        icSourceCat = dataRef.get("icSrc", immediate=True)
        exposureIdInfo = dataRef.get("expIdInfo")

        return pipeBase.Struct(exposure=exposure,
                               exposureIdInfo=exposureIdInfo,
                               background=background,
                               icSourceCat=icSourceCat)

    def writeOutputData(self, dataRef, calRes):
        """Write output data thru Butler based on config"""
        if self.config.doWrite:
            CalibrateTask.writeOutputs(
                CalibrateTask(config=self.config, log=self.log, icSourceSchema=self.icSourceSchema),
                dataRef=dataRef,
                exposure=calRes.exposure,
                background=calRes.background,
                sourceCat=calRes.sourceCat,
                astromMatches=calRes.astromMatches,
                matchMeta=self.matchMeta,
            )

    @pipeBase.timeMethod
    def execute(self, dataRef):
        """!Characterize a science image

        @param dataRef: butler data reference
        @return a pipeBase Struct containing the results
        """
        self.log.info("Performing Super Calibrate on sensor data ID %s" % (dataRef.dataId,))

        self.log.info("Reading input data using dataRef")
        inputData = self.readInputData(dataRef)

        self.log.info("Running operations. The run() method should not take anything Butler")
        if self.config.doWrite and self.config.doAstrometry:
            self.matchMeta = createMatchMetadata(inputData.getDict()['exposure'], border=self.pixelMargin)
        else:
            self.matchMeta = None
        # this is likely wrong, e.g. schema?
        result = CalibrateTask.calibrate(CalibrateTask(config=self.config, log=self.log, icSourceSchema=self.icSourceSchema),
                                         **inputData.getDict())

        self.log.info("Writing output data using dataRef")
        self.writeOutputData(dataRef, result)

        return result

    def getSchemaCatalogs(self):
        """Return a dict of empty catalogs for each catalog dataset produced by this task.
        """
        sourceCat = afwTable.SourceCatalog(self.schema)
        sourceCat.getTable().setMetadata(self.algMetadata)
        return {"src": sourceCat}

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
            datasetType="icExp",
            help="data IDs, e.g. --id visit=12345 ccd=1,2^0,3")
        return parser