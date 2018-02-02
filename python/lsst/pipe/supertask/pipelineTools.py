#
# LSST Data Management System
# Copyright 2018 LSST Corporation.
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
"""
Module defining few methods to manipulate or query pipelines.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["isPipelineOrdered"]

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import sys

#-----------------------------
# Imports for other modules --
#-----------------------------
from .config import InputDatasetConfig, OutputDatasetConfig

#----------------------------------
# Local non-exported definitions --
#----------------------------------

#------------------------
# Exported definitions --
#------------------------


def isPipelineOrdered(pipeline, taskFactory=None):
    """Checks whether tasks in pipeline are correctly ordered.

    Pipeline is correctly ordered if for any DatasetType produced by a task
    in a pipeline all its consumer tasks are located after producer.

    Parameters
    ----------
    pipeline : `pipe.supertask.Pipeline`
        Pipeline description.
    taskFactory: `pipe.supertask.TaskFactory`, optional
        Instance of an object which knows how to import task classes. It is only
        used if pipeline task definitions do not define task classes.

    Returns
    -------
    True for correctly ordered pipeline, False otherwise.

    Raises
    ------
    `ImportError` is raised when task class cannot be imported.
    `ValueError` is raised when there is more than one producer for a dataset
    type.
    `ValueError` is also raised when TaskFactory is needed but not provided.
    """
    # Build a map of DatasetType name to producer's index in a pipeline
    producerIndex = {}
    for idx, taskDef in enumerate(pipeline):

        # we will need task class for next operations, make sure it is loaded
        if not taskDef.taskClass:
            if not taskFactory:
                raise ValueError("Task class is not defined but task factory "
                                 "instance is not provided")
            taskDef.taskClass = taskFactory.loadTaskClass(taskDef.taskName)

        # get task output DatasetTypes, this can only be done via class method
        outputs = taskDef.taskClass.getOutputDatasetTypes(taskDef.config)
        for dsType in outputs.values():
            if dsType.name in producerIndex:
                raise ValueError("DatasetType `{}' appears more than once as"
                                 " output".format(dsType.name))
            producerIndex[dsType.name] = idx

    print(producerIndex)

    # check all inputs that are also someone's outputs
    for idx, taskDef in enumerate(pipeline):

        # get task input DatasetTypes, this can only be done via class method
        inputs = taskDef.taskClass.getInputDatasetTypes(taskDef.config)
        for dsType in inputs.values():
            # all pre-existing datasets have effective index -1
            prodIdx = producerIndex.get(dsType.name, -1)
            if prodIdx >= idx:
                # not good, producer is downstream
                return False

    return True
