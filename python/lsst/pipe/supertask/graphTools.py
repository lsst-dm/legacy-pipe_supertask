#
# LSST Data Management System
# Copyright 2018 AURA/LSST.
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

__all__ = []

# -------------------------------
#  Imports of standard modules --
# -------------------------------

# -----------------------------
#  Imports for other modules --
# -----------------------------

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


def graph2dot(qgraph, file):
    """Convert QuantumGraph into GraphViz digraph.

    This method is mostly for documentation/presentation purposes.

    Parameters
    ----------
    qgraph: `pipe.supertask.QuantumGraph`
        QuantumGraph instance.
    file : str or file object
        File where GraphViz graph (DOT language) is written, can be a file name
        or file object.

    Raises
    ------
    `OSError` is raised when output file cannot be open.
    `ImportError` is raised when task class cannot be imported.
    """

    def _renderTaskNode(nodeName, taskDef, file):
        """Render GV node for a task"""
        label = [taskDef.taskName.rpartition('.')[2]]
        if taskDef.label:
            label += ["label: {}".format(taskDef.label)]
        label = r'\n'.join(label)
        attrib = dict(shape="box",
                      style="filled,bold",
                      fillcolor="gray70",
                      label=label)
        attrib = ['{}="{}"'.format(key, val) for key, val in attrib.items()]
        print("{} [{}];".format(nodeName, ", ".join(attrib)), file=file)

    def _renderDSNode(nodeName, dsRef, file):
        """Render GV node for a dataset"""
        label = [dsRef.datasetType.name]
        for key in sorted(dsRef.dataId.keys()):
            label += [key + "=" + str(dsRef.dataId[key])]
        label = r'\n'.join(label)
        attrib = dict(shape="box",
                      style="rounded,filled",
                      fillcolor="gray90",
                      label=label)
        attrib = ['{}="{}"'.format(key, val) for key, val in attrib.items()]
        print("{} [{}];".format(nodeName, ", ".join(attrib)), file=file)

    def _datasetRefId(dsRef):
        """Make an idetifying string for given ref"""
        idStr = dsRef.datasetType.name
        for key in sorted(dsRef.dataId.keys()):
            idStr += ":" + key + "=" + str(dsRef.dataId[key])
        return idStr

    def _makeDSNode(dsRef, allDatasetRefs, file):
        """Make new node for dataset if  it does not exist.

        Returns node name.
        """
        dsRefId = _datasetRefId(dsRef)
        nodeName = allDatasetRefs.get(dsRefId)
        if nodeName is None:
            idx = len(allDatasetRefs)
            nodeName = "dsref_{}".format(idx)
            allDatasetRefs[dsRefId] = nodeName
            _renderDSNode(nodeName, dsRef, file)
        return nodeName

    # open a file if needed
    close = False
    if not hasattr(file, "write"):
        file = open(file, "w")
        close = True

    print("digraph QuantumGraph {", file=file)

    allDatasetRefs = {}
    for taskId, nodes in enumerate(qgraph):

        taskDef = nodes.taskDef

        for qId, quantum in enumerate(nodes.quanta):

            # node for a task
            taskNodeName = "task_{}_{}".format(taskId, qId)
            _renderTaskNode(taskNodeName, taskDef, file)

            # quantum inputs
            for dsRefs in quantum.predictedInputs.values():
                for dsRef in dsRefs:
                    nodeName = _makeDSNode(dsRef, allDatasetRefs, file)
                    print("{} -> {};".format(nodeName, taskNodeName), file=file)

            # quantum outputs
            for dsRefs in quantum.outputs.values():
                for dsRef in dsRefs:
                    nodeName = _makeDSNode(dsRef, allDatasetRefs, file)
                    print("{} -> {};".format(taskNodeName, nodeName), file=file)

    print("}", file=file)
    if close:
        file.close()
