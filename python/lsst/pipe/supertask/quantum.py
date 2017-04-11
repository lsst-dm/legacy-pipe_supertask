"""Module which defines Quantum class and related methods.

Quantum describes data used and produced by a particular execution of a
particular SuperTask, in addition to data description quantum may include
metadata relevant to that particular execution.

Set of quanta is produced by `SuperTask.define_quanta()` method which
typically splits the space of inputs into the units of size digestible by
that particular SuperTask. Activators pass individual quanta to each
invocation of `SuperTask.run_quantum()` method. Activators can also examine
quanta when they build execution chain consisting of multiple SuperTasks.

Quanta are passed from preparation stage to execution stage which typically
happens on different hosts/processes, to support that Quantum class has to
provide (de-)serialization methods for some standard external representation,
e.g. Pickle.
"""


class Quantum(object):
    """Defines the minimum unit of work that a SuperTask may perform.

    Attributes
    ----------
    inputs: `dict` of `{datasetType: <list of DataIds>}`
        All input datasets and DataIds required by this run of the
        SuperTask.
    outputs: `dict` of `{datasetType: <list of DataIds>}`
        All output datasets and DataIds that can be produced by this run
        of the SuperTask.
    extras: object
        Any additional SuperTask-specific information relevant to this
        run of a SuperTask. This is not examined by Activator but simply
        passed from `define_quanta` to `run_quantum`.
    director: str or None
        Name of a "director" dataset which a primary input or output
        dataset for a SuperTask. Not used currently, kept for future
        extension. If defined then the name must be present in either
        `inputs` or `outputs` dictionaries.

    Parameters
    ----------
    inputs: `dict` of `{datasetType: <list of DataIds>}`
        Value for `inputs` attribute.
    outputs: `dict` of `{datasetType: <list of DataIds>}`
        Value for `outputs` attribute.
    extras: object, optional
        Value for `extras` attribute. If provided it has to be serializable
        in the same representation (TBD) as Quantum class itself. Otherwise
        its structure is opaque and only known to a SuperTask.
    director: str, optional
        Value for `director` attribute.
    """

    def __init__(self, inputs, outputs, extras=None, director=None):
        self.inputs = inputs
        self.outputs = outputs
        self.extras = extras
        self.director = director

        # check consistency
        if director and director not in inputs and director not in outputs:
            raise ValueError("Director dataset ({}) is not in inputs or outputs".format(director))
