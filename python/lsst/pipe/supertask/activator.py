"""Module which defines Activator interface and related methods.

Activator is an abstraction for the complex process which configures and
executes pipelines of SuperTasks. This module defines an interface for
this abstraction and few simple shared methods. Specific implementations
of this interface will be defined for each of the typical execution
environments (e.g. command-line activator).

Activator only deals with SuperTasks (classes and their instances) and
does not care about Task class which is a base class of SuperTask. Any
mentioning of "task" below actually means SuperTask in the context of
activator.

Typical sequence of operations that activator needs to perform to execute
a pipeline may look like this:

- Instantiate and configure activator instance
- Obtain the sequence of SuperTasks that comprise a pipeline, together
  with their configuration overrides.
- Locate and import SuperTask modules
- Create data butler instance
- Create per-task configurations by instantiating task-defined configuration
  classes and applying their overrides.
- Instantiate tasks using configurations created in previous step.
- Call each task define_quanta() method to obtain inputs/outputs needed for
  each task execution.
- Produce "execution plan" based on the data from define_quanta().
- Execute each task on each of its quanta by calling run_quantum() method.

In simplest case tasks are executed sequentially according to their order
in pipeline, though run_quantum() of the same task can be executed in
parallel. There is an implicit synchronization point between execution of
subsequent tasks. In more complex cases (e.g. full workflow system of DRP)
execution of tasks could happen in parallel taking into account data
dependencies between steps.

In general instance of a task executing define_quanta() method and
instance(s) of a task executing run_quantum() method are different
instances. It is also likely that each execution of run_quantum() will
be done with different instance of a task (though all task instances
should be configured identically) in a separate process. Instances of
a task can be created from scratch with predefined configuration or
in some cases (e.g. same-host execution) can be "cloned" via forking
of the process.

Activator itself in non-trivial cases will consist of more than one
cooperating process running on different hosts. One could think of it as two
separate stages:
- preparation of an execution plan, possibly transforming that plan in a
  form usable by activator-specific workflow system
- actual execution of that plan, one SuperTask and one quantum at a time
  which usually happens in a separate process, multiple quanta can be
  executed in parallel.

This module defines base classes for each of the two stages, though in some
cases, e.g. for in-process command-line activator, implementing of a second
stage subclass may not be needed.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["ButlerFactory", "ActivatorPre", "ActivatorExec"]

from builtins import object

import abc
import copy

import lsst.log as lsstLog

_LOG = lsstLog.Log.getLogger(__name__)



class ButlerFactory(object):
    """Base class for activator classes which defines interface for
    instantiating data butler.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def getButler(self):
        """Return instance of a data butler.

        Returns
        -------
        Data butler instance.
        """


class ActivatorPre(object):
    """Class which defines activator base class for preparation stage.

    This class is responsible for all operations in execution plan
    preparation. It has a simple implementation which uses methods of two
    abstract classes (ButlerFactory and SuperTaskFactory) defined above and
    few abstract methods defined in this class. Specific implementations of
    this interface will have to provide implementation of all those
    abstractions.

    Methods
    -------
    runPipeline()
        Main method of this class, implemented using abstract methods.

    Parameters
    ----------
    butlerFactory : `ButlerFactory`
        Instance which creates data butler.
    superTaskFactory : `SuperTaskFactory`
        Instance which creates super tasks.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, butlerFactory, superTaskFactory):
        self._butlerFactory = butlerFactory
        self._superTaskFactory = superTaskFactory

    def runPipeline(self):
        """Configure and execute the whole pipeline.

        Executing in this context does not necessarily means that it needs
        to wait until execution has finished, it can simply submit the task
        execution to a workflow system and return some info that can be used
        later in the context of the workflow system to identify pipeline.
        In a trivial case of command-line task execution this should actually
        run the tasks (possibly parallelizing execution of quanta) and wait
        until it all finishes.

        Returns
        -------
        Whatever is returned from `_submitForExecution()` method, this is
        activator-specific.
        """
        # generate a plan
        plan = self._makeExecutionPlan()

        # send it for execution (can wait to finish)
        return self._submitForExecution(plan)

    def _makeExecutionPlan(self):
        """Create execution plan for a pipeline.

        Execution plan is a list of execution steps, each step specifies
        task to be run at that step, its configuration, and a set of data
        quanta.

        Returns
        -------
        Sequence of execution steps, each step includes task class name,
        configuration overrides (or maybe full configuration), task instance,
        and a set of quanta.
        """

        # make a data butler
        butler = self._butlerFactory.getButler()

        # instantiate all tasks
        pipeline = self._getPipeline()
        taskList = []
        stf = self._superTaskFactory
        for taskName, overrides in pipeline:
            taskList += [stf.makeTaskInstance(taskName, None, overrides, butler)]

        # retrieve pipeline-level inputs or outputs
        inputs, outputs = self._getInputsOutputs()

        # call define_quanta on tasks in direct or reverse order
        execSeq = []
        if not outputs:
            # either inputs are given or use whatever is in butler as input
            inputs = copy.copy(inputs or dict())
            for task, (taskName, overrides) in zip(taskList, pipeline):
                # ask task for what it needs
                quanta = task.define_quanta(inputs, dict(), butler)

                # add task outputs as available inputs for next task
                for quantum in quanta:
                    for dataset, dataIds in quantum.outputs:
                        inputs.setdefault(dataset, []).extend(dataIds)

                execSeq.append((taskName, overrides, task, quanta))

        else:
            # start from tail
            outputs = copy.copy(outputs)
            for task, (taskName, overrides) in reversed([zip(taskList, pipeline)]):
                # ask task for what it needs
                quanta = task.define_quanta(dict(), outputs, butler)

                # add task inputs as needed outputs for previous task
                for quantum in quanta:
                    for dataset, dataIds in quantum.inputs:
                        outputs.setdefault(dataset, []).extend(dataIds)

                execSeq.insert(0, (taskName, overrides, task, quanta))

        return execSeq

    @abc.abstractmethod
    def _getPipeline(self):
        """Return pipeline that was passed to this activator.

        Returns
        -------
        List of `(taskName, overrides)`, `taskName` is a string representing
        SuperTask class name and `overrides` is an instance of
        `ConfigOverrides` class.
        """

    @abc.abstractmethod
    def _getInputsOutputs(self):
        """Return inputs and outputs for pipeline.

        Currently inputs and outputs are exclusive, if returned tuple has one
        element non-empty then another element must be empty or None. This may
        change in the future if we discover how to run in mixed mode.

        Returns
        -------
        inputs : dict or None
            Dictionary with keys being the dataset types and values - dataIds.
        outputs : dict or None
            Same structure as `inputs`.
        """

    @abc.abstractmethod
    def _submitForExecution(self, plan):
        """Execute pipeline.

        This method has to be implemented in actual activator class,
        typical implementation splits the plan into individual quanta,
        serializes all necessary info and forwards that to the instances
        of the `ActivatorExec` for execution.

        This is an interface to the actual workflow system. One trivial
        option for running pipeline is executing each task run_quantum()
        method in the same process as an activator, maybe using forking
        to parallelize quanta execution. Another option is to create
        configuration for external workflow system which will distribute
        workload and will run tasks on remote nodes, those processes do not
        even have to run the same activator class (remote activator only
        cares about providing correct parameters to `run_quantum()`).

        Parameters
        ----------
        List of execution steps, each step is represented by a tuple with
        elements (taskName, overrides, task, quanta)

        Returns
        -------
        It is allowed to return anything, the information returned is
        activator-specific.
        """


class ActivatorExec(object):
    """Class that implements execution stage for single quantum.

    This class corresponds to a "remote" worker in a typical workflow
    system. Note that activator class on the "head node" which is
    responsible for dividing the work and activator class on worker
    side do not have to be the same class, worker side only needs
    to implement this single method (and `_getButler()`) to do its
    job.

    This class may not be even used for some activators, for example
    command line activator will likely have more optimal way to run
    steps.

    Methods
    -------
    executeQuantum(taskName, config, overrides, quantum)
        Main method of this class, implemented using services of the
        butler factory and super task factory.

    Parameters
    ----------
    butlerFactory : `ButlerFactory`
        Instance which creates data butler.
    superTaskFactory : `SuperTaskFactory`
        Instance which creates super tasks.
    """

    def __init__(self, butlerFactory, superTaskFactory):
        self._butlerFactory = butlerFactory
        self._superTaskFactory = superTaskFactory

    def executeQuantum(self, taskName, config, overrides, quantum):
        """Execute single pipeline task on a single quantum.

        Current method signature has both `config` and `overrides`
        parameters, implementation may chose to either pass whole config
        object or only overrides from "head" to "worker", so only one of
        these is supposed to be not ``None``.

        Parameters
        ----------
        taskName : str
            Name of the SuperTask class, interpretation depends entirely on
            activator, e.g. it may or may not include dots.
        config : `pex.Config` or None
            Configuration object, if ``None`` then use task-defined
            configuration class to create new instance.
        overrides : `ConfigOverrides` or None
            Configuration overrides, this should contain all overrides to be
            applied to a default task config, including camera-specific,
            obs-package specific, and possibly command-line overrides.
        quantum : `Quantum`
            Data quantum for this execution.
        """
        # make a data butler
        butler = self._butlerFactory.getButler()

        # instantiate task
        stf = self._superTaskFactory
        task = stf.makeTaskInstance(taskName, config, overrides, butler)

        # execute the quantum, return its result to caller
        return task.run_quantum(quantum, butler)
