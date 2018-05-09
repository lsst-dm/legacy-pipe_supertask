#
# LSST Data Management System
# Copyright 2017 AURA/LSST.
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
Module defining TaskLoader class and related methods.
"""
from builtins import object

# "exported" names
__all__ = ['TaskLoader']

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import importlib
import inspect
import pkgutil

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.pipe.base import CmdLineTask, Task
import lsst.log as lsstLog
from .superTask import SuperTask

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------


def _task_kind(task_class):
    """Determine task kind.

    Parameters
    ----------
    task_class
        Python class object

    Returns
    -------
    None if `task_class` is not a class or does not inherit from Task.
    Otherwise returns one of KIND_TASK, KIND_CMDLINETASK, or KIND_SUPERTASK.
    """
    kind = None
    if inspect.isclass(task_class):
        bases = inspect.getmro(task_class)
        if SuperTask in bases:
            kind = KIND_SUPERTASK
        elif CmdLineTask in bases:
            kind = KIND_CMDLINETASK
        elif Task in bases:
            kind = KIND_TASK
    return kind

# ------------------------
#  Exported definitions --
# ------------------------


KIND_TASK = 'Task'
KIND_CMDLINETASK = 'CmdLineTask'
KIND_SUPERTASK = 'SuperTask'


class TaskLoader(object):
    """Task responsible for finding and loading tasks.

    Parameters
    ----------
    packages : `list` of `str`, optional
        Defines the set of package names to look for tasks. There is a small
        pre-defined set of packages that is used by default.
    """

    # default locations for packages
    # TODO: examples should be removed later.
    DEFAULT_PACKAGES = ['lsst.pipe.supertask.examples', 'lsst.pipe.tasks']

    def __init__(self, packages=None):
        if not packages:
            packages = TaskLoader.DEFAULT_PACKAGES
        self._packages = packages
        self._log = lsstLog.Log.getLogger(self.__class__.__name__)

    @property
    def packages(self):
        """Return current set of packages in search path.
        """
        return self._packages

    def modules(self):
        """Return set of modules and sub-packages found in the packages.

        Returns
        -------
        `list` of tuples (name, flag), `name` is the module or sub-package
        name (includes dot-separated parent package name), `flag` is set to
        False for module and True for sub-package.

        Raises
        ------
        `ImportError`
            If fails to import any package
        """
        modules = []
        for pkg_name in self._packages:
            self._log.debug("get modules from package %r", pkg_name)
            pkg = importlib.import_module(pkg_name)
            for _, module_name, ispkg in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + '.'):
                self._log.debug("found module %r ispkg=%s", module_name, ispkg)
                modules.append((module_name, ispkg))
        return modules

    def tasks(self):
        """Return list of all tasks in the packages.

        Returns
        -------
        `list` of tuples (name, kind), `name` is the full task name including package
        and module name, `kind` is a task kind, one of the constants `KIND_TASK`,
        `KIND_CMDLINETASK`, or `KIND_SUPERTASK`.

        Raises
        ------
        `ImportError`
            If fails to import any package
        """
        tasks = []
        for pkg_name in self._packages:
            self._log.debug("importing package %r", pkg_name)
            pkg = importlib.import_module(pkg_name)
            for _, module_name, ispkg in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + '.'):
                self._log.debug("found module %r ispkg=%s", module_name, ispkg)
                # classes should not live in packages
                if not ispkg:
                    try:
                        self._log.debug("importing module %r", module_name)
                        mod = importlib.import_module(module_name)
                    except ImportError as exc:
                        self._log.debug("import failed: %s", exc)
                    else:
                        for name, obj in vars(mod).items():
                            if inspect.isclass(obj) and inspect.getmodule(obj) is mod:
                                kind = _task_kind(obj)
                                if kind is not None:
                                    tasks.append((module_name + '.' + name, kind))
        return tasks

    def loadTaskClass(self, task_class_name):
        """Find and load a task class.

        If `task_class_name` is a simple identifier without dots then search
        for that class in all modules in package list, this means importing
        all modules which can be slow.

        Otherwise if `task_class_name` has dots then we try to import it
        directly assuming name is ``package.module.Class``, if that fails
        then try importing it assuming its name is relative to a package
        names in the known package list.

        Parameters
        ----------
        task_class_name : `str`
            Task class name which can include package and module names
            separated by dots.

        Returns
        -------
        task_class
            Python class object for a task, `None` if class was not found
        taks_name
            fully-qualified class name ("package.module.TaskClass")
        taks_kind
            one of KIND_TASK, KIND_CMDLINETASK, or KIND_SUPERTASK

        Raises
        ------
        `ImportError` is raised when task class cannot be imported.
        """
        self._log.debug("load_task_class: will look for %r", task_class_name)
        dot = task_class_name.rfind('.')
        if dot > 0:

            # name is package.module.Class or module.Class, either absolute
            # or relative to package list
            module_name = task_class_name[:dot]
            class_name = task_class_name[dot + 1:]

            for package in [None] + self._packages:

                full_module_name = module_name
                if package:
                    full_module_name = package + '.' + full_module_name

                try:
                    self._log.debug("load_task_class: try module %r", full_module_name)
                    module = importlib.import_module(full_module_name)
                    self._log.debug("load_task_class: imported %r", full_module_name)
                except ImportError:
                    pass
                else:
                    # get Class from module, if not there try other options
                    klass = getattr(module, class_name, None)
                    if klass is not None:
                        kind = _task_kind(klass)
                        self._log.debug("load_task_class: found %r in %r, kind: %s",
                                        class_name, full_module_name, kind)
                        if kind is not None:
                            return (klass, full_module_name + '.' + class_name, kind)
                    else:
                        self._log.debug("load_task_class: no class %r in module %r",
                                        class_name, full_module_name)

        else:

            # simple name, search for it in all modules in every package, not
            # very efficient
            for pkg_name in self._packages:
                self._log.debug("load_task_class: importing package %r", pkg_name)
                pkg = importlib.import_module(pkg_name)
                for _, module_name, ispkg in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + '.'):
                    self._log.debug("load_task_class: found module %r ispkg=%s", module_name, ispkg)
                    # classes should not live in packages
                    if not ispkg:
                        try:
                            self._log.debug("load_task_class: importing module %r", module_name)
                            mod = importlib.import_module(module_name)
                        except ImportError as exc:
                            self._log.debug("load_task_class: import failed: %s", exc)
                        else:
                            obj = getattr(mod, task_class_name, None)
                            if inspect.isclass(obj) and inspect.getmodule(obj) is mod:
                                kind = _task_kind(obj)
                                self._log.debug("load_task_class: found class %r kind: %s",
                                                task_class_name, kind)
                                if kind is not None:
                                    return (obj, module_name + '.' + task_class_name, kind)

        raise ImportError('Cannot find class named "' + task_class_name + '" in known packages')
