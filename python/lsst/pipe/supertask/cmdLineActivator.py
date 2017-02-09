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
"""
Module defining CmdLineActivator class and related methods.
"""

from __future__ import print_function
from builtins import object

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import sys
import traceback

#-----------------------------
# Imports for other modules --
#-----------------------------
import lsst.log as lsstLog
import lsst.pipe.base.argumentParser as pipeBaseArgParser
from lsst.pipe.base.task import TaskError
from .activator import profile
from .parser import makeParser
from .taskLoader import (TaskLoader, KIND_CMDLINETASK, KIND_SUPERTASK)

# "exported" names
__all__ = ['CmdLineActivator']

#----------------------------------
# Local non-exported definitions --
#----------------------------------

# logging properties
_logProp = """\
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.err
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern={}
"""


def _printTable(rows, header):
    """Nice formatting of 2-column table.

    Parameters
    ----------
    rows : `list` of `tuple`
        Each item in the list is a 2-tuple containg left and righ column values
    header: `tuple` or `None`
        If `None` then table header are not prined, otherwise it's a 2-tuple
        with column headings.
    """
    if not rows:
        return
    width = max(len(x[0]) for x in rows)
    if header:
        width = max(width, len(header[0]))
        print(header[0].ljust(width), header[1])
        print("".ljust(width, "-"), "".ljust(len(header[1]), "-"))
    for col1, col2 in rows:
        print(col1.ljust(width), col2)

#------------------------
# Exported definitions --
#------------------------


class CmdLineActivator(object):
    """
    CmdLineActivator implements an activator for SuperTasks which executes
    tasks from command line.

    In addition to executing taks this activator provides additional methods
    for task management like dumping configuration or execution chain.
    """

    def parseAndRun(self, argv=None):
        """
        This method is a main entry point for this class, it parses command
        line and executes all commands.

        Parameters
        ----------
        argv : `list` of `str`, optional
            list of command line arguments, if not specified then
            `sys.argv[1:]` is used
        """

        if argv is None:
            argv = sys.argv[1:]

        # start with parsing command line, only do partial parsing now as
        # the tasks can add more arguments later
        parser = makeParser()
        args, extra_args = parser.parse_known_args(argv)

        # First thing to do is to setup logging. Note that task parsers
        # also parse logging options and re-initialize logging later.
        self.configLog(args.longlog, args.loglevel)

        # make task loader
        loader = TaskLoader(args.packages)

        if args.subcommand == "list":
            # just dump some info about where things may be found
            return self.doList(loader, args.show, args.show_headers)

        # for run/show command class name can be missing but this is only
        # allowed with --help option
        if args.taskname is None:
            if args.do_help:
                args.subparser.print_help()
                return
            else:
                args.subparser.error("taskname is required")

        # load task class
        task_class, task_name, task_kind = loader.loadTaskClass(args.taskname)
        if task_class is None:
            print("Failed to load task `{}'".format(args.taskname))
            return 2
        if task_kind not in (KIND_CMDLINETASK, KIND_SUPERTASK):
            print("Task `{}' is not a SuperTask or CmdLineTask".format(task_name))
            return 2

        # Dispatch to one of the methods
        if task_kind == KIND_CMDLINETASK:
            return self.doCmdLineTask(args, extra_args, task_class)
        elif task_kind == KIND_SUPERTASK:
            return self.doSuperTask(args, extra_args, task_class)

    def configLog(self, longlog, logLevels):
        """Configure logging system.

        Parameters
        ----------
        longlog : bool
            If True then make log messages appear in "long format"
        logLevels : `list` of `tuple`
            per-component logging levels, each item in the list is a tuple
            (component, level), `component` is a logger name or `None` for root
            logger, `level` is a logging level name ('DEBUG', 'INFO', etc.)
        """
        if longlog:
            message_fmt = "%-5p %d{yyyy-MM-ddThh:mm:ss.sss} %c (%X{LABEL})(%F:%L)- %m%n"
        else:
            message_fmt = "%c %p: %m%n"

        # global logging config
        lsstLog.configure_prop(_logProp.format(message_fmt))

        # configure individual loggers
        for component, level in logLevels:
            level = getattr(lsstLog.Log, level.upper(), None)
            if level is not None:
                logger = lsstLog.Log.getLogger(component or "")
                logger.setLevel(level)

    def doList(self, loader, show, show_headers):
        """Implementation of the "list" command.

        Parameters
        ----------
        loader : `TaskLoader`
        show : `list` of `str`
            List of items to show.
        show_headers : `bool`
            True to display additional headers
        """

        if not show:
            show = ["super-tasks"]

        if "packages" in show:
            if show_headers:
                print()
                print("Modules search path")
                print("-------------------")
            for pkg in sorted(loader.packages):
                print(pkg)

        if "modules" in show:
            try:
                modules = loader.modules()
            except ImportError as exc:
                print("Failed to import package, check --package option or $PYTHONPATH:", exc,
                      file=sys.stderr)
                return 2
            modules = [(name, "package" if flag else "module") for name, flag in sorted(modules)]
            headers = None
            if show_headers:
                print()
                headers = ("Module or package name", "Type    ")
            _printTable(modules, headers)

        if "tasks" in show or "super-tasks" in show:
            try:
                tasks = loader.tasks()
            except ImportError as exc:
                print("Failed to import package, check --packages option or PYTHONPATH:", exc,
                      file=sys.stderr)
                return 2

            if "tasks" not in show:
                # only show super-tasks
                tasks = [(name, kind) for name, kind in tasks if kind == KIND_SUPERTASK]
            tasks.sort()

            headers = None
            if show_headers:
                print()
                headers = ("Task class name", "Kind     ")
            _printTable(tasks, headers)

    def doSuperTask(self, args, extra_args, task_class):
        """Implementation of run/show for SuperTask.

        Parameters
        ----------
        args : `argparse.Namespace`
            Parsed command line
        extra_args : `list` of `str`
            extra arguments for sub-command which were not parsed by parser
        task_class : `type`
            Class object for SuperTask.
        """

        if args.do_help:
            # before dispaying help populate sub-parser with task-specific options
            self._copyParserOptions(task_class.makeArgumentParser(), args.subparser)
            args.subparser.print_help()
            return

        if args.subcommand not in ("run", "show"):
            print("unexpected command {}".format(args.subcommand),
                  file=sys.stderr)
            return 2

        # make task instance
        task, task_args = self._makeSuperTask(task_class, args, extra_args)

        # do all --show first
        # currently task parser handles that, we have to implement something
        # similar when we get rid of that

        if args.subcommand == "show":
            # stop here
            return

        # execute it
        if self.precall(task, task_args):
            profile_name = getattr(task_args, "profile", None)
            log = task_args.log
            target_list = self.makeTargetList(task_args)
            if target_list:
                with profile(profile_name, log):
                    for target in target_list:
                        args, kwargs = target
                        task.execute(*args, **kwargs)
            else:
                log.warn("Not running the task because there is no data to process; "
                         "you may preview data using \"--show-data\"")

    def makeTargetList(self, task_args):
        """Make the target list from the command line argument.

        We look at all dataId arguments in parsed command line, if there is
        an argument with name "id" then take its dataRefs and call task on
        each separate dataRef. For all other dataId arguments we add keyword
        argments with the names of the dataId arguments and values being the
        full list of its corresponding dataRefs.

        .. note:: This is a temporary solution until we replace task parser with
                better approach

        Parameters
        ----------
        task_args : `argparse.Namespace`
            Parsed command line from task parser (not activator parser)

        Returns
        -------
        `list` of (args, kwargs) tuples which are arguments to be passed to
        task execute() method.
        """
        refList = None
        kwargs = {}
        for optname, optval in vars(task_args).items():
            if isinstance(optval, pipeBaseArgParser.DataIdContainer):
                if optname == "id":
                    refList = optval.refList
                else:
                    kwargs[optname] = optval.refList
        if refList is not None:
            return [([ref], kwargs) for ref in refList]
        else:
            return [([], kwargs)]

    def _precallImpl(self, task, task_args):
        """The main work of 'precall'

        We write package versions, schemas and configs, or compare these to existing
        files on disk if present.

        Parameters
        ----------
        task
            instance of `task_class`
        task_args : `argparse.Namespace`
            command line as parsed by a task parser
        """
#         if not task_args.noVersions:
#             task.writePackageVersions(task_args.butler, clobber=task_args.clobberVersions)
        do_backup = not task_args.noBackupConfig
        task.write_config(task_args.butler, clobber=task_args.clobberConfig, do_backup=do_backup)
        task.write_schemas(task_args.butler, clobber=task_args.clobberConfig, do_backup=do_backup)

    def precall(self, task, task_args):
        """Hook for code that should run exactly once, before multiprocessing is invoked.

        The default implementation writes package versions, schemas and configs, or compares
        them to existing files on disk if present.

        Parameters
        ----------
        task
            instance of `task_class`
        task_args : `argparse.Namespace`
            command line as parsed by a task parser

        Returns
        -------
        `bool`, True if SuperTask should subsequently be called.
        """
        if task_args.doraise:
            self._precallImpl(task, task_args)
        else:
            try:
                self._precallImpl(task, task_args)
            except Exception as exc:
                task.log.fatal("Failed in task initialization: %s", exc)
                if not isinstance(exc, TaskError):
                    traceback.print_exc(file=sys.stderr)
                return False
        return True

    def _makeSuperTask(self, task_class, args, extra_args):
        """Make a fully-configured SuperTask instance.

        Parameters
        ----------
        task_class : `type`
            Class object for SuperTask.
        args : `argparse.Namespace`
            Parsed command line
        extra_args : `list` of `str`
            extra arguments for sub-command which were not parsed by parser

        Returns
        -------
        task
            instance of `task_class`
        task_args : `argparse.Namespace`
            command line as parsed by a task parser
        """
        # We need to make Config instance and update if from overrides.
        # To that now now we will be using argument parser that is provided
        # by task class, later we'll get rid of that parser and use better
        # method.

        # build command line
        argv = self._makeArgumentList(args, extra_args)

        # parse command line in a new parser
        parser = task_class.makeArgumentParser()
        config = task_class.ConfigClass()
        task_args = parser.parse_args(config, args=argv)

        butler = getattr(task_args, "butler", None)
        task = task_class(config=config, butler=butler)
        return task, task_args

    def doCmdLineTask(self, args, extra_args, task_class):
        """Implementation of run/show for CmdLineTask.

        Most of this is a terrible hack trying to make it look more or less
        like SuperTask.

        Parameters
        ----------
        args : `argparse.Namespace`
            Parsed command line
        extra_args : `list` of `str`
            extra arguments for sub-command which were not parsed by parser
        task_class : `type`
            Class object for CmdLineTask.
        """

        if args.do_help:
            # before dispaying help populate sub-parser with task-specific options
            self._copyParserOptions(task_class._makeArgumentParser(), args.subparser)
            args.subparser.print_help()
            return

        if args.subcommand not in ("run", "show"):
            print("unexpected command {}".format(args.subcommand),
                  file=sys.stderr)
            return 2

        if args.subcommand == "show":
            # Make sure that we have something to show
            if not args.show:
                args.show = ["config"]

        # build command line and forward it to the CmdLineTask
        argv = self._makeArgumentList(args, extra_args)

        # pass everyhting to a task class
        task_class.parseAndRun(argv)

    def _copyParserOptions(self, from_parser, to_parser):
        """Find what extra option CmdLineTask adds to a parser and add them to
        our parser too.

        This is only used for displaying help so there is no need to do it
        perfectly, best guess is OK.

        Parameters
        ----------
        from_parser : `argparse.ArgumentParser`
            Parser instantiated by a task which has extra options that we
            want to copy to destination parser.
        to_parser : `argparse.ArgumentParser`
            Parser (or subparser) to add options to.
        """

        # idea is to instantiate a standard parser and compare its options
        # to task-produced parser, this depends on knowing internal of a parser
        std_parser = pipeBaseArgParser.ArgumentParser("")
        std_options = set()
        for action in std_parser._actions:
            if action.option_strings:
                std_options |= set(action.option_strings)
            elif action.dest:
                std_options.add(action.dest)

        # check all actions (privately stored) of a task parser
        group = None
        for action in from_parser._actions:
            args = []
            if action.option_strings:
                if not (set(action.option_strings) & std_options):
                    # options are not in standard parser use them
                    args += action.option_strings
            elif action.dest not in std_options:
                args.append(action.dest)

            if args:
                kwargs = {}
                if action.nargs:
                    kwargs["nargs"] = action.nargs
                if action.help:
                    kwargs["help"] = action.help
                if action.metavar:
                    kwargs["metavar"] = action.metavar

                if not group:
                    group = to_parser.add_argument_group("task-specific options")
                group.add_argument(*args, **kwargs)

    def _makeArgumentList(self, args, extra_args):
        """Build command line suitable for Cmdline task.

        Looks at the arguments provided to activator and creates new command
        options line to pass to CmdLineTask (or to SuperTask until we switch
        to better implementation).

        Parameters
        ----------
        args : `argparse.Namespace`
            Parsed command line
        extra_args : `list` of `str`
            extra arguments for sub-command which were not parsed by activator

        Returns
        -------
        `list` of `str`
        """
        argv = []
        if args.inputRepo is not None:
            argv += [args.inputRepo]
        if args.calibRepo is not None:
            argv += ["--calib", args.inputRepo]
        if args.outputRepo is not None:
            argv += ["--output", args.outputRepo]
        if args.rerun is not None:
            argv += ["--rerun", args.rerun]
        if args.clobberOutput:
            argv += ["--clobber-output"]
        if args.clobberConfig:
            argv += ["--clobber-config"]
        if args.noBackupConfig:
            argv += ["--no-backup-config"]
        if args.clobberVersions:
            argv += ["--clobber-versions"]
        if args.noVersions:
            argv += ["--no-versions"]
        for comp, loglevel in args.loglevel:
            if comp is None:
                argv += ["--loglevel", loglevel]
            else:
                argv += ["--loglevel", comp + "=" + loglevel]
        if args.longlog:
            argv += ["--longlog"]
        if args.debug:
            argv += ["--debug"]
        if args.doraise:
            argv += ["--doraise"]
        if args.profile is not None:
            argv += ["--profile=" + args.profile]
        if args.processes != 1:
            argv += ["--processes={}".format(args.processes)]
        if args.timeout is not None:
            argv += ["--timeout={}".format(args.timeout)]
        for override in args.config_overrides:
            if override.type == "override":
                argv += ["--config", override.value]
            elif override.type == "file":
                argv += ["--configfigfile", override.value]

        show_argv = args.show[:]
        if show_argv:
            show_argv.insert(0, "--show")
            if args.subcommand == "run":
                show_argv.append("run")
            argv += show_argv

        # also add all unparsed options, let task parser handle that
        argv += extra_args

        return argv
