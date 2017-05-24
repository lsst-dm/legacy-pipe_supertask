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
Module defining CmdLineParser class and related methods.
"""

#--------------------------------
#  Imports of standard modules --
#--------------------------------
from argparse import Action, ArgumentParser, RawDescriptionHelpFormatter
import collections
import itertools
import re
import textwrap

#-----------------------------
# Imports for other modules --
#-----------------------------
import lsst.pipe.base.argumentParser as base_parser

# "exported" names
__all__ = ['makeParser']

#----------------------------------
# Local non-exported definitions --
#----------------------------------

# Class which keeps a single override, each -c and -C option is converted into
# instance of this class and are put in a single ordered collection so that
# overrides can be applied later.
_Override = collections.namedtuple("_Override", "type,value")


class _LogLevelAction(Action):
    """Action class which collects logging levels.

    This action class collects arguments in the form "LEVEL" or
    "COMPONENT=LEVEL" where LEVEL is the name of the logging level (case-
    insensitive). It converts the series of arguments into the list of
    tuples (COMPONENT, LEVEL). If component name is missing then first
    item in tuple is set to `None`. Second item in tuple is converted to
    upper case.
    """

    permittedLevels = set(['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'])

    def __call__(self, parser, namespace, values, option_string=None):
        """Re-implementation of the base class method.

        See `argparse.Action` documentation for parameter description.
        """
        dest = getattr(namespace, self.dest)
        if dest is None:
            dest = []
            setattr(namespace, self.dest, dest)
        for componentLevel in values:
            component, sep, levelStr = componentLevel.partition("=")
            if not levelStr:
                levelStr, component = component, None
            logLevelUpr = levelStr.upper()
            if logLevelUpr not in self.permittedLevels:
                parser.error("loglevel=%s not one of %s" % (levelStr, tuple(self.permittedLevels)))
            dest.append((component, logLevelUpr))


class _AppendFlattenAction(Action):
    """Action class which appends all items of multi-value option to the
    same destination.

    This is different from standard 'append' action which does not flatten
    the option values.
    """

    def __call__(self, parser, namespace, values, option_string=None):
        """Re-implementation of the base class method.

        See `argparse.Action` documentation for parameter description.
        """
        dest = getattr(namespace, self.dest)
        if dest is None:
            dest = []
            setattr(namespace, self.dest, dest)
        dest += values

# copied with small mods from pipe.base.argumentParser
class _IdValueAction(Action):
    """argparse action callback to process a data ID into a dict
    """

    def __call__(self, parser, namespace, values, option_string):
        """Parse dataId option values

        The option value format is:
        key1=value1_1[^value1_2[^value1_3...] key2=value2_1[^value2_2[^value2_3...]...

        The values (e.g. value1_1) may either be a string, or of the form "int..int" (e.g. "1..3")
        which is interpreted as "1^2^3" (inclusive, unlike a python range). So "0^2..4^7..9" is
        equivalent to "0^2^3^4^7^8^9".  You may also specify a stride: "1..5:2" is "1^3^5"

        The cross product is computed for keys with multiple values. For example:
            --id visit 1^2 ccd 1,1^2,2
        results in the following data ID dicts being appended to namespace.\<argument>.idList:
            {"visit":1, "ccd":"1,1"}
            {"visit":2, "ccd":"1,1"}
            {"visit":1, "ccd":"2,2"}
            {"visit":2, "ccd":"2,2"}

        Option values are store in the dict were the key is the value of
        --type option used before --id and the value is the list of dicts
        corresponding to each --id option values.

        Parameters
        ----------
        parser : argparse.ArgumentParser
            argument parser
        namespace : argparse.Namespace
            parsed command
        values : list
            a list of data IDs; see option value format
        option_string : str
            option name specified by the user
        """

        # need --type option before --id
        if not hasattr(namespace, "_data_type"):
            parser.error("--type option must be defined in parser")
        if namespace._data_type is None:
            parser.error("%s option requires --type to be used before it" % (option_string))

        dest = getattr(namespace, self.dest)
        if dest is None:
            dest = {}
            setattr(namespace, self.dest, dest)

        idDict = collections.OrderedDict()
        for nameValue in values:
            name, sep, valueStr = nameValue.partition("=")
            if name in idDict:
                parser.error("%s appears multiple times in one ID argument: %s" % (name, option_string))
            idDict[name] = []
            for v in valueStr.split("^"):
                mat = re.search(r"^(\d+)\.\.(\d+)(?::(\d+))?$", v)
                if mat:
                    v1 = int(mat.group(1))
                    v2 = int(mat.group(2))
                    v3 = mat.group(3)
                    v3 = int(v3) if v3 else 1
                    for v in range(v1, v2 + 1, v3):
                        idDict[name].append(str(v))
                else:
                    idDict[name].append(v)

        iterList = [idDict[key] for key in idDict.keys()]
        idDictList = [collections.OrderedDict(zip(idDict.keys(), valList))
                      for valList in itertools.product(*iterList)]

        dest.setdefault(namespace._data_type, []).extend(idDictList)

def _config_override(value):
    """This callable is used as "type" for -c option.
    """
    return _Override("override", value)


def _config_file(value):
    """This callable is used as "type" for -C option.
    """
    return _Override("file", value)


_epilog = """\
Notes:
  * --config, --configfile, --id, --loglevel and @file may appear multiple times;
    all values are used, in order left to right
  * @file reads command-line options from the specified file:
    * data may be distributed among multiple lines (e.g. one option per line)
    * data after # is treated as a comment and ignored
    * blank lines and lines starting with # are ignored
  * To specify multiple values for an option, do not use = after the option name:
    * right: --configfile foo bar
    * wrong: --configfile=foo bar
"""

#------------------------
# Exported definitions --
#------------------------
DEFAULT_INPUT_NAME = base_parser.DEFAULT_INPUT_NAME
DEFAULT_CALIB_NAME = base_parser.DEFAULT_CALIB_NAME
DEFAULT_OUTPUT_NAME = base_parser.DEFAULT_OUTPUT_NAME


def makeParser(fromfile_prefix_chars='@', parser_class=ArgumentParser, **kwargs):
    """Make instance of command line parser for
    :py:`lsst.pipe.supertask.CmdLineActivator`.

    Creates instance of parser populated with all options that are supported
    by command line activator. There is no additional logic in this class,
    all semantics is handled by the activator class.

    Parameters
    ----------
    fromfile_prefix_chars : `str`, optional
        Prefix for arguments to be used as options files (default: `@`)
    parser_class : `type`, optional
        Specifiest the class of the argument parser, by default
        `argparse.ArgumentParser` is used.
    kwargs : extra keyword arguments
        Passed directly to `parser_class` constructor

    Returns
    -------
    instance of `parser_class`
    """

    parser = parser_class(usage="%(prog)s [global-options] subcommand [command-options]",
                          fromfile_prefix_chars=fromfile_prefix_chars,
                          epilog=_epilog,
                          formatter_class=RawDescriptionHelpFormatter,
                          **kwargs)

    # global options which come before sub-command

    group = parser.add_argument_group("Task search options")
    group.add_argument("-p", "--package", action="append", dest="packages", default=[],
                       metavar="NAME1.NAME2.NAME3",
                       help=("Package to search for task classes. Package name is specified as "
                             "dot-separated names found in $PYTHON PATH (e.g. lsst.pipe.tasks). "
                             "It should not include module name. This option overrides default "
                             "built-in list of modules. It can be used multiple times."))

    # output options
    group = parser.add_argument_group("Data selection options")
    group.add_argument("--type", action="store", dest="_data_type", default=None,
                       help=("Defines datatype name for next --id options, must be used before "
                             "any --id option appears"))
    group.add_argument("--id", nargs="+", dest="id", action=_IdValueAction,
                       metavar="KEY=VALUE1[^VALUE2[^VALUE3...]",
                       help="Specifies Data ID, --type option is required before --id.")

    # repository options
    group = parser.add_argument_group('Data repository options')
    group.add_argument("-i", "--input", metavar="INPUT", dest="inputRepo",
                       help="path to input data repository, relative to ${}".format(DEFAULT_INPUT_NAME))
    group.add_argument("--calib", dest="calibRepo",
                       help="path to input calibration repository, "
                       "relative to ${}".format(DEFAULT_CALIB_NAME))
    group.add_argument("--output", dest="outputRepo",
                       help="path to output data repository (need not exist), "
                       "relative to ${}".format(DEFAULT_OUTPUT_NAME))
    group.add_argument("--rerun", dest="rerun", metavar="[INPUT:]OUTPUT",
                       help="rerun name: sets OUTPUT to ROOT/rerun/OUTPUT; "
                       "optionally sets ROOT to ROOT/rerun/INPUT")
    group.add_argument("--clobber-output", action="store_true", dest="clobberOutput", default=False,
                       help=("remove and re-create the output directory if it already exists "
                             "(safe with -j, but not all other forms of parallel execution)"))

    # output options
    group = parser.add_argument_group("Meta-information output options")
    group.add_argument("--clobber-config", action="store_true", dest="clobberConfig", default=False,
                       help=("backup and then overwrite existing config files instead of checking them "
                             "(safe with -j, but not all other forms of parallel execution)"))
    group.add_argument("--no-backup-config", action="store_true", dest="noBackupConfig", default=False,
                       help="Don't copy config to file~N backup.")
    group.add_argument("--clobber-versions", action="store_true", dest="clobberVersions", default=False,
                       help=("backup and then overwrite existing package versions instead of checking"
                             "them (safe with -j, but not all other forms of parallel execution)"))
    group.add_argument("--no-versions", action="store_true", dest="noVersions", default=False,
                       help="don't check package versions; useful for development")

    # logging/debug options
    group = parser.add_argument_group("Execution and logging options")
    group.add_argument("-L", "--loglevel", nargs="+", action=_LogLevelAction, default=[],
                       help="logging level; supported levels are [trace|debug|info|warn|error|fatal]",
                       metavar="LEVEL|COMPONENT=LEVEL")
    group.add_argument("--longlog", action="store_true", help="use a more verbose format for the logging")
    group.add_argument("--debug", action="store_true", help="enable debugging output?")
    group.add_argument("--doraise", action="store_true",
                       help="raise an exception on error (else log a message and continue)?")
    group.add_argument("--profile", metavar="PATH", help="Dump cProfile statistics to filename")

    # parallelism options
    group.add_argument("-j", "--processes", type=int, default=1, help="Number of processes to use")
    group.add_argument("-t", "--timeout", type=float,
                       help="Timeout for multiprocessing; maximum wall time (sec)")

    # define sub-commands
    subparsers = parser.add_subparsers(dest="subcommand",
                                       title="commands",
                                       description=("Valid commands, use `<command> --help' to get "
                                                    "more info about each command:"),
                                       prog=parser.prog)
    # Python3 workaround, see http://bugs.python.org/issue9253#msg186387
    # The issue was fixed in Python 3.6, workaround is not need starting with that version
    subparsers.required = True

    # list sub-command
    subparser = subparsers.add_parser("list",
                                      usage="%(prog)s [options]",
                                      description="Display information about tasks and where they are "
                                      "found. If none of the optios are specified then --super-tasks "
                                      "is used by default")
    subparser.set_defaults(show=[], subparser=subparser)
    subparser.add_argument("-P", "--packages", dest="show", action="append_const", const="packages",
                           help="Shows list of the packages to search for tasks")
    subparser.add_argument("-m", "--modules", dest="show", action="append_const", const='modules',
                           help="Shows list of all modules existing in current list of packages")
    subparser.add_argument("-t", "--tasks", dest="show", action="append_const", const="tasks",
                           help="Shows list of all tasks (any sub-class of Task) existing"
                           " in current list of packages")
    subparser.add_argument("-s", "--super-tasks", dest="show", action="append_const", const="super-tasks",
                           help="(default) Shows list of all super-tasks existing in current set of packages")
    subparser.add_argument("--no-headers", dest="show_headers", action="store_false", default=True,
                           help="Do not display any headers on output")

    for subcommand in ("show", "run"):
        # show/run sub-commands, they are all identical except for the
        # command itself and description

        if subcommand == "show":
            description = textwrap.dedent("""\
                Display various information about given task. By default all information is
                displayed, use options to select only subset of the information.

                Note: Specify task name with --help to also see task-specific options""")
        else:
            description = textwrap.dedent("""\
                Execute specified task.

                Note: Specify task name with --help to also see task-specific options""")

        subparser = subparsers.add_parser(subcommand,
                                          add_help=False,
                                          usage="%(prog)s taskname [options]",
                                          description=description,
                                          epilog=_epilog,
                                          formatter_class=RawDescriptionHelpFormatter)
        subparser.set_defaults(config_overrides=[], subparser=subparser)
        subparser.add_argument("taskname", nargs="?", default=None,
                               help="Name of the task to execute. This can be a simple name without dots "
                               "which will be found in one of the modules located in the known "
                               "packages or packages specified in --package option. If name contains dots "
                               "it is assumed to be a fully qualified name of a class found in $PYTHONPATH")
        subparser.add_argument("-h", "--help", action="store_true", dest="do_help", default=False,
                               help="Show this help message and exit")
        subparser.add_argument("-c", "--config", dest="config_overrides", nargs="+",
                               action=_AppendFlattenAction, type=_config_override,
                               help="Configuration override(s), e.g. -c foo=newfoo bar.baz=3",
                               metavar="NAME=VALUE")
        subparser.add_argument("-C", "--configfile", dest="config_overrides", nargs="+",
                               action=_AppendFlattenAction, type=_config_file,
                               metavar="PATH", help="Configuration override file(s)")
        subparser.add_argument("--show", metavar="ITEM|ITEM=VALUE", nargs="+", default=[],
                               help="Dump various info to standard output. Possible items are: "
                               "`config', `config=<PATTERN>' or `config=<PATTERN>:NOIGNORECASE' "
                               "to dump configuration possibly matching given pattern; "
                               "`history=<FIELD>' to dump configuration history for a field, "
                               "field name is specified as Task.SubTask.Field; "
                               "`data' to show information about data in butler; "
                               "`tasks' to show task composition.")

    return parser
