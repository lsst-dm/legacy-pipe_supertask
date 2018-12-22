"""Module which defines ConfigOverrides class and related methods.
"""

__all__ = ["ConfigOverrides"]

from builtins import object
import ast

from lsst.pipe.base import PipelineTaskConfig
import lsst.pex.config as pexConfig
import lsst.pex.exceptions as pexExceptions


class ConfigOverrides(object):
    """Defines a set of overrides to be applied to a task config.

    Overrides for task configuration need to be applied by activator when
    creating task instances. This class represents an ordered set of such
    overrides which activator receives from some source (e.g. command line
    or some other configuration).

    Methods
    ----------
    addFileOverride(filename)
        Add overrides from a specified file.
    addValueOverride(field, value)
        Add override for a specific field.
    applyTo(config)
        Apply all overrides to a `config` instance.

    Notes
    -----
    Serialization support for this class may be needed, will add later if
    necessary.
    """

    def __init__(self):
        self._overrides = []

    def addFileOverride(self, filename):
        """Add overrides from a specified file.

        Parameters
        ----------
        filename : str
            Path to the override file.
        """
        self._overrides += [('file', filename)]

    def addValueOverride(self, field, value):
        """Add override for a specific field.

        This method is not very type-safe as it is designed to support
        use cases where input is given as string, e.g. command line
        activators. If `value` has a string type and setting of the field
        fails with `TypeError` the we'll attempt `eval()` the value and
        set the field with that value instead.

        Parameters
        ----------
        field : str
            Fully-qualified field name.
        value :
            Value to be given to a filed.
        """
        self._overrides += [('value', (field, value))]

    def addDatatypeNameSubstitution(self, nameDictString):
        """Add keys and values to be used in formatting config nameTemplates

        This method takes in a dictionary passed in on the command line as
        a string in the format of key:value which will be used to format
        fields in all of the nameTemplates found in a config object. I.E.
        a nameDictString = {'input': deep} would be used to format a
        nameTemplate of "{input}CoaddDatasetProduct".

        Parameters
        ----------
        nameDictString : str
            String formatted as a python dict used in formatting nameTemplates
        """
        self._overrides += [('namesDict', (nameDictString))]

    def applyTo(self, config):
        """Apply all overrides to a task configuration object.

        Parameters
        ----------
        config : `pex.Config`

        Raises
        ------
        `Exception` is raised if operations on configuration object fail.
        """
        for otype, override in self._overrides:
            if otype == 'file':
                config.load(override)
            elif otype == 'value':
                field, value = override
                field = field.split('.')
                # find object with attribute to set, throws if we name is wrong
                obj = config
                for attr in field[:-1]:
                    obj = getattr(obj, attr)
                # If the type of the object to set is a list field, the value to assign
                # is most likely a list, and we will eval it to get a python list object
                # which will be used to set the objects value
                # This must be done before the try, as it will otherwise set a string which
                # is a valid iterable object when a list is the intended object
                if isinstance(getattr(obj, field[-1]), pexConfig.listField.List) and isinstance(value, str):
                    try:
                        value = eval(value, {})
                    except Exception:
                        # Something weird happened here, try passing, and seeing if further
                        # code can handle this
                        raise pexExceptions.RuntimeError(f"Unable to parse {value} into a valid list")
                try:
                    setattr(obj, field[-1], value)
                except TypeError:
                    if not isinstance(value, str):
                        raise
                    # this can throw
                    value = eval(value, {})
                    setattr(obj, field[-1], value)
            elif otype == 'namesDict':
                try:
                    parsedNamesDict = ast.literal_eval(override)
                    if not isinstance(parsedNamesDict, dict):
                        raise ValueError()
                except ValueError:
                    raise pexExceptions.RuntimeError(f"Unable parse --dataset-name-substitution {override} "
                                                     "into a valid dict")
                if not isinstance(config, PipelineTaskConfig):
                    raise pexExceptions.RuntimeError("Dataset name substitution can only be used on Tasks "
                                                     "with a ConfigClass that is a subclass of "
                                                     "PipelineTaskConfig")
                config.formatTemplateNames(parsedNamesDict)
