# coding:utf-8
class ConfigurationError(Exception):
    """Exception indicating a required configuration not set .
    """

    def __init__(self, conf: str):
        """Initialize the ConfigurationError .

        :param conf: name of the configuration
        :type conf: str
        """

        self.conf = conf
        self.message = "configuration '{}' must be set".format(conf)
        super().__init__(self.message)


class EnvironmentConfigurationError(ConfigurationError):
    """Exception indicating a required configuration **environment** not set .
    """

    def __init__(self, env: str):
        """Initialize the EnvironmentConfigurationError .
        
        :param env: environment name of the configuration
        :type env: str
        """

        self.env = env
        self.message = "environment '{}' must be set".format(env)
        super().__init__(self.message)


class NotFoundError(Exception):
    """Exception indicating an object not found error
    """

    def __init__(self, typ: str, name: str):
        """Initialize the NotFoundError .

        :param typ: object type, e.g. Table Workflow
        :type typ: str
        :param name: object name
        :type name: str
        """
        self.message = "{} '{}' not found".format(typ, name)
        super().__init__(self.message)


class ParameterError(Exception):
    """Exception indicating a required parameter not valid
    """

    def __init__(self, name: str):
        """Initialize the ParameterError .

        :param name: name of the parameter
        :type name: str
        """
        self.message = "parameter '{}' invalid / not found".format(name)
        super().__init__(self.message)


class ConflictError(Exception):
    """Exception indicating a conflict
    """

    def __init__(self, name: str, msg: str = ""):
        """Initialize the ConflictError .

        :param name: name of the parameter
        :type name: str
        :param msg: conflict reason
        :type msg: str
        """
        self.message = "parameter '{}' conflicts".format(
            name) + f": {msg}" if msg else ""
        super().__init__(self.message)


class NotLoggedInError(Exception):
    """Exception indicating current user has not logged in yet
    """

    def __init__(self):
        """Initialize the NotLoggedInError .
        """
        self.message = "not logged in yet, please call bioos.login to login"
        super().__init__(self.message)
