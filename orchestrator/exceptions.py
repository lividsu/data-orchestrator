class OrchestratorError(Exception):
    pass


class ConnectorNotFoundError(OrchestratorError):
    pass


class ConnectorAlreadyRegisteredError(OrchestratorError):
    pass


class NotifierNotFoundError(OrchestratorError):
    pass


class NotifierAlreadyRegisteredError(OrchestratorError):
    pass


class ConnectorPingFailedError(OrchestratorError):
    pass


class CyclicDependencyError(OrchestratorError):
    pass


class TaskNotFoundError(OrchestratorError):
    pass


class TaskTimeoutError(OrchestratorError):
    pass


class ConfigValidationError(OrchestratorError):
    pass


class ConfigNotFoundError(OrchestratorError):
    pass
