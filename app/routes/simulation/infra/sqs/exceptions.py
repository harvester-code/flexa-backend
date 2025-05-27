class SQSClientInitializationError(Exception):
    """Exception raised when the SQS client cannot be initialized."""

    pass


class SQSMessageSendError(Exception):
    """Exception raised when sending a message to SQS fails."""

    pass
