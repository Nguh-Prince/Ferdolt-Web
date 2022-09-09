class InvalidDatabaseConnectionParameters(Exception):
    """Raised when the database connection parameters provided are invalid"""

class NotSupported(Exception):
    """Raised when an unimplemented feature is being accessed"""