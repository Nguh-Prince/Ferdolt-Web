class InvalidDatabaseConnectionParameters(Exception):
    """Raised when the database connection parameters provided are invalid"""

class NotSupported(Exception):
    """Raised when an unimplemented feature is being accessed"""

class InvalidDatabaseStructure(Exception):
    """Raised when a target database has an invalid structure e.g. a table without a primary key"""