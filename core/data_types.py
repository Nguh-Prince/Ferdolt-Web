data_types = { 
    "varchar", "date", "datetime", 
    "integer", "float", "double", "bigint", 
    "smallint", "boolean", "numeric", 
    "text", "char", "timestamp"
}

data_type_mappings = {
    "datetime": {
        "postgres": {
            "timestamp"
        }
    },
    "timestamp": {
        "sqlserver": {
            "datetime"
        }
    },
    "boolean": {
        "mysql": {
            "tinyint"
        }
    }
}