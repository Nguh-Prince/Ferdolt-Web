from core.functions import get_dbms_booleans, get_default_schema


def get_default_schema_for_database(database):
    dbms_booleans = get_dbms_booleans(database)

    default_schema_name = get_default_schema(**dbms_booleans)

    query = database.databaseschema_set.filter(
        name__iexact=default_schema_name
    )

    if query.exists():
        return query.first()

    return None