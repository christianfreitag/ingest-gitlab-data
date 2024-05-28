import requests
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

load_dotenv()
BASE_URL = os.getenv('GITLAB_URL')
ACCESS_TOKEN = os.getenv('GITLAB_ACESS_TOKEN')

SQL_RESERVED_WORDS = [
    'abort', 'action', 'add', 'after', 'all', 'alter', 'analyze', 'and', 'as', 'asc', 'attach', 'autoincrement',
    'before', 'begin', 'between', 'by', 'cascade', 'case', 'cast', 'check', 'collate', 'column', 'commit',
    'conflict', 'constraint', 'create', 'cross', 'current_date', 'current_time', 'current_timestamp', 'database',
    'default', 'deferrable', 'deferred', 'delete', 'desc', 'detach', 'distinct', 'drop', 'each', 'else', 'end',
    'escape', 'except', 'exclusive', 'exists', 'explain', 'fail', 'for', 'foreign', 'from', 'full', 'glob', 'group',
    'having', 'if', 'ignore', 'immediate', 'in', 'index', 'indexed', 'initially', 'inner', 'insert', 'instead',
    'intersect', 'into', 'is', 'isnull', 'join', 'key', 'left', 'like', 'limit', 'match', 'natural', 'no', 'not',
    'notnull', 'null', 'of', 'offset', 'on', 'or', 'order', 'outer', 'plan', 'pragma', 'primary', 'query', 'raise',
    'recursive', 'references', 'regexp', 'reindex', 'release', 'rename', 'replace', 'restrict', 'right', 'rollback',
    'row', 'savepoint', 'select', 'set', 'table', 'temp', 'temporary', 'then', 'to', 'transaction', 'trigger',
    'union', 'unique', 'update', 'using', 'vacuum', 'values', 'view', 'virtual', 'when', 'where', 'with', 'without'
]
RESOURCES = {
    "projects": {
        "projects": {"primary": {"project_id": 'INTEGER', "group_id": 'INTEGER'}},
        "issues": {"primary": {"id": 'INTEGER', "project_id": 'INTEGER'}},
        "pipelines": {"primary": {"id": 'INTEGER', "project_id": 'INTEGER'}},
        "releases": {"primary": {"project_id": 'INTEGER'}},
        "milestones": {"primary": {"id": 'INTEGER', "project_id": 'INTEGER'}},
        "labels": {"primary": {"id": 'INTEGER', "project_id": 'INTEGER'}},
        "members": {"primary": {"id": 'INTEGER', "project_id": 'INTEGER'}},
        "deployments": {"primary": {"id": 'INTEGER', "project_id": 'INTEGER'}},
        "variables": {"primary": {"id": 'INTEGER', "project_id": 'INTEGER'}},
        "protected_branches": {"primary": {"id": 'INTEGER', "project_id": 'INTEGER'}},
        "protected_tags": {"primary": {"id": 'INTEGER', "project_id": 'INTEGER'}},
        "boards": {"primary": {"id": 'INTEGER', "project_id": 'INTEGER'}},
        "merge_requests": {"primary": {"id": 'INTEGER', "project_id": 'INTEGER'}},
        "repository/commits": {"primary": {"id": 'VARCHAR(300)', "project_id": 'INTEGER'}},
        "repository/branches": {"primary": {"name": 'VARCHAR(300)', "project_id": 'INTEGER'}},
    },
    "groups": {
        "groups": {"primary": {"group_id": 'INTEGER'}},
        "subgroups": {"primary": {"id": 'INTEGER', "group_id": 'INTEGER'}},
        "wikis": {"primary": {"id": 'INTEGER', "group_id": 'INTEGER'}},
        "repository_files": {"primary": {"id": 'INTEGER', "group_id": 'INTEGER'}},
    },
}

def create_connection():
    connection = psycopg2.connect(
        host="127.0.0.1",
        port="54321",
        database="postgres",
        user="postgres",
        password="1qaz3edc5tgb7ujm9ol."
    )
    return connection

def get_gitlab_data(endpoint, per_page=100, page=1):
    try:
        headers = {"private-Token": ACCESS_TOKEN}
        response = requests.get(BASE_URL + endpoint, headers=headers, params={"per_page": per_page, "page": page})

        if response.status_code != 200:
            print(f"Failed to get data from {endpoint}")
            return [], 0
        else:
            data = response.json()
            total_pages = int(response.headers.get('X-Total-Pages', 0))
            return data, total_pages
    except Exception as e:
        print(f"Failed to get data from {endpoint}: {e}")
        return [], 0

def create_tables(connection, RESOURCES):
    cursor = connection.cursor()
    for resource_type, resource_info in RESOURCES.items():
        if isinstance(resource_info, dict):
            for resource, info in resource_info.items():
                table_name = f"{resource_type}_{resource}".replace("/", "_")
                columns = ', '.join([f'{k} {v}' for k, v in info['primary'].items()])
                cursor.execute(sql.SQL('''CREATE TABLE IF NOT EXISTS {} (
                    {},
                    PRIMARY KEY ({})
                    )''').format(
                        sql.Identifier(table_name),
                        sql.SQL(columns),
                        sql.SQL(', ').join(map(sql.Identifier, info['primary'].keys()))
                    ))
    connection.commit()
    cursor.close()

def add_missing_columns(cursor, table_name, column_names):
    table_name = table_name.replace("/", "_")
    cursor.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_name = {}").format(sql.Literal(table_name)))
    existing_columns = [row[0] for row in cursor.fetchall()]

    for column_name in column_names:
        if column_name not in existing_columns:
            cursor.execute(sql.SQL('ALTER TABLE {} ADD COLUMN {} TEXT').format(
                sql.Identifier(table_name),
                sql.Identifier(column_name)
            ))

def build_where_clause_and_params(primary_keys, entry):
    where_clause = sql.SQL(' AND ').join(
        sql.SQL("{} = %s").format(sql.Identifier(key)) for key in primary_keys.keys()
    )
    params = tuple(entry.get(key) for key in primary_keys.keys())
    return where_clause, params

def clean_values(entry):
    return [str(value) if isinstance(value, (dict, list)) else value for value in entry.values()]

def execute_sql(cursor, table_name, column_names, values):
    placeholders = sql.SQL(', ').join(sql.Placeholder() * len(values))
    escaped_column_names = [sql.Identifier(column) if column in SQL_RESERVED_WORDS else sql.Identifier(column) for column in column_names]
    cursor.execute(sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(escaped_column_names),
        placeholders
    ), values)

def insert_data(connection, table_name, data, RESOURCES, parent_id):
    if len(data) > 0 and isinstance(data[0], dict):
        data_parent = table_name.split("_")[0][:-1]
        sub_data = table_name.split("_", 1)[1] if "_" in table_name else table_name
        primary_from_RESOURCES = RESOURCES[(data_parent + 's')][sub_data]['primary']
        cursor = connection.cursor()

        for entry in data:

            column_names = list(entry.keys())
            if (data_parent + '_id') not in column_names:
                column_names.append((data_parent + '_id'))
            add_missing_columns(cursor, table_name, column_names)

            if (data_parent + '_id') not in entry:
                entry[(data_parent + '_id')] = parent_id

            where_clause, params = build_where_clause_and_params(primary_from_RESOURCES, entry)
            table_name = table_name.replace("/", "_")
            cursor.execute(sql.SQL("SELECT * FROM {} WHERE {}").format(
                sql.Identifier(table_name),
                where_clause
            ), params)

            if not cursor.fetchone():
                cleaned_values = clean_values(entry)
                execute_sql(cursor, table_name, column_names, cleaned_values)
            else:
                print('Item already exists')
        connection.commit()
        cursor.close()

def main():
    # Create a connection to the database
    connection = create_connection()
    create_tables(connection, RESOURCES)

    # Get projects data
    projects_data, total_pages = get_gitlab_data("projects", 100, 1)
    for page in range(2, total_pages + 1):
        projects_data += get_gitlab_data("projects", 100, page)[0]

    for proj in projects_data:
        proj['project_id'] = proj['id']
        proj['group_id'] = proj['namespace']['id']
        proj.pop('namespace', None)
    insert_data(connection, "projects_projects", projects_data, RESOURCES, None)

    # Get groups data
    groups_data, total_pages = get_gitlab_data("groups", 100, 1)
    for page in range(2, total_pages + 1):
        groups_data += get_gitlab_data("groups", 100, page)[0]

    for gp in groups_data:
        gp['group_id'] = gp['id']
        gp.pop('projects', None)
    insert_data(connection, "groups_groups", groups_data, RESOURCES, None)

    # Insert data into tables
    for resource_type, RESOURCES_list in RESOURCES.items():
        for index, resource in enumerate(RESOURCES_list):
            if(index != 0):
                if resource_type == "projects":
                    if projects_data:
                        for project in projects_data:
                            endpoint = f"projects/{project['id']}/{resource}"
                            data, _ = get_gitlab_data(endpoint)
                            if data and isinstance(data[0], dict):
                                table_name = f"projects_{resource}"
                                insert_data(connection, table_name, data, RESOURCES, project['id'])
                                print(f"Inserted {len(data)} rows in {table_name}")

                if resource_type == "groups":
                    if groups_data:
                        for group in groups_data:
                            endpoint = f"groups/{group['id']}/{resource}"
                            data, _ = get_gitlab_data(endpoint)
                            if data and isinstance(data[0], dict):
                                table_name = f"groups_{resource}"
                                insert_data(connection, table_name, data, RESOURCES, group['id'])
                                print(f"Inserted {len(data)} rows in {table_name}")

    # Close the database connection
    connection.close()

if __name__ == "__main__":
    main()
