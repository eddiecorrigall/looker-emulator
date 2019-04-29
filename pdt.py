import re
import os
import sys
import yaml
import pickle
import logging

import psycopg2  # force required driver to import

from collections import defaultdict
from datetime import datetime

from sqlalchemy import create_engine


logging.basicConfig(filename='looker_emulator.log', level=logging.INFO)

log_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stdout_log_handler = logging.StreamHandler(sys.stdout)
stdout_log_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.addHandler(stdout_log_handler)
logger.setLevel(logging.DEBUG)


DATABASE_URI_FORMAT = (
    '{protocol}://{username}:{password}@{hostname}:{port}/{database}')


class LookerView(object):
    def __init__(self, looker_context, view_object):
        if 'view' not in view_object:
            raise Exception(
                'Invalid view object: contains no view key-value pair!')
        self.looker_context = looker_context
        self.view_object = view_object
        # Update/register lookml context parameters...
        sql_table_name_parameter = '{}.SQL_TABLE_NAME'.format(self.view_name)
        self.looker_context.sql_table_name_parameters[
            sql_table_name_parameter] = '{schema}.{table}'.format(
                schema=self.sql_schema_name,
                table=self.sql_table_name)

    @property
    def _pickle_path(self):
        return '{}.pickle'.format(self.view_name)

    @property
    def trigger_value(self):
        try:
            with open(self._pickle_path, 'rb') as file:
                return pickle.load(file)
        except FileNotFoundError:
            return None

    @trigger_value.setter
    def trigger_value(self, x):
        with open(self._pickle_path, 'wb') as file:
            pickle.dump(x, file)

    @property
    def view_name(self):
        return self.view_object['view']

    @property
    def is_derived_table(self):
        return 'derived_table' in self.view_object

    @property
    def is_persisted_derived_table(self):
        return (
            self.is_derived_table and
            'sql_trigger_value' in self.view_object['derived_table'])

    @property
    def sql_schema_name(self):
        if self.is_derived_table:
            return 'looker_scratch'
        else:
            return self.looker_context.default_schema

    @property
    def sql_table_name(self):
        if self.is_derived_table:
            return '_{}'.format(self.view_name)
        else:
            return self.view_object['sql_table_name']

    @property
    def view_dependencies(self):
        if not self.is_derived_table:
            return set()
        view_dependencies = set()
        if self.is_persisted_derived_table:
            sql = self.view_object['derived_table']['sql']
            for match in re.finditer("\${\s*(.+)\s*}", sql):
                param = match.group(1)
                view_name, _ = param.split('.')
                view_dependencies.add(self.looker_context.views[view_name])
        return view_dependencies

    def _regenerate_derived_table_sql(self, sql):
        # TODO: Make this configurable, can this instead go in a file?
        if self.looker_context.sql_dialect == 'postgresql':
            return """
                DROP TABLE IF EXISTS {sql_schema_name}.{sql_table_name};
                CREATE TABLE {sql_schema_name}.{sql_table_name}
                    DISTSTYLE {distribution_style}
                    {interleaved_sortkey} AS {sql};
            """.format(
                sql_schema_name=self.sql_schema_name,
                sql_table_name=self.sql_table_name,
                distribution_style=self.view_object['derived_table'].get(
                    'distribution_style', 'ALL'),
                interleaved_sortkey=(
                    'INTERLEAVED SORTKEY({indexes})'.format(
                        indexes=','.join(
                            self.view_object['derived_table']['indexes']))
                    if 'indexes' in self.view_object['derived_table'] else
                    str()
                ),
                sql=sql)
        else:
            raise Exception('Unknown SQL dialect {}'.format(
                self.looker_context.sql_dialect))

    @property
    def regenerate_derived_table_sql(self):
        # TODO: Handle filter templates too
        # TODO: Regex parsing: parse view name, and property for object lookup
        if self.is_persisted_derived_table:
            sql = self.view_object['derived_table']['sql']
            # Python use single % for formating string
            # Replace % with %% to prevent SQLAlchemy errors
            sql = sql.replace('%', '%%')
            # Iterate over all parameters, and
            # replace any non-sql LookML patterns
            for name, value in self.looker_context.all_parameters.items():
                # TODO: Need to consider whitespace
                pattern = '${{{0}}}'.format(name)
                if pattern in sql:
                    sql = sql.replace(pattern, value)
            return self._regenerate_derived_table_sql(sql)
        else:
            raise Exception(
                'Looker view {} is not a derived_table'.format(self.view_name))


class LookerContext(object):
    views = dict()
    sql_table_name_parameters = dict()

    def __init__(self, sql_dialect, database_uri_config):
        self.sql_dialect = sql_dialect
        self.database_uri_config = database_uri_config
        self.engine = create_engine(
            DATABASE_URI_FORMAT.format(**database_uri_config))

    @property
    def all_parameters(self):
        return dict(
            [],
            **self.sql_table_name_parameters)

    def load_lookml(self, lookml_yaml_path):
        logger.info('Loading lookml yaml path: {}'.format(lookml_yaml_path))
        with open(lookml_yaml_path) as file:
            for view_object in yaml.load(file):
                if 'view' in view_object:
                    view = LookerView(
                        looker_context=self, view_object=view_object)
                    self.views[view.view_name] = view

    def get_latest_sql_trigger_value(self, connection, view):
        result = connection.execute(
            view.view_object['derived_table']['sql_trigger_value'])
        if result.rowcount > 0:
            # first row, first column
            for row in result:
                return row[0]
        else:
            raise Exception(
                'Looker `sql_trigger_value` query did not return any value')

    def regenerate_persisted_derived_table(self, connection, view):
        connection.execute(view.regenerate_derived_table_sql)

    @property
    def view_topological_order(self):
        # Generate an adjacency list
        adjacency_lists = defaultdict(list)
        for view in self.views.values():
            for view_dependency in view.view_dependencies:
                adjacency_lists[view.view_name].append(
                    view_dependency.view_name)
        # Perform Depth-first Search Topological Ordering
        ordered_view_names = list()
        permanent_visit_set = set()
        temporary_visit_set = set()

        def visit(view_name):
            if view_name in permanent_visit_set:
                return
            if view_name in temporary_visit_set:
                raise Exception(
                    'Views contain a circular dependency {}'.format(
                        view_name))
            temporary_visit_set.add(view_name)
            for adjacent_view_name in adjacency_lists[view_name]:
                visit(adjacent_view_name)
            temporary_visit_set.remove(view_name)
            permanent_visit_set.add(view_name)
            ordered_view_names.insert(0, view_name)
        # Entry point for topological ordering
        while True:
            done = True
            for view in self.views.values():
                if view.view_name in permanent_visit_set:
                    pass
                else:
                    visit(view.view_name)
            if done:
                break
        # Return the ordered view names
        ordered_view_names.reverse()
        return ordered_view_names

    @property
    def default_schema(self):
        if self.sql_dialect == 'postgresql':
            return 'public'
        raise Exception('Unknown SQL dialect {}'.format(self.sql_dialect))

    def _has_table_sql(self, table, schema):
        if self.sql_dialect == 'postgresql':
            return """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table}'
                );
            """.format(schema=schema, table=table)
        raise Exception('Unknown SQL dialect {}'.format(self.sql_dialect))

    def has_table(self, connection, table, schema):
        result = connection.execute(self._has_table_sql(table, schema))
        if result.rowcount > 0:
            for row in result:
                return row[0]
        else:
            raise Exception('Has table sql returned no result')

    def _create_schema(self, schema):
        if self.sql_dialect == 'postgresql':
            return """
                CREATE SCHEMA IF NOT EXISTS {schema};
            """.format(schema=schema)
        raise Exception('Unknown SQL dialect {}'.format(self.sql_dialect))

    def create_schema(self, connection, schema):
        connection.execute(self._create_schema(schema))

    def create_looker_scratch_schema(self, connection):
        self.create_schema(connection, 'looker_scratch')

    def trigger_view(self, connection, view):
        if not view.is_persisted_derived_table:
            raise Exception(
                'View cannot be triggered: its not a persisted derived table!')
        try:
            has_table = self.has_table(
                connection, view.sql_table_name, view.sql_schema_name)
            if has_table:
                # Query the sql_trigger_value
                latest_trigger_value = self.get_latest_sql_trigger_value(
                    connection, view)
                # Determine if the derived table needs to be regenerated
                if view.trigger_value is not None:
                    if view.trigger_value == latest_trigger_value:
                        logger.info('Trigger value has not yet changed for view {}'.format(view.view_name))
                        return

            logger.info(
                'Attempting to regenerate view {}'.format(view.view_name))
            # Regenerate the derived table
            start_time = datetime.now()
            self.regenerate_persisted_derived_table(connection, view)
            # Get elapsed time
            end_time = datetime.now()
            elapsed_time = end_time - start_time
            # Update the trigger value
            view.trigger_value = latest_trigger_value
            # ...
            logger.info(
                'Successfully regenerated derived table {} with elapsed time of {}'.format(
                    view.view_name, elapsed_time))
        except Exception as e:
            # Since these PDTs have a unknown dependency,
            # then the program will attempt to regenerate in no specific order,
            # however, this will obviously cause failures.
            logger.error('Looker regenerate view failed for {}'.format(
                view.view_name), e)
            raise

    def trigger_all(self):
        # Determine dependency order
        view_name_order = self.view_topological_order
        logger.info(
            'Triggering views in the following order: {}'.format(
                view_name_order))
        # ...
        with self.engine.connect() as connection:
            try:
                # Ensure looker_scratch schema exists
                self.create_looker_scratch_schema(connection)
                # For each view, regenerate persisted derived tables
                for view_name in view_name_order:
                    view = self.views[view_name]
                    if not view.is_persisted_derived_table:
                        continue
                    transaction = connection.begin()
                    self.trigger_view(connection, view)
                    transaction.commit()
            except Exception as e:
                logger.error('Failed to trigger all views', e)
                transaction.rollback()


def trigger_all(connection, lookml_directory):
    logger.info('Connecting to {}'.format(connection['name']))

    engine_uri_config = dict(
        protocol='{dialect}+{driver}'.format(
            dialect=connection['dialect'],
            driver=connection['driver']),
        username=connection['username'],
        password=connection['password'],
        hostname=connection['hostname'],
        port=connection.get('port', 5439),
        database=connection['database'])

    looker = LookerContext(
        sql_dialect=connection['dialect'],
        database_uri_config=engine_uri_config)

    for filename in os.listdir(lookml_directory):
        if filename.endswith('.lookml'):
            lookml_path = os.path.join(lookml_directory, filename)
            looker.load_lookml(lookml_path)

    looker.trigger_all()


if __name__ == '__main__':
    # TODO: user click for CLI
    connections_path = sys.argv[1]
    lookml_directory = sys.argv[2]

    with open(connections_path) as file:
        for connection in yaml.load(file):
            trigger_all(
                connection=connection,
                lookml_directory=lookml_directory)
