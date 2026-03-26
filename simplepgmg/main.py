"""A module containing the functions that make up simplepgmg, including the main CLI function."""
import argparse
import datetime
import hashlib
import os
import psycopg
import re
from typing import List, Tuple


def get_database_creds() -> Tuple[str, str]:
    username = input('Database username: ')
    password = input(f'Password for user {username}: ')
    return username, password


def is_valid_version_str(version_str: str) -> bool:
    return bool(re.match(r'^V(\d+\.){3}\d+$', version_str))


def is_greater_version(version1: str, version2: str) -> bool:
    numbers1 = tuple(map(int, version1.replace('V', '').split('.')))
    numbers2 = tuple(map(int, version2.replace('V', '').split('.')))
    for i in range(len(numbers1)):
        if numbers1[i] > numbers2[i]:
            return True

    return False


def get_migration_file_list(directory: str) -> List[str]:
    if not os.path.isdir(directory):
        raise OSError(f"directory '{directory}' does not exist.")

    files = []
    pattern = re.compile(r'^V(\d+\.){3}\d+__.+\.sql$')
    for file in os.listdir(directory):
        if pattern.match(file):
            files.append(file)

    # Sorting files in ascending order by their version tags
    files = sorted(files, key=lambda x: tuple(map(int, x.split('__', 1)[0].replace('V', '').split('.'))))

    # Adding the directory path to each file name and looking for files with the same version tag
    if len(directory) >= 1 and (directory[-1] == '/' or directory[-1] == '\\'):
        directory = directory[:-1]

    prev_tag = ''
    for i in range(len(files)):
        tag = files[i].split('__', 1)[0]
        if tag == prev_tag:
            raise ValueError(f'multiple migration files have version {tag}.')

        prev_tag = tag
        files[i] = f'{directory}/{files[i]}'

    return files


def get_file_version(migration_file: str) -> str:
    return migration_file.split('/')[-1].split('__', 1)[0]


def get_migration(migration_file: str) -> Tuple[str, str, str]:
    intermediate = migration_file.split('/')[-1].split('__', 1)
    with open(migration_file, 'r') as file:
        migration_text = file.read()

    # Returning: migration version, migration name, full migration file text
    return intermediate[0], intermediate[1].replace('.sql', ''), migration_text


def get_migration_hash(migration_text: str) -> str:
    return hashlib.sha256(migration_text.encode('utf-8')).hexdigest()


def get_migration_blocks(migration_text: str) -> Tuple[str, str]:
    # Getting the raw text of each block
    halves = migration_text.split('--down', 1)
    # Raising the error if the down block is missing
    if len(halves) != 2:
        raise SyntaxError('no down block in file.')

    # Stripping irrelevant characters out of each block
    up_block = re.sub('--.*\n', '', halves[0]).replace('\n', ' ').replace('\t', '')
    down_block = re.sub('--.*\n', '', halves[1]).replace('\n', ' ').replace('\t', '')
    return up_block, down_block


def get_last_applied_migration(database: str, username: str, password: str) -> Tuple[str, str, datetime.datetime]:
    """A function that retrieves info for the migration last applied to the given database.

    Parameters
    ----------
    database : str
        The name of the database to get migration records from.
    username : str
        The user to connect to the database as. Ideally the owner of the database.
    password : str
        The password of the database user.

    Returns
    -------
    tuple[str, str, datetime.datetime]
        Two strings and a datetime object. In order: the version of the last applied migration, its name, and when it
        was applied.

    Raises
    ------
    RuntimeError
        If the migration schema and/or table don't exist, or if no migration records exist.
    psycopg.Error
        And its subclasses, on database-related errors.

    """

    with (psycopg.connect(f'dbname={database} user={username} password={password} connect_timeout=5', autocommit=True)
          as conn):
        with conn.cursor() as cur:
            try:
                cur.execute("""
                SELECT
                    version,
                    name,
                    applied_timestamp
                FROM migrations.migration_history
                ORDER BY applied_timestamp DESC
                LIMIT 1
                """)
                result = cur.fetchone()
                if result is None:
                    raise RuntimeError('no applied migrations were found.')

                return result[0], result[1], result[2]
            except psycopg.errors.UndefinedTable:
                raise RuntimeError('migration schema and/or table do not exist. Please apply at least one migration.')


def apply_migrations(migration_path: str, database: str, username: str, password: str,
                     version_target: str | None = None, **kwargs: bool) -> None:
    """A function that applies all new migrations to the given database up to and including the given version target.

    Parameters
    ----------
    migration_path : str
        The absolute path to the migration files.
    database : str
        The name of the database that the migrations will be applied to.
    username : str
        The user to connect to the database as. Ideally the owner of the database.
    password : str
        The password of the database user.
    version_target : str | None
        Optional. A version tag of the form V.x.x.x.x. migrations up to and including this version will be applied.
        If unspecified, all new migrations will be applied.

    Raises
    ------
    ValueError
        If the specified version target is not in the expected format.
    OSError
        If the specified migration path is not an existing directory.
    FileNotFoundError
        If the migration path directory is empty.
    SyntaxError
        If one or more migration files aren't formatted properly into up and down blocks.
    RuntimeError
        If some kind of issue occurred when attempting to verify or apply migrations.
    psycopg.Error
        And its subclasses, on database-related errors.

    """

    feedback=False
    if 'feedback' in kwargs:
        if kwargs['feedback']:
            feedback = True

    if version_target is not None and not is_valid_version_str(version_target):
        raise ValueError('version target is not in the expected format (Vx.x.x.x).')

    migration_files = get_migration_file_list(migration_path)
    if len(migration_files) == 0:
        raise FileNotFoundError('no migration files found.')

    # Making sure that the version target always maps to an existing migration file
    if version_target is None:
        version_target = get_file_version(migration_files[-1])  # The newest migration version
    else:
        for i in range(len(migration_files) - 1, -1, -1):
            version = get_file_version(migration_files[i])
            if version <= version_target:
                version_target = version
                break

            if i == 0:
                if feedback:
                    print('No new migration(s) to apply')

                return

    with (psycopg.connect(f'dbname={database} user={username} password={password} connect_timeout=5', autocommit=True)
          as conn):
        with conn.cursor() as cur:
            # Checking to see if the version target and/or above have already been applied
            try:
                cur.execute("""
                SELECT
                    version
                FROM migrations.migration_history
                WHERE version = %s
                """, (version_target,))
                if cur.fetchone() is not None:
                    if feedback:
                        print('No new migration(s) to apply')

                    return

            # Making the migrations schema and table if they aren't defined
            except psycopg.errors.UndefinedTable:
                conn.rollback()
                with conn.transaction():
                    cur.execute("""
                    CREATE SCHEMA IF NOT EXISTS migrations;
                    CREATE TABLE IF NOT EXISTS migrations.migration_history(
                        version TEXT UNIQUE NOT NULL,
                        name TEXT NOT NULL,
                        applied_timestamp TIMESTAMPTZ NOT NULL,
                        checksum TEXT NOT NULL)
                    """)

            start_index = 0

            # Checking to make sure that the migration chain is intact (i.e. that all previously applied migration files
            #   haven't been altered and are still present, and that no new ones have been inserted in between old ones)
            cur.execute("""
            SELECT
                version,
                name,
                checksum
            FROM migrations.migration_history
            ORDER BY applied_timestamp
            """)
            applied_migrations = cur.fetchall()
            if applied_migrations is not None:
                for i in range(len(applied_migrations)):
                    applied_version, applied_name, applied_checksum = applied_migrations[i]
                    if i < len(migration_files):
                        version, name, text = get_migration(migration_files[i])
                        if applied_version != version:
                            if applied_version < version:
                                raise RuntimeError(f'the file for previously applied migration '
                                                   f'{applied_version}__{applied_name} is missing.')
                            else:
                                raise RuntimeError(f'migration {version}__{name} was never applied.')

                        checksum = get_migration_hash(text)
                        if applied_checksum != checksum:
                            raise RuntimeError(f'file for migration {version}__{name} has changed since migration was '
                                               f'applied.')
                    else:
                        raise RuntimeError(f'the file for previously applied migration '
                                           f'{applied_version}__{applied_name} is missing.')

                    start_index += 1

            # Applying new migrations
            for i in range(start_index, len(migration_files)):
                version, name, text = get_migration(migration_files[i])
                if is_greater_version(version, version_target):
                    break

                if feedback:
                    print(f'Applying migration {version}__{name}')

                try:
                    up_block, down_block = get_migration_blocks(text)
                except SyntaxError:
                    raise SyntaxError(f'no rollback block found in migration {version}__{name}')

                # Checking to make sure that the migration can be successfully applied and rolled back
                try:
                    with conn.transaction():
                        cur.execute(up_block)
                        cur.execute(down_block)

                except psycopg.Error as ex:
                    raise RuntimeError(f'encountered an exception when verifying migration {version}__{name}: '
                                       f'{ex}.')

                # Applying the migration
                try:
                    with conn.transaction():
                        cur.execute(up_block)

                        # Making a migration record
                        checksum = get_migration_hash(text)
                        cur.execute("""
                        INSERT INTO migrations.migration_history(version, name, applied_timestamp, checksum)
                        VALUES (%s, %s, NOW(), %s)
                        """, (version, name, checksum))

                except psycopg.Error as ex:
                    raise RuntimeError(f'encountered an exception when applying migration {version}__{name}: '
                                       f'{ex}.')

    if feedback:
        print('Migration(s) applied successfully')


def rollback_migrations(migration_path: str, database: str, username: str, password: str,
                        version_target: str | None =None, **kwargs: bool) -> None:
    """A function that rolls back migrations later than the specified version target.

    Parameters
    ----------
    migration_path : str
        The absolute path to the migration files.
    database : str
        The name of the database that the migrations will be rolled back from.
    username : str
        The user to connect to the database as. Ideally the owner of the database.
    password : str
        The password of the database user.
    version_target : str | None
        Optional. A version tag of the form V.x.x.x.x. migrations later than this version will be rolled back. If
        unspecified, only the most recent migration will be rolled back.

    Raises
    ------
    ValueError
        If the specified version target is not in the expected format.
    OSError
        If the specified migration path is not an existing directory.
    FileNotFoundError
        If the migration path directory is empty.
    SyntaxError
        If one or more migration files aren't formatted properly into up and down blocks.
    RuntimeError
        If some kind of issue occurred when attempting to verify or roll back migrations.
    psycopg.Error
        And its subclasses, on database-related errors.

    """

    feedback = False
    if 'feedback' in kwargs:
        if kwargs['feedback']:
            feedback = True

    if version_target is not None and not is_valid_version_str(version_target):
        raise ValueError('version target is not in the expected format (Vx.x.x.x).')

    migration_files = get_migration_file_list(migration_path)[::-1]
    if len(migration_files) == 0:
        raise FileNotFoundError('no migration files found.')

    with (psycopg.connect(f'dbname={database} user={username} password={password} connect_timeout=5', autocommit=True)
          as conn):
        with conn.cursor() as cur:
            # Attempting to get a list of all applied migrations
            try:
                cur.execute("""
                SELECT
                    version,
                    name,
                    checksum
                FROM migrations.migration_history
                ORDER BY applied_timestamp DESC
                """)
                applied_migrations = cur.fetchall()
                if applied_migrations is None:
                    if feedback:
                        print('No migration(s) to roll back')

                    return

            except psycopg.errors.UndefinedTable:
                if feedback:
                    print('No migration(s) to roll back')

                return

            # Setting the version target if necessary
            if version_target is None:
                if len(applied_migrations) > 1:
                    # Setting the version target to the version right before the last applied one
                    version_target = applied_migrations[1][0]
                else:
                    version_target = 'V0.0.0.0'

            # Checking to see if the passed version target was actually applied
            else:
                if version_target != 'V0.0.0.0':
                    cur.execute("""
                    SELECT
                        version
                    FROM migrations.migration_history
                    WHERE version = %s
                    """, (version_target, ))
                    result = cur.fetchone()
                    if result is None:
                        raise RuntimeError(f'cannot roll back to migration version {version_target} because it was '
                                           f'never applied.')

            # Checking to see if a rollback is necessary
            if version_target == applied_migrations[0][0]:
                if feedback:
                    print('No migration(s) to roll back')

                return

            # Attempting to do rollbacks
            i = 0
            for applied_version, applied_name, applied_checksum in applied_migrations:
                if applied_version == version_target:
                    break

                if feedback:
                    print(f'Rolling back migration {applied_version}__{applied_name}')

                # Looking for the right migration file
                while i < len(migration_files):
                    if get_file_version(migration_files[i]) == applied_version:
                        break

                    i += 1

                if i == len(migration_files):
                    raise RuntimeError(f'file for migration {applied_version}__{applied_name} is missing.')

                version, name, text = get_migration(migration_files[i])
                # Making sure that the file hasn't been changed since the migration was applied
                checksum = get_migration_hash(text)
                if checksum != applied_checksum:
                    raise RuntimeError(f'file for migration {applied_version}__{applied_name} has changed since '
                                       f'migration was applied.')

                _, down_block = get_migration_blocks(text)
                try:
                    with conn.transaction():
                        cur.execute(down_block)

                        # Deleting the migration record
                        cur.execute("""
                        DELETE FROM migrations.migration_history
                        WHERE version = %s
                        """, (version,))

                except psycopg.Error as ex:
                    raise RuntimeError(f'encountered an exception when rolling back migration {version}__{name}: '
                                       f'{ex}')

    if feedback:
        print('Migration(s) rolled back successfully')


def main() -> None:
    try:
        parser = argparse.ArgumentParser(prog='simplepgmg',
                                         description='A simple python tool for applying and rolling back PostgreSQL '
                                                     'database migrations')
        subparsers = parser.add_subparsers(title='Subcommands', dest='subcommand')

        version_parser = subparsers.add_parser('version', help='Get the version and name of the last applied migration')
        version_parser.add_argument('database', help='The name of the database')

        migration_parser = subparsers.add_parser('apply', help='Apply new migrations to the database')
        migration_parser.add_argument('migration_path', help='The absolute path to the migration files')
        migration_parser.add_argument('database', help='The name of the database')
        migration_parser.add_argument('version_target', nargs='?', default=None,
                                      help='Optional. A version tag of the form V.x.x.x.x. migrations up to and '
                                           'including this version will be applied. If unspecified, all new migrations '
                                           'will be applied')

        rollback_parser = subparsers.add_parser('rollback', help='Roll back migrations previously applied to the '
                                                                 'database')
        rollback_parser.add_argument('migration_path', help='The absolute path to the migration files')
        rollback_parser.add_argument('database', help='The name of the database')
        rollback_parser.add_argument('version_target', nargs='?', default=None,
                                     help='Optional. A version tag of the form V.x.x.x.x. migrations later than this '
                                          'version will be rolled back. If unspecified, only the most recent migration '
                                          'will be rolled back.')

        args = parser.parse_args()
        if args.subcommand == 'version':
            username, password = get_database_creds()
            try:
                version, name, time = get_last_applied_migration(args.database, username, password)
                print(f'Last applied migration: {version}__{name} at {time}')
            except RuntimeError:
                print(f'Error: unable to find migration records. Please apply at least one migration.')
            except psycopg.Error as ex:
                print(f'Error: {ex}.')

        elif args.subcommand == 'apply':
            username, password = get_database_creds()
            try:
                apply_migrations(args.migration_path, args.database, username, password,
                                 version_target=args.version_target, feedback=True)
            except (ValueError, OSError, FileNotFoundError, SyntaxError, RuntimeError) as ex:
                print(f'Error: {ex}')
            except psycopg.Error as ex:
                print(f'Error: {ex}.')

        elif args.subcommand == 'rollback':
            username, password = get_database_creds()
            try:
                rollback_migrations(args.migration_path, args.database, username, password,
                                    version_target=args.version_target, feedback=True)
            except (ValueError, OSError, FileNotFoundError, SyntaxError, RuntimeError) as ex:
                print(f'Error: {ex}')
            except psycopg.Error as ex:
                print(f'Error: {ex}.')

        else:
            parser.print_help()

    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()