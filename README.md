# simplepgmg
## A simple Python tool for applying and rolling back PostgreSQL database migrations.
### To install the tool, run the following pip command:

    pip install simplepgmg@git+https://github.com/JSheare/simplepgmg

## **Usage**
This tool is intended for smaller projects that don't need/want to worry about the complexity of tools like Flyway or 
Liquibase. It takes a path to a directory of migration files and the name of a database running on localhost and 
applies or rolls the migrations back, depending on the command.

Migration files must be of the following form:

    Va.b.c.d__migration_name.sql

where 'a' is digits corresponding to major version number, 'b' is digits corresponding to minor version number, 'c' is
digits corresponding to patch number, 'd' is digits corresponding to change number, and 'migration_name' is whatever 
name you choose for the migration. 

Here's an example of a migration file name that would be accepted:

    V1.0.0.1__first_change.sql

The SQL inside each migration file must also be divided into two blocks: one for applying the migration, and one for 
rolling it back. The two blocks are separated by the SQL comment '--down'. Here's an example:

```
--up
CREATE TABLE IF NOT EXISTS some_schema.example(
    thingy TEXT);
    
--down
DROP TABLE IF EXISTS some_schema.example;
```

Some advice on making migration files:
- Try to make each file as atomic as possible. One change per file is ideal.
- Ideally, migration and rollback SQL should be idempotent.
- Once you've made a migration file and applied it to the database, don't change it.

Also note that the tool will set up its own schema and table inside the database for migration-related bookkeeping. 
Don't modify this table, or you may cause issues.

## **Getting Started**
First install the tool. Then, if you haven't already, create the database where migrations will be applied (and
a Postgres role that can apply them, ideally the owner of the database).

The tool supports three commands:

### **Get Last**
You can get the version and name of the last migration applied to the database with a command of the following form:

    simplepgmg version database

where 'database' is the name of the database to check the version on.

### **Apply Migrations**
You can apply migrations to the database with a command of the following form:

    simplepgmg apply migration_path database [version_target]

where 'migration_path' is the absolute path to the directory containing the migration files and 'database' is the name 
of the database which the migrations will be applied to. 

By default, all the newest migrations are applied, but you can specify a maximum migration version to apply with the
optional parameter 'version_target'. It should be of the form 'Va.b.c.d', where 'a' is digits corresponding to major 
version number, 'b' is digits corresponding to minor version number, 'c' is digits corresponding to patch number, and 
'd' is digits corresponding to change number.

### **Roll back Migrations**
You can roll back migrations with a command of the following form:

    simplepgmg rollback migration_path database [version_target]

where 'migration_path' is the absolute path to the directory containing the migration files and 'database' is the name 
of the database which the migrations will be rolled back on. 

By default, only the last applied migration is rolled back, but you can specify a migration version to roll back to with
the optional parameter 'version_target'. It should be of the form 'Va.b.c.d', where 'a' is digits corresponding to major 
version number, 'b' is digits corresponding to minor version number, 'c' is digits corresponding to patch number, and 
'd' is digits corresponding to change number.

## **Using the Tool Programmatically**
In addition to the command line interface, the functions of the tool can also be used in Python scripts. Simply import 
the tool and make use of the three functions corresponding to the tool's three commands:
```python
import simplepgmg

migration_path = 'path/to/migrations'
username = 'alice'
password = 'foo'
database = 'bar'

version, name = simplepgmg.get_last_applied_migration(database, username, password)
simplepgmg.apply_migrations(migration_path, database, username, password, version_target='V1.2.3.4')
simplepgmg.rollback_migrations(migration_path, database, username, password, version_target='V0.0.0.0')
```
Run help() with any of these functions for more information about their usage.