import pandas as pd
import sqlalchemy.exc
from sqlalchemy import MetaData, text
from sqlalchemy.orm import Session
from os.path import join
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class FFIDatabase:
    """
    this represents everything you will need from an FFI database. Contains some SQLAlchemy bits, as well as all primary
    and foreign keys
    """

    def __init__(self, engine, new=False):
        self.engine = engine
        self.meta = MetaData()
        self.meta.reflect(self.engine)
        self.tables = self.meta.tables
        self._primary_keys = None
        self._foreign_keys = None

        self._populate_pks()
        self._populate_fks()

    def _populate_pks(self):
        if not self._primary_keys:
            pks = {table: [column.name for column in self.tables[table].primary_key.columns]
                             for table in self.tables}
            self._primary_keys = pks

    def _populate_fks(self):
        if not self._foreign_keys:
            fks = {table: {column.name: [(fk.column.table.name, fk.column.name)
                                         for fk in column.foreign_keys]
                           for constraint in list(self.tables[table].foreign_key_constraints)
                           for column in constraint.columns
                           }
                   for table in self.tables}
            self._foreign_keys = fks

    def get_primary_keys(self):
        return self._primary_keys

    def get_foreign_keys(self):
        return self._foreign_keys

    def get_engine(self):
        return self.engine

    def start_session(self):
        """
        Starts a session for executing SQL statements
        """
        return Session(self.engine)

    def create_schema(self, schema_name):
        """
        Attempts to create a new schema on the server.
        """
        with self.start_session() as s:
            try:
                s.execute(text(f"CREATE SCHEMA {schema_name}"))
                s.commit()
            except sqlalchemy.exc.ProgrammingError:
                pass

    def insert_codes(self):
        """
        This will insert the data for damage codes, damage severity, and species saved as .csv in the program directory
        :return: static method
        """

        for table in ['LUDamageCodes', 'LUDamageSev', 'LUSpecies']:
            csv_file = table + '.csv'
            csv_path = f'{ROOT}/extra/{csv_file}'

            df = pd.read_csv(csv_path, encoding='latin')
            with Session(self.engine) as conn:
                try:
                    df.to_sql(table, conn.bind, if_exists='append', index=False)
                except Exception as e:
                    conn.rollback()
                    print(e)


