import os
import re
from sqlite3 import ProgrammingError

from sqlalchemy import text
from xml.etree import ElementTree
from pandas import DataFrame, options
from re import findall
from ..tools.functions import strip_namespace, convert_datetime
from ..tools.server import FFIDatabase
from copy import deepcopy
import datetime
import warnings
import logging
import uuid

warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from pathlib import Path
import duckdb

SRC_ROOT = Path(__file__).resolve().parent
ROOT = SRC_ROOT.parent

options.mode.chained_assignment = None
LOG_NAME = 'C:/Users/Corey/OneDrive/OneDrive - New Mexico Highlands University/Python/FFI/Export-ETL_NEW/log/data.log'
logging.basicConfig(filename=LOG_NAME,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M',
                    level=logging.INFO)


class FFIFile:
    """
    this is a class that represents the entire XML file. It can be thought of as a collection of 'tables' represented by
    the element names that appear in the XML file.
    """

    def __init__(self, source):
        """
        parses a ElementTree root element and creates the FFIFile class
        """
        # with open(file) as open_file:
        #     f_gen = (open_file.readline() for i in range(50000))
        #     f = '\n'.join(f_gen)
        #     file_hash = sha256(f.encode())
        #     file_id = file_hash.hexdigest()

        # self._id = file_id
        self.uploaded_file = False

        if hasattr(source, 'name'):
            if source.name.endswith('xml'):
                self.uploaded_file = True
                self.data_type = 'xml'
        elif isinstance(source, str):
            if source.endswith('xml'):
                self.data_type = 'xml'
        elif isinstance(source, FFIDatabase):
            self.data_type = 'database'
        else:
            self.data_type = 'unknown'

        if self.data_type == 'xml':
            if self.uploaded_file:
                self.source = source.name
            else:
                self.source = os.path.basename(source)
            self._tree = ElementTree.parse(source)
            self._root = self._tree.getroot()
            self._namespace = findall(r'\{http://\w+\.\w{3}[\w/.\d]+\}', self._root.tag)[0].strip('{}')
        elif self.data_type == 'database':
            self.source = source
            self._tree = None
            self._root = None
            self._namespace = None
        elif self.data_type == 'unknown':
            raise NotImplementedError(f"The object {source.name} is not yet an implemented type.")

        self._data_map = {}
        self._method_rename = {}
        self._excluded = ['FuelConstants_DL', 'FuelConstants_ExpDL', 'FuelConstants_FWD', 'FuelConstants_Veg',
                          'FuelConstants_CWD', 'Program', 'Project', 'DataGridViewSettings', 'Protocol',
                          'MasterSpecies_LastModified', 'Settings', 'MetaData', 'AuxSpecies', 'MacroPlots_NotImported',
                          'Organization', 'OrganizationGroup', 'SpeciesPickList', 'ProtocolVersion', 'Reference_Book',
                          'Reference_Journal', 'Reference_WebSite', 'SampleEvent_NotModified', 'MM_Method_Reference',
                          'MM_LocalSpecies_SpeciesPickList', 'MM_Organization_Method', 'MM_Project_Protocol',
                          'MM_Protocol_Method', 'MM_SampleEvent_Protocol']

        self._included = ['MacroPlot', 'RegistrationUnit', 'MM_ProjectUnit_MacroPlot', 'ProjectUnit', 'SampleEvent',
                         'MM_MonitoringStatus_SampleEvent', 'MonitoringStatus', 'MethodAttribute', 'AttributeRow',
                         'AttributeData', 'Method', 'LU_DataType', 'Schema_Version', 'MasterSpecies', 'SampleData',
                         'SampleAttribute', 'LocalSpecies', 'SampleRow']
        self._processed = []
        self.many_tables = False
        self._user = os.environ['USERNAME']

        self.table_map = pd.read_csv(f"{ROOT}/files/TableMap.csv")
        self.field_map = pd.read_csv(f"{ROOT}/files/FieldMap.csv")

        # Excel keeps generating long trails of ' ' at the end of strings. This fixes that
        self.field_map = self.field_map.map(lambda x: x.strip()
                                                 if isinstance(x, str)
                                                 else x)
        self.table_map = self.table_map.map(lambda x: x.strip()
                                                 if isinstance(x, str)
                                                 else x)

        self.new_tables = ['AdminUnit', 'Plot', 'SampleEvent', 'GroundCover', 'Canopy', 'AerialCover',
                           'Fuels1000Hr', 'FuelsDuffLitter', 'FuelsVegetation', 'FuelsFine', 'Project', 'Trees',
                           'Saplings', 'Seedlings', 'WitnessTree', 'Transect', 'ProjectVisit', 'Disturbance',
                           'CoverPoints', 'CoverSpecies', 'GroundCover_Sample', 'Canopy_Sample', 'AerialCover_Sample',
                           'Fuels1000Hr_Sample', 'FuelsDuffLitter_Sample', 'FuelsVegetation_Sample', 'FuelsFine_Sample',
                           'Project_Sample', 'Trees_Sample', 'Saplings_Sample', 'Seedlings_Sample', 'WitnessTree_Sample',
                           'Transect_Sample', 'ProjectVisit_Sample', 'Disturbance_Sample', 'CoverPoints_Sample',
                           'CoverSpecies_Sample']

        self.version = None
        self.admin_unit = None
        self.insert_failed = []

    def __setitem__(self, key, value):
        if type(value) == pd.DataFrame:
            self._data_map[key] = value
        else:
            raise TypeError(f"Please provide a DataFrame object, not a {type(value)}.")

    def __getitem__(self, item):
        """
        I needed to create some way to index the FFIFile class, so this will pass the index to the data_map and return
        whatever that operation returns.

        e.g <FFIFile>['column'] returns <internal DataFrame>['column']
        """

        if item in self._data_map.keys():
            return self._data_map[item]
        else:
            raise KeyError(f'{item} not in FFI XML file.')

    @staticmethod
    def update_last_modified(session, table, rows, action):
        """
        Just updates the LastModified table with current user
        """
        # gather computer name and username as well as current time
        comp_name = os.environ['COMPUTERNAME']
        user = os.environ['USERNAME']
        now = str(datetime.datetime.now())

        # use a dict of this info to create a DataFrame
        lm_dict = {'last_edit_date': [now],
                   'Machine': [comp_name],
                   'Username': [user],
                   'Table_Modified': [table],
                   'Rows_Modified': [rows],
                   'Action': [action]}
        last_modified = DataFrame(lm_dict)

        # overwrite last modified
        last_modified.to_sql('Last_Modified_Date', session.bind, index=False, if_exists='replace')

    def _parse_et_data(self):
        """
        Iterates through each element name that was produced in the __init__ operation. This is what actually populates
        the data_map element
        """

        temp_map = {}
        tags = set([strip_namespace(element.tag) for element in self._root])
        for tag in tags:
            if tag in self._included:
                all_data = self._root.findall(tag, namespaces={'': self._namespace})
                dfs = [
                    {strip_namespace(attr.tag): attr.text for attr in data_set}
                    for data_set in all_data
                ]
                df = DataFrame(dfs)
                for col in df.columns:
                    if '_GUID' in col:
                        df[col] = df[col].apply(lambda row: row.upper())
                    elif 'Date' in col or 'Time' in col:
                        df[col] = df[col].apply(lambda row: convert_datetime(row))
                temp_map[strip_namespace(tag)] = df.reset_index(drop=True)

        return temp_map

    def _parse_db_tables(self):

        """
        Iterate through the _included tables and create the datamap by reading those tables from the database
        :return:
        """

        data_map = {}
        with self.source.get_engine().connect() as con:
            for table in self._included:
                print(table)
                if table == "AttributeData":  # AttrData and SampData tables use the sql_variant data type
                    query = ("SELECT CAST(AttributeData_Value AS NVARCHAR(MAX)) AS AttributeData_Value,"
                             "AttributeData_MethodAtt_ID, AttributeData_DataRow_ID,"
                             "AttributeData_SampleRow_ID FROM AttributeData")
                    df = pd.read_sql(query, con)
                elif table == "SampleData":  # which needs to be converted to NVARCHAR(MAX)
                    query = ("SELECT SampleData_SampleEvent_GUID, CAST(SampleData_Value AS NVARCHAR(MAX)) AS SampleData_Value,"
                             "SampleData_SampleAtt_ID, SampleData_SampleRow_ID "
                             "FROM SampleData")
                    df = pd.read_sql(query, con)
                else:  # otherwise just read the table from the server
                    df = pd.read_sql_table(table, con=con)

                for col in df.columns:
                    if '_GUID' in col:  # ensure GUIDs are capitalized
                        df[col] = df[col].apply(lambda row: row.upper())
                    elif 'Date' in col or 'Time' in col:  # datetimes need to be represented as such in pandas
                        df[col] = df[col].apply(lambda row: convert_datetime(row))

                data_map[table] = df

        return data_map

    def set_data_map(self):

        if self.data_type == "xml":
            data_map = self._parse_et_data()
        elif self.data_type == "database":
            data_map = self._parse_db_tables()
        else:
            data_map = {}

        self._data_map = data_map

    def _attach_visit(self):
        events = self["SampleEvent"]
        visits = self["MM_MonitoringStatus_SampleEvent"]
        new_df = events.merge(visits, left_on="SampleEvent_GUID", right_on="MM_SampleEvent_GUID")

        self["SampleEvent"] = new_df

    def _create_attr_table(self):
        """
        Converts the AttributeData and AttributeRow long formats into the many-tables flat format
        """
        sql = """SELECT SampleData_SampleEvent_GUID,
                        AttributeRow_DataRow_GUID,
                        MethodAtt_FieldName,
                        AttributeData_Value,
                        Method_Name
                FROM AttributeRow
                LEFT JOIN AttributeData ON AttributeRow_ID=AttributeData_DataRow_ID
                LEFT JOIN MethodAttribute ON AttributeData_MethodAtt_ID=MethodAtt_ID
                LEFT JOIN Method ON MethodAtt_Method_GUID=Method_GUID
                LEFT JOIN SampleRow ON AttributeData_SampleRow_ID=SampleRow_ID
                LEFT JOIN SampleData ON AttributeData_SampleRow_ID=SampleData_SampleRow_ID
                LEFT JOIN SampleEvent ON SampleData_SampleEvent_GUID=SampleEvent_GUID"""

        attr_row = self['AttributeRow']
        attr_data = self['AttributeData']
        method_attr = self['MethodAttribute']
        method = self['Method']
        sample_row = self['SampleRow']
        sample_data = self['SampleData']
        sample_event = self['SampleEvent']
        tables = {'AttributeRow': attr_row,
                  'AttributeData': attr_data,
                  'MethodAttribute': method_attr,
                  'Method': method,
                  'SampleRow': sample_row,
                  'SampleData': sample_data,
                  'SampleEvent': sample_event}

        conn = duckdb.connect(database=':memory:')
        for table_name in tables.keys():
            conn.register(table_name, tables[table_name])
        conn.execute(sql)

        attr_long = conn.fetchdf()

        return attr_long

    def set_attr_long(self):
        """Sets the Attribute Table in long format to the data map"""
        attr = self._create_attr_table()
        self['AttributeTable'] = attr

    def _attr_to_many(self):
        """
        Breaks out the Attribute long table into many tables, each for each method.
        This is where custom normalization logic goes
        :return:
        """

        method_tables = {}

        attr_long = self['AttributeTable']
        methods = attr_long['Method_Name'].unique()  # get unique table names

        # Iterates through each method name, subsets those data from the main dataset, and performs some transformations
        for method in methods:
            # print(method)
            temp = attr_long.loc[attr_long['Method_Name'] == method].drop_duplicates()  # make sure no duplicate data

            # We have to take the subset before pivoting - this pivots on the appropriate unique identifiers
            subset = temp.pivot(index=['SampleData_SampleEvent_GUID', 'AttributeRow_DataRow_GUID'],
                                columns=['MethodAtt_FieldName'],
                                values='AttributeData_Value').reset_index()

            # Normalize the table names from previous naming convention
            table_name = method.replace(' ', '').replace('-', '_').replace('(', '_').replace(')', '_').strip('_')

            for col in subset.columns:
                ###############################################################
                # Put custom logic for specific columns here
                ###############################################################

                # Ensures species columns use the actual USDA code, not the Spp_GUID
                if 'Spp' in col:
                    spp_df = self['LocalSpecies']
                    subset['Species'] = subset.apply(lambda row:
                                                     spp_df.loc[
                                                         spp_df['LocalSpecies_GUID'] == row[col].upper()
                                                         ].iloc[0]['LocalSpecies_Symbol'],
                                                     axis=1)
                    subset.drop([col], axis=1, inplace=True)

            # Assign tree stem count to TreesIndv
            if method == 'Trees - Individuals':
                subset['StemNum'] = subset.groupby(['SampleData_SampleEvent_GUID', 'Species', 'TagNo']).cumcount() + 1

            # If TagNo is not included in witness tree info, assign new counts. Might need to QC this.
            elif method == 'Plot Info Wit Trees Comments3':
                if 'WitTreeTagNo' not in subset.columns:
                    subset['WitTreeTagNo'] = subset.groupby(['SampleData_SampleEvent_GUID']).cumcount() + 1
                # also ensures there isn't more than one witness tree per plot/event
                subset.sort_values(['SampleData_SampleEvent_GUID', 'WitDBH'], inplace=True)
                subset.drop_duplicates('SampleData_SampleEvent_GUID', keep='first', inplace=True)

            # 6/10/25 logic for Canopy dots or squares method
            elif method == 'Canopy - Densiometer':
                if 'DotsCount' in subset.columns:
                    subset['Method'] = subset.apply(lambda row: 'Dots' if row['DotsCount'] == '96'
                    else 'Squares' if row['DotsCount'] == '24'
                    else '',
                                                    axis=1)

            # Drop all rows with null EventIDs
            subset.dropna(subset=['SampleData_SampleEvent_GUID'], inplace=True)
            subset.drop(["AttributeRow_DataRow_GUID", "Index"], axis=1, inplace=True, errors='ignore')
            # subset.rename(columns={"SampleData_SampleEvent_GUID": "EventID"}, inplace=True)

            # Assign table
            method_tables[method] = subset

        return method_tables

    def set_attr_method_tables(self, replace_attr=True):

        methods = self._attr_to_many()

        if replace_attr:
            del self._data_map["AttributeTable"]

        for m in methods.keys():
            df = methods[m]
            if 'ID' not in df.columns:
                df['ID'] = [str(uuid.uuid4()).upper() for _ in range(len(df.index))]
            self[m] = df

        return methods

    def _create_sample_table(self):
        """
        Converts the AttributeData and AttributeRow long formats into the many-tables flat format
        """
        sql = """
            SELECT SampleRow_Original_GUID,
                SampleData_SampleEvent_GUID,
                SampleAtt_FieldName,
                SampleData_Value,
                Method_Name,
                Method_UnitSystem
            FROM SampleRow sr
            LEFT JOIN SampleData sd ON sd.SampleData_SampleRow_ID=sr.SampleRow_ID
            LEFT JOIN SampleAttribute sa ON sa.SampleAtt_ID=sd.SampleData_SampleAtt_ID
            LEFT JOIN Method m ON m.Method_GUID=sa.SampleAtt_Method_GUID
            WHERE SampleAtt_FieldName <> 'Visited'
        """

        samp_row = self['SampleRow']
        samp_data = self['SampleData']
        sample_attr = self['SampleAttribute']
        method = self['Method']
        tables = {'SampleRow': samp_row,
                  'SampleData': samp_data,
                  'SampleAttribute': sample_attr,
                  'Method': method}

        conn = duckdb.connect(database=':memory:')
        for table_name in tables.keys():
            conn.register(table_name, tables[table_name])
        conn.execute(sql)

        samp_long = conn.fetchdf()

        return samp_long

    def set_sample_table(self):
        df = self._create_sample_table()
        self['SampleTable'] = df

    def _sample_to_many(self):
        method_tables = {}

        samp_long = self['SampleTable']
        methods = samp_long['Method_Name'].unique()  # get unique table names

        # Iterates through each method name, subsets those data from the main dataset, and performs some transformations
        for method in methods:
            # print(method)
            temp = samp_long.loc[samp_long['Method_Name'] == method].drop_duplicates()  # make sure no duplicate data

            # We have to take the subset before pivoting - this pivots on the appropriate unique identifiers
            subset = temp.pivot(index=['SampleData_SampleEvent_GUID', 'SampleRow_Original_GUID'],
                                columns=['SampleAtt_FieldName'],
                                values='SampleData_Value').reset_index()

            # Normalize the table names from previous naming convention
            # Map new table name and assign
            # try:
            #     new_name = self.table_rename[method]
            #     if new_name == "WitnessTree":
            #         new_name = "PlotDetail"
            # except KeyError:
            #     print(f"{method} not specified.")
            #     pass

            for col in subset.columns:
                ###############################################################
                # Put custom logic for specific columns here
                ###############################################################
                pass
            # Drop all rows with null EventIDs
            subset_nona = subset.dropna(subset=["SampleData_SampleEvent_GUID"])
            subset_drop = subset_nona.drop(["SampleRow_Original_GUID"], axis=1)
            subset_rename = subset_drop.rename(columns={"SampleData_SampleEvent_GUID": "EventID",
                                                        "SaComment": "Comment"})
            if 'Plot Info' in method:
                method_tables["PlotDetail"] = subset_rename
            else:
                method_tables[f"{method}_Sample"] = subset_rename

        return method_tables

    def set_sample_method_tables(self, replace_samp=True):

        methods = self._sample_to_many()

        if replace_samp:
            del self._data_map["SampleTable"]

        for m in methods.keys():
            df = methods[m]
            if 'ID' not in df.columns:
                df['ID'] = [str(uuid.uuid4()).upper() for _ in range(len(df.index))]
            self[m] = df

    def _rename_fields(self, table_name, df):
        """
        Renames fields in a table based on the mapping provided in FieldMap
        """

        # field_map = pd.read_csv("files/FieldMap.csv")

        temp_field_map = self.field_map.loc[self.field_map['TableName'] == table_name]
        if temp_field_map.empty:
            return df

        # # Excel keeps generating long trails of ' ' at the end of strings. This fixes that
        # field_map = field_map.applymap(lambda x: x.replace(' ', '')
        #                     if type(x) == str
        #                     else x)

        temp_df = deepcopy(df)

        # Form the mappings as a dictionary
        this_field_map = dict(zip(list(temp_field_map['OldColumn']), list(temp_field_map['ColumnName'])))
        table_fields = [col for col in list(temp_field_map['OldColumn']) if col != 'nan']

        # Only select fields in the table
        select_fields = [field for field in table_fields if field in temp_df.columns]
        almost_final_table = temp_df[select_fields].copy()
        final_table = almost_final_table.rename(columns=this_field_map)

        return final_table

    def _rename_tables(self, missing_only=False):
        """
        Files need to be renamed so the data_map keys align with the table names in the database
        """
        new_names = list(self.table_map['FFITable'])
        new_map = {}

        table_names = list(self._data_map.keys())

        for table in table_names:
            sample = False
            if table.endswith("_Sample"):
                sample = True
                base_str = re.findall("(.*)_Sample$", table)[0]
                mapper = self.table_map.loc[self.table_map['FFITable'] == base_str]
            else:
                mapper = self.table_map.loc[self.table_map['FFITable'] == table]

            table_df = self[table]

            if not mapper.empty:
                if not missing_only:
                    if sample:
                        new_table = mapper['NewTable'].values[0] + "_Sample"
                    else:
                        new_table = mapper['NewTable'].values[0]
            else:
                new_table = table

            if not missing_only:
                new_df = self._rename_fields(new_table, table_df)
                new_map[new_table] = new_df
        self._data_map = new_map

    def _create_table(self, table_name):
        """
        Custom logic for breaking out new tables.
        """

        if table_name == 'Transect':
            new_df = self['Surface Fuels - Fine'] \
                .merge(self['SampleEvent'], left_on='SampleData_SampleEvent_GUID', right_on='SampleEvent_GUID', how='left')
            temp_df = new_df[['SampleData_SampleEvent_GUID', 'Transect', 'Azimuth', 'Slope']].drop_duplicates()
            temp_df = temp_df.dropna(subset=['Azimuth'])
            temp_df['Length'] = 75
            temp_df['ID'] = [str(uuid.uuid4()).upper() for _ in range(len(temp_df.index))]
            self['Transect'] = temp_df

        elif table_name == 'ProjectVisit':
            temp_df = self['MonitoringStatus']
            self['ProjectVisit'] = temp_df

    # def filter_plots(self):
    #     """ Weird duplicate plot issues in certain cases where there are no events associated with the plot"""
    #
    #     plots = self['MacroPlot']
    #     events = self['SampleEvent']
    #
    #     df = plots.merge(events, left_on='MacroPlot_GUID', right_on='SampleEvent_Plot_GUID', how='left')
    #     df.dropna(subset=['SampleEvent_GUID'], inplace=True)
    #     df.drop_duplicates(subset=['MacroPlot_GUID'], keep='first', inplace=True)
    #
    #     self['MacroPlot'] = df

    @staticmethod
    def query_uq_cols(ffi_db, table_name):

        query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE CONSTRAINT_NAME = 'UQ_{table_name}'
        """
        with ffi_db.engine.connect() as con:
            df = pd.read_sql(query, con)
        cols = list(df["COLUMN_NAME"])

        return cols

    def generate_merge_query(self, ffi_db, table, table_df):

        # handle key constraints
        fks = ffi_db.get_foreign_keys()

        try:
            table_fks = fks[table]
        except KeyError:
            table_fks = []

        unique_cols = self.query_uq_cols(ffi_db, table)

        # we need to ensure that the tables on which there are foreign key constraints are entered before we upload
        # new data to the current table. This will produce a recursive pattern to insert all dependencies first.
        if len(table_fks) > 0:
            for const in table_fks:
                const_list = table_fks[const]
                for tup in const_list:
                    add_table = tup[0]
                    if add_table not in self._processed and \
                            add_table in self._data_map:
                        print(f'Adding foreign key dependency: {add_table}')
                        self._insert_into_db(ffi_db, add_table)
                        self._processed.append(add_table)

        # constructs comma-delimited lists of column names
        cols = list(table_df.columns)
        cols_str = ', '.join(cols)
        source_cols = [f'source.{c}'
                       for c in cols]
        source_col_str = ', '.join(source_cols)

        # Constructs identity relations for primary keys
        pk_strings = [
            (f"(CASE WHEN target.{pk} IS NULL THEN '' ELSE target.{pk} END) = "
             f"(CASE WHEN source.{pk} IS NULL THEN '' ELSE source.{pk} END)")
            for pk in unique_cols
            if pk in cols
        ]
        pk_part = ' AND '.join(pk_strings)

        # Generate the full MERGE INTO statement
        merge_into_sql = f"""
                MERGE INTO dbo.{table} WITH (HOLDLOCK) AS target
                USING temp.{table} AS source 
                    ON {pk_part}
                WHEN NOT MATCHED BY target THEN
                    INSERT ({cols_str})
                    VALUES ({source_col_str});
                """

        return merge_into_sql

    def _insert_into_db(self, ffi_db, table):
        """
        Checks foreign key constraints and inserts any necessary tables first.

        Then generates a MERGE INTO statement directly creating a query with the data values and executes that statement

        :param ffi_db: FFIDatabase object representing the database connection
        :param table: the table to be inserted into the database, as a string
        """

        if table in self.new_tables:

            xml_table = self[table]

            merge_into_sql = self.generate_merge_query(ffi_db, table, xml_table)

            ffi_db.create_schema('temp')

            with ffi_db.start_session() as sesh:
                xml_table.to_sql(table, sesh.bind, schema='temp', if_exists='replace', index=False)
                try:

                    count_sql = f"SELECT COUNT(*) AS Size FROM {table}"
                    before_df = pd.read_sql(count_sql, sesh.bind)  # Compare before and after row count of tables
                    before_count = before_df['Size'].values[0]

                    # # Insert data
                    sesh.execute(text(merge_into_sql))
                    sesh.execute(text(f"DROP TABLE IF EXISTS temp.{table}"))
                    sesh.commit()
                    #
                    # # Get count after insert
                    after_df = pd.read_sql(count_sql, sesh.bind)
                    after_count = after_df['Size'].values[0]
                    #
                    count_diff = after_count - before_count
                    #
                    # # Then we insert into our logging table for who made changes to the database tables

                    if count_diff != 0:
                        change_type = "INSERT" if count_diff > 0 else "DELETE"

                        # handle datetime
                        dt = str(datetime.datetime.now())
                        new_dt = re.findall(r'(.*)\.\d{4}', dt)[0]

                        # construct log data and insert
                        change_df = DataFrame({'User': [self._user],
                                               'Time': [new_dt],
                                               'Table': [table],
                                               'File': [self.source],
                                               'ChangeType': [change_type],
                                               'Changes': [abs(count_diff)]})
                        change_df.to_sql('UpdateLog', sesh.bind, if_exists='append', index=False)

                    print(f"Inserted {count_diff} rows into {table}.")
                    logging.info(f"Inserted {count_diff} rows into {table}.")
                    self._processed.append(table)

                except Exception as e:
                    # If there's any issues merging the data, throw an error and rollback the MERGE
                    print(f"Failed to insert data into {table}.")
                    self.insert_failed.append(table)
                    sesh.rollback()
                    error = str(e)
                    if len(error) > 0:
                        error_text = re.findall(r'\[SQL Server\](.*)\(\d+\)', error)
                        if len(error_text) > 0:
                            error = error_text[0]
                        logging.warning(f"Failed to insert data for {table}.")
                        logging.error(error, exc_info=False)
                        print(error)

    def get_data_map(self):
        return self._data_map

    def get_tables(self):
        return list(self._data_map.keys())

    def extract(self):

        print(f"Reading data for {self.source}")
        logging.info(f"Transforming tables for {self.source}")
        self.set_data_map()
        self.version = self['Schema_Version']['Schema_Version'][0]
        self.admin_unit = self['RegistrationUnit']['RegistrationUnit_Name'][0]

    def transform(self, custom_logic=True):

        self.set_attr_long()
        self.set_attr_method_tables()

        self.set_sample_table()
        self.set_sample_method_tables()

        # self.filter_plots()

        if custom_logic:
            self._attach_visit()
            self._create_table('Transect')
            self._create_table('ProjectVisit')

        self._rename_tables()

    def load(self, ffi_db):
        """
        Iterates through each table in the data map and inserts it into the database
        """
        print(f'Inserting data for {self.source}')
        logging.info(f"Inserting data for {self.source}")

        for table in self._data_map:
            if table in self.new_tables:
                if table not in self._processed:
                    self._insert_into_db(ffi_db, table)

    def tables_to_csv(self):

        if not os.path.isdir('csv'):
            os.mkdir('csv')

        for table in self._data_map:
            df = self._data_map[table]
            df.to_csv(f'csv/{table}.csv')
