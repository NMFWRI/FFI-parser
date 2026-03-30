import os
import re
from uuid import uuid4

from xml.etree import ElementTree
from pandas import DataFrame, options
from re import findall
from ..tools.functions import strip_namespace, convert_datetime
from copy import deepcopy
import datetime
import warnings
import logging
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from pathlib import Path

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

    def __init__(self, file):
        """
        parses a ElementTree root element and creates the FFIFile class
        """
        # with open(file) as open_file:
        #     f_gen = (open_file.readline() for i in range(50000))
        #     f = '\n'.join(f_gen)
        #     file_hash = sha256(f.encode())
        #     file_id = file_hash.hexdigest()

        # self._id = file_id
        self.file = file.name.strip('.xml')
        self._tree = ElementTree.parse(file)
        self._root = self._tree.getroot()
        self._namespace = findall(r'\{http://\w+\.\w{3}[\w/.\d]+\}', self._root.tag)[0].strip('{}')
        self._data_map = {}
        self._excluded = ['FuelConstants_DL', 'FuelConstants_ExpDL', 'FuelConstants_FWD', 'FuelConstants_Veg',
                          'FuelConstants_CWD', 'Schema_Version', 'Program', 'Project', 'DataGridViewSettings',
                          'MasterSpecies_LastModified', 'Settings']
        self._processed = []
        self.many_tables = False
        self._user = os.environ['USERNAME']

        self.table_map = pd.read_csv(f"{ROOT}/files/TableMap.csv")
        self.field_map = pd.read_csv(f"{ROOT}/files/FieldMap.csv")

        # Excel keeps generating long trails of ' ' at the end of strings. This fixes that
        self.field_map = self.field_map.applymap(lambda x: x.replace(' ', '')
                                        if isinstance(x, str)
                                        else x)

        self.new_tables = ['AdminUnit', 'Plot', 'Event', 'GroundCover', 'CanopyDensiometer', 'AerialCover',
                           'Fuels1000Hr', 'FuelsDuffLitter', 'FuelsVegetation', 'FuelsFine', 'Project', 'TreesIndv',
                           'TreesSaplings', 'TreesSeedlings', 'WitnessTree', 'Transect', 'ProjectVisit',
                           'Transect', 'TreeDamage', 'DisturbanceHistory']

        self.version = None
        self.admin_unit = None
        self.insert_failed = []
        # current = datetime.datetime.now()
        # self.log_file = f"Migration_Log_{current.year}{current.month}{current.day}{current.hour}{current.minute}{current.second}.log"

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
    def _update_last_modified(session):
        """
        Just updates the LastModified table with current user
        """
        # gather computer name and username as well as current time
        comp_name = os.environ['COMPUTERNAME']
        user = os.environ['USERNAME']
        now = str(datetime.datetime.now())

        # use a dict of this info to create a DataFrame
        lm_dict = {'last_edit_date': [now],
                   'Machine_Name': [comp_name],
                   'User_Name': [f'{comp_name}\\{user}']}
        last_modified = DataFrame(lm_dict)

        # overwrite last modified
        last_modified.to_sql('Last_Modified_Date', session.bind, index=False, if_exists='replace')

    def _parse_data(self):
        """
        Iterates through each element name that was produced in the __init__ operation. This is what actually populates
        the data_map element
        """
        needed_tables = ['MacroPlot', 'RegistrationUnit', 'MM_ProjectUnit_MacroPlot', 'ProjectUnit', 'SampleEvent',
                         'MM_MonitoringStatus_SampleEvent', 'MonitoringStatus', 'MethodAttribute', 'AttributeRow',
                         'AttributeData', 'Method', 'LU_DataType', 'Schema_Version', 'MasterSpecies', 'SampleData',
                         'SampleAttribute', 'LocalSpecies', 'SampleRow']

        tags = set([strip_namespace(element.tag) for element in self._root])
        for tag in tags:
            if tag in needed_tables:
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
                self._data_map[strip_namespace(tag)] = df.reset_index(drop=True)

    def _parse_idents(self):
        """
        This generates PlotID and EventID columns that are used in insertions into the Access database.
        Logic is in Python as it's way easier to handle than SQL.

        Calling will result in the MacroPlot table to have a new column called 'PlotID', and  the SampleEvent table
        to have a new column called 'EventID'.

        This will allow easy lookup when converting from the GUID to the generated ID when methods (and everything else)
        get inserted into the tables, and we need the generated ID for key matching.
        """

        def create_id(row, id_type):
            """
            These functions within functions are annoying, but easier than having it at a higher scope.

            This is just a helper function in generating appropriate IDs for plots and events to match with Access

            For use only within an .apply(), where the row is guaranteed to have the defined columns. Otherwise, this
            will all break. You can also easily add logic for additional identifiers that need to be created fairly
            easily by adding additional elif statements to both the create_id function and the outer logic.
            """

            id_value = ""
            if id_type == 'plot':
                admin_guid = row['MacroPlot_RegistrationUnit_GUID']
                plot_guid = row['MacroPlot_GUID']
                plot_name = row['MacroPlot_Name']

                admin_unit = self['RegistrationUnit'].loc[
                    self['RegistrationUnit']['RegistrationUnit_GUID'] == admin_guid
                ]['RegistrationUnit_Name'].values[0]
                admin_unit = admin_unit.replace(" ", "").replace("_", "").replace("-", "").replace(".", "").upper()
                plot_name = plot_name.replace(" ", "").replace("_", "").replace("-", "").replace(".", "").upper()
                id_value = admin_unit[:5] + plot_name

            elif id_type == 'event':
                plot_guid = row['SampleEvent_Plot_GUID']
                try:
                    plot_id = self['MacroPlot'].loc[
                        self['MacroPlot']['MacroPlot_GUID'] == plot_guid
                    ]['PlotID'].values[0]
                except IndexError:
                    plot_id = ''

                if plot_id != '':
                    date_re = re.findall(r'(\d{4}-\d{2}-\d{2})', row['SampleEvent_Date'])
                    date_raw = date_re[0]
                    event_date = date_raw.replace('-', '')
                    id_value = plot_id + event_date
                else:
                    id_value = ''

            return id_value

        for table in ['MacroPlot', 'SampleEvent']:
            temp_df = self[table]

            if table == 'MacroPlot':
                temp_df['PlotID'] = temp_df.apply(create_id, axis=1, args=('plot',))
                temp_df.dropna(subset='MacroPlot_DateIn', inplace=True)
                temp_df = temp_df.sort_values('MacroPlot_DateIn').drop_duplicates('PlotID', keep='first')
            elif table == 'SampleEvent':
                temp_df['EventID'] = temp_df.apply(create_id, axis=1, args=('event',))
                temp_df.dropna(subset='EventID', inplace=True)
                temp_df = temp_df[temp_df['EventID'] != '']
                temp_df = temp_df.sort_values('EventID').drop_duplicates('EventID', keep='first')

            self[table] = temp_df

    def _attr_to_many(self):
        """
        Converts the AttributeData and AttributeRow tables into the many-tables format used by FFIMT
        """

        # These first few blocks are self-explanatory
        select_list = ['EventID', 'SampleData_SampleEvent_GUID', 'AttributeRow_DataRow_GUID',
                       'MethodAtt_FieldName', 'AttributeData_Value', 'Method_Name', 'Method_UnitSystem']

        # Weird edge case stuff
        select_rename = {'AttributeRow_DataRow_GUID': 'AttributeData_DataRow_GUID',
                         'SampleRow_Original_GUID': 'AttributeData_SampleRow_GUID',
                         'AttributeRow_Original_GUID': 'AttributeData_Original_GUID',
                         'AttributeRow_CreatedBy': 'AttributeData_CreatedBy',
                         'AttributeRow_CreatedDate': 'AttributeData_CreatedDate',
                         'AttributeRow_ModifiedBy': 'AttributeData_ModifiedBy',
                         'AttributeRow_ModifiedDate': 'AttributeData_ModifiedDate'}

        attr_data = self['AttributeRow'] \
            .merge(self['AttributeData'],
                   left_on='AttributeRow_ID',
                   right_on='AttributeData_DataRow_ID', how='left') \
            .merge(self['MethodAttribute'],
                   left_on='AttributeData_MethodAtt_ID',
                   right_on='MethodAtt_ID', how='left') \
            .merge(self['Method'],
                   left_on='MethodAtt_Method_GUID',
                   right_on='Method_GUID', how='left') \
            .merge(self['SampleRow'],
                   left_on='AttributeData_SampleRow_ID',
                   right_on='SampleRow_ID', how='left') \
            .merge(self['SampleData'],
                   left_on='AttributeData_SampleRow_ID',
                   right_on='SampleData_SampleRow_ID', how='left') \
            .merge(self['SampleEvent'],
                   left_on='SampleData_SampleEvent_GUID',
                   right_on='SampleEvent_GUID', how='left')
        try:
            attr_select = attr_data[select_list]
        except KeyError:  # these fields are in the SQL tables, but aren't included in the XML
            # I can probably get rid of this, but I'm not sure how the indexing and renaming would work,
            # so I'll leave it
            attr_data['AttributeRow_CreatedBy'] = pd.NA
            attr_data['AttributeRow_CreatedDate'] = pd.NA
            attr_data['AttributeRow_ModifiedBy'] = pd.NA
            attr_data['AttributeRow_ModifiedDate'] = pd.NA
            attr_select = attr_data[select_list]

        attr_long = attr_select.rename(columns=select_rename)  # renaming columns
        methods = attr_long['Method_Name'].unique()  # get unique table names

        # Iterates through each method name, subsets those data from the main dataset, and performs some transformations
        for method in methods:
            print(method)
            temp = attr_long.loc[attr_long['Method_Name'] == method].drop_duplicates()  # make sure no duplicate data

            # We have to take the subset before pivoting - this pivots on the appropriate unique identifiers
            # Some of this behavior is artefactual of attempting to reproduce FFI tables, but that plan was abandoned,
            # so some of this is superfluous to the actual code behavior.
            # TODO: Remove superfluous code behavior
            subset = temp.pivot(index=['EventID', 'SampleData_SampleEvent_GUID',
                                       'AttributeData_DataRow_GUID', 'Method_UnitSystem'],
                                columns=['MethodAtt_FieldName'],
                                values='AttributeData_Value').reset_index()
            unit_systems = subset['Method_UnitSystem'].unique()

            # Normalize the table names from previous naming convention
            table_name = method.replace(' ', '').replace('-', '_').replace('(', '_').replace(')', '_').strip('_')

            for col in subset.columns:
                # Ensures species columns use the actual USDA code, not the Spp_GUID
                if 'Spp' in col:
                    spp_df = self['LocalSpecies']
                    subset['Species'] = subset.apply(lambda row:
                                                     spp_df.loc[
                                                         spp_df['LocalSpecies_GUID'] == row[col].upper()
                                                     ].iloc[0]['LocalSpecies_Symbol'],
                                                     axis=1)

            # Assign tree stem count to TreesIndv
            if method == 'Trees - Individuals':
                subset['StemNum'] = subset.groupby(['EventID', 'Species', 'TagNo']).cumcount() + 1
                # if 'DamCd1' in subset.columns:
                #     subset['DamSev1'] = subset.apply(lambda row: str(round(float(row['CharHt']) / float(row['Ht']), 1) * 100)
                #                                      if row['DamCd1'] == '30000'
                #                                      else
                #                                      ''
                #                                      if pd.isna(row['DamSev1']) and not pd.isna(row['DamCd1'])
                #                                      else
                #                                      row['DamSev1'],
                #                                      axis=1)
                # if 'DamCd2' in subset.columns:
                #     subset['DamSev2'] = subset.apply(lambda row: str(round(float(row['CharHt']) / float(row['Ht']), 1) * 100)
                #                                      if row['DamCd2'] == '30000'
                #                                      else
                #                                      ''
                #                                      if pd.isna(row['DamSev2']) and not pd.isna(row['DamCd2'])
                #                                      else
                #                                      row['DamSev2'],
                #                                      axis=1)
                # if 'DamCd3' in subset.columns:
                #     subset['DamSev3'] = subset.apply(lambda row: str(round(float(row['CharHt']) / float(row['Ht']), 1) * 100)
                #                                      if row['DamCd3'] == '30000'
                #                                      else
                #                                      ''
                #                                      if pd.isna(row['DamSev3']) and not pd.isna(row['DamCd3'])
                #                                      else
                #                                      row['DamSev3'],
                #                                      axis=1)
                # if 'DamCd4' in subset.columns:
                #     subset['DamSev4'] = subset.apply(lambda row: str(round(float(row['CharHt']) / float(row['Ht']), 1) * 100)
                #                                      if row['DamCd4'] == '30000'
                #                                      else
                #                                      ''
                #                                      if pd.isna(row['DamSev4']) and not pd.isna(row['DamCd4'])
                #                                      else
                #                                      row['DamSev4'],
                #                                      axis=1)
                # if 'DamCd5' in subset.columns:
                #     subset['DamSev5'] = subset.apply(lambda row: str(round(float(row['CharHt']) / float(row['Ht']), 1) * 100)
                #                                      if row['DamCd5'] == '30000'
                #                                      else
                #                                      ''
                #                                      if pd.isna(row['DamSev5']) and not pd.isna(row['DamCd5'])
                #                                      else
                #                                      row['DamSev5'],
                #                                      axis=1)

            # If TagNo is not included in witness tree info, assign new counts. Might need to QC this.
            elif method == 'Plot Info Wit Trees Comments3':
                if 'WitTreeTagNo' not in subset.columns:
                    subset['WitTreeTagNo'] = subset.groupby(['EventID']).cumcount() + 1
                # also ensures there isn't more than one witness tree per plot/event
                subset.sort_values(['EventID', 'WitDBH'], inplace=True)
                subset.drop_duplicates('EventID', keep='first', inplace=True)

            # 6/10/25 logic for CanopyDensiometer dots or squares method
            # 11/12/25 commenting out because I moved this to a calculated column in SQL
            # elif method == 'Canopy - Densiometer':
            #     if 'DotsCount' in subset.columns:
                    # subset['Method'] = subset.apply(lambda row: 'Dots' if row['DotsCount'] == '96'
                    #                                 else 'Squares' if row['DotsCount'] == '24'
                    #                                 else '',
                    #                                 axis=1)

            # Drop all rows with null EventIDs
            subset.dropna(subset=['EventID'], inplace=True)

            # Some tables have Metric and English unit systems. We need to break these out so we don't mess up the names
            if len(unit_systems) > 1:
                for unit_system in unit_systems:
                    unit_subset = subset.loc[subset['Method_UnitSystem'] == unit_system]
                    if unit_system != 'English':
                        sql_table = f"{table_name}_{unit_system}_Attribute"
                    else:
                        sql_table = f"{table_name}_Attribute"
                    self._data_map[sql_table] = unit_subset
            else:
                # TODO: drop this and we won't need to use the TableMap anymore
                sql_table = f"{table_name}_Attribute"  # Rename to FFI table standard
                subset.drop(columns=['Method_UnitSystem'], axis=1, inplace=True)  # Drop the unit column
                self._data_map[sql_table] = subset  # Add table to our data map

    def _sample_to_many(self):
        """
        Breaks the methods into their SampleData components in a similar way as AttributeData.
        SampleData is metadata on how the data was sampled (e.g. personnel, plot size, units, notes, etc)
        """

        select_list = ['SampleRow_Original_GUID', 'SampleData_SampleEvent_GUID', 'SampleAtt_FieldName',
                       'SampleData_Value', 'SampleRow_CreatedBy', 'SampleRow_CreatedDate', 'SampleRow_ModifiedBy',
                       'SampleRow_ModifiedDate', 'Method_Name', 'Method_UnitSystem']
        select_rename = {'SampleRow_Original_GUID': 'SampleData_SampleRow_GUID',
                         'SampleRow_CreatedBy': 'SampleData_CreatedBy',
                         'SampleRow_CreatedDate': 'SampleData_CreatedDate',
                         'SampleRow_ModifiedBy': 'SampleData_ModifiedBy',
                         'SampleRow_ModifiedDate': 'SampleData_ModifiedDate'}

        sample_data = self['SampleRow'] \
            .merge(self['SampleData'],
                   left_on='SampleRow_ID',
                   right_on='SampleData_SampleRow_ID', how='left') \
            .merge(self['SampleAttribute'],
                   left_on='SampleData_SampleAtt_ID',
                   right_on='SampleAtt_ID', how='left')\
            .merge(self['Method'],
                   left_on='SampleAtt_Method_GUID',
                   right_on='Method_GUID', how='left')
        try:
            sample_select = sample_data[select_list]
        except KeyError:
            sample_data['SampleRow_CreatedBy'] = pd.NA
            sample_data['SampleRow_CreatedDate'] = pd.NA
            sample_data['SampleRow_ModifiedBy'] = pd.NA
            sample_data['SampleRow_ModifiedDate'] = pd.NA
            sample_select = sample_data[select_list]

        sample_long = sample_select.rename(columns=select_rename)

        # Some weird stuff with FFI where the SampleData doesn't actually have GUID assigned, so we need to create one.
        sample_long['SampleData_Original_GUID'] = sample_long.apply(lambda _: str(uuid4()).upper())
        methods = sample_long['Method_Name'].unique()

        # Iterate through each method and create a _Sample table for it.
        for method in methods:
            temp = sample_long.loc[sample_long['Method_Name'] == method]
            subset = temp.pivot(index=['SampleData_SampleRow_GUID', 'SampleData_SampleEvent_GUID',
                                       'SampleData_Original_GUID', 'SampleData_CreatedBy',
                                       'SampleData_CreatedDate', 'SampleData_ModifiedBy',
                                       'SampleData_ModifiedDate', 'Method_UnitSystem'],
                                columns=['SampleAtt_FieldName'],
                                values='SampleData_Value').reset_index()
            unit_systems = subset['Method_UnitSystem'].unique()
            table_name = method.replace(' ', '').replace('-', '_').replace('(', '_').replace(')', '_').strip('_')
            if len(unit_systems) > 1:
                for unit_system in unit_systems:
                    unit_subset = subset.loc[subset['Method_UnitSystem'] == unit_system]
                    unit_subset.drop(columns=['Method_UnitSystem'], axis=1, inplace=True)
                    if unit_system != 'English':
                        sql_table = f"{table_name}_{unit_system}_Sample"
                    else:
                        sql_table = f"{table_name}_Sample"
                    self._data_map[sql_table] = unit_subset
            else:
                sql_table = f"{table_name}_Sample"
                subset.drop(columns=['Method_UnitSystem'], axis=1, inplace=True)
                self._data_map[sql_table] = subset

    def _process_events(self):
        """
        SampleEvent needs some processing. Some of the FFI protocols have multiple "Personnel" columns that track
        who collected data and who recorded. This weird function combines all the FieldTeam and EntryTeam columns,
        respectively.
        """
        def parse_list_val(val):
            if (val is not None) and (str(val) != 'nan') and str(val) != '' and str(val) != ' ':
                comma_parse = val.split(',')
                comma_items = len(comma_parse)

                space_parse = val.split(' ')
                space_items = len(space_parse)

                slash_parse = val.split('/')
                slash_items = len(slash_parse)

                if (comma_items == space_items and comma_items > 1) or (comma_items > 1 and space_items > 0):
                    return [x.strip() for x in comma_parse]
                elif comma_items == 1 and space_items > 1:
                    return [x.strip() for x in space_parse]
                elif slash_items > 1:
                    return [x.strip() for x in slash_parse]
                else:
                    return [x.strip() for x in comma_parse]
            else:
                return []

        def combine_teams(row, return_field):
            duff_field = row['DuffFieldTeam']
            duff_entry = row['DuffEntryTeam']
            hr_field = row['HrFieldTeam']
            hr_entry = row['HrEntryTeam']
            fine_field = row['FineFieldTeam']
            fine_entry = row['FineEntryTeam']
            veg_field = row['VegFieldTeam']
            veg_entry = row['VegEntryTeam']
            trees_field = row['TreesFieldTeam']
            trees_entry = row['TreesEntryTeam']
            sap_field = row['SapFieldTeam']
            sap_entry = row['SapEntryTeam']
            seed_field = row['SeedFieldTeam']
            seed_entry = row['SeedEntryTeam']

            if return_field == 'FuelsObserver':
                fuels_field = ', '.join(
                    list(set(
                        parse_list_val(duff_field) +
                        parse_list_val(hr_field) +
                        parse_list_val(fine_field) +
                        parse_list_val(veg_field)
                    ))
                )
                return fuels_field
            elif return_field == 'FuelsRecorder':
                fuels_entry = ', '.join(
                    list(set(
                        parse_list_val(duff_entry) +
                        parse_list_val(hr_entry) +
                        parse_list_val(fine_entry) +
                        parse_list_val(veg_entry)
                    ))
                )
                return fuels_entry
            elif return_field == 'TreeObserver':
                all_tree_field = ', '.join(
                    list(set(
                        parse_list_val(trees_field) +
                        parse_list_val(sap_field) +
                        parse_list_val(seed_field)
                    ))
                )
                return all_tree_field
            elif return_field == 'TreeRecorder':
                all_tree_entry = ', '.join(
                    list(set(
                        parse_list_val(trees_entry) +
                        parse_list_val(sap_entry) +
                        parse_list_val(seed_entry)
                    ))
                )
                return all_tree_entry

        # 1/29/2025: KeyError exceptions added to handle circumstances where personnel was not entered
        try:
            self['SurfaceFuels_Duff_Litter_Sample']['DuffFieldTeam'] = self['SurfaceFuels_Duff_Litter_Sample']['FieldTeam']
        except KeyError:
            self['SurfaceFuels_Duff_Litter_Sample']['DuffFieldTeam'] = ""

        try:
            self['SurfaceFuels_Duff_Litter_Sample']['DuffEntryTeam'] = self['SurfaceFuels_Duff_Litter_Sample']['EntryTeam']
        except KeyError:
            self['SurfaceFuels_Duff_Litter_Sample']['DuffEntryTeam'] = ""

        try:
            self['SurfaceFuels_1000Hr_Sample']['HrFieldTeam'] = self['SurfaceFuels_1000Hr_Sample']['FieldTeam']
        except KeyError:
            self['SurfaceFuels_1000Hr_Sample']['HrFieldTeam'] = ""

        try:
            self['SurfaceFuels_1000Hr_Sample']['HrEntryTeam'] = self['SurfaceFuels_1000Hr_Sample']['EntryTeam']
        except KeyError:
            self['SurfaceFuels_1000Hr_Sample']['HrEntryTeam'] = ""

        try:
            self['SurfaceFuels_Fine_Sample']['FineFieldTeam'] = self['SurfaceFuels_Fine_Sample']['FieldTeam']
        except KeyError:
            self['SurfaceFuels_Fine_Sample']['FineFieldTeam'] = ""

        try:
            self['SurfaceFuels_Fine_Sample']['FineEntryTeam'] = self['SurfaceFuels_Fine_Sample']['EntryTeam']
        except KeyError:
            self['SurfaceFuels_Fine_Sample']['FineEntryTeam'] = ""

        try:
            self['SurfaceFuels_Vegetation_Sample']['VegFieldTeam'] = self['SurfaceFuels_Vegetation_Sample']['FieldTeam']
        except KeyError:
            self['SurfaceFuels_Vegetation_Sample']['VegFieldTeam'] = ""

        try:
            self['SurfaceFuels_Vegetation_Sample']['VegEntryTeam'] = self['SurfaceFuels_Vegetation_Sample']['EntryTeam']
        except KeyError:
            self['SurfaceFuels_Vegetation_Sample']['VegEntryTeam'] = ""

        try:
            self['Trees_Individuals_Sample']['TreesFieldTeam'] = self['Trees_Individuals_Sample']['FieldTeam']
        except KeyError:
            self['Trees_Individuals_Sample']['TreesFieldTeam'] = ""

        try:
            self['Trees_Individuals_Sample']['TreesEntryTeam'] = self['Trees_Individuals_Sample']['EntryTeam']
        except KeyError:
            self['Trees_Individuals_Sample']['TreesEntryTeam'] = ""

        try:
            self['Trees_Saplings_DiameterClass_Sample']['SapFieldTeam'] = self['Trees_Saplings_DiameterClass_Sample']['FieldTeam']
        except KeyError:
            self['Trees_Saplings_DiameterClass_Sample']['SapFieldTeam'] = ""

        try:
            self['Trees_Saplings_DiameterClass_Sample']['SapEntryTeam'] = self['Trees_Saplings_DiameterClass_Sample']['EntryTeam']
        except KeyError:
            self['Trees_Saplings_DiameterClass_Sample']['SapEntryTeam'] = ""

        try:
            self['Trees_Seedlings_HeightClass_Sample']['SeedFieldTeam'] = self['Trees_Seedlings_HeightClass_Sample']['FieldTeam']
        except KeyError:
            self['Trees_Seedlings_HeightClass_Sample']['SeedFieldTeam'] = ""
        try:
            self['Trees_Seedlings_HeightClass_Sample']['SeedEntryTeam'] = self['Trees_Seedlings_HeightClass_Sample']['EntryTeam']
        except KeyError:
            self['Trees_Seedlings_HeightClass_Sample']['SeedEntryTeam'] = ""

        temp_events = self['SampleEvent'] \
            .merge(self['MacroPlot'], left_on='SampleEvent_Plot_GUID',
                   right_on='MacroPlot_GUID', how='left', suffixes=('', '_mp')) \
            .merge(self['SurfaceFuels_Duff_Litter_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left', suffixes=('', '_dl')) \
            .merge(self['SurfaceFuels_1000Hr_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left', suffixes=('', '_1000')) \
            .merge(self['SurfaceFuels_Fine_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left', suffixes=('', '_fine')) \
            .merge(self['SurfaceFuels_Vegetation_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left', suffixes=('', '_veg')) \
            .merge(self['Trees_Individuals_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left', suffixes=('', '_ti')) \
            .merge(self['Trees_Saplings_DiameterClass_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left', suffixes=('', '_sapl')) \
            .merge(self['Trees_Seedlings_HeightClass_Sample'], left_on='SampleEvent_GUID',
                   right_on='SampleData_SampleEvent_GUID', how='left', suffixes=('', '_seed'))
        keep_cols = []
        for col in temp_events.columns:
            if ~(col.endswith('_mp') or col.endswith('_dl') or col.endswith('_1000') or col.endswith('_fine')
                 or col.endswith('_veg') or col.endswith('_ti') or col.endswith('_sapl') or col.endswith('_seed')):
                keep_cols.append(col)
        temp_events = temp_events[keep_cols]


        # temp_events = self['SampleEvent'] \
        #     .merge(self['MacroPlot'], left_on='SampleEvent_Plot_GUID', right_on='MacroPlot_GUID', how='left') \
        #     .merge(self['SurfaceFuels_Duff_Litter_Sample'], left_on='SampleEvent_GUID',
        #            right_on='SampleData_SampleEvent_GUID', how='left') \
        #     .merge(self['SurfaceFuels_1000Hr_Sample'], left_on='SampleEvent_GUID',
        #            right_on='SampleData_SampleEvent_GUID', how='left') \
        #     .merge(self['SurfaceFuels_Fine_Sample'], left_on='SampleEvent_GUID',
        #            right_on='SampleData_SampleEvent_GUID', how='left') \
        #     .merge(self['SurfaceFuels_Vegetation_Sample'], left_on='SampleEvent_GUID',
        #            right_on='SampleData_SampleEvent_GUID', how='left') \
        #     .merge(self['Trees_Individuals_Sample'], left_on='SampleEvent_GUID',
        #            right_on='SampleData_SampleEvent_GUID', how='left') \
        #     .merge(self['Trees_Saplings_DiameterClass_Sample'], left_on='SampleEvent_GUID',
        #            right_on='SampleData_SampleEvent_GUID', how='left') \
        #     .merge(self['Trees_Seedlings_HeightClass_Sample'], left_on='SampleEvent_GUID',
        #            right_on='SampleData_SampleEvent_GUID', how='left')
        temp_events['FuelsObserver'] = temp_events.apply(combine_teams, args=('FuelsObserver',), axis=1)
        temp_events['FuelsRecorder'] = temp_events.apply(combine_teams, args=('FuelsRecorder',), axis=1)
        temp_events['TreeObserver'] = temp_events.apply(combine_teams, args=('TreeObserver',), axis=1)
        temp_events['TreeRecorder'] = temp_events.apply(combine_teams, args=('TreeRecorder',), axis=1)

        # sel_events = temp_events[['EventID', 'PlotID', 'SampleEvent_Date', 'SampleEvent_GUID',
        #                           'SampleEvent_Comment', 'SampleEvent_Who', 'TreeObserver', 'TreeRecorder',
        #                           'FuelsObserver', 'FuelsRecorder']]
        self._data_map['SampleEvent'] = temp_events

    def _process_projects(self):
        """
        Projects also need some processing; we need to extract the year for visits and construct a VisitID.
        This enables us to connect monitoring status to our events.
        """
        temp_df = self['MonitoringStatus'] \
            .merge(self['MM_MonitoringStatus_SampleEvent'],
                   how='left',
                   left_on='MonitoringStatus_GUID',
                   right_on='MM_MonitoringStatus_GUID') \
            .merge(self['SampleEvent'],
                   how='left',
                   left_on='MM_SampleEvent_GUID',
                   right_on='SampleEvent_GUID') \
            .merge(self['ProjectUnit'],
                   how='left',
                   left_on='MonitoringStatus_ProjectUnit_GUID',
                   right_on='ProjectUnit_GUID')

        temp_df['VisitYear'] = pd.DatetimeIndex(temp_df['SampleEvent_Date']).year
        temp_df['VisitID'] = temp_df.apply(lambda row: row['ProjectID'] + (
                                                        str(int(row['VisitYear']))
                                                        if not pd.isna(row['VisitYear']) else ''
                                                        ) +
                                                       str(row['MonitoringStatus_Prefix']).strip(' ') +
                                                       (
                                                            str(row['MonitoringStatus_Base']).strip(' ')
                                                            if row['MonitoringStatus_Base'] == 'Fire' else ''
                                                       ) +
                                                       (
                                                           ''
                                                           if 'MonitoringStatus_Suffix' not in temp_df.columns
                                                           else
                                                           str(row['MonitoringStatus_Suffix']).strip(' ')
                                                           if (not pd.isna(row['MonitoringStatus_Suffix'])) and (row['MonitoringStatus_Suffix'] != 'Immediate')
                                                           else
                                                           str(row['MonitoringStatus_Suffix'])[:3]
                                                           if row['MonitoringStatus_Suffix'] == 'Immediate'
                                                           else ''
                                                        ),
                                           axis=1)

        # Join the new data onto Event and write that (and ProjectVisit) to our data map
        event_df = self['SampleEvent'] \
            .merge(temp_df[['MM_SampleEvent_GUID', 'VisitID']],
                   how='left',
                   left_on='SampleEvent_GUID',
                   right_on='MM_SampleEvent_GUID')

        self._data_map['SampleEvent'] = event_df
        self._data_map['ProjectVisit'] = temp_df

    # @staticmethod
    def _rename_fields(self, table_name, df):
        """
        Renames fields in a table based on the mapping provided in FieldMap
        """

        # field_map = pd.read_csv("files/FieldMap.csv")
        if self.field_map.loc[self.field_map['TableName']==table_name].empty:
            return df

        # # Excel keeps generating long trails of ' ' at the end of strings. This fixes that
        # field_map = field_map.applymap(lambda x: x.replace(' ', '')
        #                     if type(x) == str
        #                     else x)

        temp_df = deepcopy(df)
        temp_field_map = self.field_map.loc[self.field_map['TableName'] == table_name]

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
        table_names = list(self.table_map['FFITable'])
        new_map = {}

        for old_table in table_names:
            this_table_map = self.table_map.loc[self.table_map['FFITable'] == old_table]
            if old_table == 'TreeDamage':
                continue

            if old_table not in self._data_map.keys():  # handle methods with no data
                new_name = this_table_map['NewTable'].values[0]  # get new table name
                field_df = self.field_map.loc[self.field_map['TableName'] == new_name]  # get list of fields
                fields = list(field_df['OldColumn'])
                new_df = pd.DataFrame(columns=fields)  # create empty dataframe with those fields
                self._data_map[old_table] = new_df  # insert empty dataframe into the data map

            if not missing_only:
                table_df = self[old_table]
                new_table = this_table_map['NewTable'].values[0]
                new_df = self._rename_fields(new_table, table_df)
                new_map[new_table] = new_df

        if not missing_only:
            self._data_map = new_map

    def _create_table(self, table_name):
        """
        Custom logic for breaking out new tables.
        """

        possible_tables = ['Transect', 'TreeDamage', 'Personnel', 'LUDamageCode', 'LUSevCode']
        if self.many_tables:
            if table_name in possible_tables:
                if table_name == 'Transect':
                    new_df = self['SurfaceFuels_Fine_Attribute']\
                            .merge(self['SampleEvent'], on='EventID', how='left')
                    # for transects at the Plot-level
                    # .merge(self['MacroPlot'], left_on='SampleEvent_Plot_GUID', right_on='MacroPlot_GUID', how='left')
                    temp_df = new_df[['EventID', 'Transect', 'Azimuth', 'Slope']].drop_duplicates()
                    temp_df = temp_df.dropna(subset=['Azimuth'])
                    temp_df['Length'] = 75
                    self['Transect'] = temp_df

                elif table_name == 'TreeDamage':
                    damage_tables = []
                    select_cols = ['EventID', 'TagNo', 'StemNum']
                    val_cols = ['DamCd1', 'DamSev1', 'DamCd2', 'DamSev2', 'DamCd3', 'DamSev3', 'DamCd4', 'DamSev4',
                                'DamCd5', 'DamSev5']
                    new_df = self['Trees_Individuals_Attribute'] \
                        .merge(self['SampleEvent'], on='EventID', how='left')

                    if 'DamCd1' in new_df.columns:
                        if 'DamSev1' in new_df.columns:
                            d1 = new_df[select_cols + ['DamCd1', 'DamSev1']]
                            d1.rename({'DamCd1': 'DamageCode', 'DamSev1': 'SeverityCode'}, axis=1, inplace=True)
                            damage_tables.append(d1)

                    if 'DamCd2' in new_df.columns:
                        if 'DamSev2' in new_df.columns:
                            d2 = new_df[select_cols + ['DamCd2', 'DamSev2']]
                            d2.rename({'DamCd2': 'DamageCode', 'DamSev2': 'SeverityCode'}, axis=1, inplace=True)
                            damage_tables.append(d2)

                    if 'DamCd3' in new_df.columns:
                        if 'DamSev3' in new_df.columns:
                            d3 = new_df[select_cols + ['DamCd3', 'DamSev3']]
                            d3.rename({'DamCd3': 'DamageCode', 'DamSev3': 'SeverityCode'}, axis=1, inplace=True)
                            damage_tables.append(d3)

                    if 'DamCd4' in new_df.columns:
                        if 'DamSev4' in new_df.columns:
                            d4 = new_df[select_cols + ['DamCd4', 'DamSev4']]
                            d4.rename({'DamCd4': 'DamageCode', 'DamSev4': 'SeverityCode'}, axis=1, inplace=True)
                            damage_tables.append(d4)

                    if 'DamCd5' in new_df.columns:
                        if 'DamSev5' in new_df.columns:
                            d5 = new_df[select_cols + ['DamCd5', 'DamSev5']]
                            d5.rename({'DamCd5': 'DamageCode', 'DamSev5': 'SeverityCode'}, axis=1, inplace=True)
                            damage_tables.append(d5)
                    if len(damage_tables) > 0:
                        code_df = pd.concat(damage_tables)
                        final_df = code_df.dropna(subset=['DamageCode', 'SeverityCode'], how='all')
                        final_df = final_df.dropna(subset=['DamageCode'], how='all')
                        self._data_map['TreeDamage'] = final_df
                        self['Trees_Individuals_Attribute'].drop(columns=[col for col in val_cols
                                                                          if col in self[
                                                                              'Trees_Individuals_Attribute'].columns],
                                                                 inplace=True)

                elif table_name == 'LUDamageCode':
                    df = pd.read_csv(f'{ROOT}/files/LUDamageCodes.csv', encoding='latin-1')
                    self._data_map['LUDamageCode'] = df
                elif table_name == 'LUDamageSev':
                    df = pd.read_csv(f'{ROOT}/files/LUDamageSev.csv', encoding='latin-1')
                    self._data_map['LUDamageCode'] = df

    def _insert_into_db(self, ffi_db, table):
        """
        Checks foreign key constraints and inserts any necessary tables first.

        Then generates a MERGE INTO statement directly creating a query with the data values and executes that statement

        :param ffi_db: FFIDatabase object representing the database connection
        :param table: the table to be inserted into the database, as a string
        """

        # Next block pulls in the field and table mappings that were built for the old column and table names to align
        # with the new ones.
        # table_map = pd.read_csv("files/TableMap.csv")
        # field_map = pd.read_csv("files/FieldMap.csv")
        # field_map['TableName'] = field_map.apply(lambda r: r['TableName'].strip(), axis=1)
        # field_map['OldColumn'] = field_map.apply(lambda r: str(r['OldColumn']).strip(), axis=1)
        # field_map['ColumnName'] = field_map.apply(lambda r: str(r['ColumnName']).strip(), axis=1)

        # this_table_map = table_map.loc[table_map['FFITable'] == table]
        # if not this_table_map.empty:
            # new_table_name = this_table_map['NewTable'].values[0]
            # table_name = new_table_name
            #
            # temp_field_map = field_map.loc[field_map['TableName'] == table_name]
            # this_field_map = dict(zip(list(temp_field_map['OldColumn']), list(temp_field_map['ColumnName'])))
            # table_fields = [col for col in list(temp_field_map['OldColumn']) if col != 'nan']

            # handle key constraints
        if table in self.new_tables:
            pks = ffi_db.get_primary_keys()
            fks = ffi_db.get_foreign_keys()

            table_fks = fks[table]
            table_pks = pks[table]
            # multi_pk = len(table_pks) > 1

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

            xml_table = self[table]
            # select_fields = [field for field in table_fields if field in xml_table.columns]
            # final_table = xml_table[select_fields].copy()

            if table == 'ProjectVisit':
                xml_table.drop_duplicates(inplace=True)

            # Construct the VALUES part of the statement
            val_list = []
            col_list = []
            null_cols = []

            # enumerate rows and each value in each row
            # for _, row in xml_table.iterrows():
            #     row_vals = []
            #     for idx, val in enumerate(row):
            #         col = xml_table.columns[idx]
            #         col_type = xml_table[col].dtype
            #         # new_col = this_field_map[col]
            #
            #         if col not in col_list:
            #             col_list.append(col)
            #
            #         # make sure strings get tick marks and ' is converted to '' for the SQL
            #         if col_type in ['float64', 'int64', 'boolean']:
            #             row_vals.append(str(val))
            #         elif (val is None) or (str(val) == 'nan') or (col == 'Offset' and val in ['False', 'True']):
            #             row_vals.append('NULL')
            #         else:
            #             row_vals.append(f"""'{str(val).replace("'", "''")}'""")
            #     val_list.append(f"({', '.join(row_vals)})")
            #
            # values_part = ', '.join(val_list)

            # constructs comma-delimited lists of column names
            cols = list(xml_table.columns)
            cols_str = ', '.join(cols)
            source_cols = [f'source.{c}'
                           for c in cols]
            source_col_str = ', '.join(source_cols)

            # Constructs identity relations for primary keys
            if table == 'TreeDamage':
                pk_strings = [f'target.{c} = source.{c}'
                              for c in cols]
            else:
                pk_strings = [f'target.{pk} = source.{pk}'
                              for pk in table_pks]
            pk_part = ' AND '.join(pk_strings)

            # Generate the full MERGE INTO statement
            merge_into_sql = f"""
            MERGE INTO dbo.{table} WITH (HOLDLOCK) AS target
            USING staging.{table} AS source 
                ON {pk_part}
            WHEN NOT MATCHED BY target THEN
                INSERT ({cols_str})
                VALUES ({source_col_str});
            """

            ffi_db.create_schema('staging')

            with ffi_db.start_session() as sesh:
                xml_table.to_sql(table, sesh.bind, schema='staging', if_exists='replace', index=False)
                try:

                    count_sql = f"SELECT COUNT(*) AS Size FROM {table}"
                    before_df = pd.read_sql(count_sql, sesh.bind)  # Compare before and after row count of tables
                    before_count = before_df['Size'].values[0]

                    # # Insert data
                    sesh.execute(merge_into_sql)
                    sesh.execute(f"DROP TABLE IF EXISTS staging.{table}")
                    sesh.commit()
                    #
                    # # Get count after insert
                    after_df = pd.read_sql(count_sql, sesh.bind)
                    after_count = after_df['Size'].values[0]
                    #
                    count_diff = after_count - before_count
                    #
                    # # Then we insert into our logging table for who made changes to the database tables
                    # if count_diff != 0:
                    #     change_type = "INSERT" if count_diff > 0 else "DELETE"
                    #
                    #     # handle datetime
                    #     dt = str(datetime.datetime.now())
                    #     new_dt = re.findall(r'(.*)\.\d{4}', dt)[0]
                    #
                    #     # construct log data and insert
                    #     change_df = DataFrame({'User': [self._user],
                    #                            'Time': [new_dt],
                    #                            'Table': [table],
                    #                            'ChangeType': [change_type],
                    #                            'Changes': [abs(count_diff)]})
                    #     change_df.to_sql('UpdateLog', sesh.bind, if_exists='append', index=False)
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

    def extract(self):

        print(f"Reading data for {self.file}")
        logging.info(f"Transforming tables for {self.file}")
        self._parse_data()
        self._parse_idents()
        self.version = self['Schema_Version']['Schema_Version'][0]
        self.admin_unit = self['RegistrationUnit']['RegistrationUnit_Name'][0]

    def transform(self, pivot_only=False):

        self._attr_to_many()
        self._sample_to_many()
        self.many_tables = True

        # need to normalize project names for ProjectID
        self['ProjectUnit']['ProjectID'] = self['ProjectUnit'].apply(
            lambda row: row['ProjectUnit_Name'].replace('_', '').replace(' ', ''),
            axis=1
        )

        # add admin unit for data quality
        self['ProjectUnit']['AdminUnit'] = self.admin_unit
        self['MacroPlot']['AdminUnit'] = self.admin_unit

        # Create transects from SurfaceFuels_Fine_Attribute
        # new_df = self['SurfaceFuels_Fine_Attribute']\
        #     .merge(self['SampleEvent'], on='EventID', how='left')
        # # for transects at the Plot-level
        # # .merge(self['MacroPlot'], left_on='SampleEvent_Plot_GUID', right_on='MacroPlot_GUID', how='left')
        # temp_df = new_df[['EventID', 'Transect', 'Azimuth', 'Slope']].drop_duplicates()
        # temp_df = temp_df.dropna(subset=['Azimuth'])
        # temp_df['Length'] = 75
        # self['Transect'] = temp_df

        self._create_table('LUDamageCode')
        self._create_table('LUSevCode')
        self._create_table('Transect')
        self._create_table('TreeDamage')
        self._process_events()
        self._process_projects()

        if not pivot_only:
            self._rename_tables()
        elif pivot_only:
            self._rename_tables(missing_only=True)

        print("exit")
        # del self._data_map['SampleData']
        # del self._data_map['SampleRow']
        # del self._data_map['AttributeRow']
        # del self._data_map['AttributeData']

        # self._rename_tables()

    def load(self, ffi_db):
        """
        Iterates through each table in the data map and inserts it into the database
        """
        print(f'Inserting data for {self.file}')
        logging.info(f"Inserting data for {self.file}")

        # need to insert these tables first
        # self._insert_into_db(ffi_db, 'AdminUnit')
        # self._insert_into_db(ffi_db, 'Project')
        # self._insert_into_db(ffi_db, 'Plot')
        # self._insert_into_db(ffi_db, 'Event')
        # self._processed = self._processed + ['AdminUnit', 'Project', 'Plot', 'Event']
        #
        # self._data_map = [temp for temp in self._data_map if temp not in first_tables]
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
