import datetime
import os.path

import pandas as pd


class BaseTable:
    engine = None

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls)
        for attr in vars(__class__):
            attr_value = getattr(__class__, attr)
            if isinstance(attr_value, dict):
                if not hasattr(cls, attr):
                    setattr(cls, attr, attr_value.copy())
                else:
                    attr_obj = getattr(cls, attr)
                    attr_obj.update(attr_value)
        return obj

    _errors = {
        'init__validate_data_type': '{model} initialization error: "{field}" field have type "{curr_type}", but "{reference_type}" expected!'
    }

    _init_data_type = {
        'table': str,
    }
    _init_data_type_extra = []

    def __init__(self, table, fields, *args):
        self.table = table
        self.fields = fields
        for field_name, value in fields.items():
            setattr(self, field_name, value)
        self.fields_values = args
        self._validate_init_data()

    def _make_exception(self, key, **kwargs): #TODO проверить
        return Exception(self._errors[key].format(**kwargs))

    def _validate_init_data_type(self):
        for attr_name, attr_reference_type in self._init_data_type.items():
            attr = getattr(self, attr_name)
            test = isinstance(attr, attr_reference_type)
            if not test:
                raise Exception(BaseTable._errors['init__validate_data_type'].format(
                    model=self.__class__.__name__, field=attr_name, curr_type=type(attr), reference_type=attr_reference_type
                ))

    def _validate_init_data(self):
        self._validate_init_data_type()
        for func_attr in self._init_data_type_extra:
            func = getattr(self, func_attr)
            func()

    def get_id_field_name(self):
        return getattr(self, 'id')

    def get_fields(self):
        out = {}
        for attr_name in self._init_data_type:
            out.update({attr_name: getattr(self, attr_name)})
        return out

    def get_table_name(self):
        return getattr(self, 'table')

    def get_engine_name(self):
        return self.engine

    def _check_list_for_str_contains_only(self, list_fns):
        if len(list_fns):
            for value in list_fns:
                if not isinstance(value, str):
                    raise Exception(self._errors['init__validate_values_fns_containing'].format(
                        model=self.__class__.__name__, field='values_fns', reference_type=str
                    ))
        else:
            raise Exception(self._errors['init__validate_values_fns_count'].format(
                model=self.__class__.__name__, field='values_fns'
            ))



class Table(BaseTable):
    _init_data_type = {
        'create_dt': str,
        'update_dt': str,
        'id': str,
    }

    def __init__(self, table, create_dt, update_dt, id, *args):
        fields = {'create_dt': create_dt, 'update_dt': update_dt, 'id': id}
        super().__init__(table, fields, *args)


class StageDeleteTable(BaseTable):
    engine = 'STG_TRG_engine'
    _init_data_type = {
        'id': str,
    }

    def __init__(self, table, id):
        fields = {'id': id}
        super().__init__(table, fields)


class SourceTable(Table):
    engine = 'SRC_engine'


class StageTable(Table):
    engine = 'STG_TRG_engine'


class StageTableWithoutCreate(BaseTable):
    engine = 'STG_TRG_engine'

    _init_data_type = {
        'update_dt': str,
        'id': str,
    }

    def __init__(self, table, update_dt, id, *args):
        fields = {'update_dt': update_dt, 'id': id}
        super().__init__(table, fields, *args)


class StageTableWithoutDates(BaseTable):
    engine = 'STG_TRG_engine'

    _init_data_type = {
        'id': str,
    }

    def __init__(self, table, id, *args):
        fields = {'id': id}
        super().__init__(table, fields, *args)


class TargetTableWithoutDates(BaseTable):
    engine = 'STG_TRG_engine'

    _init_data_type = {
        'id': str,
    }

    def __init__(self, table, id, *args):
        fields = {'id': id}
        super().__init__(table, fields, *args)


class TargetTable(BaseTable):
    engine = 'STG_TRG_engine'

    _init_data_type = {
        'start_dt': str,
        'end_dt': str,
        'deleted_flag': str,
        'id': str,
    }

    def __init__(self, table, start_dt, end_dt, deleted_flag, id, *args):
        fields = {'start_dt': start_dt, 'end_dt': end_dt, 'deleted_flag': deleted_flag, 'id': id}
        super().__init__(table, fields, *args)


class StageFactTable(BaseTable):
    engine = 'STG_TRG_engine'

    _init_data_type = {
    }

    def __init__(self, table, *args):
        fields = {}
        super().__init__(table, fields, *args)



class MetaTable(BaseTable):
    engine = 'STG_TRG_engine'

    _init_data_type = {
        'schema_name': str,
        'table_name': str,
        'max_update_dt': str,
    }

    def __init__(self, table, schema_name, table_name, max_update_dt):
        fields = {'schema_name': schema_name, 'table_name': table_name, 'max_update_dt': max_update_dt}
        super().__init__(table, fields)


class FileTable():
    def __init__(self, slug, extension, path):
        self.slug = slug
        self.extension = extension
        self.path = path

    def get_dt_from_file_name(self, file_name):
        dateonly = file_name.replace(self.slug, '').replace(self.extension, '').replace(self.path, '')
        datetime_ = datetime.datetime.strptime(dateonly, '%d%m%Y')
        return datetime_

    def get_file_by_slug(self):
        list_dir = os.listdir(self.path)
        file_name_curr = None
        for file_name in list_dir:
            if file_name.find(self.slug) > -1 and file_name.find(self.extension) > -1:
                file_name_curr = file_name
                break
        path = self.path + file_name_curr
        return path, self.get_dt_from_file_name(path)


    _pd_func_ext_dict = {
        '.xlsx': pd.read_excel,
        '.csv': pd.read_csv,
    }
    _pd_kwargs = {
        '.xlsx': {},
        '.csv': {
            'delimiter': ';',
        },
    }

    def get_df_dim(self):
        path, datetime_ = self.get_file_by_slug()
        if path:
            df = self._pd_func_ext_dict[self.extension](path, header=0, index_col=None, **self._pd_kwargs[self.extension])
            datetime_str = datetime_.strftime('%Y-%m-%d')
            df['update_dt'] = datetime_str
            print(datetime_str)
            return df

