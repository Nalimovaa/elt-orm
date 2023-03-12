import datetime

import pandas as pd
from sqlalchemy import create_engine, MetaData, Connection, literal, and_
from sqlalchemy import sql as qs
from sqlalchemy import Table
from sqlalchemy.dialects import postgresql

from TableTypes import BaseTable, FileTable
from datetime import timedelta

class ILoadMeta(type):
    _engines = ['SRC_engine', 'STG_TRG_engine']
    _schemas = {'SRC_engine': 'SRC_schema', 'STG_TRG_engine': 'STG_TRG_schema'}
    _connections = {'SRC_engine': 'SRC_connect', 'STG_TRG_engine': 'STG_TRG_connect'}
    _alchemy_metas = {'SRC_engine': 'SRC_schema', 'STG_TRG_engine': 'STG_TRG_schema'}

    def _get_alchemy_engine_by_name(cls, attrs, engine_name):
        db_connection = attrs.get('db_connection')
        engine_obj = db_connection.get(engine_name)
        return engine_obj

    def _get_alchemy_meta_by_engine_name(cls, attrs, engine_name):
        db_connection = attrs.get('db_connection')
        meta_obj = db_connection.get(cls._alchemy_metas.get(engine_name))
        return meta_obj

    def _make_connect(cls, cls_name, attrs: dict):
        meta = attrs.get('Meta')
        del attrs['Meta']
        if not meta:
            raise TypeError('{} => Meta subclass not found'.format(cls_name))
        meta_vars = vars(meta)
        db_connection = {}
        for engine_attr_name in cls._engines:
            engine_str = meta_vars.get(engine_attr_name)
            engine_obj = create_engine(engine_str)
            db_connection.update({engine_attr_name: engine_obj})
            attrs.update({cls._connections[engine_attr_name]: engine_obj.connect()})
            schema = meta_vars.get(cls._alchemy_metas[engine_attr_name])
            metadata_alchemy = MetaData(schema=schema)
            db_connection.update({cls._alchemy_metas[engine_attr_name]: metadata_alchemy})
        attrs.update({'db_connection': db_connection})

    def _replace_fields_to_alchemy_tables(cls, cls_name, attrs):
        tables = attrs.get('tables')
        for table_, table_obj in tables.items():
            print(table_, table_obj)
            table_name = table_obj.get_table_name()
            engine_name = table_obj.get_engine_name()
            metadata_obj = cls._get_alchemy_meta_by_engine_name(cls=cls, attrs=attrs, engine_name=engine_name)
            engine_obj = cls._get_alchemy_engine_by_name(cls=cls, attrs=attrs, engine_name=engine_name)
            table = Table(table_name, metadata_obj, autoload_with=engine_obj)
            attrs.update({table_: table})


    def __new__(cls, name, bases, attrs):
        if name == 'ILoad':
            return type.__new__(cls, name, bases, attrs)

        fields = {field_name: attr for field_name, attr in attrs.items() if isinstance(attr, BaseTable)}
        attrs_clean = {field_name: attr for field_name, attr in attrs.items() if not isinstance(attr, BaseTable)}
        attrs_clean['tables'] = fields
        cls._make_connect(cls=cls, cls_name=name, attrs=attrs_clean)
        cls._replace_fields_to_alchemy_tables(cls=cls, cls_name=name, attrs=attrs_clean)
        #print(attrs_clean)
        return super().__new__(cls, name, bases, attrs_clean)


class ILoad(metaclass=ILoadMeta):
    # For IDE
    STG_TRG_connect = Connection
    SRC_connect = Connection

    @classmethod
    def GTTM(cls, table_name):
        """
        get table type model
        :param table_name: имя переменной в объекте
        :return: table type obj
        """
        return cls.tables.get(table_name)

    @classmethod
    def get_create_dt_else_update_dt(cls, vartable_name):
        ALCtable = getattr(cls, vartable_name)
        if hasattr(cls.tables.get(vartable_name), 'create_dt'):
            column_name = getattr(cls.tables.get(vartable_name), 'create_dt')
        else:
            column_name = getattr(cls.tables.get(vartable_name), 'update_dt')
        ALCculumn = getattr(ALCtable.columns, column_name)
        return ALCculumn

    @classmethod
    def GAC(cls, vartable_name, varcolumn_name):
        """
        get sqlalchemy culumn from reference names
        Данная функция является get'тером в механизме виртуализации названий полей в таблицах.

        :param vartable_name: имя переменной таблицы (в формате str)
        :param varcolumn_name: референсное название колонки (_init_data_type в TableTypes объектах)
        :return: sqlalchemy culumn OBJ
        """
        ALCtable = getattr(cls, vartable_name)
        field_name = getattr(cls.tables.get(vartable_name), varcolumn_name)
        ALCculumn = getattr(ALCtable.columns, field_name)
        return ALCculumn

    @classmethod
    def get_collums_name_for_ALT(cls, obj):
        tmp = []
        for column in obj.columns:
            tmp.append(str(column.name))
        return tmp

    @classmethod
    def get_engine(cls, name):
        if hasattr(cls, 'db_connection'):
            db_connection = getattr(cls, 'db_connection')
            return db_connection.get(name + '_engine')

    @classmethod
    def get_keys_from_cursor(cls, cursor):
        return [col for col in cursor.keys()]

    @classmethod
    def QC(cls, q):
        """
        QC - Querry Compiler - нужно выполнять с Querry перед execute
        :param q: querry
        :return: compiled query
        """
        return q.compile(dialect=postgresql.dialect())

    @classmethod
    def get_value_ALC_columns_from_vartable_name(cls, vartable):
        table_obj = getattr(cls, vartable)
        fields_values_tuple = cls.GTTM(vartable).fields_values
        res = []
        for fields_name in fields_values_tuple:
            res.append(getattr(table_obj.c, fields_name))
        return res

    @classmethod
    def generate_qs_or_for_value_columns(cls, vartable_stg, vartable_trg):
        stg_value_columns = cls.get_value_ALC_columns_from_vartable_name(vartable_stg)
        trg_value_columns = cls.get_value_ALC_columns_from_vartable_name(vartable_trg)
        query = []
        for stg_column, trg_column in zip(stg_value_columns, trg_value_columns):
            query.append(qs.or_(
                stg_column != trg_column,
                stg_column == None,
                stg_column == None,
            ))
        return qs.or_(*query)

    # INCREMENTAL LOAD START
    @classmethod
    def clean_stage_tables(cls):
        stage = getattr(cls, 'stg_table')
        q1 = qs.delete(stage)
        cls.STG_TRG_connect.execute(q1)
        if hasattr(cls, 'stg_del_table'):
            stage_del = getattr(cls, 'stg_del_table')
            q2 = qs.delete(stage_del)
            cls.STG_TRG_connect.execute(q2)
        cls.STG_TRG_connect.commit()

    @classmethod
    def сapture_data_from_source(cls):
        if hasattr(cls, 'meta_table'):
            meta_max_dt = cls.GAC('meta_table', 'max_update_dt')
            meta_schema = cls.GAC('meta_table', 'schema_name')
            meta_table = cls.GAC('meta_table', 'table_name')

            meta_schema_name = cls.meta_table.schema
            meta_table_name = cls.meta_table_name
            q = qs.select(meta_max_dt).where(meta_schema==meta_schema_name).where(meta_table==meta_table_name)
            compiled_query = cls.QC(q)

            max_dt_set = cls.STG_TRG_connect.execute(compiled_query)
            max_dt = max_dt_set.fetchall()
            max_dt = max_dt[0][0]



        if isinstance(cls.src_table, FileTable):
            stg_AL_obj = getattr(cls, 'stg_table')
            names = cls.get_collums_name_for_ALT(stg_AL_obj)
            df = cls.src_table.get_df_dim()

        else:
            src_AL_obj = getattr(cls, 'src_table')
            src_table__create_dt = cls.GAC('src_table', 'create_dt')
            q = qs.select(src_AL_obj).where(src_table__create_dt > max_dt)
            compiled_query = cls.QC(q)
            cursor = cls.SRC_connect.execute(compiled_query)
            records = cursor.fetchall()

            stg_AL_obj = getattr(cls, 'stg_table')
            names = cls.get_collums_name_for_ALT(stg_AL_obj)
            df = pd.DataFrame(records, columns=names)

        q = qs.insert(stg_AL_obj).values(df.values.tolist())
        q = cls.QC(q)
        cls.STG_TRG_connect.execute(q)
        cls.STG_TRG_connect.commit()

    @classmethod
    def сapture_deleted_ids(cls):
        if isinstance(cls.src_table, FileTable):
            stg_AL_obj = getattr(cls, 'stg_table')
            names = cls.get_collums_name_for_ALT(stg_AL_obj)
            df = cls.src_table.get_df_dim()
            id_field_name = cls.GAC('trg_table', 'id').name
        else:
            src_AL_obj = getattr(cls, 'src_table')
            q = qs.select(src_AL_obj)
            compiled_query = cls.QC(q)
            cursor = cls.SRC_connect.execute(compiled_query)
            records = cursor.fetchall()
            names = cls.get_collums_name_for_ALT(src_AL_obj)
            df = pd.DataFrame(records, columns=names)
            id_field_name = cls.GAC('src_table', 'id').name
        ids = df[[id_field_name]].values.tolist()
        stg_del_AL_obj = getattr(cls, 'stg_del_table')
        q = qs.insert(stg_del_AL_obj).values(ids)
        q = cls.QC(q)
        cls.STG_TRG_connect.execute(q)
        cls.STG_TRG_connect.commit()

    @classmethod
    def stage_to_target_fact(cls):
        stg = getattr(cls, 'stg_table')
        tgt = getattr(cls, 'trg_table')

        stmt = qs.insert(tgt).from_select(tgt.c, stg)
        q = cls.QC(stmt)

        cls.STG_TRG_connect.execute(q)
        cls.STG_TRG_connect.commit()

    @classmethod
    def stage_to_target(cls):
        stg = getattr(cls, 'stg_table')
        tgt = getattr(cls, 'trg_table')

        s = qs.select(
            cls.GAC('stg_table', 'id'),
            *(cls.get_value_ALC_columns_from_vartable_name('stg_table')),
            cls.get_create_dt_else_update_dt('stg_table'),
            literal("9999-12-31").label(cls.GAC('trg_table', 'end_dt').name),
            literal("N").label(cls.GAC('trg_table', 'deleted_flag').name),
        ) \
            .select_from(stg.join(tgt,
                                  and_(cls.GAC('stg_table', 'id') == cls.GAC('trg_table', 'id'),
                                       cls.GAC('trg_table', 'end_dt') == literal("'9999-12-31'"),
                                       cls.GAC('trg_table', 'deleted_flag') == literal("'N'")),
                                  isouter=True)) \
            .where(cls.GAC('trg_table', 'id').is_(None))

        #stmt = qs.insert(tgt).from_select(['account_num', 'valid_to', 'client', 'start_dt', 'end_dt', 'deleted_flg'], s)
        stmt = qs.insert(tgt).from_select(tgt.c, s)
        #print(stmt)
        q = cls.QC(stmt)

        cls.STG_TRG_connect.execute(q)
        cls.STG_TRG_connect.commit()

    @classmethod
    def add_updates_from_source(cls):
        stg_table = getattr(cls, 'stg_table')
        trg_table = getattr(cls, 'trg_table')

        subq = (
            qs.select(cls.GAC('stg_table', 'id'), cls.GAC('stg_table', 'update_dt'))
                .select_from(stg_table.join(trg_table, and_(
                cls.GAC('stg_table', 'id') == cls.GAC('trg_table', 'id'),
                cls.GAC('trg_table', 'end_dt') == qs.text("to_date('9999-12-31', 'YYYY-MM-DD')"),
                cls.GAC('trg_table', 'deleted_flag') == 'N',
            )))
                .where(cls.generate_qs_or_for_value_columns('stg_table', 'trg_table'))
        )
        update_dt_subq_field = getattr(subq.c, cls.GAC('stg_table', 'update_dt').name)
        update_value_kwargs = {cls.GAC('trg_table', 'end_dt').name: update_dt_subq_field - timedelta(seconds=1)}
        q = trg_table.update().values(**update_value_kwargs).where(cls.GAC('trg_table', 'id') == getattr(subq.c, cls.GAC('stg_table', 'id').name))

        q = cls.QC(q)
        cls.STG_TRG_connect.execute(q)
        cls.STG_TRG_connect.commit()

        q = qs.insert(trg_table).from_select(
            trg_table.c,
            qs.select(
                cls.GAC('stg_table', 'id'),
                *(cls.get_value_ALC_columns_from_vartable_name('stg_table')),
                cls.GAC('stg_table', 'update_dt'),
                literal("9999-12-31").label(cls.GAC('trg_table', 'end_dt').name),
                literal("N").label(cls.GAC('trg_table', 'deleted_flag').name),
            ).select_from(
                stg_table.join(
                    trg_table,
                    and_(
                        cls.GAC('stg_table', 'id') == cls.GAC('trg_table', 'id'),
                        cls.GAC('trg_table', 'end_dt') == cls.get_create_dt_else_update_dt('stg_table') - qs.text("interval '1 second'"),
                        cls.GAC('trg_table', 'deleted_flag') == 'N'
                    )
                )
            ).where(cls.generate_qs_or_for_value_columns('stg_table', 'trg_table'))
        )

        q = cls.QC(q)
        cls.STG_TRG_connect.execute(q)
        cls.STG_TRG_connect.commit()


    @classmethod
    def mark_as_delete(cls):
        stg_table = getattr(cls, 'stg_table')
        trg_table = getattr(cls, 'trg_table')


        select_stmt = qs.select(cls.GAC('trg_table', 'id'),
                              *(cls.get_value_ALC_columns_from_vartable_name('trg_table')),
                              literal(datetime.datetime.now()).label(cls.GAC('trg_table', 'start_dt').name),
                              literal("9999-12-31").label(cls.GAC('trg_table', 'end_dt').name),
                              literal("Y").label(cls.GAC('trg_table', 'deleted_flag').name)). \
            where(cls.GAC('trg_table', 'id').in_(
            qs.select(cls.GAC('trg_table', 'id')). \
                select_from(trg_table. \
                            outerjoin(stg_table, cls.GAC('stg_table', 'id') == cls.GAC('trg_table', 'id'))). \
                where(cls.GAC('stg_table', 'id').is_(None)). \
                where(cls.GAC('trg_table', 'end_dt') == "9999-12-31"). \
                where(cls.GAC('trg_table', 'deleted_flag') == "N")
        )). \
            where(cls.GAC('trg_table', 'end_dt') == "9999-12-31"). \
            where(cls.GAC('trg_table', 'deleted_flag') == "N")

        # Build the INSERT statement
        insert_stmt = qs.insert(trg_table). \
            from_select(trg_table.c, select_stmt)

        q = cls.QC(insert_stmt)
        cls.STG_TRG_connect.execute(q)
        cls.STG_TRG_connect.commit()


        values_kwargs = {cls.GAC('trg_table', 'end_dt').name: literal(datetime.datetime.now()) - qs.text("interval '1 second'")}
        stmt = qs.update(trg_table).where(
            cls.GAC('trg_table', 'id').in_(
                qs.select(cls.GAC('trg_table', 'id')).select_from(
                    trg_table.join(stg_table, cls.GAC('stg_table', 'id') == cls.GAC('trg_table', 'id'), isouter=True)
                ).where(cls.GAC('stg_table', 'id') == None)
                    .where(cls.GAC('trg_table', 'end_dt') == qs.func.to_date("9999-12-31", "YYYY-MM-DD"))
                    .where(cls.GAC('trg_table', 'deleted_flag') == "N")
            )
        ).where(cls.GAC('trg_table', 'end_dt') == qs.func.to_date("9999-12-31", "YYYY-MM-DD"))\
            .where(cls.GAC('trg_table', 'deleted_flag') == "N")\
            .values(**values_kwargs)


        q = cls.QC(stmt)
        cls.STG_TRG_connect.execute(q)
        cls.STG_TRG_connect.commit()

    @classmethod
    def update_meta(cls):
        stg_table = getattr(cls, 'stg_table')
        meta_table = getattr(cls, 'meta_table')

        meta_schema_name = cls.meta_table.schema
        meta_table_name = cls.meta_table_name

        values_kwargs = {cls.GAC('meta_table', 'max_update_dt').name: qs.select(qs.func.coalesce(
                qs.select(qs.func.max(cls.get_create_dt_else_update_dt('stg_table'))),
                qs.select(cls.GAC('meta_table', 'max_update_dt')).where(
                    cls.GAC('meta_table', 'schema_name') == meta_schema_name
                ).where(
                    cls.GAC('meta_table', 'table_name') == meta_table_name
                )
            ))}

        q = qs.update(meta_table).where(
            cls.GAC('meta_table', 'schema_name') == meta_schema_name
        ).where(
            cls.GAC('meta_table', 'table_name') == meta_table_name
        ).values(**values_kwargs)

        q = cls.QC(q)
        cls.STG_TRG_connect.execute(q)
        cls.STG_TRG_connect.commit()






