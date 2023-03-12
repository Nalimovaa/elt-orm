import datetime

from Engine import ILoad
from TableTypes import SourceTable, StageTable, StageDeleteTable, TargetTable, MetaTable, FileTable, StageFactTable, \
    StageTableWithoutCreate, StageTableWithoutDates, TargetTableWithoutDates

meta_table_global = MetaTable('nali_meta', 'schema_name', 'table_name', 'max_update_dt')


class MetaSRCdb:
    SRC_engine = "postgresql+psycopg2://bank_etl:bank_etl_password@de-edu-db.chronosavant.ru/bank"
    STG_TRG_engine = "postgresql+psycopg2://de11an:peregrintook@de-edu-db.chronosavant.ru/edu"

    SRC_schema = 'info'
    STG_TRG_schema = 'de11an'


class ILaccounts(ILoad):
    src_table = SourceTable('accounts', 'create_dt', 'update_dt', 'account', 'valid_to', 'client')
    stg_table = StageTable('nali_stg_accounts', 'create_dt', 'update_dt', 'account_num', 'valid_to', 'client')
    stg_del_table = StageDeleteTable('nali_stg_del', 'id')
    trg_table = TargetTable('nali_dwh_dim_accounts_hist', 'start_dt', 'end_dt', 'deleted_flg', 'account_num', 'valid_to', 'client')
    meta_table = meta_table_global
    Meta = MetaSRCdb
    meta_table_name = 'nali_SOURCE_accounts'

    @classmethod
    def load(cls):
        cls.clean_stage_tables()
        cls.сapture_data_from_source()
        cls.сapture_deleted_ids()
        cls.stage_to_target()
        cls.add_updates_from_source()
        cls.mark_as_delete()
        cls.update_meta()


class ILcards(ILoad):
    src_table = SourceTable('cards', 'create_dt', 'update_dt', 'card_num', 'account')
    stg_table = StageTable('nali_stg_cards', 'create_dt', 'update_dt', 'card_num', 'account_num')
    stg_del_table = StageDeleteTable('nali_stg_del', 'id')
    trg_table = TargetTable('nali_dwh_dim_cards_hist', 'start_dt', 'end_dt', 'deleted_flg', 'card_num', 'account_num')
    meta_table = meta_table_global
    Meta = MetaSRCdb
    meta_table_name = 'nali_SOURCE_cards'

    @classmethod
    def load(cls):
        cls.clean_stage_tables()
        cls.сapture_data_from_source()
        cls.сapture_deleted_ids()
        cls.stage_to_target()
        cls.add_updates_from_source()
        cls.mark_as_delete()
        cls.update_meta()


class ILclients(ILoad):
    src_table = SourceTable('clients', 'create_dt', 'update_dt', 'client_id', 'last_name', 'first_name', 'patronymic', 'date_of_birth', 'passport_num', 'passport_valid_to', 'phone')
    stg_table = StageTable('nali_stg_clients', 'create_dt', 'update_dt', 'client_id', 'last_name', 'first_name', 'patronymic', 'date_of_birth', 'passport_num', 'passport_valid_to', 'phone')
    stg_del_table = StageDeleteTable('nali_stg_del', 'id')
    trg_table = TargetTable('nali_dwh_dim_clients_hist', 'start_dt', 'end_dt', 'deleted_flg', 'client_id', 'last_name', 'first_name', 'patronymic', 'date_of_birth', 'passport_num', 'passport_valid_to', 'phone')
    meta_table = meta_table_global
    Meta = MetaSRCdb
    meta_table_name = 'nali_SOURCE_clients'

    @classmethod
    def load(cls):
        cls.clean_stage_tables()
        cls.сapture_data_from_source()
        cls.сapture_deleted_ids()
        cls.stage_to_target()
        cls.add_updates_from_source()
        cls.mark_as_delete()
        cls.update_meta()


class ILterminals(ILoad):
    src_table = FileTable('terminals_', '.xlsx', 'data/')
    stg_table = StageTableWithoutCreate('nali_stg_terminals', 'update_dt', 'terminal_id', 'terminal_type', 'terminal_city', 'terminal_address')
    stg_del_table = StageDeleteTable('nali_stg_del', 'id')
    trg_table = TargetTable('nali_dwh_dim_terminals_hist', 'start_dt', 'end_dt', 'deleted_flg', 'terminal_id', 'terminal_type', 'terminal_city', 'terminal_address')
    meta_table = meta_table_global
    Meta = MetaSRCdb
    meta_table_name = 'nali_SOURCE_terminals'

    @classmethod
    def load(cls):
        cls.clean_stage_tables()
        cls.сapture_data_from_source()
        cls.сapture_deleted_ids()
        cls.stage_to_target()
        cls.add_updates_from_source()
        cls.mark_as_delete()
        cls.update_meta()

class ILtransactions(ILoad):
    src_table = FileTable('transactions_', '.csv', 'data/')
    stg_table = StageTableWithoutDates('nali_stg_transactions', 'transaction_id', 'transaction_date', 'amount', 'card_num', 'oper_type', 'oper_result', 'terminal')
    trg_table = TargetTableWithoutDates('nali_dwh_fact_transactions', 'trans_id', 'trans_date', 'amt', 'card_num', 'oper_type', 'oper_result', 'terminal')
    Meta = MetaSRCdb

    @classmethod
    def load(cls):
        cls.clean_stage_tables()
        cls.сapture_data_from_source()
        cls.stage_to_target_fact()


class ILpassport_blacklist(ILoad):
    src_table = FileTable('passport_blacklist_', '.xlsx', 'data/')
    stg_table = StageTableWithoutDates('nali_stg_passport_blacklist', 'passport_num', 'entry_dt')
    trg_table = TargetTableWithoutDates('nali_dwh_fact_passport_blacklist', 'passport_num', 'entry_dt')
    Meta = MetaSRCdb

    @classmethod
    def load(cls):
        cls.clean_stage_tables()
        cls.сapture_data_from_source()
        cls.stage_to_target_fact()


