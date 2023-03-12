import psycopg2


def make_report(date_):
    date_ = (date_,)
    conn = psycopg2.connect(database="edu",
                            host="de-edu-db.chronosavant.ru",
                            user="de11an",
                            password="peregrintook",
                            port="5432", )
    cursor = conn.cursor()
    cursor.execute("""
    insert into de11an.nali_rep_fraud
    select distinct
    	event_dt,
    	passport_num as passport,
    	last_name||' '||first_name||' '||patronymic as fio,
    	phone,
    	1 as event_type,
    	%s as report_dt
    	from
    (select
    	account_num,
    	last_name,
    	first_name,
    	patronymic,
    	passport_num,
    	passport_valid_to,
    	phone
    from
    (select
    	client_id,
    	last_name,
    	first_name,
    	patronymic,
    	passport_num,
    	passport_valid_to,
    	phone
    from de11an.nali_dwh_dim_clients_hist
    where  deleted_flg = 'N'
    and passport_valid_to is null
    or passport_valid_to < '2021-03-03'
    or passport_num in (select distinct
    					fact.passport_num
    				from de11an.nali_dwh_fact_passport_blacklist fact
    				left join de11an.nali_dwh_dim_clients_hist scd
    				on scd.passport_num = fact.passport_num
    				and scd.start_dt = (select max(start_dt)
    									from de11an.nali_dwh_dim_clients_hist scd2
    									where scd2.passport_num = fact.passport_num
    									and scd2.start_dt <= fact.entry_dt)
    				where client_id is not null)) clt
    left join de11an.nali_dwh_dim_accounts_hist acc
    on  clt.client_id = acc.client
    where deleted_flg = 'N') x
    left join
    (select
    	fct.trans_date as event_dt,
    	trim(sd.card_num) as card_num,
    	sd.account_num,
    	sd.start_dt,
    	sd.end_dt
    from de11an.nali_dwh_fact_transactions fct
    left join de11an.nali_dwh_dim_cards_hist sd
    on trim(sd.card_num) = fct.card_num
    and sd.start_dt = (select max(start_dt)
    					from de11an.nali_dwh_dim_cards_hist sd2
    					where trim(sd2.card_num) = fct.card_num
    					and sd2.start_dt <= fct.trans_date)
    order by fct.trans_date) y
    on x.account_num = y.account_num;
    
    """, date_)
    cursor.execute("""
    insert into de11an.nali_rep_fraud
    select distinct
    	event_dt,
    	passport_num as passport,
    	last_name||' '||first_name||' '||patronymic as fio,
    	phone,
    	2 as event_type,
    	%s as report_dt
    	from
    (select
    	client,
    	account_num,
    	valid_to,
    	last_name,
    	first_name,
    	patronymic,
    	passport_num,
    	phone,
    	acc.deleted_flg
    	from
    (select
        client,
    	account_num,
    	valid_to,
    	deleted_flg
    from de11an.nali_dwh_dim_accounts_hist
    where  deleted_flg = 'N'
    and valid_to < '2021-03-03') acc
    left join de11an.nali_dwh_dim_clients_hist clt
    on clt.client_id = acc.client) x
    
    left join
    (select
    	fct.trans_date as event_dt,
    	trim(sd.card_num) as card_num,
    	sd.account_num,
    	sd.start_dt,
    	sd.end_dt
    from de11an.nali_dwh_fact_transactions fct
    left join de11an.nali_dwh_dim_cards_hist sd
    on trim(sd.card_num) = fct.card_num
    and sd.start_dt = (select max(start_dt)
    					from de11an.nali_dwh_dim_cards_hist sd2
    					where trim(sd2.card_num) = fct.card_num
    					and sd2.start_dt <= fct.trans_date)
    order by fct.trans_date) y
    on x.account_num = y.account_num;
    """, date_)
    conn.commit()

