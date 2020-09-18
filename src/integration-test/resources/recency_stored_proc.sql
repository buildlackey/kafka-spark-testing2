create or replace PACKAGE PA_ETL_LOG as

    procedure SP_INS_RECENCY(i_str_JOB_NAME varchar2,
                             i_str_BATCH_NAME varchar2 default null,
                             i_str_datastream varchar2 default null,
                             i_dat_RECENCY_TIME timestamp,
                             i_str_db_name varchar2 default null,
                             i_str_MESSAGE varchar2,
                             i_num_DATACENTER_ID number default 2) ;
end PA_ETL_LOG;
/

create or replace PACKAGE BODY PA_ETL_LOG
as
    procedure SP_INS_RECENCY(i_str_JOB_NAME varchar2,
                             i_str_BATCH_NAME varchar2 default null,
                             i_str_datastream varchar2 default null,
                             i_dat_RECENCY_TIME timestamp,
                             i_str_db_name varchar2 default null,
                             i_str_MESSAGE varchar2,
                             i_num_DATACENTER_ID number default 2)
        Is
    Begin
        insert into mock_recency values ('procedure.called.ok');
        commit;
    end;

end PA_ETL_LOG;
/

