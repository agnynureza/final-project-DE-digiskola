from datetime import datetime,timedelta
import subprocess

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

def _install_packages(package_list):
    for package in package_list: 
        try:
            subprocess.check_call(['pip3', 'install', package])
            print(f'Successfully install {package}')
        except subprocess.CalledProcessError:
            print(f'Error install {package}')
        except Exception as e:
            print(f'Something went wrong {e}')
            
def _import_file_to_mysql(ti):
    packages_to_install = ['pandas']
    _install_packages(packages_to_install)

    import pandas as pd
    
    try:
        dataset = ti.xcom_pull(key="dataset", task_ids="get_data_covid")        
        df = pd.DataFrame(dataset)
    
        engine = _mysql_connection()
        table_name = 'staging_covid_dataset'

        df.index += 1
        df.to_sql(table_name, engine, if_exists='replace', index=True, index_label='id')  
        
        engine.dispose()
        
        return "import to mysql"
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return "end"
    
def _get_data_covid(ti):
    packages_to_install = ['requests']
    _install_packages(packages_to_install)
    
    import requests
    
    api_url = 'http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab'
    response = requests.get(api_url)

    if response.status_code == 200:
        data = response.json()        
        ti.xcom_push(key="dataset", value=data["data"]["content"])
        return "get_data_covid"
    else:
        print(f"Request failed with status code: {response.status_code}")
        return "end"
    
def _create_ddl_postgres():
    packages_to_install = ['sqlalchemy']
    _install_packages(packages_to_install)
    
    from sqlalchemy import Column, Integer, String, Date, ForeignKey
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker

    Base = declarative_base()

    class Province(Base):
        __tablename__ = 'dim_province'
        province_id = Column(Integer, primary_key=True)
        province_name = Column(String)

    class District(Base):
        __tablename__ = 'dim_district'
        district_id = Column(Integer, primary_key=True)
        province_id = Column(Integer)
        district_name = Column(String)

    class Case(Base):
        __tablename__ = 'dim_case'
        id = Column(Integer, primary_key=True)
        status_name = Column(String)
        status_detail = Column(String)

    # Fact Tables
    class ProvinceDaily(Base):
        __tablename__ = 'fact_province_daily'
        id = Column(Integer, primary_key=True, autoincrement=True)
        province_id = Column(Integer)
        case_id = Column(Integer)
        date = Column(String)
        total = Column(Integer)

    class ProvinceMonthly(Base):
        __tablename__ = 'fact_province_monthly'
        id = Column(Integer, primary_key=True, autoincrement=True)
        province_id = Column(Integer)
        case_id = Column(Integer)
        month = Column(String)
        total = Column(Integer)
        
    class ProvinceYearly(Base):
        __tablename__ = 'fact_province_yearly'
        id = Column(Integer, primary_key=True, autoincrement=True)
        province_id = Column(Integer)
        case_id = Column(Integer)
        year = Column(String)
        total = Column(Integer)
        
    class DistrictMonthly(Base):
        __tablename__ = 'fact_district_monthly'
        id = Column(Integer, primary_key=True, autoincrement=True)
        district_id = Column(Integer)
        case_id = Column(Integer)
        month = Column(String)
        total = Column(Integer)
        
    class DistrictYearly(Base):
        __tablename__ = 'fact_district_yearly'
        id = Column(Integer, primary_key=True, autoincrement=True)
        district_id = Column(Integer)
        case_id = Column(Integer)
        year = Column(String)
        total = Column(Integer)

    engine = _postgres_connection()
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    session.commit()

    print("success migrate table")
    session.close()
 
def _postgres_connection():
    packages_to_install = ['sqlalchemy']
    _install_packages(packages_to_install)
    
    from sqlalchemy import create_engine
    
    username = Variable.get(key='POSTGRES_USER')
    password = Variable.get(key='POSTGRES_PASSWORD')
    port = Variable.get(key='POSTGRES_PORT')
    db = Variable.get(key='POSTGRES_DB')
    host = Variable.get(key='POSTGRES_HOST')

    # Database connection setup
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    return engine
    
def _mysql_connection():
    packages_to_install = ['sqlalchemy', 'pymysql']
    _install_packages(packages_to_install)
    
    from sqlalchemy import create_engine
    
    db = Variable.get(key='MYSQL_DATABASE')
    port = Variable.get(key='MYSQL_PORT')
    password = Variable.get(key='MYSQL_PASSWORD')
    user = Variable.get(key='MYSQL_USER')
    hostname = Variable.get(key='MYSQL_HOST')
    
    connection_string = f"mysql+pymysql://{user}:{password}@{hostname}:{port}/{db}"

    engine = create_engine(connection_string)
    return engine
           
def _aggregate_dim_table():
    packages_to_install = ['sqlalchemy']
    _install_packages(packages_to_install)
    
    from sqlalchemy import Column, Integer, String, select, inspect, distinct, text
    from sqlalchemy.orm import sessionmaker, declarative_base

    mysql_engine = _mysql_connection()
    postgres_engine = _postgres_connection()
    
    session_mysql = sessionmaker(bind=mysql_engine)()
    session_pg = sessionmaker(bind=postgres_engine)()
    inspector = inspect(mysql_engine)
    
    Base = declarative_base()
    
    class StagingData(Base):
        __tablename__ = 'staging_covid_dataset'
        
        id = Column(Integer, primary_key=True)
        nama_kab = Column(String)
        nama_prov = Column(String)
        kode_prov = Column(Integer)
        kode_kab = Column(Integer)
        
    class Province(Base):
        __tablename__ = 'dim_province'
        province_id = Column(Integer, primary_key=True)
        province_name = Column(String)

    class District(Base):
        __tablename__ = 'dim_district'
        district_id = Column(Integer, primary_key=True)
        province_id = Column(Integer)
        district_name = Column(String)

    class Case(Base):
        __tablename__ = 'dim_case'
        id = Column(Integer, primary_key=True)
        status_name = Column(String)
        status_detail = Column(String)
        
    try:
        # dim_province table
        select_query = select([distinct(StagingData.nama_prov).label('province_name'), StagingData.kode_prov.label('province_id')])    
        all_rows = session_mysql.execute(select_query)
        
        truncate_statement = f"TRUNCATE TABLE dim_province"
        session_pg.execute(truncate_statement)
        
        objects_to_insert = [Province(**data) for data in all_rows]
        session_pg.bulk_save_objects(objects_to_insert)
        session_pg.commit()
        
        
        # dim_district table
        select_query = select([distinct(StagingData.kode_kab).label('district_id'), 
                               StagingData.nama_kab.label('district_name'), 
                               StagingData.kode_prov.label('province_id')])
        all_rows = session_mysql.execute(select_query)
        
        truncate_statement = f"TRUNCATE TABLE dim_district"
        session_pg.execute(truncate_statement)
        
        objects_to_insert = [District(**data) for data in all_rows]
        session_pg.bulk_save_objects(objects_to_insert)
        session_pg.commit()
        
        # dim_case table
        truncate_statement = f"TRUNCATE TABLE dim_case"
        session_pg.execute(truncate_statement)
        
        column_names = inspector.get_columns("staging_covid_dataset")
        const_status = ['suspect', 'closecontact', 'probable', 'confirmation']
        list_case = []
        serialize = 1
        for column in column_names:
            for status in const_status: 
                if status in column['name']:
                    case = {
                        'id': serialize,
                        'status_name': status,
                        'status_detail': column['name']
                    }
                    serialize += 1
                    list_case.append(case)
        objects_to_insert = [Case(**data) for data in list_case]
        session_pg.bulk_save_objects(objects_to_insert)
        session_pg.commit()
        
        session_mysql.close()
        session_pg.close()
        
        return "insert dim table"
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return "end"
    
def _aggregate_province_daily():
    try:
        _base_aggregate(time="tanggal", scope="kode_prov", table_name="fact_province_daily")
        return "aggregate_province_daily"
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return "end"

def _aggregate_province_monthly():
    try:
        _base_aggregate(time="month", scope="kode_prov", table_name="fact_province_monthly")
        return "aggregate_province_monthly"
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return "end"

def _aggregate_province_yearly():
    try:
        _base_aggregate(time="year", scope="kode_prov", table_name="fact_province_yearly")
        return "aggregate_province_yearly"
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return "end"

def _aggregate_district_monthly():
    try:
        _base_aggregate(time="month", scope="kode_kab", table_name="fact_district_monthly")
        return "aggregate_district_monthly"
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return "end"

def _aggregate_district_yearly():
    try:
        _base_aggregate(time="year", scope="kode_kab", table_name="fact_district_yearly")
        return "aggregate_district_yearly"
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return "end"

def _base_aggregate(time="tanggal", scope="kode_prov", table_name="fact_province_daily"):
    packages_to_install = ['pandas']
    _install_packages(packages_to_install)
    
    import pandas as pd

    mysql_engine = _mysql_connection()
    postgres_engine = _postgres_connection()
    
    try:
        # get all data from mysql staging
        sql_query = 'SELECT * FROM staging_covid_dataset'
        staging_df = pd.read_sql(sql_query, mysql_engine)
        
        # get dim case from postgres
        sql_query = 'SELECT * FROM dim_case'
        case_df = pd.read_sql(sql_query, postgres_engine)
        
        # prepare data
        status_detail = case_df['status_detail'].tolist()
        sum_value = 'sum'
        aggregate_sum = {key: sum_value for key in status_detail}
        
        staging_df['tanggal_parse'] = pd.to_datetime(staging_df['tanggal'])
        if time == "year":
            staging_df['year'] = staging_df['tanggal_parse'].dt.strftime('%Y')
        elif time == "month":
            staging_df['month'] = staging_df['tanggal_parse'].dt.strftime('%Y-%m')
            
        # aggregate    
        agg_df = staging_df.groupby([time, scope])[status_detail] \
                    .agg(aggregate_sum) \
                    .reset_index()
        
        # pivot table
        melted_df = pd.melt(agg_df, id_vars=[time, scope], var_name='Case', value_name='total')

        extract_id = lambda row: case_df[case_df['status_detail'] == row['Case']]['id'].values[0]
        melted_df['case_id'] = melted_df.apply(extract_id, axis=1)

        melted_df = melted_df.drop('Case', axis=1)
        melted_df[scope] = melted_df[scope].astype(int)

        time = "date" if time == "tanggal" else time
        scope_id = "district_id" if scope == "kode_kab" else "province_id"
        melted_df.columns = [time, scope_id, 'total', 'case_id']

        melted_df[scope_id] = melted_df[scope_id].astype(int)
        melted_df.index = range(1, len(melted_df) + 1)
        
        # insert to postgres
        melted_df.to_sql(table_name, postgres_engine, if_exists='replace', index=True, index_label='id')

        # Commit and close the connection
        postgres_engine.dispose()
        mysql_engine.dispose()
        
        return "base_aggregate"
    except Exception as e:
        print(e)
        raise Exception(f"An error occurred: {str(e)}")
    
with DAG(
    dag_id='etl_postgresql',
    start_date=datetime(2024,1,1),
    schedule_interval='0 0 * * *',
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
) as dag: 
    start_task=EmptyOperator(
        task_id='start_task'
    )
    
    get_data_covid=PythonOperator(
        task_id='get_data_covid',
        python_callable=_get_data_covid,
        execution_timeout=timedelta(minutes=5)
    )
    
    import_file_to_mysql=PythonOperator(
        task_id='import_file_to_mysql',
        python_callable=_import_file_to_mysql,
        execution_timeout=timedelta(minutes=5)
    )
    
    create_ddl_postgres=PythonOperator(
        task_id='create_ddl_postgres',
        python_callable=_create_ddl_postgres,
        execution_timeout=timedelta(minutes=5)
    )
    
    aggregate_dim_table=PythonOperator(
        task_id='aggregate_dim_table',
        python_callable=_aggregate_dim_table,
        execution_timeout=timedelta(minutes=5)
    )
    
    aggregate_province_daily=PythonOperator(
        task_id='aggregate_province_daily',
        python_callable=_aggregate_province_daily,
        execution_timeout=timedelta(minutes=5)
    )
    
    aggregate_province_monthly=PythonOperator(
        task_id='aggregate_province_monthly',
        python_callable=_aggregate_province_monthly,
        execution_timeout=timedelta(minutes=5)
    )
    
    aggregate_province_yearly=PythonOperator(
        task_id='aggregate_province_yearly',
        python_callable=_aggregate_province_yearly,
        execution_timeout=timedelta(minutes=5)
    )
    
    aggregate_district_monthly=PythonOperator(
        task_id='aggregate_district_monthly',
        python_callable=_aggregate_district_monthly,
        execution_timeout=timedelta(minutes=5)
    )
    
    aggregate_district_yearly=PythonOperator(
        task_id='aggregate_district_yearly',
        python_callable=_aggregate_district_yearly,
        execution_timeout=timedelta(minutes=5)
    )
    
    end_task=EmptyOperator(
        task_id='end_task'
    )
    
    start_task >> get_data_covid >> import_file_to_mysql >> create_ddl_postgres >> aggregate_dim_table
    aggregate_dim_table >> aggregate_district_monthly >> end_task
    aggregate_dim_table >> aggregate_district_yearly >> end_task
    aggregate_dim_table >> aggregate_province_daily >> end_task
    aggregate_dim_table >> aggregate_province_monthly >> end_task
    aggregate_dim_table >> aggregate_province_yearly >> end_task
    