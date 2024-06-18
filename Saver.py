import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column, String, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import logging

host = ''
username = ''
password = ''
port = ''
database_name = ''
connection_url = f'mysql://{username}:{password}@{host}:{port}/{database_name}'
engine = create_engine(url=connection_url)


#Create the ORM object
Base = declarative_base()

class sales_table(Base):
    __tablename__ = 'BP_download_sales'
    id = Column(Integer, primary_key=True)
    product_name = Column(String(500))
    day = Column(DateTime)
    store_name = Column(String(500))
    brand_name = Column(String(500))
    venture = Column(String(500))
    sku_id = Column(String(500))
    units_sold = Column(Integer)
    revenue = Column(Integer)
    created = Column(DateTime)

class traffic_table(Base):
    __tablename__ = 'BP_download_traffic'
    id = Column(Integer, primary_key=True)
    day = Column(DateTime)
    product_name = Column(String(500))
    store_name = Column(String(500))
    brand_name = Column(String(500))
    venture = Column(String(500))
    sku_id = Column(String(500))
    page_view = Column(Integer)
    created = Column(DateTime)

logging.basicConfig(level=logging.INFO)

def save_sales(sales_list, current_time):
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        sales_instance_list = []
        for sales_value in sales_list:
            sales_data_point = {
            'day':sales_value[0],
            'product_name':sales_value[1],
            'store_name':sales_value[2],
            'brand_name':sales_value[3],
            'venture':sales_value[4],
            'sku_id':sales_value[5],
            'units_sold':sales_value[6],
            'revenue':sales_value[7],
            'created':current_time
            }
            sales_data_instance = sales_table(**sales_data_point)
            sales_instance_list.append(sales_data_instance)
        session.add_all(sales_instance_list)
        session.commit()
        logging.info(f"Saved {len(sales_instance_list)} sales records to the database")
    except Exception as e:
        logging.error(f"Error saving sales records: {e}")

def save_traffic(traffic_list, current_time):
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        traffic_instance_list = []
        for traffic_value in traffic_list:
            traffic_data_point = {
            'day':traffic_value[0],
            'product_name':traffic_value[1],
            'store_name':traffic_value[2],
            'brand_name':traffic_value[3],
            'venture':traffic_value[4],
            'sku_id':traffic_value[5],
            'page_view':traffic_value[6],
            'created':current_time
            }
            traffic_data_instance = traffic_table(**traffic_data_point)
            traffic_instance_list.append(traffic_data_instance)
        session.add_all(traffic_instance_list)
        session.commit()
        logging.info(f"Saved {len(traffic_instance_list)} traffic records to the database")
    except Exception as e:
        logging.error(f"Error saving traffic records: {e}")

def save_to_database(df):
    traffic_columns = ['Day', 'Product Name', 'Store Name', 'Brand Name', 'Venture', 'Product ID', 'Product/SKU Pageviews']
    sales_columns = ['Day', 'Product Name', 'Store Name', 'Brand Name', 'Venture', 'Product ID', 'Units Sold', 'Revenue']
    traffic_df = df.loc[:, traffic_columns]
    sales_df = df.loc[:, sales_columns]
    sales_df['Revenue'] = sales_df['Revenue'].str.replace('$', '')
    sales_list = sales_df.values.tolist()
    traffic_list = traffic_df.values.tolist()
    with ThreadPoolExecutor(max_workers=7) as executor:
        current_time = datetime.now()
        executor.submit(save_sales, sales_list, current_time)
        executor.submit(save_traffic, traffic_list, current_time)