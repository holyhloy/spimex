import datetime
import os
import urllib.request
import re
import os

import xlrd
from sqlalchemy import select
from time import time

import pandas as pd
from pangres import upsert

from sync.db import create_db, engine, Session
from sync.models.spimex_trading_results import SpimexTradingResult

create_db()
if not os.path.isdir('../tables/'):
	os.makedirs('../tables/', exist_ok=True)

class URLManager:

	def __init__(self) -> None:
		self.url = 'https://spimex.com/markets/oil_products/trades/results/'
		self.page_number = 0
		self.href_pattern = re.compile(r'/upload/reports/oil_xls/oil_xls_202[3-9]\d*')
		self.tables_hrefs = []
		self.existing_files = os.listdir('../tables/')
		self.dataframes = {}

	def get_data_from_query(self) -> None:
		print('Getting data from URL...')
		while True:
			self.page_number += 1
			response = urllib.request.urlopen(self.url+f'?page=page-{self.page_number}')
			data = response.read()
			hrefs = re.findall(self.href_pattern, data.decode('utf-8'))
			if hrefs:
				for href in hrefs:
					href = f'https://spimex.com/{href}'
					self.tables_hrefs.append(href)
					break
			else:
				break

	def download_xls(self) -> None:
		print('Downloading tables...')
		for href in self.tables_hrefs:
			file_path = f'../tables/{href[-22:]}.xls'
			if file_path not in self.existing_files:
				urllib.request.urlretrieve(href, file_path)
				break

	def convert_to_df(self) -> None:
		print('Converting tables to dataframes...')
		for table_file in os.listdir('../tables/'):
			file_path = f'../tables/{table_file}'
			df = pd.read_excel(file_path, usecols='B:F,O', engine='xlrd')
			self.dataframes[file_path] = df
			break

	def validate_tables(self) -> None:
		print('Validating tables...')
		search_tonn = 'Единица измерения: Метрическая тонна'
		table_borders_pattern = re.compile(r'\b(?=[A-Z-])([A-Z0-9-]+[A-Z]+[A-Z0-9-]*)\b')
		prev_df_length = 1
		for file_path, df in self.dataframes.items():
			tonn_index = df.loc[df.isin([search_tonn]).any(axis=1)].index.tolist()
			new_df = pd.read_excel(file_path, header=tonn_index[0] + 2, usecols='B:F,O', skiprows=[tonn_index[0] + 3])
			first_column_list = new_df['Код\nИнструмента'].tolist()
			footer_index = 0
			for code in first_column_list:
				if re.match(table_borders_pattern, code):
					continue
				footer_index = first_column_list.index(code)
				break
			new_df = new_df[:footer_index - 1]
			new_df.columns = ['exchange_product_id',
						  'exchange_product_name',
						  'delivery_basis_name',
						  'volume',
						  'total',
						  'count']
			new_df = new_df[new_df['count'] != '-']
			new_df = new_df.reset_index(drop=True)
			new_df['id'] = pd.RangeIndex(prev_df_length, len(new_df) + prev_df_length)
			prev_df_length += len(new_df)
			new_df.set_index(['id'], inplace=True, drop=True)
			self.dataframes[file_path] = new_df

	def add_columns(self) -> None:
		print('Adding columns...')
		for path, df in self.dataframes.items():
			date = '{0}.{1}.{2}'.format(path[-12:-10], path[-14:-12], path[-18:-14])
			date = datetime.datetime.strptime(date, '%d.%m.%Y').date()
			df['date'] = date
			df['created_on'] = datetime.date.today()
			for index, row in df.iterrows():
				oil_id = row['exchange_product_id'][:4]
				delivery_basis_id = row['exchange_product_id'][4:7]
				delivery_type_id = row['exchange_product_id'][-1]

				df.loc[index, 'oil_id'] = oil_id
				df.loc[index, 'delivery_basis_id'] = delivery_basis_id
				df.loc[index, 'delivery_type_id'] = delivery_type_id


	def load_to_db(self) -> None:
		print('Loading to database...')
		rows_affected = 0
		for file_path, df in self.dataframes.items():
			print(type(df.iloc[0]['volume']))
			with Session() as session:
				amount_of_rows = session.query(SpimexTradingResult).count()
				for index, rows in df.iterrows():
					stmt = session.execute(
							select(SpimexTradingResult).where(SpimexTradingResult.id == index)).scalars().all()
					if stmt:
						df.loc[index, 'updated_on'] = datetime.date.today()
					else:
						df.loc[index, 'updated_on'] = None
						df.loc[index, 'created_on'] = datetime.date.today()
			upsert(engine, df, 'spimex_trading_results', 'update')
			with Session() as session:
				amount_of_rows_after = session.query(SpimexTradingResult).count()
			rows_affected += amount_of_rows_after - amount_of_rows

		if rows_affected > 0:
			print(f'{rows_affected} rows inserted!')
		else:
			print('None of rows have been inserted')


if __name__ == '__main__':
	t0 = time()
	ur = URLManager()
	getting_data_start = time()
	ur.get_data_from_query()
	print(f'Время получения ссылок - {int((time() - getting_data_start))} сек')
	download_start = time()
	ur.download_xls()
	print(f'Время скачивания - {int((time() - download_start))} сек')
	converting_start = time()
	ur.convert_to_df()
	print(f'Время конвертации - {int((time() - converting_start))} сек')
	validation_start = time()
	ur.validate_tables()
	print(f'Время валидации - {int((time() - validation_start))} сек')
	adding_columns_start = time()
	ur.add_columns()
	print(f'Время добавления столбцов - {int((time() - adding_columns_start))} сек')
	loading_to_db_start = time()
	ur.load_to_db()
	print(f'Время загрузки в базу - {int((time() - loading_to_db_start))} сек')
	print(f'Общее время работы - {int((time() - t0))} сек')