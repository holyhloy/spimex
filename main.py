import datetime
import os
import urllib.request
import re
import xlrd

import pandas as pd
from pangres import upsert

from db import create_db, engine
from models.spimex_trading_results import SpimexTradingResult

create_db()

class URLManager:

	def __init__(self) -> None:
		self.url = 'https://spimex.com/markets/oil_products/trades/results/'
		self.page_number = 0
		self.href_pattern = re.compile(r'/upload/reports/oil_xls/oil_xls_202[3-9]\d*')
		self.tables_hrefs = []
		self.existing_files = os.listdir('tables/')
		self.dataframes = {}

	def get_data_from_query(self) -> []:
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
			else:
				break
		return self.tables_hrefs

	def download_xls(self) -> None:
		table_hrefs = self.get_data_from_query()
		print('Downloading tables...')
		for href in table_hrefs:
			file_path = f'tables/{href[-22:]}.xls'
			if file_path not in self.existing_files:
				urllib.request.urlretrieve(href, file_path)

	def convert_to_df(self):
		print('Converting tables to dataframes...')
		for table_file in self.existing_files:
			file_path = f'tables/{table_file}'
			df = pd.read_excel(file_path, usecols='B:F,O', engine='xlrd')
			self.dataframes[file_path] = df

	def validate_tables(self):
		print('Validating tables...')
		search_tonn = 'Единица измерения: Метрическая тонна'
		table_borders_pattern = re.compile(r'\b(?=[A-Z-])([A-Z0-9-]+[A-Z]+[A-Z0-9-]*)\b')
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
			new_df = new_df.reset_index(drop=False)
			self.dataframes[file_path] = new_df

	def add_columns(self):
		print('Adding columns...')
		for path, df in self.dataframes.items():
			oil_id = df['exchange_product_id'][0][:4]
			delivery_basis_id = df['exchange_product_id'][0][4:7]
			delivery_type_id = df['exchange_product_id'][0][-1]
			date = '{0}.{1}.{2}'.format(path[-12:-10], path[-14:-12], path[-18:-14])
			date = datetime.datetime.strptime(date, '%d.%m.%Y').date()

			df['oil_id'] = oil_id
			df['delivery_basis_id'] = delivery_basis_id
			df['delivery_type_id'] = delivery_type_id
			df['date'] = date
			df['created_on'] = datetime.date.today()
			df['updated_on'] = datetime.date.today()
			df['id'] = 0

	def load_to_db(self):
		print('Loading to database...')
		for file_path, df in self.dataframes.items():
			# df.set_index(['index'], inplace=True, drop=False)
			upsert(engine, df, 'spimex_trading_results', 'ignore')
		print('All tables loaded!')


ur = URLManager()
ur.convert_to_df()
ur.validate_tables()
ur.add_columns()
ur.load_to_db()

# ввести pangres, чтобы избежать дублирования данных