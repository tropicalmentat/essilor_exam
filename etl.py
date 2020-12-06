# transform headers to UPPERCASE
# split file into multiple files according entity/subentity
# children files must have the same format as the parent file

# Uses Python's native packages so that 
# you don't need to install big dependencies
# like Pandas or Numpy

import os
import sqlite3
import csv

class etl():

	def __init__(self,input_fpath,output_fpath):

		self.input_fpath = input_fpath
		self.out_fpath = output_fpath

		self.file_bin = None

		self.data = None
		self.header = None
		self.ddl = None

		self.connection = None
		self.cursor = None

	def extract(self):

		self.file_bin = open(self.input_fpath,'r')

	def read(self):
		
		self.data = self.file_bin.readlines()

	def create_db(self):

		self.connection = sqlite3.connect(':memory:')

	def create_cursor(self):

		self.cursor = self.connection.cursor()

	def transform_header(self):

		self.header = self.data[0].upper().replace('\n','')

	def build_ddl(self):
		"""
		Build data definition language from headers of source file.
		All fields defined as sqlite3 TEXT
		"""

		q_string = [elm + ' text' for elm in self.header.split(',')]

		fields = '(' + ', '.join(q_string) + ')'

		self.ddl = '''CREATE TABLE tmp ''' + fields
		
	def build_tmp(self):
		"""
		Create tmp sqlite table in memory.
		"""
		insert_stmt = "INSERT INTO tmp VALUES "

		def enclose(values):

			return ','.join(["'" + v + "'" for v in values.split(',')])

		def concatenate(values):

			return insert_stmt + '(' + enclose(values.replace('\n', '')) + ')' 

		cmds = list(map(concatenate, self.data[1:]))

		self.cursor.execute(self.ddl)

		for i in cmds:
			self.cursor.execute(i)

		return

	def split(self):
		"""
		Split parent table into child tables based on unique
		SUB_ENTITY field values and dump as separate .csv
		per unique value
		"""
		self.cursor.execute('SELECT DISTINCT SUB_ENTITY FROM tmp')
	
		uniq = [v[0] for v in self.cursor.fetchall()]

		for v in uniq:
			
			self.cursor.execute('SELECT * FROM tmp WHERE SUB_ENTITY="{}"'.format(v))

			with open(os.path.join(self.out_fpath,'{}.csv'.format(v)),"w") as output:
				csv_out = csv.writer(output)

				csv_out.writerow(self.header.split(','))

				for result in self.cursor:
					csv_out.writerow(result)

	def close_cnxn(self):

		self.connection.close()

		return

	def close_file(self):

		self.file_bin.close()

		return

	def change_input_fpath(self,new_input_fpath):

		self.input_fpath = new_input_fpath

		return

	def change_input_fpath(self,old_output_fpath):

		self.output_fpath = old_output_fpath

		return

def main():
	
	in_fpath = r'data/example-file.csv'
	out_fpath = r'data/'

	transformer = etl(in_fpath,out_fpath)

	transformer.extract()

	transformer.read()

	transformer.transform_header()

	transformer.create_db()

	transformer.create_cursor()

	transformer.build_ddl()

	transformer.build_tmp()

	transformer.split()

	transformer.close_cnxn()

	transformer.close_file()

if __name__ == '__main__':
	main()