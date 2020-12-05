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

		self.data = self.extract(input_fpath)
		self.header = self.transform_header()
		self.ddl = self.build_ddl()

		self.connection = self.create_db()
		self.cursor = self.connection.cursor()

		self.out_fpath = output_fpath

	def extract(self,input_fpath):

		data = open(input_fpath,'r')

		return data.readlines()

	def create_db(self):
		return sqlite3.connect(':memory:')

	def transform_header(self):
		return self.data[0].upper().replace('\n','')

	def build_ddl(self):
		"""
		Build data definition language from headers of source file.
		All fields defined as sqlite3 TEXT
		"""

		q_string = [elm + ' text' for elm in self.header.split(',')]

		fields = '(' + ', '.join(q_string) + ')'

		return '''CREATE TABLE tmp ''' + fields
		
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
			print(v)
			self.cursor.execute('SELECT * FROM tmp WHERE SUB_ENTITY="{}"'.format(v))

			with open(os.path.join(self.out_fpath,'{}.csv'.format(v)),"w") as output:
				csv_out = csv.writer(output)

				csv_out.writerow(self.header.split(','))

				for result in self.cursor:
					csv_out.writerow(result)


def main():
	
	in_fpath = r'data/example-file.csv'
	out_fpath = r'data/'

	transformer = etl(in_fpath,out_fpath)

	transformer.build_tmp()

	transformer.split()

if __name__ == '__main__':
	main()