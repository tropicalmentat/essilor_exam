
# transform headers to UPPERCASE
# split file into multiple files according entity/subentity
# children files must have the same format as the parent file

# Uses Python's native packages so that 
# you don't need to install big dependencies
# like Pandas or Numpy

import sqlite3
import csv

class etl():

	def __init__(self,content):

		self.data = content.readlines()
		self.header = self.transform_header()
		self.ddl = self.build_ddl()

		self.connection = self.create_db()
		self.cursor = self.connection.cursor()

		self.child_fns = list()

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

		cmds = list(map(concatenate, self.data))

		self.cursor.execute(self.ddl)

		for i in cmds:
			self.cursor.execute(i)

		return

	def split(self):
		self.cursor.execute('SELECT DISTINCT SUB_ENTITY FROM tmp')
	
		uniq = [v[0] for v in self.cursor.fetchall()]

		for v in uniq:

			self.cursor.execute('SELECT * FROM tmp WHERE SUB_ENTITY="{}"'.format(v))

			with open('{}.csv'.format(v),"w") as output:
				csv_out = csv.writer(output)

				csv_out.writerow(self.header.split(','))

				for result in self.cursor:
					csv_out.writerow(result)


def build_ddl(headers):
	"""
	Build ddl from headers of source file
	"""

	q_string = [elm + ' text' for elm in headers.split(',')]

	fields = '(' + ', '.join(q_string) + ')'

	return '''CREATE TABLE tmp ''' + fields


def build_tmp(cursor, ddl, data):
	"""
	Create tmp sqlite table in memory.
	"""
	insert_stmt = "insert_stmt INTO tmp VALUES "

	def enclose(values):

		return ','.join(["'" + v + "'" for v in values.split(',')])

	def concatenate(values):

		return insert_stmt + '(' + enclose(values.replace('\n', '')) + ')' 

	cmds = list(map(concatenate, data))

	# create table
	cursor.execute(ddl)

	for i in cmds:
		cursor.execute(i)


	return

def split(hdr, cursor):
	"""
	Split parent table into separate child tables based on SUB_ENTITY unique values
	"""

	cursor.execute('SELECT DISTINCT SUB_ENTITY FROM tmp')
	
	uniq = [v[0] for v in cursor.fetchall()]

	for v in uniq:

		cursor.execute('SELECT * FROM tmp WHERE SUB_ENTITY="{}"'.format(v))

		with open('{}.csv'.format(v),"w") as output:
			csv_out = csv.writer(output)

			csv_out.writerow(hdr.split(','))

			for result in cursor:
				csv_out.writerow(result)


def _main():
	fn = r'data/example-file.csv'

	conn = sqlite3.connect(':memory:')
	crsr = conn.cursor()

	with open(fn,"r") as data:
		# get header and create table with text datatype for all fields
		lines = data.readlines()
		hdr = lines[0].upper().replace('\n','')

		ddl = build_ddl(hdr)

		build_tmp(crsr, ddl, lines[1:])

		conn.commit()

		split(hdr, crsr)

	conn.close()

def main():
	fn = r'data/example-file.csv'

	data = open(fn,'r')

	transformer = etl(data)

	# print(transformer.ddl)

	transformer.build_tmp()

	transformer.split()

	# for r in transformer.cursor.execute('SELECT * FROM tmp LIMIT 10'):
	# 	print(r)

if __name__ == '__main__':
	main()