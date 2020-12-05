
# transform headers to UPPERCASE
# split file into multiple files according entity/subentity
# children files must have the same format as the parent file

# Uses Python's native packages so that 
# you don't need to install big dependencies
# like Pandas or Numpy

import sqlite3
import csv

class etl(content):
	def __init__(self):
		self.data = content.readlines()
		self.header = transform_header() 
		self.ddl = ""
		self.database = sqlite3.connect(':memory:')
		self.child_fns = list()

	def transform_header(self):
		self.header = self.data[0].upper().replace('\n','')

	def build_ddl(self):
		"""
		Build ddl from headers of source file
		"""

		q_string = [elm + ' text' for elm in headers.split(',')]

		fields = '(' + ', '.join(q_string) + ')'

		self.ddl = '''CREATE TABLE tmp ''' + fields
		
	
	def build_tmp(self):
		pass

	def split(self):
		pass


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
	insert = "INSERT INTO tmp VALUES "

	def enclose(values):

		return ','.join(["'" + v + "'" for v in values.split(',')])

	def concatenate(values):

		return insert + '(' + enclose(values.replace('\n', '')) + ')' 

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


if __name__ == '__main__':
	main()