# Q1A3.py
import matplotlib.pyplot as plt
import time
import sqlite3

# 
class Db:
	def __init__(self, dbName = None):
		self.dbName = dbName
		self.conn = None
		self.connStatus = 'none'

	def __del__(self):
		if (self.connStatus == 'connected'):
			try:
				self.conn.close()
			except Exception as err:
				print('!!!Db@__del__:', err)

	def connect(self, dbName = None):
		try:
			if (self.connStatus == 'connected'):
				self.close()
			if (dbName == None):
				self.conn = sqlite3.connect(self.dbName + '.db')
			else:
				self.conn = sqlite3.connect(dbName + '.db')
				self.dbName = dbName
			self.connStatus = 'connected'
		except Exception as err:
			print(f'!!!Db@connect({self.dbName}):', err)

	def close(self):
		if (self.connStatus == 'connected'):
			try:
				self.conn.close()
				self.connStatus = 'closed'
			except Exception as err:
				print('!!!Db@close:', err)

	def setScenario(self, scenario):
		self.scenario = scenario
		try:
			if (scenario == 'Uninformed'):
				sql = 'PRAGMA automatic_index=0;'
				self.conn.execute(sql)
				sql = 'PRAGMA foreign_keys=0;'
				self.conn.execute(sql)
				sql = '''DROP INDEX IF EXISTS "Orders_customerid";'''
				self.conn.execute(sql)
				sql = '''DROP INDEX IF EXISTS "Customers_zipcode";'''
				self.conn.execute(sql)
				sql = '''DROP INDEX IF EXISTS "Sellers_zipcode";'''
				self.conn.execute(sql)
			elif (scenario == 'Self-optimized'):
				sql = 'PRAGMA automatic_index=1;'
				self.conn.execute(sql)
				sql = 'PRAGMA foreign_keys=1;'
				self.conn.execute(sql)
				sql = '''DROP INDEX IF EXISTS "Orders_customerid";'''
				self.conn.execute(sql)
				sql = '''DROP INDEX IF EXISTS "Customers_zipcode";'''
				self.conn.execute(sql)
				sql = '''DROP INDEX IF EXISTS "Sellers_zipcode";'''
				self.conn.execute(sql)
			elif (scenario == 'User-optimized'):
				sql = 'PRAGMA automatic_index=1;'
				self.conn.execute(sql)
				sql = 'PRAGMA foreign_keys=1;'
				self.conn.execute(sql)
				sql = '''CREATE INDEX IF NOT EXISTS "Orders_customerid" ON "Orders"("customer_id");'''
				self.conn.execute(sql)
				sql = '''CREATE INDEX IF NOT EXISTS "Customers_zipcode" ON "Customers"("customer_postal_code");'''
				self.conn.execute(sql)
				sql = '''CREATE INDEX IF NOT EXISTS "Sellers_zipcode" ON "Sellers"("seller_postal_code");'''
				self.conn.execute(sql)
		except Exception as err:
			print(f'!!!Db@setScenario({scenario}):', err)

	def createTables(self):
		if (self.connStatus == 'connected'):
			try:
				sql = '''CREATE TABLE IF NOT EXISTS "Customers"(
					"customer_id" TEXT,
					"customer_postal_code" INTEGER,
					PRIMARY KEY("customer_id"));'''
				self.conn.execute(sql)

				sql = '''CREATE TABLE IF NOT EXISTS "Sellers"(
					"seller_id" TEXT,
					"seller_postal_code" INTEGER,
					PRIMARY KEY("seller_id"));'''
				self.conn.execute(sql)

				sql = '''CREATE TABLE IF NOT EXISTS "Orders"(
					"order_id" TEXT,
					"customer_id" TEXT,
					PRIMARY KEY("order_id"));'''
				self.conn.execute(sql)

				sql = '''CREATE TABLE IF NOT EXISTS "Order_items"(
					"order_id" TEXT,
					"order_item_id" INTEGER,
					"product_id" TEXT,
					"seller_id" TEXT,
					PRIMARY KEY("order_id","order_item_id","product_id","seller_id"),
					FOREIGN KEY("seller_id") REFERENCES "Sellers"("seller_id"));'''
				self.conn.execute(sql)

				sql = '''CREATE VIEW IF NOT EXISTS "OrderSize" AS
					select oid, count(iid) size
					from (
						select o.order_id oid, i.order_item_id iid
						from Orders o
						left join Order_items i on o.order_id=i.order_id)
					group by oid;'''
				self.conn.execute(sql)
			except Exception as err:
				print('!!!Db@createTables:', err)

	def vacuum(self):
		if (self.connStatus == 'connected'):
			self.conn.execute('VACUUM;')

	def insert(self, table, row):
		if (self.connStatus == 'connected'):
			try:
				cursor = self.conn.cursor()
				if (table == 'Customers'):
					sql = 'insert into "Customers"("customer_id","customer_postal_code") values(?,?);'
					cursor.execute(sql, (row[0], int(row[2])))
				elif (table == 'Sellers'):
					sql = 'insert into "Sellers"("seller_id","seller_postal_code") values(?,?);'
					cursor.execute(sql, (row[0], int(row[1])))
				elif (table == 'Orders'):
					sql = 'insert into "Orders"("order_id","customer_id") values(?,?);'
					cursor.execute(sql, (row[0], row[1]))
				elif (table == 'Order_items'):
					sql = 'insert into "Order_items"("order_id","order_item_id","product_id","seller_id") values(?,?,?,?);'
					cursor.execute(sql, (row[0], int(row[1]), row[2], row[3]))
				self.conn.commit()
			except Exception as err:
				print(f'!!!Db@insert({table},{row}):', err)

	def query(self, sql, args = None):
		result = []
		if (self.connStatus == 'connected'):
			try:
				cursor = self.conn.cursor()
				if (args is None):
					cursor.execute(sql)
				else:
  					cursor.execute(sql, args)
				for row in cursor:
					result.append(row)
				cursor.close()   # cache clear
			except Exception as err:
				print(f'!!!Db@query({sql}):', err)

		return result
# end class

# 
class TaskQ1:
	def __init__(self):
		self.db = Db()
		self.codes = None

	def getPostalcodes(self, dbName, num):
		self.db.connect(dbName)
		sql = f'select distinct customer_postal_code from Customers order by random() limit {num};'
		self.codes = self.db.query(sql)
		self.db.close()

	def queryOrdersCount(self, postalcode):
		sql = f'''select * from Orders o join Customers c on c.customer_id = o.customer_id 
			where customer_postal_code=?;'''
		return self.db.query(sql, postalcode)
	
	def __exec(self, dbName, scenario):
		self.db.connect(dbName)
		self.db.setScenario(scenario)

		start = time.time_ns()
		for code in self.codes:
			result = self.queryOrdersCount(code)
		end = time.time_ns()

		self.db.close()

		return (end - start) / len(self.codes) / 1000000  # ms

	# Result:
	def getResult(self):
		samplenum = 50
		res = []
		self.getPostalcodes('A3Small', samplenum)
		res.append(self.__exec('A3Small', 'Uninformed'))
		res.append(self.__exec('A3Small', 'Self-optimized'))
		res.append(self.__exec('A3Small', 'User-optimized'))

		self.getPostalcodes('A3Medium', samplenum)
		res.append(self.__exec('A3Medium', 'Uninformed'))
		res.append(self.__exec('A3Medium', 'Self-optimized'))
		res.append(self.__exec('A3Medium', 'User-optimized'))

		self.getPostalcodes('A3Large', samplenum)
		res.append(self.__exec('A3Large', 'Uninformed'))
		res.append(self.__exec('A3Large', 'Self-optimized'))
		res.append(self.__exec('A3Large', 'User-optimized'))

		return res
# end class

#------------------------
# Chart

# Generate graph 1
def graph_one():
    q1 = TaskQ1()
    stacked_bar_chart(q1.getResult(), 1)
    return

# Generates layered bar chart
def stacked_bar_chart(runtimes, query):
    labels = ['SmallDB', 'MediumDB', 'LargeDB']
    small_runtimes = []
    medium_runtimes = []
    large_runtimes = []
    small_medium_runtimes = []
    small_runtimes.append(runtimes[0])
    small_runtimes.append(runtimes[3])
    small_runtimes.append(runtimes[6])
    medium_runtimes.append(runtimes[1])
    medium_runtimes.append(runtimes[4])
    medium_runtimes.append(runtimes[7])
    large_runtimes.append(runtimes[2])
    large_runtimes.append(runtimes[5])
    large_runtimes.append(runtimes[8])
    small_medium_runtimes.append((small_runtimes[0])+(medium_runtimes[0]))
    small_medium_runtimes.append(small_runtimes[1]+medium_runtimes[1])
    small_medium_runtimes.append(small_runtimes[2]+medium_runtimes[2])
    width = 0.35       # the width of the bars: can also be len(x) sequence
    
    fig, ax = plt.subplots()
    
    ax.bar(labels, small_runtimes, width, label='Uninformed')
    ax.bar(labels, medium_runtimes, width, bottom=small_runtimes, label='Self Optimized')
    ax.bar(labels, large_runtimes, width, bottom=small_medium_runtimes, label='User Optimized')

    ax.set_title('Optimized DB Query Runtimes')
    ax.legend()
    
    path = './Q' + str(query) + 'A3chart.png'
    plt.savefig(path)
    print('Chart saved to file Q4A3chart.png'.format(path))
    
    # close figure so it doesn't display
    plt.close() 
    return

def main():
	graph_one()
	return

if __name__ == "__main__":
	main()

