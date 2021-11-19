# Q2A3.py
import sqlite3
import time
import matplotlib.pyplot as plt

def query2(conn):
    runtime = 0
    cursor = conn.cursor()
    cursor.execute('''CREATE VIEW OrderSize(oid, size) 
                          AS SELECT order_id, COUNT(order_id)
                          FROM Order_items 
                          GROUP BY order_id;''')
    for i in range(50):
        # Selecting random customer_postal_code
        cursor.execute("SELECT customer_postal_code FROM Customers ORDER BY RANDOM() LIMIT 1;")
        random_customer = cursor.fetchall()
        random_customer = random_customer[0][0]

        # Query2 start time
        start = time.time()
        cursor.execute('''SELECT AVG(size)
                          FROM OrderSize
                          WHERE oid IN (SELECT Orders.order_id
                                        FROM Orders
                                        WHERE Orders.customer_id IN (SELECT Customers.customer_id
                                                                     FROM Customers
                                                                     WHERE Customers.customer_postal_code = :random));''', {"random": random_customer})
        # Query2 end time
        end = time.time()
        result = cursor.fetchall()
        runtime += (end - start)*1000
    # Drop the view we just created
    cursor.execute('''DROP VIEW OrderSize''')

    return runtime

def uninformed(conn):
    '''
    This function turns off foreign keys and automatic indexing and drops all primary keys
    '''
    cursor = conn.cursor()
    cursor.execute('PRAGMA foreign_keys=FALSE;')
    cursor.execute('PRAGMA automatic_index=FALSE;')

    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Customers RENAME TO Old_Customers;")
    cursor.execute("CREATE TABLE Customers (customer_id TEXT, customer_postal_code INTEGER);")
    cursor.execute("INSERT INTO Customers SELECT customer_id, customer_postal_code FROM Old_Customers;")
    cursor.execute("DROP TABLE Old_Customers")

    cursor.execute("ALTER TABLE Sellers RENAME TO Old_Sellers;")
    cursor.execute("CREATE TABLE Sellers (seller_id TEXT, seller_postal_code INTEGER);")
    cursor.execute("INSERT INTO Sellers SELECT seller_id, seller_postal_code FROM Old_Sellers;")
    cursor.execute("DROP TABLE Old_Sellers")

    cursor.execute("ALTER TABLE Orders RENAME TO Old_Orders;")
    cursor.execute("CREATE TABLE Orders (order_id TEXT, customer_id TEXT);")
    cursor.execute("INSERT INTO Orders SELECT order_id, customer_id FROM Old_Orders;")
    cursor.execute("DROP TABLE Old_Orders")

    cursor.execute("ALTER TABLE Order_items RENAME TO Old_Order_items;")
    cursor.execute("CREATE TABLE Order_items (order_id TEXT, order_item_id INTEGER, product_id TEXT, seller_id TEXT);")
    cursor.execute("INSERT INTO Order_items SELECT order_id, order_item_id, product_id, seller_id FROM Old_Order_items;")
    cursor.execute("DROP TABLE Old_Order_items")
    conn.commit() 

    return

def selfOptimized(conn):
    '''
    This function redfines our primary keys and foreign keys and turns on automatic indexing
    '''
    cursor = conn.cursor()
    cursor.execute('PRAGMA foreign_keys=TRUE;')
    cursor.execute('PRAGMA automatic_index=TRUE;')
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Customers RENAME TO Old_Customers;")
    cursor.execute('''CREATE TABLE Customers ("customer_id" TEXT, "customer_postal_code" INTEGER, PRIMARY KEY("customer_id"));''')
    cursor.execute('''INSERT INTO Customers SELECT customer_id, customer_postal_code FROM Old_Customers;''')
    cursor.execute("DROP TABLE Old_Customers")
    
    cursor.execute("ALTER TABLE Sellers RENAME TO Old_Sellers;")
    cursor.execute("CREATE TABLE Sellers (seller_id TEXT, seller_postal_code INTEGER, PRIMARY KEY(seller_id));")
    cursor.execute("INSERT INTO Sellers SELECT seller_id, seller_postal_code FROM Old_Sellers;")
    cursor.execute("DROP TABLE Old_Sellers")
    
    cursor.execute("ALTER TABLE Orders RENAME TO Old_Orders;")
    cursor.execute("CREATE TABLE Orders (order_id TEXT, customer_id TEXT, FOREIGN KEY(customer_id) REFERENCES Customers(customer_id), PRIMARY KEY(order_id));")
    cursor.execute("INSERT INTO Orders SELECT order_id, customer_id FROM Old_Orders;")
    cursor.execute("DROP TABLE Old_Orders")
    
    cursor.execute("ALTER TABLE Order_items RENAME TO Old_Order_items;")
    cursor.execute("CREATE TABLE Order_items (order_id TEXT, order_item_id INTEGER, product_id TEXT, seller_id TEXT, PRIMARY KEY(order_id, order_item_id, product_id, seller_id), FOREIGN KEY(seller_id) REFERENCES Sellers(seller_id), FOREIGN KEY(order_id) REFERENCES Orders(order_id));")
    cursor.execute("INSERT INTO Order_items SELECT order_id, order_item_id, product_id, seller_id FROM Old_Order_items;")
    cursor.execute("DROP TABLE Old_Order_items")
    conn.commit()

    return

def userOptimized(conn):
    '''
    This function creates our own user defined indexes
    '''
    cursor = conn.cursor()
    cursor.execute('''CREATE INDEX Orders_customer_id_idx ON Orders (customer_id);''')

    conn.commit()

    return

def dropIndex(conn):
    '''
    This function drops our created indexes
    '''
    cursor = conn.cursor()
    cursor.execute('''DROP INDEX Orders_customer_id_idx;''')


    conn.commit()

    return

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

    ax.set_title(f'Query {str(query)} (runtime in ms)')
    ax.legend()
    
    path = './Q' + str(query) + 'A3chart.png'
    plt.savefig(path)
    print('Chart saved to file Q2A3chart.png'.format(path))
    
    # close figure so it doesn't display
    plt.close() 
    return

def q2_main():
    dbs = ["A3Small.db", "A3Medium.db", "A3Large.db"]
    allRuntimes = []
    for db in dbs:
        # Uninformed query
        conn = sqlite3.connect(db)
        uninformed(conn)
        runtime = query2(conn)
        allRuntimes.append(runtime)
        conn.commit()
        conn.close()

        # Self-optimized query
        conn = sqlite3.connect(db)
        selfOptimized(conn)
        runtime = query2(conn)
        allRuntimes.append(runtime)
        conn.commit()
        conn.close()

        # User-optimized query
        conn = sqlite3.connect(db)
        userOptimized(conn)
        runtime = query2(conn)
        dropIndex(conn)
        allRuntimes.append(runtime)
        conn.commit()
        conn.close()
        
    
    stacked_bar_chart(allRuntimes, 2)



if __name__ == "__main__":
    q2_main()
