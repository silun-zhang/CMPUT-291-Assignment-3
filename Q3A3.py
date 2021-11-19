# Q3A3.py
import sqlite3
import time
import matplotlib.pyplot as plt

def query3(conn):
    runtime = 0
    cursor = conn.cursor()
    cursor.execute('''DROP TABLE IF EXISTS OrderSize;''')
    cursor.execute('''CREATE TABLE OrderSize
                      AS SELECT order_id AS oid, COUNT(order_id) AS size
                      FROM Order_items 
                      GROUP BY order_id;''')
    for i in range(50):
        cursor.execute("SELECT customer_postal_code FROM Customers ORDER BY RANDOM() LIMIT 1;")
        random_customer = cursor.fetchall()
        random_customer = random_customer[0][0]
        start = time.time()

        cursor.execute('''SELECT oid, AVG(size)
                          FROM OrderSize
                          WHERE oid IN (SELECT order_id
                                        FROM Orders
                                        WHERE Orders.customer_id IN (SELECT Customers.customer_id
                                                                     FROM Customers
                                                                     WHERE Customers.customer_postal_code = :random));''', {"random": random_customer})
        
        end = time.time()
        runtime += (end - start)*1000
    cursor.execute('''DROP TABLE IF EXISTS OrderSize;''')

    return runtime

def uninformed(conn):
    cursor = conn.cursor()
    cursor.execute('PRAGMA foreign_keys=FALSE;')
    cursor.execute('PRAGMA automatic_index=FALSE;')
    conn.commit()
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Customers RENAME TO Old_Customers;")
    cursor.execute("CREATE TABLE Customers (customer_id TEXT, customer_postal_code INTEGER);")
    cursor.execute("INSERT INTO Customers SELECT * FROM Old_Customers;")
    cursor.execute("DROP TABLE Old_Customers")
    conn.commit()
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Sellers RENAME TO Old_Sellers;")
    cursor.execute("CREATE TABLE Sellers (seller_id TEXT, seller_postal_code INTEGER);")
    cursor.execute("INSERT INTO Sellers SELECT * FROM Old_Sellers;")
    cursor.execute("DROP TABLE Old_Sellers")
    conn.commit()
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Orders RENAME TO Old_Orders;")
    cursor.execute("CREATE TABLE Orders (order_id TEXT, customer_id TEXT);")
    cursor.execute("INSERT INTO Orders SELECT * FROM Old_Orders;")
    cursor.execute("DROP TABLE Old_Orders")
    conn.commit()
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Order_items RENAME TO Old_Order_items;")
    cursor.execute("CREATE TABLE Order_items (order_id TEXT, order_item_id INTEGER, product_id TEXT, seller_id TEXT);")
    cursor.execute("INSERT INTO Order_items SELECT * FROM Old_Order_items;")
    cursor.execute("DROP TABLE Old_Order_items")
    conn.commit() 



    return

def selfOptimized(conn):
    cursor = conn.cursor()
    cursor.execute('PRAGMA foreign_keys=TRUE;')
    cursor.execute('PRAGMA automatic_index=TRUE;')
    conn.commit()
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Customers RENAME TO Old_Customers;")
    cursor.execute("CREATE TABLE Customers (customer_id TEXT, customer_postal_code INTEGER, PRIMARY KEY(customer_id));")
    cursor.execute("INSERT INTO Customers SELECT * FROM Old_Customers;")
    cursor.execute("DROP TABLE Old_Customers")
    conn.commit() 
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Sellers RENAME TO Old_Sellers;")
    cursor.execute("CREATE TABLE Sellers (seller_id TEXT, seller_postal_code INTEGER, PRIMARY KEY(seller_id));")
    cursor.execute("INSERT INTO Sellers SELECT * FROM Old_Sellers;")
    cursor.execute("DROP TABLE Old_Sellers")
    conn.commit()
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Orders RENAME TO Old_Orders;")
    cursor.execute("CREATE TABLE Orders (order_id TEXT, customer_id TEXT, FOREIGN KEY(customer_id) REFERENCES Customers(customer_id), PRIMARY KEY(order_id));")
    cursor.execute("INSERT INTO Orders SELECT * FROM Old_Orders;")
    cursor.execute("DROP TABLE Old_Orders")
    conn.commit()      
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Order_items RENAME TO Old_Order_items;")
    cursor.execute("CREATE TABLE Order_items (order_id TEXT, order_item_id INTEGER, product_id TEXT, seller_id TEXT, PRIMARY KEY(order_id, order_item_id, product_id, seller_id), FOREIGN KEY(seller_id) REFERENCES Sellers(seller_id), FOREIGN KEY(order_id) REFERENCES Orders(order_id));")
    cursor.execute("INSERT INTO Order_items SELECT * FROM Old_Order_items;")
    cursor.execute("DROP TABLE Old_Order_items")
    conn.commit()

    return

def userOptimized(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE INDEX Orders_customer_id_idx ON Orders (customer_id);''')
    cursor.execute('''CREATE INDEX Customer_id_idx ON Customers (customer_id);''')
    conn.commit()

    return

def dropIndex(conn):
    cursor = conn.cursor()
    cursor.execute('''DROP INDEX Orders_customer_id_idx;''')
    cursor.execute('''DROP INDEX Customer_id_idx;''')
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
    print('Chart saved to file Q3A3chart.png'.format(path))
    
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
        runtime = query3(conn)
        allRuntimes.append(runtime)
        conn.commit()
        conn.close()

        # Self-optimized query
        conn = sqlite3.connect(db)
        selfOptimized(conn)
        runtime = query3(conn)
        allRuntimes.append(runtime)
        conn.commit()
        conn.close()

        # User-optimized query
        conn = sqlite3.connect(db)
        userOptimized(conn)
        runtime = query3(conn)
        dropIndex(conn)
        allRuntimes.append(runtime)
        conn.commit()
        conn.close()
        
    
    stacked_bar_chart(allRuntimes, 3)



if __name__ == "__main__":
    q2_main()