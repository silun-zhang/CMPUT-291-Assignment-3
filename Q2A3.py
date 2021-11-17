# Q2A3.py
import sqlite3
import time

def query3(conn):
    runtime = 0
    cursor = conn.cursor()

    for i in range(50):
        start = time.time()
        cursor.execute('''CREATE VIEW OrderSize(oid, size) 
                          AS SELECT order_id, SUM(*) 
                          FROM Orders O, Order_items OI
                          WHERE O.order_id = OI.order_id
                          GROUP BY O.order_id;''')

        cursor.execute('''DROP VIEW OrderSize''')
        end = time.time()
        runtime += (end - start) * 1000

    return runtime

def uninformed(conn):
    cursor = conn.cursor()
    cursor.execute('PRAGMA primary_keys=OFF;')
    cursor.execute('PRAGMA foreign_keys=OFF;')
    cursor.execute('PRAGMA automatic_index=FALSE;')
    conn.commit()
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Customers RENAME TO Old_Customers;")
    cursor.execute("CREATE TABLE Customers (customer_id TEXT, customer_postal_code INTEGER);")
    cursor.execute("INSERT INTO Customers SELECT * FROM Old_Customers;")
    cursor.execute("DROP TABLE Old_Customers")
    conn.commit() 
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Order_items RENAME TO Old_Order_items;")
    cursor.execute("CREATE TABLE Order_items (order_id TEXT, order_item_id INTEGER, product_id TEXT, seller_id TEXT);")
    cursor.execute("INSERT INTO Order_items SELECT * FROM Old_Order_items;")
    cursor.execute("DROP TABLE Old_Order_items")
    conn.commit()    
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Orders RENAME TO Old_Orders;")
    cursor.execute("CREATE TABLE Orders (order_id TEXT, customer_id TEXT);")
    cursor.execute("INSERT INTO Orders SELECT * FROM Old_Orders;")
    cursor.execute("DROP TABLE Old_Orders")
    conn.commit()    
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Sellers RENAME TO Old_Sellers;")
    cursor.execute("CREATE TABLE Sellers (seller_id TEXT, seller_postal_code INTEGER);")
    cursor.execute("INSERT INTO Sellers SELECT * FROM Old_Sellers;")
    cursor.execute("DROP TABLE Old_Sellers")
    conn.commit()  

    return

def selfOptimized(conn):
    cursor = conn.cursor()
    cursor.execute('PRAGMA primary_keys=ON;')
    cursor.execute('PRAGMA foreign_keys=ON;')
    cursor.execute('PRAGMA automatic_index=TRUE;')
    conn.commit()
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Customers RENAME TO Old_Customers;")
    cursor.execute("CREATE TABLE Customers (customer_id TEXT, customer_postal_code INTEGER, PRIMARY KEY(customer_id));")
    cursor.execute("INSERT INTO Customers SELECT * FROM Old_Customers;")
    cursor.execute("DROP TABLE Old_Customers")
    conn.commit() 
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Orders RENAME TO Old_Orders;")
    cursor.execute("CREATE TABLE Orders (order_id TEXT, customer_id TEXT, FOREIGN KEY(customer_id) REFERENCES Customers(customer_id), PRIMARY KEY(order_id));")
    cursor.execute("INSERT INTO Orders SELECT * FROM Old_Orders;")
    cursor.execute("DROP TABLE Old_Orders")
    conn.commit()       
    cursor.execute("BEGIN TRANSACTION;")
    cursor.execute("ALTER TABLE Sellers RENAME TO Old_Sellers;")
    cursor.execute("CREATE TABLE Sellers (seller_id TEXT, seller_postal_code INTEGER, PRIMARY KEY(seller_id));")
    cursor.execute("INSERT INTO Sellers SELECT * FROM Old_Sellers;")
    cursor.execute("DROP TABLE Old_Sellers")
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
    cursor.execute('''CREATE INDEX Orders_order_id_idx ON Orders (order_id)''')
    cursor.execute('''CREATE INDEX Order_items_order_id_idx ON Order_items (order_id)''')
    conn.commit()

    return

def dropIndex(conn):
    cursor = conn.cursor()
    cursor.execute('''DROP INDEX Order_items_order_id_idx''')
    cursor.execute('''DROP INDEX Orders_order_id_idx''')
    conn.commit()

    return

def main():
    dbs = ["A3Small.db", "A3Medium.db", "A3Large.db"]
    allRuntimes = []
    for db in dbs:
        # Uninformed query
        conn = sqlite3.connect('./Databases/' + db)
        uninformed(conn)
        runtime = query3(conn)
        allRuntimes.append(runtime)
        conn.commit()
        conn.close()

        # Self-optimized query
        conn = sqlite3.connect('./Databases/' + db)
        selfOptimized(conn)
        runtime = query3(conn)
        allRuntimes.append(runtime)
        conn.commit()
        conn.close()

        # User-optimized query
        conn = sqlite3.connect('./Databases/' + db)
        userOptimized(conn)
        runtime = query3(conn)
        dropIndex(conn)
        allRuntimes.append(runtime)
        conn.commit()
        conn.close()
        
    
    return allRuntimes



if __name__ == "__main__":
    runtimes = main()
    print(runtimes)