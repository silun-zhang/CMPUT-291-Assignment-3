# Q4A3.py

import sqlite3
import time
import matplotlib.pyplot as plt

# establish connection to the database
# db: database to connect with
def connect(db):
    con = sqlite3.connect(db)
    return con


# close connection to the database
# con: database connection to be closed
def close(con):
    con.commit()
    con.close()
    return    


def query_q4(con):
    execution_time = 0
    cur = con.cursor()
    for i in range(50):
        cur.execute("SELECT order_id FROM Orders ORDER BY RANDOM() LIMIT 1;")
        random_order = cur.fetchall()
        random_order = random_order[0][0]
        start_time = time.time()
        cur.execute("SELECT DISTINCT seller_postal_code FROM Sellers WHERE seller_id = :order;", {"order":random_order})
        end_time = time.time()
        execution_time += (end_time-start_time)*1000
    return execution_time    


def uninformed_q4(con):
    cur = con.cursor()
    cur.execute("PRAGMA automatic_index = off")
    cur.execute("PRAGMA foreign_keys = off")
    
    cur.execute("BEGIN TRANSACTION;")
    cur.execute("ALTER TABLE Customers RENAME TO Old_Customers;")
    cur.execute("CREATE TABLE Customers (customer_id TEXT, customer_postal_code INTEGER);")
    cur.execute("INSERT INTO Customers SELECT * FROM Old_Customers;")
    cur.execute("DROP TABLE Old_Customers")
    con.commit() 
    cur.execute("BEGIN TRANSACTION;")
    cur.execute("ALTER TABLE Order_items RENAME TO Old_Order_items;")
    cur.execute("CREATE TABLE Order_items (order_id TEXT, order_item_id INTEGER, product_id TEXT, seller_id TEXT);")
    cur.execute("INSERT INTO Order_items SELECT * FROM Old_Order_items;")
    cur.execute("DROP TABLE Old_Order_items")
    con.commit()    
    cur.execute("BEGIN TRANSACTION;")
    cur.execute("ALTER TABLE Orders RENAME TO Old_Orders;")
    cur.execute("CREATE TABLE Orders (order_id TEXT, customer_id TEXT);")
    cur.execute("INSERT INTO Orders SELECT * FROM Old_Orders;")
    cur.execute("DROP TABLE Old_Orders")
    con.commit()    
    cur.execute("BEGIN TRANSACTION;")
    cur.execute("ALTER TABLE Sellers RENAME TO Old_Sellers;")
    cur.execute("CREATE TABLE Sellers (seller_id TEXT, seller_postal_code INTEGER);")
    cur.execute("INSERT INTO Sellers SELECT * FROM Old_Sellers;")
    cur.execute("DROP TABLE Old_Sellers")
    con.commit()  
    return

def self_optimized_q4(con):
    cur = con.cursor()
    cur.execute("PRAGMA automatic_index = on")
    cur.execute("PRAGMA foreign_keys = on")
    
    cur.execute("BEGIN TRANSACTION;")
    cur.execute("ALTER TABLE Customers RENAME TO Old_Customers;")
    cur.execute("CREATE TABLE Customers (customer_id TEXT, customer_postal_code INTEGER, PRIMARY KEY(customer_id));")
    cur.execute("INSERT INTO Customers SELECT * FROM Old_Customers;")
    cur.execute("DROP TABLE Old_Customers")
    con.commit() 
    cur.execute("BEGIN TRANSACTION;")
    cur.execute("ALTER TABLE Orders RENAME TO Old_Orders;")
    cur.execute("CREATE TABLE Orders (order_id TEXT, customer_id TEXT, FOREIGN KEY(customer_id) REFERENCES Customers(customer_id), PRIMARY KEY(order_id));")
    cur.execute("INSERT INTO Orders SELECT * FROM Old_Orders;")
    cur.execute("DROP TABLE Old_Orders")
    con.commit()       
    cur.execute("BEGIN TRANSACTION;")
    cur.execute("ALTER TABLE Sellers RENAME TO Old_Sellers;")
    cur.execute("CREATE TABLE Sellers (seller_id TEXT, seller_postal_code INTEGER, PRIMARY KEY(seller_id));")
    cur.execute("INSERT INTO Sellers SELECT * FROM Old_Sellers;")
    cur.execute("DROP TABLE Old_Sellers")
    con.commit()      
    cur.execute("BEGIN TRANSACTION;")
    cur.execute("ALTER TABLE Order_items RENAME TO Old_Order_items;")
    cur.execute("CREATE TABLE Order_items (order_id TEXT, order_item_id INTEGER, product_id TEXT, seller_id TEXT, PRIMARY KEY(order_id, order_item_id, product_id, seller_id), FOREIGN KEY(seller_id) REFERENCES Sellers(seller_id), FOREIGN KEY(order_id) REFERENCES Orders(order_id));")
    cur.execute("INSERT INTO Order_items SELECT * FROM Old_Order_items;")
    cur.execute("DROP TABLE Old_Order_items")
    con.commit()  
    return
    
def user_optimized_q4(con):
    cur = con.cursor()
    cur.execute("CREATE INDEX idx_sellers_seller_id ON Sellers (seller_id);")
    con.commit()     
    return

# Generates layered bar chart
def stacked_bar_chart(runtimes):
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
    
    path = './Q4A3chart.png'
    plt.savefig(path)
    print('Chart saved to file Q4A3chart.png'.format(path))
    
    # close figure so it doesn't display
    plt.close() 
    return
    
def main():
    db_list = ["A3Small.db", "A3Medium.db", "A3Large.db"]
    runtime_list = []
    for i in db_list:
        con = connect(i)
        uninformed_q4(con)
        runtime_list.append(query_q4(con))
        close(con)
        con = connect(i)
        self_optimized_q4(con)
        runtime_list.append(query_q4(con))
        close(con)
        con = connect(i)
        user_optimized_q4(con)
        runtime_list.append(query_q4(con))
        close(con)    
    stacked_bar_chart(runtime_list)
    return
        
if __name__ == "__main__":
    main()