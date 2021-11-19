Group Number: 52
CCIDs: silun1, kwu
Names: Si Lun Zhang, Kevin Wu

We declare that we did not collaborate with anyone outside our own group in this assignment. 


Reasoning for choices made for each query under the "User Optimized" Scenario

Query #1


Query #2: 

We executed the following SQL query:

SELECT AVG(size)
FROM OrderSize
WHERE oid IN (SELECT Orders.order_id
              FROM Orders
              WHERE Orders.customer_id IN (SELECT Customers.customer_id
                                           FROM Customers
                                           WHERE Customers.customer_postal_code = :random));''', {"random": random_customer}

We created indexes for customer_id for Orders because when we do a select on Orders.order_id, we are trying to select all possible order_ids for for that customer_id
so we when create an index for customer_id, all possible order_ids are next to each other reducing query time for selecting the order_ids


Query #3:

We executed the following SQL query:

SELECT AVG(size)
FROM OrderSize
WHERE oid IN (SELECT Orders.order_id
              FROM Orders
              WHERE Orders.customer_id IN (SELECT Customers.customer_id
                                           FROM Customers
                                           WHERE Customers.customer_postal_code = :random));''', {"random": random_customer}

We created indexes for customer_id for Orders because when we do a select on Orders.order_id, we are trying to select all possible order_ids for for that customer_id
so we when create an index for customer_id, all possible order_ids are next to each other reducing query time for selecting the order_ids


Query #4

We executed the following SQL query

SELECT DISTINCT seller_postal_code 
FROM Sellers S, Order_items O
WHERE O.order_id = :order 
AND S.seller_id = O.seller_id;

(:order is a randomly selected order_id from Orders)
(the random selection process was not considered in the execution time
as the query specifies "Given a". We assumed that the random selection
is done before the execution time is considered)

We assumed SQLite would create indices on seller_id and order_id
for the table Order_items since they are primary keys for that table
and seller_id for the Sellers table. Since the query searches for an
order and then matches the order_id to a seller_id to find the postal code
we thought to combine the order_id and seller_id in an index 
(Order_items.order_id+Order_items.seller_id). After testing the query with
EXPLAIN QUERY PLAN, we verified that the query does utilize the newly 
created index.