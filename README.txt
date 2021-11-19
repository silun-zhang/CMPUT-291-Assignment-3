Reasoning for choices made for each query under the “User Optimized” Scenario

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