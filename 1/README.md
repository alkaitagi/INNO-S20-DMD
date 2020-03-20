# Data Modelling and Databases II - Assignment 1

## Description

In this assignment, you are asked to move the database of your company from an RDBMS to one of the NoSQL databases from the list below:

- MongoDB
- Neo4J
- Redis
- Firebase Realtime

All the tables and their content should be moved, without loss of information. The dump file of the original database is available on Moodle.

Moreover, the queries that were previously executed on the original platform should be still available. The following queries and reports should be implemented:

1. Retrieve all the customers that rented movies of at least two different categories during the current year. The current year is the year of the most recent records in the table rental.
2. Create a report that shows a table actor (rows) vs actor (columns) with the number of movies the actors co-starred.
3. A report that lists all films, their film category and the number of times it has been rented by a customer.
4. A report that, given a certain customer (parameter) and his/her historical data on rented movies, recommends other movies. Such a recommendation should be based on customers that watched similar sets of movies. Create a metric (any) to assess to which degree a movie is a good recommendation.
5. You probably have heard of the “Bacon’s Law”. Create a report that shows the degrees of separation between a certain actor (choose one, this should be fixed) and any other actor.

The queries and reports and the general appearance of the output will be considered in the grade.

## Deliverables

You should submit in Moodle:

- Program files of any programming language to move the content from RDMBS to the selected NoSQL database.
- Program or text files containing the queries. One file per query.
- One PDF file of exactly one page describing:
  - The process of moving the database (e.g. which scripts were executed and for what). A comprehensive Component diagram must be included. See: <https://plantuml.com/component-diagram>
  - Descriptions of the adjustments that were necessary for the new database.
  - Comments on the performance of the queries.

You must submit a single zip file containing all the deliverables. Also, a brief presentation demonstrating the database and the queries on your own computer is expected.

## Plagiarism

The assignment is individual. In the case of signs of plagiarism (at the discretion of the instructors), all the involved students will have grade 0 (zero).

## Deadline

21th of March 2020, 23:59. Submissions after this day will be ignored.

## To beging with

1. Create the database dvdrental
2. In the bin folder of PostgreSQL:
`psql -U postgres -h localhost -p 5433 -d dvdrental -f "...\restore.sql"`
