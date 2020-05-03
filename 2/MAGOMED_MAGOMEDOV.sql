CREATE INDEX query_0_52 ON rental (rental_id, customer_id, staff_id, last_update);
CREATE INDEX query_0_66 ON rental (last_update, customer_id, staff_id, rental_id);
CREATE INDEX query_0_72 ON payment (rental_id, payment_date);

CREATE INDEX query_1_11 ON film (rental_rate, release_year);

CREATE INDEX query_2_97 ON inventory (film_id, inventory_id);
CREATE INDEX query_2_89 ON rental (inventory_id, customer_id, rental_id);
CREATE INDEX query_2_99 ON payment USING hash (rental_id);
CREATE INDEX query_2_104 ON film_actor (film_id, actor_id);
CREATE INDEX query_2_71 ON customer USING hash (first_name);
CREATE INDEX query_2_85 ON rental (customer_id, inventory_id);
CREATE INDEX query_2_93 ON inventory USING hash (inventory_id);
CREATE INDEX query_2_61 ON film (film_id, rating, release_year, title);
