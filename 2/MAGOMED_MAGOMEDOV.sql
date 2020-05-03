CREATE INDEX query_0_52 ON rental (rental_id, customer_id, staff_id, last_update);
CREATE INDEX query_0_66 ON rental (last_update, customer_id, staff_id, rental_id);
CREATE INDEX query_0_72 ON payment (rental_id, payment_date);

CREATE INDEX query_1_5 ON film (release_year, rental_rate);

CREATE INDEX query_2_26 ON inventory (film_id);
