-- Creation of events table
CREATE TABLE IF NOT EXISTS events (
                                       "start_event_time" timestamp NOT NULL,
                                       "end_event_time" timestamp NOT NULL,
                                       "nr_of_events" int NOT NULL,
                                       "nr_items_viewed" int NOT NULL,
                                       "nr_items_put_in_cart" int NOT NULL,
                                       "nr_items_sold" int NOT NULL,
                                       "value_items_sold_in_cent" int NOT NULL,
                                       PRIMARY KEY ("start_event_time","end_event_time","nr_of_events")
);