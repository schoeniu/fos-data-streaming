package com.schoen.fosreport.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;

/*
 * Primary key of JPA mapped object of aggregated event metrics.
 */
@Data
@NoArgsConstructor
@JsonDeserialize(builder = EventWindow.Builder.class)
@Embeddable
public final class EventWindow implements Serializable {
    @Column( name = "start_event_time")
    private Timestamp start_event_time;
    @Column( name = "end_event_time")
    private Timestamp end_event_time;
    @Column( name = "nr_of_events")
    private int nr_of_events;

    private EventWindow(Builder builder) {
        this.start_event_time = builder.start_event_time;
        this.end_event_time = builder.end_event_time;
        this.nr_of_events = builder.nr_of_events;
    }

    @JsonPOJOBuilder
    static class Builder {

        private Timestamp start_event_time;
        private Timestamp end_event_time;

        private int nr_of_events;

        Builder withstart_event_time(Timestamp start_event_time) {
            this.start_event_time = start_event_time;
            return this;
        }

        Builder withend_event_time(Timestamp end_event_time) {
            this.end_event_time = end_event_time;
            return this;
        }

        Builder withnr_of_events(int nr_of_events) {
            this.nr_of_events = nr_of_events;
            return this;
        }

        EventWindow build() {
            return new EventWindow(this);
        }
    }
}
