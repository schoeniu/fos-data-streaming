package com.schoen.fosreport.model;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;

import javax.validation.constraints.NotNull;

/*
 * JPA mapped object of aggregated event metrics.
 */
@Data
@Entity
@Table(name = "events")
public class EventMetrics {

    @EmbeddedId
    private EventWindow eventWindow;
    @NotNull
    @Column( name = "nr_items_viewed")
    private int nrItemsViewed;
    @NotNull
    @Column( name = "nr_items_put_in_cart")
    private int nrItemsPutInCart;
    @NotNull
    @Column( name = "nr_items_sold")
    private int nrItemsSold;
    @NotNull
    @Column( name = "value_items_sold_in_cent")
    private int valueItemsSoldInCent;

}