package com.schoen.fosproducer.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import net.sf.jsefa.csv.annotation.CsvDataType;
import net.sf.jsefa.csv.annotation.CsvField;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/*
 * Model for reading the csv file. One FosEventInput.input contains one event.
 */
@NoArgsConstructor
@ToString
@Getter
@CsvDataType() //https://jsefa.sourceforge.net/quick-tutorial.html
public class FosEventInput {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @CsvField(pos = 1)
    private String input;

    public LocalDateTime getEventTime(){
        final String[] fields = input.split(",");
        return LocalDateTime.parse(fields[0].replace(" UTC",""),formatter);
    }

}
