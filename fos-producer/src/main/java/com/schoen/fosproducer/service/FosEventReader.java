package com.schoen.fosproducer.service;

import com.schoen.fosproducer.model.FosEventInput;
import jakarta.annotation.Nullable;
import net.sf.jsefa.Deserializer;
import net.sf.jsefa.csv.CsvIOFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;

/*
 * Reader for reading the csv file.
 */
@Service
public class FosEventReader {

    private static final String FILE_NAME = System.getenv("INPUT_FILE_NAME");

    private final Deserializer deserializer;

    public FosEventReader() throws FileNotFoundException {
        deserializer = CsvIOFactory.createFactory(FosEventInput.class).createDeserializer();
        deserializer.open(createFileReader());
        deserializer.next(); //skip headers
    }

    @Nullable
    public FosEventInput nextInput(){
        return deserializer.hasNext() ? deserializer.next() : null;
    }

    private Reader createFileReader() throws FileNotFoundException {
        final String path = "/data/"+FILE_NAME;
        return new InputStreamReader(new BufferedInputStream(new FileInputStream(path)));
    }
}
