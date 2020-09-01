package Operator;

import org.apache.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import java.io.StringReader;

public class Computation {
    private String data;
    private static final Logger LOG = Logger.getLogger(Computation.class);

    public Computation(String data) {
        this.data = data;
    }


    public static String airMediocreAlert(Tuple tuple){
        //LOG.info(tuple.getString(4));
        String alert = null;
        String entree = tuple.getString(4);
        JsonReader json = Json.createReader(new StringReader(entree));
        JsonObject readingObj = json.readObject();
        JsonObjectBuilder js = Json.createObjectBuilder();
        if(readingObj.getString("qualificatif").equals("Mediocre")) {
            js.add("commune",readingObj.getString("commune"));
            js.add("date_indice",readingObj.getString("date"));
            js.add("status","Air Mediocre");
            alert = js.build().toString();
            System.out.println("Alert "+alert);

        }
        json.close();
        return alert;
    }
}
