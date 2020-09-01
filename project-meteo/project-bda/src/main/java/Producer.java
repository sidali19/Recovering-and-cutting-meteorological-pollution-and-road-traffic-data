import clojure.lang.Obj;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.shade.org.apache.http.HttpHost;
import org.apache.storm.shade.org.apache.http.HttpResponse;
import org.apache.storm.shade.org.apache.http.client.HttpClient;
import org.apache.storm.shade.org.apache.http.client.methods.HttpGet;
import org.apache.storm.shade.org.apache.http.conn.params.ConnRoutePNames;
import org.apache.storm.shade.org.apache.http.impl.client.DefaultHttpClient;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

/**
 * ce producteur va envoyer les message sur topic 0 vers un consommateur storm
 */
public class Producer {
    private static final String TRAFIC_TABLE = "grp_16_trafic";
    private static final String METEO_TABLE = "grp_16_meteo";
    private static final String ATMO_TABLE = "grp_16_atmo";
    private static final String key = "4da44eb7a5356b990e397e108afd4749";
    private static final String[] API_DATA_KEYS = {"commune", "date_indice", "valeur", "couleur_html", "qualificatif", "type_valeur"};
    private static final int DELAY = 60 * 1000 * 60; // Seconds X Minutes X Milliseconds
    private static final List<Integer> communes = Arrays.asList(
            69001, 69002,69003,69004
    );


    private static String readAll(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        // set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        // init Hive

        IOHiveStockage hiveStockage = new IOHiveStockage();
        hiveStockage.initConnection();


      if (!hiveStockage.isExist(TRAFIC_TABLE)) {
            // get Data from Csv
            List<List<Object>> xml = XmlReader.readXml(args[0], XmlReader.getColumnsXml());
            // Creating Table
            hiveStockage.createTable(TRAFIC_TABLE, XmlReader.getColumns());
            // Insert data into table
            hiveStockage.insertTable(TRAFIC_TABLE, xml);
        }
        List<List<Object>> xml = XmlReader.readXml(args[0], XmlReader.getColumnsXml());
        for (List<Object> row : xml) {
            JsonObject trafic = new JsonObject();
            trafic.addProperty("id", row.get(0).toString());
            trafic.addProperty("code", row.get(1).toString());
            trafic.addProperty("libelle", row.get(2).toString());
            trafic.addProperty("etat", row.get(3).toString());
            trafic.addProperty("date_trafic", row.get(4).toString());
            trafic.addProperty("time", row.get(5).toString());

            producer.send(new ProducerRecord<String, String>("grp-16-trafic", trafic.toString()));
        }


    List<List<Object>> csv = CSVReader.readCSV(args[1]);

        if (!hiveStockage.isExist(METEO_TABLE)) {
            // get Data from Csv
            // Creating Table
            hiveStockage.createTable(METEO_TABLE, CSVReader.getColumns());
            // Insert data into table
        }
        hiveStockage.insertTable(METEO_TABLE, csv);
        List<List<Object>> csvv = CSVReader.readCSV(args[1]);
        for (List<Object> row : csvv) {
                JsonObject meteo = new JsonObject();
                for (int i =0;i<CSVReader.getColumns().size();i++){
                    meteo.addProperty(CSVReader.getColumns().get(i), row.get(i).toString());
                }
                System.out.println("======>"+meteo);
                producer.send(new ProducerRecord<String, String>("grp-16-meteo", meteo.toString()));
        }



        try {

            // Creating Table for data collected from the API
            hiveStockage.createTable(ATMO_TABLE, Arrays.asList(API_DATA_KEYS));

            HttpClient client = new DefaultHttpClient();
            HttpGet request;
            HttpResponse response;
            HttpHost proxy_http = new HttpHost("proxy.univ-lyon1.fr", 3128, "http");
            HttpHost proxy_https = new HttpHost("proxy.univ-lyon1.fr", 3128, "http");

            try {

                List<Object> row;
                JsonNode indice;
                while (true) {

                    for (Integer com : communes
                    ) {

                        int i = 1; // numero de page
                        while (i <= 4) { // on recupere le nombre de pages de l API
                            request = new HttpGet("http://api.atmo-aura.fr/communes/" + com + "/indices?api_token=" + key + "&page=" + i);
                            client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy_http);
                            client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy_https);
                            response = client.execute(request);

                            // ici c'est la stockage de la reponse http dans un buffer
                            BufferedReader br;

                            br = new BufferedReader(new InputStreamReader(response
                                    .getEntity().getContent()));

                            String JsonText = readAll(br);

                            ObjectMapper objectMapper = new ObjectMapper();
                            JsonNode node = objectMapper.readValue(JsonText, JsonNode.class);


                            JsonNode data = node.get("indices").get("data");
                            String codeCommune = node.get("commune").textValue();
                            List<List<Object>> rows = new ArrayList<>();


                            for (int j = 0; j < data.size(); ++j) {
                                row = new ArrayList<>();
                                JsonObject atmo = new JsonObject();
                                atmo.addProperty("commune", codeCommune);
                                row.add(codeCommune);

                                indice = data.get(j);
                                for (String key : API_DATA_KEYS) {
                                    if (!key.equals("commune")) {
                                        if (key.equals("date_indice")) {
                                            key = "date";
                                        }
                                        atmo.addProperty(key, indice.get(key).textValue());
                                        row.add(indice.get(key).textValue());
                                    }
                                }

                                rows.add(row);


                                producer.send(new ProducerRecord<String, String>("grp-16-atmo", atmo.toString()));
                                producer.flush();
                            }

                            hiveStockage.insertTable(ATMO_TABLE, rows);


                            i++;
                        }
                    }
                    Thread.sleep(DELAY);
                }
            } catch (IOException | UnsupportedOperationException e) {
                e.printStackTrace();
            }

        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            hiveStockage.close();
            producer.close();
        }

    }

}
