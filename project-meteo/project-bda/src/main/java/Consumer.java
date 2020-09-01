import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;

/**
 * ce programme a besoin des messages pour etre consommés sur topic0 et topic
 *
 */
public class Consumer {
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

        final String ALERT_TABLE = "alert_groupe_16";
        final String[] ALERT_KEYS = {"commune","date_indice","status"};

        // set up house-keeping
        ObjectMapper mapper = new ObjectMapper();
        Histogram stats = new Histogram(1, 10000000, 2);
        Histogram global = new Histogram(1, 10000000, 2);

        // init Hive
        IOHiveStockage hiveStockage = new IOHiveStockage();
        hiveStockage.initConnection();

        if (!hiveStockage.isExist(ALERT_TABLE)){
            // Creating Table
            hiveStockage.createTable(ALERT_TABLE,Arrays.asList(ALERT_KEYS));
        }
        // and the consumer
        KafkaConsumer<String, String> consumer;
        try (InputStream props = Resources.getResource("consumer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            consumer = new KafkaConsumer<>(properties);
        }
        // ici il est inscrit sur les 2 topic topic0 et topic (pour data bon etat et data endommagé
        consumer.subscribe(Arrays.asList("grp-16-atmo-alert"));
        int timeouts = 0;
        //noinspection InfiniteLoopStatement
        while (true) {
            // read records with a short timeout. If we time out, we don't really care.
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
            if (records.count() == 0) {
                timeouts++;
            } else {
                System.out.printf("le tuple %d après %d s\n", records.count(), timeouts);
                timeouts = 0;
            }

            JsonNode dataStation;
            for (ConsumerRecord<String, String> record : records)
                switch (record.topic()) {
                    case "grp-16-atmo-alert":
                        List<List<Object>> rows = new ArrayList<>();
                        List<Object> row = new ArrayList<>();
                        // on envoie les données qui concerne une station de vélo
                        dataStation = mapper.readTree(record.value());
                        System.out.printf(dataStation + "\n");

                        for (String key : ALERT_KEYS){
                            row.add(dataStation.get(key));
                        }
                        rows.add(row);

                        // Insert data into table
                        hiveStockage.insertTable(ALERT_TABLE,rows);

                        System.out.println("je stocke dans hive");
                        break;
                    default:
                        throw new IllegalStateException("c'est pas possible d'avoir le message sur " + record.topic());
                }
        }
    }
}
