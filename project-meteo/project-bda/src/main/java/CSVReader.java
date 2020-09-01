import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class CSVReader {

    public static List<List<Object>> readCSV(String pathToFile) {

        BufferedReader br = null;
        List<List<Object>> values = new ArrayList<>();
        String line = "";
        String fileContent = "";

        try {

            br = new BufferedReader(new FileReader(pathToFile));
            while ((line = br.readLine()) != null) {
                String[] vals = line.split(";");
                values.add(Arrays.<Object>asList(vals));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        return values;
    }

    public static List<String> getColumns() {
        List<String> columns = new ArrayList<String>();
        columns.add("numer_sta");
        columns.add("date");
        columns.add("pmer");
        columns.add("tend");
        columns.add("cod_tend");
        columns.add("dd");
        columns.add("ff");
        columns.add("t");
        columns.add("td");
        columns.add("u");
        columns.add("vv");
        columns.add("ww");
        columns.add("w1");
        columns.add("w2");
        columns.add("n");
        columns.add("nbas");
        columns.add("hbas");
        columns.add("cl");
        columns.add("cm");
        columns.add("ch");
        columns.add("pres");
        columns.add("niv_ba");
        columns.add("geop");
        columns.add("tend24");
        columns.add("tn12");
        columns.add("tn24");
        columns.add("tx12");
        columns.add("tx24");
        columns.add("tminsol");
        columns.add("sw");
        columns.add("tw");
        columns.add("raf10");
        columns.add("rafper");
        columns.add("per");
        columns.add("etat_sol");
        columns.add("ht_neige");
        columns.add("ssfrai");
        columns.add("perssfrai");
        columns.add("rr1");
        columns.add("rr3");
        columns.add("rr6");
        columns.add("rr12");
        columns.add("rr24");
        columns.add("phenspe1");
        columns.add("phenspe2");
        columns.add("phenspe3");
        columns.add("phenspe4");
        columns.add("nnuage1");
        columns.add("ctype1");
        columns.add("hnuage1");
        columns.add("nnuage2");
        columns.add("ctype2");
        columns.add("hnuage2");
        columns.add("nnuage3");
        columns.add("ctype3");
        columns.add("hnuage3");
        columns.add("nnuage4");
        columns.add("ctype4");
        columns.add("hnuage4");

        return columns;
    }

}
