import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XmlReader {

    public static List<String> getColumnsXml() {
        List<String> columns = new ArrayList<String>();
        columns.add("id");
        columns.add("code");
        columns.add("libelle");
        columns.add("etat");
        columns.add("dateMaj");
        return columns;
    }


    public static List<String> getColumns() {
        List<String> columns = new ArrayList<String>();
        columns.add("id");
        columns.add("code");
        columns.add("libelle");
        columns.add("etat");
        columns.add("date_trafic");
        columns.add("time");

        return columns;
    }

    public static List<List<Object>> readXml(String xmlFilePath, List<String> columns) {
        List<List<Object>> rows = new ArrayList<>();

        File xmlFile = new File(xmlFilePath);
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        Document doc;
        try {
            DocumentBuilder dBuilder = factory.newDocumentBuilder();
            doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();
            NodeList nList = doc.getElementsByTagName("troncon_web_infotrafic");
            for (int i = 0; i < nList.getLength(); i++) {
                List<Object> row  = new ArrayList<>();
                Node nNode = nList.item(i);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element elem = (Element) nNode;
                    for (String column:columns) {
                        Node node = elem.getElementsByTagName(column).item(0);
                        String value = node.getTextContent();
                        if (column.equals("dateMaj")){
                            String[] parts = value.split(",");
                            String date = parts[0];
                            String time = parts[1];
                            row.add(date);
                            row.add(time);
                        }else {

                             value = value.replaceAll("'", "\\\\'");
                            row.add(value);
                        }
                    }
                    rows.add(row);
                }
            }
        } catch (SAXException | ParserConfigurationException | IOException e) {
            e.printStackTrace();
        }
        return rows;
    }
}
