import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Lab4 {

    private static final Map<String, String> STATEMENTS = new HashMap<>();

    private static final String GET_BOOK = "select * from stored_on where isbn='%s';";

    private static final String UPDATE_NUMBER_OF_COPIES = "update stored_on SET total_copies = %s where isbn='%s' and name='%s';";

    private static final String GET_CHECKOUT_ENTRY = "select * from borrowed_by where isbn='%s' and checkin_date is null;";

    private static final String GET_BORROW_BY_CONTENTS = "select * from borrowed_by;";

    private static final String GET_CHECKED_OUT_BOOKS = "select * from borrowed_by where checkin_date is null;";

    private static final String GET_BOOK_TITLE = "select title from book where isbn='%s';";

    private static final String GET_MEMBER = "select last_name, first_name, member_id from member where member_id=%s;";

    private static final String DB_URL = "jdbc:mysql://faure/%s?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";

    private static String username;

    private static String password;

    private Lab4(String username, String password) {
        this.username = username;
        this.password = password;
    }

    private void readXML(String fileName) {
        try {
            File file = new File(fileName);
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(file);
            doc.getDocumentElement().normalize();
            NodeList nodeLst = doc.getElementsByTagName("Borrowed_by");

            for (int s = 0; s < nodeLst.getLength(); s++) {

                Node fstNode = nodeLst.item(s);

                if (fstNode.getNodeType() == Node.ELEMENT_NODE) {

                    Element sectionNode = (Element) fstNode;

                    String checkin = getStringFromElement(sectionNode, "Checkin_date");
                    String checkout = getStringFromElement(sectionNode, "Checkout_date");
                    String memberId = getStringFromElement(sectionNode, "MemberID");
                    String isbn = getStringFromElement(sectionNode, "ISBN");

                    if (checkin.equals("N/A")) {
                        // checkout
                        String insert = String.format("insert into borrowed_by (isbn, member_id, checkout_date) values ('%s', %s, '%s');", isbn, memberId, formatDate(checkout));
                        STATEMENTS.put(isbn, insert);
                    } else if (checkout.equals("N/A")) {
                        // checkin
                        String insert = String.format("update borrowed_by SET checkin_date = '%s' where isbn = '%s' and checkin_date is null and member_id = %s;", formatDate(checkin), isbn, memberId);
                        STATEMENTS.put(isbn, insert);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String formatDate(String date) {
        String[] parsedDate = date.split("/");
        String year = parsedDate[2];
        String month = parsedDate[0];
        String day = parsedDate[1];
        return String.format("%s-%s-%s", year, month, day);
    }

    private String getStringFromElement(Element sectionNode, String tagName) {
        NodeList cidateElementList = sectionNode.getElementsByTagName(tagName);
        Element cidElmnt = (Element) cidateElementList.item(0);
        NodeList cid = cidElmnt.getChildNodes();
        return cid.item(0).getNodeValue().trim();
    }

    private static void executeStatements() {
        try {

            Class.forName("com.mysql.jdbc.Driver");
            String url = String.format(DB_URL, username);
            Connection con = DriverManager.getConnection(url, username, password);
            Statement stmt = con.createStatement();

            for (Map.Entry<String, String> entry : STATEMENTS.entrySet()) {
                if (entry.getValue().contains("insert")) {
                    checkoutBook(entry, stmt);
                } else if (entry.getValue().contains("update")) {
                    checkinBook(entry, stmt);
                }
            }

            printBorrowedBy(stmt);
            printBorrowedBooks(stmt);

            con.close();
        } catch (Exception e) {
            System.out.println("Could not make connection to " + DB_URL);
            e.printStackTrace();
        }
    }

    private static void checkoutBook(Map.Entry<String, String> entry, Statement stmt) {

        String bookExists = String.format(GET_BOOK, entry.getKey());
        String isbn = null;
        String library = null;
        int copies = 0;
        try {
            ResultSet rs = stmt.executeQuery(bookExists);

            while (rs.next()) {
                isbn = rs.getString("isbn");
                copies = rs.getInt("total_copies");
                library = rs.getString("name");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (isbn == null) {
            System.out.println("Book does not exist : " + entry.getKey());
            return;
        }

        if (copies == 0) {
            System.out.println("Book has no copies in library : " + entry.getKey());
            return;
        }

        try {
            stmt.execute(entry.getValue());
            stmt.execute(String.format(UPDATE_NUMBER_OF_COPIES, --copies, isbn, library));
            System.out.println("Checked out book : " + entry.getKey());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void checkinBook(Map.Entry<String, String> entry, Statement stmt) {
        String checkoutEntry = String.format(GET_CHECKOUT_ENTRY, entry.getKey());
        String isbn = null;
        try {
            ResultSet rs = stmt.executeQuery(checkoutEntry);

            while (rs.next()) {
                isbn = rs.getString("isbn");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        String bookExists = String.format(GET_BOOK, entry.getKey());
        String library = null;
        int copies = 0;
        try {
            ResultSet rs = stmt.executeQuery(bookExists);

            while (rs.next()) {
                copies = rs.getInt("total_copies");
                library = rs.getString("name");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (isbn == null) {
            System.out.println("Book has no record of being checked out : " + entry.getKey());
            return;
        }

        try {
            stmt.execute(entry.getValue());
            stmt.execute(String.format(UPDATE_NUMBER_OF_COPIES, ++copies, isbn, library));
            System.out.println("Checked in book : " + entry.getKey());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void printBorrowedBy(Statement stmt) {
        try {
            System.out.println();
            System.out.println("Borrowed By Contents :");
            System.out.println("---------------------------------------");
            ResultSet resultSet = stmt.executeQuery(GET_BORROW_BY_CONTENTS);
            ResultSetMetaData attributeNames = resultSet.getMetaData();
            int columnsNumber = attributeNames.getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) {
                        System.out.print(",  ");
                    }

                    String columnValue = resultSet.getString(i);
                    System.out.print(attributeNames.getColumnName(i) + " : " + columnValue);
                }
                System.out.println();
            }
            System.out.println("---------------------------------------");
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printBorrowedBooks(Statement stmt) {
        try {
            System.out.println();
            System.out.println("Checked Out Books :");
            System.out.println("---------------------------------------");
            ResultSet resultSet = stmt.executeQuery(GET_CHECKED_OUT_BOOKS);

            Map<String, List<String>> checkedOutBooks = new HashMap<>();


            while (resultSet.next()) {
                String memberId = resultSet.getString("member_id");
                String isbn = resultSet.getString("isbn");
                List<String> books;
                if (checkedOutBooks.containsKey(memberId)) {
                    books = checkedOutBooks.get(memberId);
                } else {
                    books = new ArrayList<>();
                }
                books.add(isbn);
                checkedOutBooks.put(memberId, books);

            }

            for (Map.Entry<String, List<String>> entry : checkedOutBooks.entrySet()) {

                ResultSet memberResultSet = stmt.executeQuery(String.format(GET_MEMBER, entry.getKey()));
                while (memberResultSet.next()) {
                    System.out.print(memberResultSet.getString("last_name") + " ");
                    System.out.print(memberResultSet.getString("first_name") + " ");
                    System.out.print(entry.getKey());
                    System.out.println();
                }

                for (String isbn : entry.getValue()) {

                    ResultSet bookResultSet = stmt.executeQuery(String.format(GET_BOOK_TITLE, isbn));
                    while (bookResultSet.next()) {
                        System.out.println("\t" + bookResultSet.getString("title"));
                    }

                }


            }

            System.out.println("---------------------------------------");
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {
        if (args.length != 2) {
            System.out.println("Must supply username and password as arguments");
            System.out.println("Usage :  java Lab4 username password");
        }

        String username = args[0];
        String password = args[1];

        try {
            Lab4 showXML = new Lab4(username, password);
            showXML.readXML(System.getProperty("user.dir") + "/Libdata.xml");
        } catch (Exception e) {
            e.printStackTrace();
        }

        executeStatements();
    }
}