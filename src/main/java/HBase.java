/**
 * Created by Leo Schoukroun on 10/21/16.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBase {

    public static void printMenu()
    {
        System.out.println("");
        System.out.println("1 - Add a person.");
        System.out.println("2 - Display all rows.");
        System.out.println("3 - Delete a row.");
        System.out.println("4 - Check integrity");
        System.out.println("0 - Quit");
        System.out.println("Your choice: ");

    }

    public static void main(String[] args) throws IOException {

        //Used for user input in the menu
        Scanner scan = new Scanner(System.in);

        //Configuration load
        Configuration config = HBaseConfiguration.create();

        //Let's create the table that will host our Social Application
        HBaseAdmin admin = new HBaseAdmin(config);

        //We don't need to create it if it already exists
        if (admin.tableExists("lschoukro"))
            System.out.println("The table already exists, it has not been created");
        else
        {
            HTableDescriptor myTable = new HTableDescriptor("lschoukro");

            //We create one column descriptor per columns we want
            myTable.addFamily(new HColumnDescriptor("info"));
            myTable.addFamily(new HColumnDescriptor("friends"));

            admin.createTable(myTable);
        }

        //Finally we can open the table
        HTable table = new HTable(config, "lschoukro");

        //Then we can enter the REPL
        int choice = 9;

        while(choice != 0)
        {
            //Show the menu
            printMenu();
            scan.nextLine();

            //Get the input
            choice  = scan.nextInt();

            switch(choice)
            {
                //Loop to add a new row
                case 1:
                    //We get all the data
                    System.out.println("Name: ");
                    String name = scan.next();
                    System.out.println("Age: ");
                    int age = scan.nextInt();
                    System.out.println("Sex: ");
                    String sex = scan.next();
                    System.out.println("Relationship status: ");
                    String relation = scan.next();
                    System.out.println("Best friend 4 ever: ");
                    String bestFriend = scan.next();
                    System.out.println("Other friends (separated by ','): ");
                    String otherFriendsString = scan.next();
                    scan.next();
                    String[] otherFriends = otherFriendsString.split(",");

                    //No we can save it to hbase
                    Put put = new Put(Bytes.toBytes(name));
                    put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age));
                    put.add(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(sex));
                    put.add(Bytes.toBytes("info"), Bytes.toBytes("relation"), Bytes.toBytes(relation));
                    put.add(Bytes.toBytes("friends"), Bytes.toBytes("bff"), Bytes.toBytes(bestFriend));
                    put.add(Bytes.toBytes("friends"), Bytes.toBytes("friends"), Bytes.toBytes(otherFriendsString));
                    table.put(put);

                    //Feedback
                    System.out.println(name + " was added !");

                    break;

                //Display the data for all rows
                case 2:
                    Scan s = new Scan();
                    ResultScanner scanner = table.getScanner(s);
                    try {
                        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                            System.out.println("Row: " + rr);
                        }
                    }finally {
                        scanner.close();
                    }

                    break;

                //Delete a row
                case 3:
                    System.out.println("Who do you want to delete ? ");
                    String keyToDelete = scan.next();
                    Delete del = new Delete(keyToDelete.getBytes());
                    table.delete(del);
                    System.out.println(keyToDelete + " has been deleted.");
                    break;

                //Integrity check
                case 4:
                    //First we store all the row's ids in a list from which we will check if a bff exists
                    List<String> allRows = new ArrayList<String>();

                    Scan rowScan = new Scan();
                    ResultScanner rowScanner = table.getScanner(rowScan);
                    try {
                        for (Result rr = rowScanner.next(); rr != null; rr = rowScanner.next()) {
                            String row = new String(rr.getRow());
                            allRows.add(row);
                        }
                    }finally {
                        rowScanner.close();
                    }

                    //Now we get all the bffs and check if they exist
                    Scan bffsScan = new Scan();
                    bffsScan.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("bff"));
                    ResultScanner bffsScanner = table.getScanner(bffsScan);
                    try {
                        for (Result rr = bffsScanner.next(); rr != null; rr = bffsScanner.next()) {
                            String row = new String(rr.getRow());
                            String bff = new String(rr.getValue(Bytes.toBytes("friends"), Bytes.toBytes("bff")));

                            String existsOrNot;
                            if(allRows.contains(bff))
                                existsOrNot = ": he exists !";
                            else
                                existsOrNot = ": he does not exist !";

                            System.out.println(row + "'s BFF is " + bff + existsOrNot);
                        }
                    }finally {
                        bffsScanner.close();
                    }
                    break;

                default:
                    break;
            }
        }
    }
}