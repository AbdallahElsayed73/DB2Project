import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface {
    Vector<String> tables;
    int maxPageSize;
    int maxIndexBucket;

    public DBApp(){
        try {
            try {
                tables = readTables();
            }
            catch (Exception e) {
                tables = new Vector<>();
                writeTables();
            }

            init();
            Properties prop = new Properties();
            String fileName = "src/main/resources/DBApp.config";
            InputStream is;
            is = new FileInputStream(fileName);
            prop.load(is);
            maxPageSize = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
            maxIndexBucket = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Override
    public void init() {
        try {
            String csv = "src/main/resources/metadata.csv";
            CSVReader reader = new CSVReader(new FileReader(csv), ',', '"', 1);
            if (reader.readNext() == null) {
                CSVWriter writer = new CSVWriter(new FileWriter(csv));
                String[] record = {"Table Name", "Column Name", "Column Type", "ClusteringKey", "Indexed", "min", "max"};
                writer.writeNext(record);
                writer.close();
            }
            String dataDirPath = "src/main/resources/data";
            File dataDir = new File(dataDirPath);

            if (!dataDir.isDirectory() || !dataDir.exists()) {
                dataDir.mkdir();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

        try {
            tables = readTables();
            if(tables.contains(tableName))
                throw new DBAppException("table is already created");


            String clusterColumn = "";
            String csv = "src/main/resources/metadata.csv";
            CSVWriter writer = new CSVWriter(new FileWriter(csv, true));

            //Create record
            Enumeration<String> enumeration = colNameType.keys();
            while (enumeration.hasMoreElements()) {
                String key = enumeration.nextElement();

                //Create record
                String type = colNameType.get(key);
                String min = (String) colNameMin.get(key);
                String max = (String) colNameMax.get(key);
                boolean clust = key.equals(clusteringKey);
                if (clust) clusterColumn = key;
                String[] record = {tableName, key, type, clust + "", "false", min, max};
                //Write the record to file
                writer.writeNext(record);

                /* close the writer */

            }
            writer.close();
            tables.add(tableName);
            Table table=new Table(tableName, clusterColumn);
            writeTable(table);
            writeTables();
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }


    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        validate(tableName, colNameValue, true);
        Table currentTable = findTable(tableName);

        Object clustObj = colNameValue.get(currentTable.clusteringColumn);


        if (currentTable.pageNames.size() == 0) {
            currentTable.pageNames.add(tableName + "0");
            currentTable.pageRanges.add(new Table.pair(clustObj, clustObj));
            currentTable.pageSizes.add(0);
            Vector<Hashtable> page = new Vector<Hashtable>();
            page.add(colNameValue);
            updatePageInfo(currentTable, page, 0);
        } else {
            int pageIndex = binarySearchTable(clustObj, currentTable);
            pageIndex = getPageIndex(currentTable, clustObj, pageIndex);
            Vector<Hashtable> currentPage = readPage(currentTable, pageIndex);

            int ind = binarySearchPage(clustObj, currentPage, currentTable);
            if (ind == -1) currentPage.add(colNameValue);
            else
            {
                if(compare(currentPage.get(ind).get(currentTable.clusteringColumn) , clustObj) == 0)
                    throw new DBAppException("the clustering value already exists");
                currentPage.add(ind, colNameValue);

            }

            updatePageInfo(currentTable, currentPage, pageIndex);

            if (currentTable.pageSizes.get(pageIndex) > maxPageSize)
                splitPage(currentTable, currentPage, pageIndex);
        }


    }

    public int binarySearchTable(Object clustVal, Table currentTable) {
        int lo = 0;
        int hi = currentTable.pageRanges.size() - 1;
        int idx = -1;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            Object min = currentTable.pageRanges.get(mid).min;
            Object max = currentTable.pageRanges.get(mid).max;
            int comMin = compare(clustVal, min);
            int comMax = compare(max, clustVal);
            if (comMin >= 0 && comMax >= 0) {
                idx = mid;
                break;
            } else if (comMax < 0) {
                idx = mid;
                lo = mid + 1;
            } else {
                idx = mid;
                hi = mid - 1;
            }
        }
        return idx;
    }

    public int binarySearchPage(Object clusterValue, Vector<Hashtable> currentPage, Table currentTable) {
        int lo = 0;
        int hi = currentPage.size() - 1;
        int ind = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >> 1;
            Object V = currentPage.get(mid).get(currentTable.clusteringColumn);
            int ans = compare(V, clusterValue);
//            if(ans==0)
//                throw new DBAppException("The given value already exists");
            if (ans >= 0) {
                ind = mid;
                hi = mid - 1;
            } else
                lo = mid + 1;
        }
        return ind;
    }
    public Object getColAsObj(Table t,String s) throws DBAppException {
       try{
           CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"), ',', '"', 1);
           String[] record;
           while ((record = reader.readNext()) != null) {
               if (record[0].equals(t.name) && record[3].equals("true")) {
                   if(record[2].contains("Integer"))
                       return Integer.parseInt(s);
                   else if(record[2].contains("Double"))
                       return  Double.parseDouble(s);
                   else if(record[2].contains("String"))
                       return s;
                   else
                   {
                       SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                       return (Date)formatter.parse(s);
                   }
               }
           }
           return null;
       }catch(Exception e)
       {
           throw new DBAppException(e.getMessage());
       }
    }

    public int compare(Object a, Object b) {
        int ans;
        if (a instanceof Integer) {
            ans = ((Integer) a).compareTo((Integer) b);
        } else if (a instanceof Double) {
            ans = ((Double) a).compareTo((Double) b);
        } else if (a instanceof String) {
            //if(((String) a).length() == ((String)b).length())
                ans = ((String) a).compareTo((String) b);
          //  else
            //    ans = ((String) a).ltryength() > ((String)b).length()? 1:-1;

        }
        else {
            ans = -1 * ((Date) a).compareTo((Date) b);
        }
        return ans;
    }

    public void splitPage(Table currentTable, Vector<Hashtable> currentPage, int i) throws DBAppException {
        String pageName;
        if (currentTable.availableNames.size() > 0) {
            pageName = currentTable.availableNames.get(0);
            currentTable.availableNames.remove(0);

        } else {
            pageName = currentTable.name + currentTable.pageSizes.size();
        }
        Vector<Hashtable> newPage = new Vector<>();
        for (int j = maxPageSize / 2; j <= maxPageSize; j++) {
            Hashtable entry = currentPage.get(j);
            newPage.add(entry);
        }
        while (currentPage.size() > maxPageSize / 2)
            currentPage.remove(currentPage.size() - 1);
        updatePageInfo(currentTable, currentPage, i);
        addNewPage(currentTable, pageName, newPage, i + 1);
    }

    public int getPageIndex(Table currentTable, Object clustVal, int i) {

        Object min = currentTable.pageRanges.get(i).min;
        Object max = currentTable.pageRanges.get(i).max;
        int ans1 = compare(clustVal, min);
        int ans2 = compare(max, clustVal);
        if (currentTable.pageSizes.get(i) == maxPageSize) {
            if (ans2 < 0) {
                int i2 = Math.min(currentTable.pageRanges.size() - 1, i + 1);
                if (currentTable.pageSizes.get(i2) < maxPageSize) i = i2;
            } else if (ans1 < 0) {
                int i2 = Math.max(0, i - 1);
                if (currentTable.pageSizes.get(i2) < maxPageSize) i = i2;
            }
        }
        return i;
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        validate(tableName, columnNameValue, false);
        Table currentTable = findTable(tableName);
        Object clust= getColAsObj(currentTable,clusteringKeyValue);

        int pageNumber = binarySearchTable(clust, currentTable);
        Object min = currentTable.pageRanges.get(pageNumber).min;
        Object max = currentTable.pageRanges.get(pageNumber).max;
        int compareMin = compare(clust, min);
        int compareMax = compare(clust, max);
        String clusteringColumn = currentTable.clusteringColumn;
        if (compareMin < 0 || compareMax > 0){
            System.out.println("0 row(s) affected");
            return;
        }
        Vector<Hashtable> currentPage = readPage(currentTable, pageNumber);
        int ind = binarySearchPage(clust, currentPage, currentTable);
        if (compare(currentPage.get(ind).get(clusteringColumn), (clust)) == 0) {
            Iterator<String> it = columnNameValue.keySet().iterator();
            while (it.hasNext()) {
                String key = it.next();
                Object val = columnNameValue.get(key);
                currentPage.get(ind).put(key, val);
            }
            System.out.println("1 row(s) affected");
            updatePageInfo(currentTable, currentPage, pageNumber);
        } else {
            System.out.println("0 row(s) affected");
        }

    }


    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException{
        validate(tableName, columnNameValue, false);
        Table currentTable = findTable(tableName);
        String clusteringColumn = currentTable.clusteringColumn;
        int c = 0;
        if (columnNameValue.contains(clusteringColumn)) {
            Object clusteringValue = columnNameValue.get(clusteringColumn);
            int pageNumber = binarySearchTable(clusteringValue, currentTable);
            int compareMin = compare(currentTable.pageRanges.get(pageNumber).min, clusteringValue);
            int compareMax = compare(currentTable.pageRanges.get(pageNumber).max, clusteringValue);
            if (compareMin > 0 || compareMax < 0){
                System.out.println("0 row(s) affected");
                return;
            }
            Vector<Hashtable> currentPage = readPage(currentTable, pageNumber);
            int ind = binarySearchPage(clusteringValue, currentPage, currentTable);
            if (compare(currentPage.get(ind).get(clusteringColumn), clusteringValue) != 0) {
                System.out.println("0 row(s) affected");
                return;
            }
            Iterator<String> it = columnNameValue.keySet().iterator();
            while (it.hasNext()) {
                String key = it.next();
                Object inputVal = columnNameValue.get(key);
                Object recordVal = currentPage.get(ind).get(key);
                if (recordVal == null || compare(inputVal, recordVal) != 0) {
                    System.out.println("0 row(s) affected");
                    return;
                }
            }
            currentPage.remove(ind);
            updatePageInfo(currentTable, currentPage, pageNumber);
            System.out.println("1 row(s) affected");
        } else {
            for (int i = 0; i < currentTable.pageNames.size(); i++) {
                boolean updatePage = false;
                Vector<Hashtable> currentPage = readPage(currentTable, i);

                loop: for (int j = 0;  j < currentPage.size() ; j++) {
                    Iterator<String> it = columnNameValue.keySet().iterator();
                    while (it.hasNext()) {
                        String key = it.next();
                        Object inputVal = columnNameValue.get(key);
                        Object recordVal = currentPage.get(j).get(key);
                        if (recordVal == null || compare(inputVal, recordVal) != 0)
                            continue loop;
                    }
                    c++;
                    currentPage.remove(j--);
                    updatePage = true;
                }
                if (updatePage) updatePageInfo(currentTable, currentPage, i);
            }
            System.out.println(c + " row(s) affected");
        }
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }


    public Table findTable(String tableName) throws DBAppException {
        Vector<String> tables = readTables();
        Table currentTable = null;

        if(tables.contains(tableName))
        {
            currentTable=readTable(tableName);
        }
        return currentTable;
    }


    public Vector<String> readTables() throws DBAppException {
        if (tables != null) return tables;
        FileInputStream fileIn = null;
        try {
            fileIn = new FileInputStream("src/main/resources/data/tables.ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            tables = (Vector<String>) in.readObject();
            in.close();
            fileIn.close();
            return tables;
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());

        }
    }

    public void writeTables() throws DBAppException {
        try {
            String dataDirPath = "src/main/resources/data";
            File dataDir = new File(dataDirPath);

            if (!dataDir.isDirectory() || !dataDir.exists()) {
                dataDir.mkdir();
            }
            FileOutputStream fileOut =
                    new FileOutputStream("src/main/resources/data/tables.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(tables);
            out.close();
            fileOut.close();
        }catch (Exception e)
        {
            throw new DBAppException(e.getMessage());
        }
    }

    public void writePage(String pageName, Vector<Hashtable> page) throws DBAppException {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream("src/main/resources/data/" + pageName + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(page);
            out.close();
            fileOut.close();
        }catch (Exception e)
        {
            throw new DBAppException(e.getMessage());
        }
    }

    public void updatePageInfo(Table currentTable, Vector<Hashtable> currentPage, int i) throws DBAppException {
        String name = currentTable.pageNames.get(i);
        File file = new File("src/main/resources/data/" + name + ".ser");
        file.delete();
        if (currentPage.size() == 0) {

            currentTable.availableNames.add(name);
            currentTable.pageNames.remove(i);
            currentTable.pageRanges.remove(i);
            currentTable.pageSizes.remove(i);
            writeTables();
            return;
        }
        Object min = currentPage.get(0).get(currentTable.clusteringColumn);
        Object max = currentPage.get(currentPage.size() - 1).get(currentTable.clusteringColumn);
        currentTable.pageSizes.set(i, currentPage.size());
        currentTable.pageRanges.set(i, new Table.pair(min, max));
        writePage(name, currentPage);
        writeTable(currentTable);
    }

    public void addNewPage(Table currentTable, String name, Vector<Hashtable> currentPage, int i) throws DBAppException {
        Object min = currentPage.get(0).get(currentTable.clusteringColumn);
        Object max = currentPage.get(currentPage.size() - 1).get(currentTable.clusteringColumn);
        currentTable.pageSizes.add(i, currentPage.size());
        currentTable.pageRanges.add(i, new Table.pair(min, max));
        currentTable.pageNames.add(i, name);
        writePage(name, currentPage);
        writeTable(currentTable);
    }

    public void validate(String tableName, Hashtable<String, Object> colNameValue, boolean insert) throws  DBAppException {
        try {
            Vector<String> tables = readTables();
            if(!tables.contains(tableName))
                throw new DBAppException("table not found");
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"), ',', '"', 1);
            Hashtable<String, Object> cloned = (Hashtable<String, Object>) colNameValue.clone();
            //Read CSV line by line and use the string array as you want
            String[] record;
            while ((record = reader.readNext()) != null) {
                if (record != null) {
                    if (record[0].equals(tableName)) {
                        String colName = record[1];
                        String colType = record[2];
                        boolean clust = Boolean.parseBoolean(record[3]);
                        String minSt = record[5], maxSt = record[6];
                        Object valobj = colNameValue.get(colName);
                        if (valobj == null) {
                            if (insert && clust)
                                throw new DBAppException("The clustering key can not be null");
                            else
                                continue;
                        }
                        cloned.remove(colName);
                        if (valobj instanceof Integer) {
                            if (colType.equals("java.lang.Integer")) {
                                int val = (Integer) valobj;
                                int min = Integer.parseInt(minSt), max = Integer.parseInt(maxSt);
                                if (val < min || val > max)
                                    throw new DBAppException(colName + " value out of bound");
                            } else
                                throw new DBAppException("not compatible data type at " + colName);

                        } else if (valobj instanceof Double) {
                            if (colType.equals("java.lang.Double")) {
                                double val = (Double) valobj;
                                double min = Double.parseDouble(minSt), max = Double.parseDouble(maxSt);
                                if (val < min || val > max)
                                    throw new DBAppException(colName + " value out of bound");
                            } else
                                throw new DBAppException("not compatible data type at " + colName);
                        } else if (valobj instanceof Date) {
                            if (colType.equals("java.util.Date")) {
                                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                                Date val = (Date) valobj;
                                Date min = formatter.parse(minSt), max = formatter.parse(maxSt);
                                if (val.compareTo(min) < 0 || val.compareTo(max) > 0)
                                    throw new DBAppException(colName + " value out of bound");
                            } else
                                throw new DBAppException("not compatible data type at " + colName);
                        } else if (valobj instanceof String) {
                            if (colType.equals("java.lang.String")) {
                                String val = (String) valobj;
                                if (compare(val, minSt) < 0 || compare(val, maxSt) > 0)
                                    throw new DBAppException(colName + " value out of bound");
                            } else
                                throw new DBAppException("not compatible data type at " + colName);
                        }

                    }

                }
            }
            if (!cloned.isEmpty()) throw new DBAppException("Invalid input column");
        }catch (Exception e)
        {
            throw new DBAppException(e.getMessage());
        }
    }

    public Vector<Hashtable> readPage(Table currentTable, int i) throws DBAppException {
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + currentTable.pageNames.get(i) + ".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Vector<Hashtable> currentPage = (Vector<Hashtable>) in.readObject();
            in.close();
            fileIn.close();
            return currentPage;
        }catch (Exception e)
        {
            throw new DBAppException(e.getMessage());
        }
    }
    public void writeTable(Table table) throws DBAppException {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream("src/main/resources/data/" + table.name + "_table.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(table);
            out.close();
            fileOut.close();
        }catch (Exception e)
        {
            throw new DBAppException(e.getMessage());
        }
    }
    public Table readTable(String tableName) throws DBAppException {
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName+ "_table.ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
           Table currentTable = (Table) in.readObject();
            in.close();
            fileIn.close();
            return currentTable;
        }catch (Exception e)
        {
            throw new DBAppException(e.getMessage());
        }
    }




    public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException, ParseException {

    DBApp db = new DBApp();
    Table t = db.readTable("pcs");
    Hashtable<String, Object> x = new Hashtable<>();
//    x.put("pc_id", 5);
//    db.insertIntoTable("pcs", x);
    db.writeTable(t);

//    Comparable x = 3;
//    Comparable y = 2;
//        System.out.println(x.compareTo(y));
    }
}
