import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.validator.ValidateWith;

import java.io.*;
import java.math.RoundingMode;
import java.nio.BufferUnderflowException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;

public class DBApp implements DBAppInterface {
    Vector<String> tables;
    int maxPageSize;
    int maxIndexBucket;
    static int bucketIndex = 0;
    static Vector<Integer> stringIndex;


    public DBApp() {
        try {
            try {
                tables = readTables();
            } catch (Exception e) {
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
            if (tables.contains(tableName))
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
            Table table = new Table(tableName, clusterColumn);
            writeTable(table);
            writeTables();
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }


    }


    public void editMetaDataIndex(String tableName, String[] columnNames) throws DBAppException {
        try{
            Vector<String[]> data = new Vector<>();
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"), ',', '"', 0);

            String[] record;
            loop: while ((record = reader.readNext()) != null) {
                if (record != null) {
                    if (record[0].equals(tableName)) {
                        for(String entry: columnNames)
                        {
                            if(entry.equals(record[1]))
                            {
                                record[4] = "true";
                                data.add(record);
                                continue loop;
                            }
                        }
                        data.add(record);


                    }
                    else{
                        data.add(record);

                    }

                }
            }
            String csv = "src/main/resources/metadata.csv";
            CSVWriter writer = new CSVWriter(new FileWriter(csv, false));
            for(String[] entry: data)
                writer.writeNext(entry);
            writer.close();
        }catch (Exception e){
            throw new DBAppException(e.getMessage());
        }

    }
    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        HashSet<String> hs = new HashSet<>();
        for (String name : columnNames)
            hs.add(name);
        Table currentTable = readTable(tableName);
        if (columnNames.length == 0) throw new DBAppException("Not specified columns");
        for (String[] in : currentTable.indices)
            if (equalArrays(columnNames, in)) throw new DBAppException("This index was already created.");
        HashMap<String, Pair> colMinMax = getMinMax(tableName, hs);
        HashMap<String, Pair[]> colRanges = new HashMap<>();
        Iterator<String> it = colMinMax.keySet().iterator();
        int numBuckets = 1;
        stringIndex = new Vector<>();
        for (String key : columnNames) {
//            String key = it.next();
            Pair val = colMinMax.get(key);
            Pair[] ranges = splitRange(val.min, val.max);
            colRanges.put(key, ranges);
            numBuckets *= ranges.length;
        }
        Pair[][] ranges = new Pair[columnNames.length][];
        for (int i = 0; i < columnNames.length; i++) {
            ranges[i] = colRanges.get(columnNames[i]);
        }
        Bucket[] buckets = new Bucket[numBuckets];
        for (int i = 0; i < buckets.length; i++)
            buckets[i] = new Bucket(i);
        bucketIndex = 0;
        Grid index = new Grid(ranges[0], columnNames[0]);
        index.charIndex = stringIndex.get(0);
        createGrid(tableName, currentTable.indices.size(), index, ranges, 1, columnNames);


        for (int i = 0; i < currentTable.pageNames.size(); i++) {
            Vector<Hashtable<String, Object>> currentPage = readPage(currentTable, i);
            for (Hashtable<String, Object> row : currentPage) {
                int idx = getBucket(index, row);
                buckets[idx].clusteringKeyValues.add(row.get(currentTable.clusteringColumn));
                Hashtable<String, Object> indexColVals = new Hashtable<>();
                for (String colName : columnNames)
                    indexColVals.put(colName, row.get(colName));
                buckets[idx].IndexColumnValues.add(indexColVals);
            }

        }
        for (Bucket b : buckets)
            divideBucket(b, tableName, currentTable.indices.size());
        currentTable.indices.add(columnNames);
        writeTable(currentTable);
        writeGrid(index, tableName+"_index_"+(currentTable.indices.size()-1));
        editMetaDataIndex(tableName, columnNames);


    }

    public Grid readGrid(String tableName, int indexNo) throws DBAppException {
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + "_index_" + indexNo + ".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);

            Grid g = (Grid) in.readObject();
            in.close();
            fileIn.close();
            return g;
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public Bucket readBucket(String bucketName) throws DBAppException {
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + bucketName + ".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);

            Bucket b = (Bucket) in.readObject();
            in.close();
            fileIn.close();
            return b;
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public void writeGrid(Grid g, String gridName) throws DBAppException {
        try {
            String dataDirPath = "src/main/resources/data";
            File dataDir = new File(dataDirPath);

            if (!dataDir.isDirectory() || !dataDir.exists()) {
                dataDir.mkdir();
            }
            FileOutputStream fileOut =
                    new FileOutputStream("src/main/resources/data/" + gridName + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(g);
            out.close();
            fileOut.close();
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }


    public void divideBucket(Bucket b, String currentTable, int indexNum) throws DBAppException {
        int numovf = b.clusteringKeyValues.size() / maxIndexBucket;
        for (int i = 0; i < numovf; i++) {
            String name = currentTable + "_index_" + indexNum + "_bucket_" + b.bucketNumber + "." + i;
            b.overflow.add(name);
            Bucket ovf = new Bucket(b.bucketNumber);
            for (int j = maxIndexBucket * (i + 1); j < maxIndexBucket * (i + 2) && j < b.clusteringKeyValues.size(); j++) {
                ovf.clusteringKeyValues.add(b.clusteringKeyValues.get(j));
                ovf.IndexColumnValues.add(b.IndexColumnValues.get(j));
            }
            writeBucket(ovf, name);
            b.sizes.add(ovf.clusteringKeyValues.size());
        }
        while (b.clusteringKeyValues.size() > maxIndexBucket) {
            b.clusteringKeyValues.remove(b.clusteringKeyValues.size() - 1);
            b.IndexColumnValues.remove(b.IndexColumnValues.size() - 1);
        }
        String name = currentTable + "_index_" + indexNum + "_bucket_" + b.bucketNumber;
        writeBucket(b, name);

    }


    public void writeBucket(Bucket b, String bucketName) throws DBAppException {
        try {
            String dataDirPath = "src/main/resources/data";
            File dataDir = new File(dataDirPath);

            if (!dataDir.isDirectory() || !dataDir.exists()) {
                dataDir.mkdir();
            }
            FileOutputStream fileOut =
                    new FileOutputStream("src/main/resources/data/" + bucketName + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(b);
            out.close();
            fileOut.close();
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }


    public int getBucket( Grid index, Hashtable<String, Object> row)
    {
        if(!row.containsKey(index.columnName)){
            int mid=0;
            if(index.references[mid] instanceof String) {
                String[] parse = ((String) index.references[mid]).split("_");
                return Integer.parseInt(parse[parse.length - 1]);
            }
            return getBucket( (Grid)index.references[mid], row);
        }
        int min =0, max = index.ranges.length-1;
        while(min<=max)
        {

            int mid = (min+max)>>1;
            int compareMin, compareMax;
//            System.out.println(index.ranges[mid].min+" "+" "+row.get(index.columnName)+" "+index.ranges[mid].max);
            if (index.charIndex != -1) {
                if (index.charIndex >= ((String) (row.get(index.columnName))).length()) {
                    if (index.references[mid] instanceof String) {
                        String[] parse = ((String) index.references[0]).split("_");
                        return Integer.parseInt(parse[parse.length - 1]);
                    }
                    return getBucket((Grid) index.references[0], row);
                }
                String rowString = (String) row.get(index.columnName);
                compareMin = compare(rowString.charAt(index.charIndex) + "", ((String) index.ranges[mid].min).charAt(index.charIndex) + "");
                compareMax = compare(((String) index.ranges[mid].max).charAt(index.charIndex) + "", rowString.charAt(index.charIndex) + "");

            } else {
                compareMin = compare(row.get(index.columnName), index.ranges[mid].min);
                compareMax = compare(index.ranges[mid].max, row.get(index.columnName));
            }
//            System.out.println(compareMin+" "+ compareMax);
            if (compareMin >= 0 && compareMax >= 0) {
                if (index.references[mid] instanceof String) {
                    String[] parse = ((String) index.references[mid]).split("_");
                    return Integer.parseInt(parse[parse.length - 1]);
                }
                return getBucket((Grid) index.references[mid], row);
            } else if (compareMax < 0) {
                min = mid + 1;
            } else if (compareMin < 0) {
                max = mid - 1;
            }
        }
        return -1;

    }


    public void createGrid(String tableName, int indexNum, Grid g, Pair[][] ranges, int j, String[] columnNames) {
        if (j == ranges.length) {
            for (int i = 0; i < g.references.length; i++) {
                g.references[i] = tableName + "_index_" + indexNum + "_bucket_" + (bucketIndex++);
            }
            return;
        }
        for (int i = 0; i < g.references.length; i++) {
            Grid g2 = new Grid(ranges[j], columnNames[j]);
            g.references[i] = g2;
            g2.charIndex = stringIndex.get(j);
            createGrid(tableName, indexNum, g2, ranges, j + 1, columnNames);
        }

    }

    public boolean equalArrays(String[] a1, String[] a2) {
        loop:
        for (String x : a1) {
            for (String y : a2) {
                if (x.equals(y))
                    continue loop;
            }
            return false;
        }


        return a1.length == a2.length;
    }

    public HashMap<String, Pair> getMinMax(String tableName, HashSet<String> columnNames) throws DBAppException {
        try {
            Vector<String> tables = readTables();
            if (!tables.contains(tableName))
                throw new DBAppException("table not found");
            HashMap<String, Pair> hm = new HashMap<String, Pair>();
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"), ',', '"', 1);
            String[] record;
            while ((record = reader.readNext()) != null) {
                if (record != null) {
                    if (record[0].equals(tableName)) {
                        String colName = record[1];
                        if (columnNames.contains(colName)) {
                            String minSt = record[5], maxSt = record[6];
                            Object min, max;
                            if (record[2].contains("Integer")) {
                                min = Integer.parseInt(minSt);
                                max = Integer.parseInt(maxSt);

                            } else if (record[2].contains("Double")) {
                                min = Double.parseDouble(minSt);
                                max = Double.parseDouble(maxSt);
                            } else if (record[2].contains("String")) {
                                min = minSt;
                                max = maxSt;
                            } else {
                                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                                min = (Date) formatter.parse(minSt);
                                max = (Date) formatter.parse(maxSt);

                            }
                            hm.put(colName, new Pair(min, max));


                        }
                    }

                }
            }
            if (hm.size() == columnNames.size())
                return hm;
            else
                throw new DBAppException("Invalid column name");
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }

    }


    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

        validate(tableName, colNameValue, true);
        Table currentTable = findTable(tableName);

        Object clustObj = colNameValue.get(currentTable.clusteringColumn);


        if (currentTable.pageNames.size() == 0) {
            currentTable.pageNames.add(tableName + "0");
            currentTable.pageRanges.add(new Pair(clustObj, clustObj));
            currentTable.pageSizes.add(0);
            Vector<Hashtable<String, Object>> page = new Vector<>();
            page.add(colNameValue);
            updatePageInfo(currentTable, page, 0);
        } else {
            int pageIndex = binarySearchTable(clustObj, currentTable);
            pageIndex = getPageIndex(currentTable, clustObj, pageIndex);
            Vector<Hashtable<String, Object>> currentPage = readPage(currentTable, pageIndex);

            int ind = binarySearchPage(clustObj, currentPage, currentTable);
            if (ind == -1) currentPage.add(colNameValue);
            else {
                if (compare(currentPage.get(ind).get(currentTable.clusteringColumn), clustObj) == 0)
                    throw new DBAppException("the clustering value already exists");
                currentPage.add(ind, colNameValue);

            }
            for (int i = 0; i < currentTable.indices.size(); i++) {
                Grid g = readGrid(currentTable.name, i);
                int bucketNo = getBucket(g, colNameValue);
                Bucket b = readBucket(tableName + "_index_" + i + "_bucket_" + bucketNo);
                insertIntoBucket(b, currentTable, colNameValue, i);
            }
            updatePageInfo(currentTable, currentPage, pageIndex);

            if (currentTable.pageSizes.get(pageIndex) > maxPageSize)
                splitPage(currentTable, currentPage, pageIndex);
        }


    }


    public void insertIntoBucket(Bucket b, Table currentTable, Hashtable<String, Object> row, int indexNo) throws DBAppException {
        String name = currentTable.name + "_index_" + indexNo + "_bucket_" + b.bucketNumber;
        Hashtable<String, Object> indexColVals = new Hashtable<>();
        for (String col : currentTable.indices.get(indexNo))
            indexColVals.put(col, row.get(col));
        if (b.clusteringKeyValues.size() < maxIndexBucket) {
            b.clusteringKeyValues.add(row.get(currentTable.clusteringColumn));

            b.IndexColumnValues.add(indexColVals);
            writeBucket(b, name);
            return;
        }
        for (int i = 0; i < b.sizes.size(); i++) {
            if (b.sizes.get(i) < maxIndexBucket) {

                Bucket ovf = readBucket(b.overflow.get(i));
                ovf.clusteringKeyValues.add(currentTable.clusteringColumn);
                ovf.IndexColumnValues.add(indexColVals);
                b.sizes.set(i, b.sizes.get(i) + 1);
                writeBucket(ovf, b.overflow.get(i));
                writeBucket(b, name);
                return;
            }
        }
        Bucket ovf = new Bucket(b.bucketNumber);
        ovf.clusteringKeyValues.add(currentTable.clusteringColumn);
        ovf.IndexColumnValues.add(indexColVals);
        b.sizes.add(1);
        String ovfName = name + "." + b.overflow.size();
        b.overflow.add(ovfName);
        writeBucket(b, name);
        writeBucket(ovf, ovfName);
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

    public int binarySearchPage(Object clusterValue, Vector<Hashtable<String, Object>> currentPage, Table currentTable) {
        int lo = 0;
        int hi = currentPage.size() - 1;
        int ind = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >> 1;
            Object V = currentPage.get(mid).get(currentTable.clusteringColumn);
//            System.out.println(V+" "+clusterValue);
            int ans = compare(V, clusterValue);
            if (ans >= 0) {
                ind = mid;
                hi = mid - 1;
            } else
                lo = mid + 1;
        }
        return ind;
    }

    public Hashtable<String, Object> getAllColsAsObj(String tableName, Hashtable<String, String> colVals) throws DBAppException {
        Hashtable<String, Object> ans = new Hashtable<>();
        try {
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"), ',', '"', 1);
            String[] record;
            while ((record = reader.readNext()) != null) {
                String val = colVals.getOrDefault(record[1], "");
                String araf = "'";
                if (record[0].equals(tableName) && colVals.containsKey(record[1])) {
                    if (record[2].contains("Integer"))
                        ans.put(record[1], Integer.parseInt(colVals.get(record[1])));
                    else if (record[2].contains("Double"))
                        ans.put(record[1], Double.parseDouble(val));
                    else if (record[2].contains("String"))
                        if ((val.charAt(0) == '"' && val.charAt(val.length() - 1) == '"') ||
                                (val.charAt(0) == araf.charAt(0) && val.charAt(val.length() - 1) == araf.charAt(0)))
                            ans.put(record[1], val.substring(1, val.length() - 1));
                        else
                            throw new DBAppException("quotations in String expected");
                    else {
                        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                        ans.put(record[1], (Date) formatter.parse(val));
                    }
                }
            }
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }

        return ans;
    }


    public Object getColAsObj(Table t, String s) throws DBAppException {
        try {
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"), ',', '"', 1);
            String[] record;
            while ((record = reader.readNext()) != null) {
                if (record[0].equals(t.name) && record[3].equals("true")) {
                    if (record[2].contains("Integer"))
                        return Integer.parseInt(s);
                    else if (record[2].contains("Double"))
                        return Double.parseDouble(s);
                    else if (record[2].contains("String"))
                        return s;
                    else {
                        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                        return (Date) formatter.parse(s);
                    }
                }
            }
            return null;
        } catch (Exception e) {
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

        } else {
            ans = ((Date) a).compareTo((Date) b);
        }
        return ans;
    }

    public void splitPage(Table currentTable, Vector<Hashtable<String, Object>> currentPage, int i) throws DBAppException {
        String pageName;
        if (currentTable.availableNames.size() > 0) {
            pageName = currentTable.availableNames.get(0);
            currentTable.availableNames.remove(0);

        } else {
            pageName = currentTable.name + currentTable.pageSizes.size();
        }
        Vector<Hashtable<String, Object>> newPage = new Vector<>();
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
        Object clust = getColAsObj(currentTable, clusteringKeyValue);

        int pageNumber = binarySearchTable(clust, currentTable);
        if(pageNumber==-1)
        {
            System.out.println("0 row(s) affected");
            return;
        }
        Object min = currentTable.pageRanges.get(pageNumber).min;
        Object max = currentTable.pageRanges.get(pageNumber).max;
        int compareMin = compare(clust, min);
        int compareMax = compare(clust, max);
        String clusteringColumn = currentTable.clusteringColumn;
        if (compareMin < 0 || compareMax > 0) {
            System.out.println("0 row(s) affected");
            return;
        }
        Vector<Hashtable<String, Object>> currentPage = readPage(currentTable, pageNumber);
        int ind = binarySearchPage(clust, currentPage, currentTable);
        Hashtable<String, Object> deletedRow = currentPage.get(ind);
        if (compare(currentPage.get(ind).get(clusteringColumn), (clust)) == 0) {
            Iterator<String> it = columnNameValue.keySet().iterator();
            while (it.hasNext()) {
                String key = it.next();
                Object val = columnNameValue.get(key);
                currentPage.get(ind).put(key, val);
            }
            Hashtable<String, Object> updatedRow = currentPage.get(ind);
            loop:
            for (int i = 0; i < currentTable.indices.size(); i++) {
                for (String col : currentTable.indices.get(i)) {
                    if (columnNameValue.containsKey(col)) {
                        Grid g = readGrid(tableName, i);
                        deleteRowFromIndex(currentTable, g, i, deletedRow);
                        int bucketNum = getBucket(g, updatedRow);
                        Bucket b = readBucket(tableName + "_index_" + i + "_bucket_" + bucketNum);
                        insertIntoBucket(b, currentTable, updatedRow, i);
                        continue loop;
                    }
                }
            }
            System.out.println("1 row(s) affected");
            updatePageInfo(currentTable, currentPage, pageNumber);
        } else {
            System.out.println("0 row(s) affected");
        }

    }

    public Vector<SQLTerm> generateSQLTerms(String tableName, Hashtable<String, Object> columnNameValue) {
        Vector<SQLTerm> terms = new Vector<>();
        Iterator<String> itr = columnNameValue.keySet().iterator();
        while (itr.hasNext()) {
            String columnName = itr.next();
            Object val = columnNameValue.get(columnName);
            SQLTerm term = new SQLTerm();
            term._strTableName = tableName;
            term._strColumnName = columnName;
            term._strOperator = "=";
            term._objValue = val;
            terms.add(term);
        }
        return terms;
    }


    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        validate(tableName, columnNameValue, false);
        Table currentTable = findTable(tableName);
        String clusteringColumn = currentTable.clusteringColumn;
        Vector<Hashtable<String, Object>> deleted = new Vector<>();
        if (columnNameValue.contains(clusteringColumn)) {
            Object clusteringValue = columnNameValue.get(clusteringColumn);
            int pageNumber = binarySearchTable(clusteringValue, currentTable);
            if(pageNumber == -1)
            {
                System.out.println("0 row(s) affected");
                return;
            }
            int compareMin = compare(currentTable.pageRanges.get(pageNumber).min, clusteringValue);
            int compareMax = compare(currentTable.pageRanges.get(pageNumber).max, clusteringValue);
            if (compareMin > 0 || compareMax < 0) {
                System.out.println("0 row(s) affected");
                return;
            }
            Vector<Hashtable<String, Object>> currentPage = readPage(currentTable, pageNumber);
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
            deleted.add(currentPage.get(ind));
            currentPage.remove(ind);
            updatePageInfo(currentTable, currentPage, pageNumber);
        } else {

            Vector<SQLTerm> terms = generateSQLTerms(tableName, columnNameValue);
            Grid g = getIndex(currentTable, terms);
            if (g == null) {
                int c = 0;
                for (int i = 0; i < currentTable.pageNames.size(); i++) {
                    boolean updatePage = false;
                    Vector<Hashtable<String, Object>> currentPage = readPage(currentTable, i);

                    loop:
                    for (int j = 0; j < currentPage.size(); j++) {
                        Iterator<String> it = columnNameValue.keySet().iterator();
                        while (it.hasNext()) {
                            String key = it.next();
                            Object inputVal = columnNameValue.get(key);
                            Object recordVal = currentPage.get(j).get(key);
                            if (recordVal == null || compare(inputVal, recordVal) != 0)
                                continue loop;
                        }
                        deleted.add(currentPage.get(j));
                        currentPage.remove(j--);
                        updatePage = true;
                    }
                    if (updatePage) updatePageInfo(currentTable, currentPage, i);
                }

            } else {
                HashSet<Object> clusteringKeys = searchIndex(g,terms);
                HashMap<Integer, Vector<Object>> mapping = new HashMap<>();
                for (Object key : clusteringKeys) {
                    int pageNumber = binarySearchTable(key, currentTable);
                    Vector<Object> tmp = mapping.getOrDefault(pageNumber, new Vector<>());
                    tmp.add(key);
                    mapping.put(pageNumber, tmp);
                }
                for (HashMap.Entry<Integer, Vector<Object>> entry : mapping.entrySet()) {
                    boolean updatePage = false;
                    int pageNumber = entry.getKey();
                    Vector<Object> keys = entry.getValue();
                    Vector<Hashtable<String, Object>> currentPage = readPage(currentTable, pageNumber);
                    loop:
                    for (Object key : keys) {
                        int idx = binarySearchPage(key, currentPage, currentTable);
                        Hashtable<String, Object> row = currentPage.get(idx);
                        for (SQLTerm term : terms) {
                            Object rowVal = row.get(term._strColumnName);
                            int comVal = compare(rowVal, term._objValue);
                            if (term._strOperator.equals("=") && comVal != 0)
                                continue loop;

                            else if (term._strOperator.equals(">") && comVal <= 0)
                                continue loop;

                            else if (term._strOperator.equals(">=") && comVal < 0)
                                continue loop;

                            else if (term._strOperator.equals("<") && comVal >= 0) continue loop;
                            else if (term._strOperator.equals("<=") && comVal > 0) continue loop;
                            else if (term._strOperator.equals("!=") && comVal == 0) continue loop;

                        }
                        deleted.add(currentPage.get(idx));
                        currentPage.remove(idx);
                        updatePage = true;
                    }
                    if (updatePage) updatePageInfo(currentTable, currentPage, pageNumber);
                }

            }

        }

        System.out.println(deleted.size() + " row(s) affected");
        for (int i = 0; i < currentTable.indices.size(); i++) {
            Grid g = readGrid(currentTable.name, i);
            deleteFromIndex(currentTable, g, i, deleted);
        }

    }

    public void deleteFromIndex(Table currentTable, Grid g, int indexNum, Vector<Hashtable<String, Object>> deleted) throws DBAppException {
        for (Hashtable<String, Object> row : deleted)
            deleteRowFromIndex(currentTable, g, indexNum, row);
    }

    public void deleteFile(String fileName) {
        File file = new File("src/main/resources/data/" + fileName + ".ser");
        file.delete();
    }

    public void deleteRowFromIndex(Table currentTable, Grid g, int indexNum, Hashtable<String, Object> row) throws DBAppException {
        int bucketNum = getBucket(g, row);
        String bucketName = currentTable.name + "_index_" + indexNum + "_bucket_" + bucketNum;
        Bucket bucket = readBucket(bucketName);
        for (int i = 0; i < bucket.IndexColumnValues.size(); i++) {
            if (compare(bucket.clusteringKeyValues.get(i), row.get(currentTable.clusteringColumn)) == 0) {
                bucket.clusteringKeyValues.remove(i);
                bucket.IndexColumnValues.remove(i);
                if (bucket.clusteringKeyValues.size() == 0) {
                    if (bucket.overflow.size() > 0) {
                        String ovfName = bucket.overflow.get(bucket.overflow.size() - 1);
                        Bucket ovfBucket = readBucket(ovfName);
                        bucket.clusteringKeyValues = ovfBucket.clusteringKeyValues;
                        bucket.IndexColumnValues = ovfBucket.IndexColumnValues;
                        bucket.overflow.remove(bucket.overflow.size() - 1);
                        bucket.sizes.remove(bucket.sizes.size() - 1);
                        deleteFile(ovfName);
                    }
                }
                writeBucket(bucket, bucketName);
                return;
            }
        }

        for (int i = 0; i < bucket.overflow.size(); i++) {
            Bucket ovfBucket = readBucket(bucket.overflow.get(i));
            for (int j = 0; j < ovfBucket.IndexColumnValues.size(); j++) {
                if (compare(ovfBucket.clusteringKeyValues.get(j), row.get(currentTable.clusteringColumn)) == 0) {
                    ovfBucket.clusteringKeyValues.remove(j);
                    ovfBucket.IndexColumnValues.remove(j);
                    if (ovfBucket.clusteringKeyValues.size() == 0) {
                        deleteFile(bucket.overflow.get(i));
                        bucket.overflow.remove(j);
                        bucket.sizes.remove(j);

                    }
                    return;
                }
            }
        }


    }

    public void validateSelection(SQLTerm[] sqlTerms, String[] arrayOperators){
        try{
            if(sqlTerms.length==0)
                throw new DBAppException("SQL terms array is empty");
            if(arrayOperators.length==0&&sqlTerms.length>1)
                throw new DBAppException("Operators array is empty");
            if(sqlTerms.length-arrayOperators.length!=1)
                throw new DBAppException("This expression is invalid");
            for(String op: arrayOperators){
                op=op.toLowerCase();
                if((!op.equals("and"))&&(!op.equals("or"))&&(!op.equals("xor")))
                    throw new DBAppException("unsupported operator: "+op+". operators must be AND,XOR or OR.");
            }
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"), ',', '"', 1);
            String[] record=reader.readNext();
            HashMap<String,String>types=new HashMap<>();
            HashSet<String>fields=new HashSet<>();
            HashSet<String>operators=new HashSet<>();
            operators.add("="); operators.add("!="); operators.add(">="); operators.add("<="); operators.add("<"); operators.add(">");
            while(record!=null)
            {
                if(record[0].equals(sqlTerms[0]._strTableName))
                {
                    types.put(record[1],record[2]);
                    fields.add(record[1]);
                }
                record = reader.readNext();
            }
            if(types.isEmpty())
                throw new DBAppException("Table: "+sqlTerms[0]._strTableName+" is not found");
            for(int i=0;i<sqlTerms.length;i++){
                if(!sqlTerms[0]._strTableName.equals(sqlTerms[i]._strTableName))
                    throw new DBAppException("Unconsistent table names: "+sqlTerms[0]._strTableName+","+sqlTerms[i]._strTableName);
                if(!fields.contains(sqlTerms[i]._strColumnName))
                    throw new DBAppException("Invalid column name '"+sqlTerms[i]._strColumnName+"'.");
                if(!isCompatible(types.get(sqlTerms[i]._strColumnName),sqlTerms[i]._objValue))
                    throw new DBAppException("Incompatible data type with "+sqlTerms[i]._strColumnName);
                if(!operators.contains(sqlTerms[i]._strOperator))
                    throw new DBAppException("Unsupported operator: "+sqlTerms[i]._strOperator);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }
    public boolean isCompatible(String type, Object val){
        if(val instanceof Integer) {
            if(!type.equals("java.lang.Integer"))
                return false;
        }
        else if(val instanceof String) {
            if(!type.equals("java.lang.String"))
                return false;
        }
        else if(val instanceof Double) {
            if(!type.equals("java.lang.Double"))
               return false;
        }
       else {
            if(!type.equals("java.util.Date"))
              return false;
        }
        return true;
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        validateSelection(sqlTerms,arrayOperators);
        Table currentTable = readTable(sqlTerms[0]._strTableName);

        HashSet<Object> resultSet = splitOR(currentTable, sqlTerms, arrayOperators);
        return filterResults(resultSet, currentTable, sqlTerms, arrayOperators);
    }


    public HashSet<Object> splitOR(Table currentTable, SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        Vector<HashSet> result = new Vector<>();
        Vector<SQLTerm> temp = new Vector<>();
        HashSet<Object> x = new HashSet<>();
        if (sqlTerms.length == 0)
            return null;
        temp.add(sqlTerms[0]);
        Vector<String> l = new Vector<>();
        for (int i = 0; i < arrayOperators.length; i++) {
            if (!arrayOperators[i].equals("OR")) {
                temp.add(sqlTerms[i + 1]);
                l.add(arrayOperators[i]);
            } else {
                x = splitXOR(currentTable, temp, l);
                result.add(x);
                temp = new Vector<>();
                l = new Vector<>();
                temp.add(sqlTerms[i + 1]);
            }
        }


        x = splitXOR(currentTable, temp, l);
        result.add(x);

        // evaluating the XOR
        HashSet<Object> finalResult = OR(result);
        return finalResult;
    }

    public HashSet<Object> splitXOR(Table currentTable, Vector<SQLTerm> sqlTerms, Vector<String> arrayOperators) throws DBAppException {
        Vector<HashSet> result = new Vector<>();
        Vector<SQLTerm> temp = new Vector<>();
        HashSet<Object> x = new HashSet<>();
        if (sqlTerms.size() == 0)
            return null;
        temp.add(sqlTerms.get(0));
        for (int i = 0; i < arrayOperators.size(); i++) {
            if (arrayOperators.get(i).equals("XOR")) {
                Grid g = getIndex(currentTable, temp);

                if (g == null)
                    x = linearSearch(currentTable, temp);
                else
                    x = searchIndex(g, temp);
                result.add(x);
                temp = new Vector<>();
            }
            temp.add(sqlTerms.get(i + 1));
        }
        Grid g = getIndex(currentTable, temp);
        if (g == null)
            x = linearSearch(currentTable, temp);
        else {
            x = searchIndex(g, temp);
        }
        result.add(x);

        // evaluating the XOR

        HashSet<Object> finalResult = XOR(result);
        return finalResult;
    }

    public HashSet<Object> linearSearch(Table currentTable, Vector<SQLTerm> terms) throws DBAppException {
        HashSet<Object> ans = new HashSet<>();
        for (int i = 0; i < currentTable.pageNames.size(); i++) {
            Vector<Hashtable<String, Object>> currentPage = readPage(currentTable, i);
            loop:
            for (Hashtable<String, Object> row : currentPage) {
                for (SQLTerm term : terms) {
                    Object rowVal = row.get(term._strColumnName);
                    int comVal = compare(rowVal, term._objValue);
                    if (term._strOperator.equals("=") && comVal != 0)
                        continue loop;

                    else if (term._strOperator.equals(">") && comVal <= 0)
                        continue loop;

                    else if (term._strOperator.equals(">=") && comVal < 0)
                        continue loop;

                    else if (term._strOperator.equals("<") && comVal >= 0) continue loop;
                    else if (term._strOperator.equals("<=") && comVal > 0) continue loop;
                    else if (term._strOperator.equals("!=") && comVal == 0) continue loop;

                }
                ans.add(row.get(currentTable.clusteringColumn));
            }
        }
        return ans;
    }


    public HashSet<Object> OR(Vector<HashSet> sqlTerms) {

        HashSet<Object> finalResult = new HashSet<>();
        for(int i=0; i<sqlTerms.size();i++)
        {
            Iterator<Object> itr = sqlTerms.get(i).iterator();
            while(itr.hasNext())
                finalResult.add(itr.next());
        }
        return finalResult;
    }

    public HashSet<Object> XOR(Vector<HashSet> sqlTerms) {
        HashSet<Object> finalResult = new HashSet<>();
        for(int i=0; i<sqlTerms.size();i++)
        {
            Iterator<Object> itr = sqlTerms.get(i).iterator();
            while(itr.hasNext())
            {
                Object key1 = itr.next();
                if (finalResult.contains(key1))
                    finalResult.remove(key1);
                else
                    finalResult.add(key1);
            }
        }
        return finalResult;

    }

    public Grid getIndex(Table currentTable, Vector<SQLTerm> terms) throws DBAppException {
        HashSet<String> cols = new HashSet<>();
        for (SQLTerm s : terms) {
            if (!s._strOperator.equals("!="))
                cols.add(s._strColumnName);
        }
        int ans = -1, max = 0;
        for (int i = 0; i < currentTable.indices.size(); i++) {
            String[] index = currentTable.indices.get(i);
            int matches = 0;
            for (int j = 0; j < index.length; j++)
                if (cols.contains(index[j])) matches++;
            if (matches > max) {
                max = matches;
                ans = i;
            }
        }

        if (ans == -1)
            return null;
//        System.out.println(Arrays.toString(currentTable.indices.get(ans)));
        return readGrid(currentTable.name, ans);

    }


    public static int Precedence(String x)
    {
        if(x.equals("or"))
            return 1;
        else if(x.equals("xor"))
            return 2;
        else if(x.equals("and"))
            return 3;
        return -1;
    }
    public static Vector<Object> inToPost(SQLTerm [] a, String [] o)
    {
        Vector<Object> v = new Vector<>();
        Vector<Object>exp = new Vector<>();
        exp.add(a[0]);
        Stack<String> s = new Stack();

        for(int i=0;i<o.length;i++)
        {
            exp.add(o[i]);
            exp.add(a[i+1]);
        }
//        System.out.println(exp);
        for(int i=0;i<exp.size();i++)
        {
            Object st = exp.get(i);
            if(st instanceof String)
            {
                while (!s.isEmpty() && Precedence((String)st)< Precedence(s.peek())){
                    v.add(s.pop());
                }
                s.push((String)st);
            }
            else
            {
                v.add(st);
            }
        }
        while(!s.isEmpty())
            v.add(s.pop());
        return v;
    }
    
    public boolean evaluateSQLTerm(Hashtable<String, Object> row, SQLTerm term)
    {
        Object rowVal = row.get(term._strColumnName);
        int comVal = compare(rowVal, term._objValue);
        if (term._strOperator.equals("=") && comVal != 0)
            return false;

        else if (term._strOperator.equals(">") && comVal <= 0)
            return false;

        else if (term._strOperator.equals(">=") && comVal < 0)
            return false;

        else if (term._strOperator.equals("<") && comVal >= 0) return false;
        else if (term._strOperator.equals("<=") && comVal > 0) return false;
        else if (term._strOperator.equals("!=") && comVal == 0) return false;
    return true;
    }

    
    public boolean evaluatePostFix(Hashtable<String, Object> row, Vector<Object> postFix)
    {
        Stack<Boolean> ans = new Stack<Boolean>();
        for(int i=0;i< postFix.size();i++)
        {
            if(postFix.get(i) instanceof SQLTerm)
            {
                ans.push(evaluateSQLTerm(row, (SQLTerm)postFix.get(i)));
            }
            else
            {
                boolean first = ans.pop(), second = ans.pop();
                String operator = (String) postFix.get(i);
                if(operator.toLowerCase().equals("or"))
                    ans.push(first | second);
                else if(operator.toLowerCase().equals("and"))
                    ans.push(first & second);
                else
                    ans.push(first ^ second);
            }
        }
        return ans.pop();
    }

    public Iterator filterResults(HashSet<Object> clusteringKeys, Table currentTable, SQLTerm[] terms, String[] operators) throws DBAppException {
        Vector<Hashtable<String, Object>> ans = new Vector<>();
        Vector<Object> postFix = inToPost(terms , operators);
        HashMap<Integer, Vector<Object>> mapping = new HashMap<>();
        for (Object key : clusteringKeys) {
            int pageNumber = binarySearchTable(key, currentTable);
            Vector<Object> tmp = mapping.getOrDefault(pageNumber, new Vector<>());
            tmp.add(key);
            mapping.put(pageNumber, tmp);
        }
        for (HashMap.Entry<Integer, Vector<Object>> entry : mapping.entrySet()) {
            int pageNumber = entry.getKey();
            Vector<Object> keys = entry.getValue();
            Vector<Hashtable<String, Object>> currentPage = readPage(currentTable, pageNumber);
            loop:
            for (Object key : keys) {
                int idx = binarySearchPage(key, currentPage, currentTable);
                Hashtable<String, Object> row = currentPage.get(idx);
                if(evaluatePostFix(row,postFix))
                    ans.add(row);
            }
        }
        return ans.iterator();
    }


    public HashSet<Object>searchIndex(Grid index, Vector<SQLTerm>terms) throws DBAppException {
        HashSet<Object>ans = new HashSet<>();
        loop: for(int i=0;i<index.ranges.length;i++)
        {
            for(SQLTerm term: terms)
            {
                if(term._strColumnName.equals(index.columnName))
                {
                    int greaterThanMin = compare(term._objValue, index.ranges[i].min);
                    int lessThanMAx = compare(index.ranges[i].max, term._objValue);
                    if(index.charIndex!=-1) {
                        String rowString = (String)term._objValue;
                        greaterThanMin = compare(rowString.charAt(index.charIndex)+"", ((String)index.ranges[i].min).charAt(index.charIndex)+"");
                        lessThanMAx = compare(((String)index.ranges[i].max).charAt(index.charIndex)+"", rowString.charAt(index.charIndex)+"");
                    }
                    if(term._strOperator.equals("="))
                        if(greaterThanMin<0 || lessThanMAx<0)continue loop;

                    else if(term._strOperator.equals(">") && lessThanMAx<=0)
                        continue loop;
                        else if (term._strOperator.equals(">") && lessThanMAx <= 0)
                            continue loop;

                        else if (term._strOperator.equals(">=") && lessThanMAx < 0)
                            continue loop;

                        else if (term._strOperator.equals("<") && greaterThanMin <= 0) continue loop;
                        else if (term._strOperator.equals("<=") && greaterThanMin < 0) continue loop;
                }
            }
            if (index.references[i] instanceof Grid) {
                HashSet<Object> tmp = searchIndex((Grid) index.references[i], terms);
                Iterator<Object> itr = tmp.iterator();
                while ((itr.hasNext()))
                    ans.add(itr.next());
            } else {
                Bucket b = readBucket((String) index.references[i]);
                loop2:
                for (int k = 0; k < b.IndexColumnValues.size(); k++) {
                    for (SQLTerm term : terms) {
                        if (b.IndexColumnValues.get(k).containsKey(term._strColumnName)) {


                            Object value = b.IndexColumnValues.get(k).get(term._strColumnName);
                            int compVal = compare(value, term._objValue);
                            if (term._strOperator.equals("=") && compVal != 0) continue loop2;
                            else if (term._strOperator.equals("!=") && compVal == 0) continue loop2;
                            else if (term._strOperator.equals(">") && compVal <= 0) continue loop2;
                            else if (term._strOperator.equals(">=") && compVal < 0) continue loop2;
                            else if (term._strOperator.equals("<") && compVal >= 0) continue loop2;
                            else if (term._strOperator.equals("<=") && compVal > 0) continue loop2;
                        }

                    }
                    ans.add(b.clusteringKeyValues.get(k));

                }
                for (String ovfName : b.overflow) {
                    Bucket ovf = readBucket(ovfName);
                    loop2:
                    for (int k = 0; k < ovf.IndexColumnValues.size(); k++) {
                        for (SQLTerm term : terms) {
                            if (ovf.IndexColumnValues.get(k).containsKey(term._strColumnName)) {


                                Object value = ovf.IndexColumnValues.get(k).get(term._strColumnName);
                                int compVal = compare(value, term._objValue);
                                if (term._strOperator.equals("=") && compVal != 0) continue loop2;
                                else if (term._strOperator.equals("!=") && compVal == 0) continue loop2;
                                else if (term._strOperator.equals(">") && compVal <= 0) continue loop2;
                                else if (term._strOperator.equals(">=") && compVal < 0) continue loop2;
                                else if (term._strOperator.equals("<") && compVal >= 0) continue loop2;
                                else if (term._strOperator.equals("<=") && compVal > 0) continue loop2;
                            }

                        }
                        ans.add(ovf.clusteringKeyValues.get(k));

                    }
                }
            }


        }
        return ans;
    }


    public Table findTable(String tableName) throws DBAppException {
        Vector<String> tables = readTables();
        Table currentTable = null;

        if (tables.contains(tableName)) {
            currentTable = readTable(tableName);
        }
        return currentTable;
    }


    public Vector<String> readTables() throws DBAppException {
        if (tables != null) return tables;
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/tables.ser");
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
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public void writePage(String pageName, Vector<Hashtable<String, Object>> page) throws DBAppException {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream("src/main/resources/data/" + pageName + ".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(page);
            out.close();
            fileOut.close();
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public void updatePageInfo(Table currentTable, Vector<Hashtable<String, Object>> currentPage, int i) throws DBAppException {
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
        currentTable.pageRanges.set(i, new Pair(min, max));
        writePage(name, currentPage);
        writeTable(currentTable);
    }

    public void addNewPage(Table currentTable, String name, Vector<Hashtable<String, Object>> currentPage, int i) throws DBAppException {
        Object min = currentPage.get(0).get(currentTable.clusteringColumn);
        Object max = currentPage.get(currentPage.size() - 1).get(currentTable.clusteringColumn);
        currentTable.pageSizes.add(i, currentPage.size());
        currentTable.pageRanges.add(i, new Pair(min, max));
        currentTable.pageNames.add(i, name);
        writePage(name, currentPage);
        writeTable(currentTable);
    }

    public void validate(String tableName, Hashtable<String, Object> colNameValue, boolean insert) throws DBAppException {
        try {
            Vector<String> tables = readTables();
            if (!tables.contains(tableName))
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
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public Vector<Hashtable<String, Object>> readPage(Table currentTable, int i) throws DBAppException {
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + currentTable.pageNames.get(i) + ".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Vector<Hashtable<String, Object>> currentPage = (Vector<Hashtable<String, Object>>) in.readObject();
            in.close();
            fileIn.close();
            return currentPage;
        } catch (Exception e) {
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
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public Table readTable(String tableName) throws DBAppException {
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + "_table.ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Table currentTable = (Table) in.readObject();
            in.close();
            fileIn.close();
            return currentTable;
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }


    public Pair[] splitRange(Object min, Object max) {
        Pair[] ans = null;

        if (min instanceof Integer) {
            int minInt = (int) min, maxInt = (int) max;
            int range = (maxInt - minInt + 1);

            int inc = (int) Math.ceil(range / 10.0);
            int size = (int) Math.ceil(range * 1.0 / inc);
            ans = new Pair[size];
            for (int i = 0; i < size; i++) {
                ans[i] = new Pair(minInt + i * inc, minInt + (i + 1) * inc - 1);
            }
            stringIndex.add(-1);


        } else if (min instanceof Double) {
            DecimalFormat df = new DecimalFormat("#.######");
            df.setRoundingMode(RoundingMode.FLOOR);
            double minDl = (double) min, maxDl = (double) max;
            double range = maxDl - minDl;
            double inc = range / 10.0;
            ans = new Pair[10];
            for (int i = 0; i <= 9; i++) {
//                double tmp1 = Double.parseDouble(df.format());
//                double tmp2 = Double.parseDouble(df.format(minDl + (i+1) * inc-1e-6));
                double tmp1 = Math.floor((minDl + i * inc) * 1000000) / 1000000;
                double tmp2 = Math.floor((minDl + (i + 1) * inc - 1e-6) * 1000000) / 1000000;
                ans[i] = new Pair(tmp1, tmp2);
            }
            ans[9].max = Math.floor(maxDl * 1000000) / 1000000;
            stringIndex.add(-1);

        } else if (min instanceof String) {

            int index = -1;
            String minString = (String) min;
            String maxString = (String) max;
            for (int i = 0; i < minString.length(); i++) {
                if (minString.charAt(i) != maxString.charAt(i)) {
                    index = i;
                    break;
                }
            }
            char begin = ' ';
            char end = ' ';
            if (index == -1) {
                index = minString.length();
                begin = 0;
                end = maxString.charAt(minString.length());
            } else {
                begin = minString.charAt(index);
                end = maxString.charAt(index);
            }
            int range = (int) (end - begin + 1);
            String tmp = minString.substring(0, index);

            int inc = (int) Math.ceil(range / 10.0);
            int size = (int) Math.ceil(range * 1.0 / inc);
            ans = new Pair[size];
            for (int i = 0; i < size; i++) {
                if (i == 0 && index == minString.length())
                    ans[i] = new Pair(minString, tmp + (char) (begin + ((i + 1) * inc - 1)));
                else
                    ans[i] = new Pair(tmp + (char) (begin + (i * inc)), tmp + (char) (begin + ((i + 1) * inc - 1)));
            }
            stringIndex.add(index);

        } else {
            Date minDate = (Date) min;
            Date maxDate = (Date) max;
            LocalDate minLD = LocalDate.of(minDate.getYear() + 1900, minDate.getMonth() + 1, minDate.getDate());
            LocalDate maxLD = LocalDate.of(maxDate.getYear() + 1900, maxDate.getMonth() + 1, maxDate.getDate());
            long range = maxLD.toEpochDay() - minLD.toEpochDay() + 1;
            long inc = (long) Math.ceil(range / 10.0);
            int size = (int) (Math.ceil(range * 1.0 / inc));

            ans = new Pair[size];
            for (int i = 0; i < size; i++) {
                LocalDate tmpMin = minLD.plusDays(i * inc);
                LocalDate tmpMax = minLD.plusDays((i + 1) * inc - 1);
                ans[i] = new Pair(new Date(tmpMin.getYear() - 1900, tmpMin.getMonthValue() - 1, tmpMin.getDayOfMonth()),
                        new Date(tmpMax.getYear() - 1900, tmpMax.getMonthValue() - 1, tmpMax.getDayOfMonth()));
            }
            stringIndex.add(-1);


        }

        return ans;
    }

    public boolean checkClustering(String tableName, String clusteringCol) throws DBAppException {
        try {
            CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"), ',', '"', 1);
            String[] record;
            while ((record = reader.readNext()) != null) {
                if (record[0].equals(tableName) && record[3].equals("true")) {
                    if (record[1].equals(clusteringCol))
                        return true;
                }
            }
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }

        return false;

    }


    @Override
    public Iterator parseSQL(StringBuffer strbufSQL) throws DBAppException {
        try{
        CharStream input = (CharStream) CharStreams.fromString(String.valueOf(strbufSQL));
        SQLiteLexer mySqlLexer = new SQLiteLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(mySqlLexer);
        SQLiteParser parser = new SQLiteParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(ThrowingErrorListener.INSTANCE);
        ParseTree tree = parser.parse();
        ParserListener listener = new ParserListener();
        ParserListener.expValues = new Vector<>();
        ParserListener.operators = new Vector<>();
        ParseTreeWalker.DEFAULT.walk(listener, tree);

        if (listener.query.isEmpty())
            throw new DBAppException("invalid SQL statement");
        String statement = listener.query.get("stmt").get(0);
        if (statement.equals("create table")) {
            String tableName = listener.query.get("table").get(0);
            String clusteringColumn = listener.query.get("primary key").get(0);
            Vector<String> colNames = listener.query.get("columns");
            Vector<String> types = listener.query.get("types");
            Vector<String> colMin = listener.query.get("colMin");
            Vector<String> colMax = listener.query.get("colMax");
            if (colNames.size() != types.size())
                throw new DBAppException("please specify all the columns you are trying to insert");
            Hashtable<String, String> colTypes = new Hashtable<>();
            Hashtable<String, String> colNameMin = new Hashtable<>();
            Hashtable<String, String> colNameMax = new Hashtable<>();
            for (int i = 0; i < colNames.size(); i++) {
                String javaType;
                String sqlType = types.get(i);
                if (sqlType.toLowerCase(Locale.ROOT).equals("int"))
                    javaType = "java.lang.Integer";
                else if (sqlType.length() >= 4 && sqlType.toLowerCase(Locale.ROOT).substring(0, 4).equals("char"))
                    javaType = "java.lang.String";
                else if (sqlType.length() >= 7 && sqlType.toLowerCase(Locale.ROOT).substring(0, 7).equals("varchar"))
                    javaType = "java.lang.String";
                else if (sqlType.toLowerCase(Locale.ROOT).equals("datetime"))
                    javaType = "java.util.Date";
                else if (sqlType.toLowerCase(Locale.ROOT).equals("decimal"))
                    javaType = "java.lang.Double";
                else {
                    throw new DBAppException("Unsupported data  type");
                }
                colTypes.put(colNames.get(i), types.get(i));
                colNameMin.put(colNames.get(i), colMin.get(i));
                colNameMax.put(colNames.get(i), colMax.get(i));

            }

            createTable(tableName, clusteringColumn, colTypes, colNameMin, colNameMax);
        } else if (statement.equals("create index")) {
            String tableName = listener.query.get("table").get(0);
            Vector<String> columns = listener.query.get("columns");
            String[] colNames = new String[columns.size()];
            for (int i = 0; i < colNames.length; i++)
                colNames[i] = columns.get(i);
             createIndex(tableName, colNames);
        } else if (statement.equals("insert")) {
            String tableName = listener.query.get("table").get(0);
            Vector<String> columnNames = listener.query.get("columns");
            Vector<String> colValues = listener.query.get("values");
            if (columnNames.size() != colValues.size())
                throw new DBAppException("Invalid SQL statement");
            Hashtable<String, String> inputHash = new Hashtable<>();
            for (int i = 0; i < columnNames.size(); i++) {
                inputHash.put(columnNames.get(i), colValues.get(i));
            }
            Hashtable<String, Object> colNameValues = getAllColsAsObj(tableName, inputHash);
            insertIntoTable(tableName, colNameValues);
        } else if (statement.equals("update")) {
            String tableName = listener.query.get("table").get(0);
            Vector<String> updatedCols = listener.query.get("columns");
            Vector<String> updatedVals = listener.query.get("updated");
            Hashtable<String, String> updated = new Hashtable<>();
            for (int i = 0; i < updatedCols.size(); i++) {
                updated.put(updatedCols.get(i), updatedVals.get(i));
            }
            boolean checkClusteringCol = checkClustering(tableName, listener.query.get("clusteringColum").get(0));
            if (checkClusteringCol) {
                Hashtable<String, Object> htblColNameValue = getAllColsAsObj(tableName, updated);
                String clusterVal=listener.query.get("clusteringValue").get(0);
                clusterVal=((clusterVal.length())>1&&
                        ((clusterVal.charAt(0)=='"'&&clusterVal.charAt(clusterVal.length()-1)=='"')||
                                (clusterVal.charAt(0)==("'").charAt(0)&&clusterVal.charAt(clusterVal.length()-1)==("'").charAt(0)))
                )?clusterVal.substring(1,clusterVal.length()-1):clusterVal;

                updateTable(tableName,clusterVal, htblColNameValue);
            } else
                throw new DBAppException("Invalid clustering column");

        } else if (statement.equals("delete")) {
            String tName = listener.query.get("table").get(0);
            Hashtable<String, String> colVals = new Hashtable<>();
            Vector<SQLTerm> exp = ParserListener.expValues;
            Vector<String> op = ParserListener.operators;
            if (op.size() != exp.size() - 1)
                throw new DBAppException("Invalid SQL statement");
            for (SQLTerm s : exp) {
                if(!s._strOperator.equals("=") )
                    throw new DBAppException("Unsupported SQL statement");
                colVals.put(s._strColumnName, (String) s._objValue);
            }
            Hashtable<String, Object> htblColNameValue = getAllColsAsObj(tName, colVals);
            deleteFromTable(tName, htblColNameValue);

        } else if (statement.equals("select")) {
            String tName = listener.query.get("table").get(0);
            Vector<SQLTerm> exp = ParserListener.expValues;
            Vector<String> op = ParserListener.operators;
            Hashtable<String, String> colVals = new Hashtable<>();
            for (SQLTerm s : exp){

                colVals.put(s._strColumnName, (String) s._objValue);
            }
            Hashtable<String, Object> htblColNameValue = getAllColsAsObj(tName, colVals);
            SQLTerm[] arrSQLTerms = new SQLTerm[exp.size()];
            String[] strarrOperators = new String[op.size()];

            for (int i = 0; i < exp.size(); i++) {
                SQLTerm s = exp.get(i);
                arrSQLTerms[i] = new SQLTerm(s._strTableName, s._strColumnName, s._strOperator, htblColNameValue.get(s._strColumnName));
            }
            for (int i = 0; i < op.size(); i++)
                strarrOperators[i] = op.get(i);

            Iterator I = selectFromTable(arrSQLTerms, strarrOperators);
            return I;
        } else {
            throw new DBAppException("Unsupported SQL statement");
        }
        return null;
        }catch (Exception e){
            throw new DBAppException(e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException, ParseException {

        DBApp app = new DBApp();

        // this is for parsing sql querries and you just need to edit the query inside the string buffer
        // iterator i is used to iterate on the result set from select statements
        // iterator i is equal to null in the remaining operations


        Iterator i=app.parseSQL(new StringBuffer("select * from courses where hours = 5"));








    }
}
