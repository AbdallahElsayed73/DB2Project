import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.junit.validator.ValidateWith;

import java.io.*;
import java.nio.BufferUnderflowException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;

public class DBApp implements DBAppInterface {
    Vector<String> tables;
    int maxPageSize;
    int maxIndexBucket;
    static int bucketIndex=0;

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

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        HashSet<String> hs = new HashSet<>();
        for (String name : columnNames)
            hs.add(name);
        Table currentTable = readTable(tableName);
        if(columnNames.length==0)throw new DBAppException("Not specified columns");
        for(String[] in : currentTable.indices)
            if(equalArrays(columnNames, in))throw new DBAppException("This index was already created.");
        HashMap<String, Pair> colMinMax = getMinMax(tableName, hs);
        HashMap<String, Pair[]> colRanges = new HashMap<>();
        Iterator<String> it = colMinMax.keySet().iterator();
        int numBuckets = 1;
        while (it.hasNext()) {
            String key = it.next();
            Pair val = colMinMax.get(key);
            Pair[] ranges = splitRange(val.min, val.max);
            colRanges.put(key, ranges);
            numBuckets*= ranges.length;
        }
        Pair[][] ranges = new Pair[columnNames.length][];
        for(int i=0;i<columnNames.length;i++)
        {
            ranges[i] = colRanges.get(columnNames[i]);
        }
        Bucket[] buckets = new Bucket[numBuckets];
        for(int i=0;i<buckets.length;i++)
            buckets[i] = new Bucket(i);
        bucketIndex=0;
        Grid index = new Grid(ranges[0],columnNames[0]);
        createGrid(tableName, currentTable.indices.size(), index, ranges, 1, columnNames);


        for(int i=0;i<currentTable.pageNames.size();i++)
        {
            Vector<Hashtable<String, Object>> currentPage = readPage(currentTable, i);
            for(Hashtable<String, Object> row: currentPage) {
                int idx = getBucket(index, row);
                buckets[idx].clusteringKeyValues.add(row.get(currentTable.clusteringColumn));
                Hashtable<String, Object> indexColVals = new Hashtable<>();
                for(String colName : columnNames)
                    indexColVals.put(colName, row.get(colName));
                buckets[idx].IndexColumnValues.add(indexColVals);
            }

        }
        for(Bucket b: buckets)
            divideBucket(b, tableName, currentTable.indices.size());
        currentTable.indices.add(columnNames);
        writeTable(currentTable);
        writeGrid(index, tableName+"_index_"+currentTable.indices.size());


    }
    public Grid readGrid(String tableName,int indexNo) throws DBAppException
    {
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + tableName + "_index_"+indexNo+".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);

            Grid g = (Grid) in.readObject();
            in.close();
            fileIn.close();
            return g;
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public Bucket readBucket(String bucketName) throws DBAppException{
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" +bucketName+".ser");
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
                    new FileOutputStream("src/main/resources/data/"+gridName+".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(g);
            out.close();
            fileOut.close();
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }


    public void divideBucket(Bucket b, String currentTable, int indexNum) throws DBAppException {
        int numovf = b.clusteringKeyValues.size()/maxIndexBucket;
        for(int i=0;i<numovf;i++)
        {
            String name = currentTable+"_index_"+indexNum+"_bucket_"+b.bucketNumber+"."+i;
            b.overflow.add(name);
            Bucket ovf = new Bucket(b.bucketNumber);
            for(int j=maxIndexBucket*(i+1); j<maxIndexBucket*(i+2) && j< b.clusteringKeyValues.size();j++)
            {
                ovf.clusteringKeyValues.add(b.clusteringKeyValues.get(j));
                ovf.IndexColumnValues.add(b.IndexColumnValues.get(j));
            }
            writeBucket(ovf, name);
            b.sizes.add(ovf.clusteringKeyValues.size());
        }
        while(b.clusteringKeyValues.size()>maxIndexBucket)
        {
            b.clusteringKeyValues.remove(b.clusteringKeyValues.size()-1);
            b.IndexColumnValues.remove(b.IndexColumnValues.size()-1);
        }
        String name = currentTable+"_index_"+indexNum+"_bucket_"+b.bucketNumber;
        writeBucket(b,name);

    }


    public void writeBucket(Bucket b, String bucketName) throws DBAppException {
        try {
            String dataDirPath = "src/main/resources/data";
            File dataDir = new File(dataDirPath);

            if (!dataDir.isDirectory() || !dataDir.exists()) {
                dataDir.mkdir();
            }
            FileOutputStream fileOut =
                    new FileOutputStream("src/main/resources/data/"+bucketName+".ser");
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
        int min =0, max = index.ranges.length-1;
        while(min<=max)
        {
            int mid = (min+max)>>1;
            int compareMin = compare(row.get(index.columnName), index.ranges[mid].min);
            int compareMax = compare(index.ranges[mid].max, row.get(index.columnName));
            if(compareMin>=0 && compareMax>=0)
            {
                if(index.references[mid] instanceof String) {
                    String[] parse = ((String) index.references[mid]).split("_");
                    return Integer.parseInt(parse[parse.length - 1]);
                }
                return getBucket( (Grid)index.references[mid], row);
            }
            else if(compareMax<0)
            {
                min=mid+1;
            }
            else if(compareMin<0)
            {
                max=mid-1;
            }
        }
        return -1;

    }


    public void createGrid(String tableName, int indexNum, Grid g,Pair[][] ranges,int j,String[] columnNames)
    {
        if(j==ranges.length)
        {
            for(int i=0;i<g.references.length;i++)
            {
                g.references[i]=tableName+"_index_"+indexNum+ "_bucket_"+(bucketIndex++);
            }
            return;
        }
        for(int i=0;i<g.references.length;i++)
        {
            Grid g2=new Grid(ranges[j],columnNames[j]);
            g.references[i]=g2;
            createGrid(tableName, indexNum, g2,ranges,j+1,columnNames);
        }

    }

    public boolean equalArrays(String[]a1, String[] a2)
    {
        loop: for(String x: a1)
        {
            for(String y: a2)
            {
              if(x.equals(y))
                  continue loop;
            }
            return false;
        }
        return true;
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
            for(int i=0;i<currentTable.indices.size();i++)
            {
                Grid g = readGrid(currentTable.name,i);
                int bucketNo= getBucket(g,colNameValue);
                Bucket b = readBucket(tableName+ "_index_"+i+"_bucket_"+bucketNo);

            }
            updatePageInfo(currentTable, currentPage, pageIndex);

            if (currentTable.pageSizes.get(pageIndex) > maxPageSize)
                splitPage(currentTable, currentPage, pageIndex);
        }


    }


    public void insertIntoBucket(Bucket b, Object clusteringKey,String tableName,int indexNo) throws DBAppException
    {
        String name = tableName+"_index_"+indexNo+"_bucket_"+b.bucketNumber;
        if(b.clusteringKeyValues.size()<maxIndexBucket)
        {
            b.clusteringKeyValues.add(clusteringKey);

            writeBucket(b,name);
            return;
        }
        for(int i=0;i<b.sizes.size();i++)
        {
            if(b.sizes.get(i)< maxIndexBucket)
            {

                Bucket ovf = readBucket(b.overflow.get(i));
                ovf.clusteringKeyValues.add(clusteringKey);
                b.sizes.set(i, b.sizes.get(i)+1);
                writeBucket(ovf, b.overflow.get(i));
                writeBucket(b,name);
                return;
            }
        }
        Bucket ovf = new Bucket(b.bucketNumber);
        ovf.clusteringKeyValues.add(clusteringKey);
        b.sizes.add(1);
        String ovfName = name+"."+b.overflow.size();
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
            int ans = compare(V, clusterValue);
            if (ans >= 0) {
                ind = mid;
                hi = mid - 1;
            } else
                lo = mid + 1;
        }
        return ind;
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
            ans = -1 * ((Date) a).compareTo((Date) b);
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

    public Vector<SQLTerm> generateSQLTerms(String tableName, Hashtable<String, Object> columnNameValue)
    {
        Vector<SQLTerm> terms = new Vector<>();
        Iterator<String> itr = columnNameValue.keySet().iterator();
        while(itr.hasNext())
        {
            String columnName = itr.next();
            Object val = columnNameValue.get(columnName);
            SQLTerm term = new SQLTerm();
            term._strTableName = tableName;
            term._strColumnName= columnName;
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
            if(g==null)
            {
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

            }
            else
            {
                HashSet<Object> clusteringKeys = new HashSet<>();
                HashMap<Integer,Vector<Object>> mapping = new HashMap<>();
                for(Object key: clusteringKeys)
                {
                    int pageNumber = binarySearchTable(key, currentTable);
                    Vector<Object> tmp = mapping.getOrDefault(pageNumber, new Vector<>());
                    tmp.add(key);
                    mapping.put(pageNumber, tmp);
                }
                for(HashMap.Entry<Integer, Vector<Object>>entry: mapping.entrySet())
                {
                    boolean updatePage = false;
                    int pageNumber = entry.getKey();
                    Vector<Object>keys = entry.getValue();
                    Vector<Hashtable<String, Object>> currentPage = readPage(currentTable, pageNumber);
                    loop : for(Object key : keys)
                    {
                        int idx = binarySearchPage(key, currentPage, currentTable);
                        Hashtable<String, Object> row = currentPage.get(idx);
                        for(SQLTerm term: terms)
                        {
                            Object rowVal = row.get(term._strColumnName);
                            int comVal = compare(rowVal, term._objValue);
                            if(term._strOperator.equals("=")&& comVal!=0)
                                continue loop;

                            else if(term._strOperator.equals(">") && comVal<=0)
                                continue loop;

                            else if(term._strOperator.equals(">=") && comVal<0)
                                continue loop;

                            else if(term._strOperator.equals("<") && comVal>=0) continue loop;
                            else if(term._strOperator.equals("<=") && comVal>0) continue loop;
                            else if(term._strOperator.equals("!=") && comVal==0) continue loop;

                        }
                        deleted.add(currentPage.get(idx));
                        currentPage.remove(idx);
                        updatePage = true;
                    }
                    if(updatePage) updatePageInfo(currentTable, currentPage, pageNumber);
                }

            }

        }

        System.out.println(deleted.size()+ "row(s) affected");
        for(int i=0;i<currentTable.indices.size();i++)
        {
            Grid g = readGrid(currentTable.name, i);
            deleteFromIndex(currentTable, g, i, deleted);
        }

    }

    public void deleteFromIndex(Table currentTable, Grid g,int indexNum, Vector<Hashtable<String, Object>> deleted) throws DBAppException {
        for(Hashtable<String, Object>row: deleted)
            deleteRowFromIndex(currentTable, g,indexNum, row);
    }

    public void deleteFile(String fileName)
    {
        File file = new File("src/main/resources/data/" + fileName + ".ser");
        file.delete();
    }

    public void deleteRowFromIndex(Table currentTable, Grid g,int indexNum, Hashtable<String, Object>row) throws DBAppException {
        int bucketNum = getBucket(g, row);
        String bucketName = currentTable.name+"_index_"+indexNum+"_bucket_"+bucketNum;
        Bucket bucket = readBucket(bucketName);
        for(int i=0;i<bucket.IndexColumnValues.size();i++)
        {
            if(compare(bucket.clusteringKeyValues.get(i), row.get(currentTable.clusteringColumn)) == 0)
            {
                bucket.clusteringKeyValues.remove(i);
                bucket.IndexColumnValues.remove(i);
                if(bucket.clusteringKeyValues.size()==0)
                {
                    if(bucket.overflow.size()>0) {
                        String ovfName = bucket.overflow.get(bucket.overflow.size() - 1);
                        Bucket ovfBucket = readBucket(ovfName);
                        bucket.clusteringKeyValues = ovfBucket.clusteringKeyValues;
                        bucket.IndexColumnValues = ovfBucket.IndexColumnValues;
                        bucket.overflow.remove(bucket.overflow.size() - 1);
                        bucket.sizes.remove(bucket.sizes.size() - 1);
                        deleteFile(ovfName);
                    }
                    writeBucket(bucket,bucketName);
                    return;
                }
            }
        }

        for(int i=0;i<bucket.overflow.size();i++)
        {
            Bucket ovfBucket = readBucket(bucket.overflow.get(i));
            for(int j=0;j<ovfBucket.IndexColumnValues.size();j++)
            {
                if(compare(ovfBucket.clusteringKeyValues.get(j), row.get(currentTable.clusteringColumn)) == 0)
                {
                    ovfBucket.clusteringKeyValues.remove(j);
                    ovfBucket.IndexColumnValues.remove(j);
                    if(ovfBucket.clusteringKeyValues.size()==0)
                    {
                        deleteFile(bucket.overflow.get(i));
                        bucket.overflow.remove(j);
                        bucket.sizes.remove(j);

                    }
                    return;
                }
            }
        }


    }

    public void validateSelection(SQLTerm[] sqlTerms, String[] arrayOperators)
    {

    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        // don't forget to validate
        Table currentTable = readTable(sqlTerms[0]._strTableName);

        HashSet<Object> resultSet = splitOR(currentTable, sqlTerms, arrayOperators);

        return filterResults(resultSet,currentTable, sqlTerms);
    }

    public HashSet<Object> splitOR (Table currentTable, SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        Vector<HashSet> result = new Vector<>();
        Vector<SQLTerm> temp = new Vector<>();
        HashSet<Object> x = new HashSet<>();
        if(sqlTerms.length==0)
            return null;
        temp.add(sqlTerms[0]);
        Vector<String> l = new Vector<>();
        for (int i = 0 ; i<arrayOperators.length; i++)
        {
            if(!arrayOperators[i].equals("OR")){
                temp.add(sqlTerms[i+1]);
                l.add(arrayOperators[i]);
            }
            else{
                x = splitXOR(currentTable, temp, l);
                result.add(x);
                temp=new Vector<>();
                l = new Vector<>();
                temp.add(sqlTerms[i+1]);
            }
        }


        x = splitXOR(currentTable, temp, l);
        result.add(x);

        // evaluating the XOR

        HashSet<Object> finalResult = OR(result);
        return finalResult;
    }

    public HashSet<Object> splitXOR (Table currentTable, Vector<SQLTerm> sqlTerms, Vector<String> arrayOperators) throws DBAppException {
        Vector<HashSet> result = new Vector<>();
        Vector<SQLTerm> temp = new Vector<>();
        HashSet<Object> x = new HashSet<>();
        if(sqlTerms.size()==0)
            return null;
        temp.add(sqlTerms.get(0));
        for (int i = 0 ; i<arrayOperators.size(); i++)
        {
            if (arrayOperators.get(i).equals("XOR")) {
                Grid g = getIndex(currentTable, temp);
                if(g==null)
                    x = linearSearch(currentTable, temp);
                else
                    x = searchIndex(g, temp);
                result.add(x);
                temp = new Vector<>();
            }
            temp.add(sqlTerms.get(i+1));
        }
        Grid g = getIndex(currentTable, temp);
        if(g==null)
            x = linearSearch(currentTable, temp);
        else
            x = searchIndex(g, temp);
        result.add(x);

        // evaluating the XOR

        HashSet<Object> finalResult = XOR(result);
        return finalResult;
    }

    public HashSet<Object> linearSearch(Table currentTable, Vector<SQLTerm> terms) throws DBAppException {
        HashSet<Object> ans = new HashSet<>();
        for(int i=0;i<currentTable.pageNames.size();i++)
        {
            Vector<Hashtable<String, Object>> currentPage = readPage(currentTable, i);
            loop: for(Hashtable<String, Object> row : currentPage)
            {
                for(SQLTerm term: terms)
                {
                    Object rowVal = row.get(term._strColumnName);
                    int comVal = compare(rowVal, term._objValue);
                    if(term._strOperator.equals("=")&& comVal!=0)
                        continue loop;

                    else if(term._strOperator.equals(">") && comVal<=0)
                        continue loop;

                    else if(term._strOperator.equals(">=") && comVal<0)
                        continue loop;

                    else if(term._strOperator.equals("<") && comVal>=0) continue loop;
                    else if(term._strOperator.equals("<=") && comVal>0) continue loop;
                    else if(term._strOperator.equals("!=") && comVal==0) continue loop;

                }
                ans.add(row.get(currentTable.clusteringColumn));
            }
        }
        return ans;
    }



    public HashSet<Object> OR (Vector<HashSet> sqlTerms){
        HashSet<Object> finalResult = sqlTerms.get(0);
        Iterator<HashSet> it = sqlTerms.iterator();
        if (it.hasNext())
            it.next();
        else
            return null;
        while (it.hasNext()) {
            HashSet<Object> key = it.next();
            Iterator<Object> it1 = key.iterator();
            while (it1.hasNext()) {
                finalResult.add(it.next());
            }
        }
        return finalResult;
    }

    public HashSet<Object> XOR (Vector<HashSet> sqlTerms){
        HashSet<Object> finalResult = sqlTerms.get(0);
        Iterator<HashSet> it = sqlTerms.iterator();
        if (it.hasNext())
            it.next();
        else
            return null;
        while (it.hasNext()) {
            HashSet<Object> key = it.next();
            Iterator<Object> it1 = key.iterator();
            while (it1.hasNext()) {
                Object key1 = it1.next();
                if (finalResult.contains(key1))
                    finalResult.remove(key1);
                else
                    finalResult.add(key1);
            }
        }
        return finalResult;
    }

    public Grid getIndex(Table currentTable, Vector<SQLTerm>terms) throws DBAppException {
      HashSet<String> cols = new HashSet<>();
      for(SQLTerm s: terms)
      {
          if(!s._strOperator.equals("!="))
             cols.add(s._strColumnName);
      }
      int ans=-1, max=0;
      for(int i=0;i<currentTable.indices.size();i++)
      {
          String[] index = currentTable.indices.get(i);
          int matches=0;
          for(int j=0;j<index.length;j++)
              if(cols.contains(index[i]))matches++;
          if(matches>max)
          {
              max = matches;
              ans = i;
          }
      }

      if(ans==-1)
        return null;

      return readGrid(currentTable.name, ans);

    }


    public Iterator filterResults(HashSet<Object>clusteringKeys, Table currentTable, SQLTerm[] terms) throws DBAppException {
        Vector<Hashtable<String,Object>> ans = new Vector<>();
        HashMap<Integer,Vector<Object>> mapping = new HashMap<>();
        for(Object key: clusteringKeys)
        {
            int pageNumber = binarySearchTable(key, currentTable);
            Vector<Object> tmp = mapping.getOrDefault(pageNumber, new Vector<>());
            tmp.add(key);
            mapping.put(pageNumber, tmp);
        }
        for(HashMap.Entry<Integer, Vector<Object>>entry: mapping.entrySet())
        {
            int pageNumber = entry.getKey();
            Vector<Object>keys = entry.getValue();
            Vector<Hashtable<String, Object>> currentPage = readPage(currentTable, pageNumber);
            loop : for(Object key : keys)
            {
                int idx = binarySearchPage(key, currentPage, currentTable);
                Hashtable<String, Object> row = currentPage.get(idx);
                for(SQLTerm term: terms)
                {
                    Object rowVal = row.get(term._strColumnName);
                    int comVal = compare(rowVal, term._objValue);
                    if(term._strOperator.equals("=")&& comVal!=0)
                        continue loop;

                    else if(term._strOperator.equals(">") && comVal<=0)
                        continue loop;

                    else if(term._strOperator.equals(">=") && comVal<0)
                        continue loop;

                    else if(term._strOperator.equals("<") && comVal>=0) continue loop;
                    else if(term._strOperator.equals("<=") && comVal>0) continue loop;
                    else if(term._strOperator.equals("!=") && comVal==0) continue loop;

                }
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
                    if(term._strOperator.equals("="))
                        if(greaterThanMin<0 || lessThanMAx<0)continue loop;

                    else if(term._strOperator.equals(">") && lessThanMAx<=0)
                        continue loop;

                    else if(term._strOperator.equals(">=") && lessThanMAx<0)
                        continue loop;

                    else if(term._strOperator.equals("<") && greaterThanMin<=0) continue loop;
                    else if(term._strOperator.equals("<=") && greaterThanMin<0) continue loop;
                }
            }
            if(index.references[i] instanceof Grid)
            {
                HashSet<Object>tmp = searchIndex((Grid)index.references[i], terms);
                Iterator<Object> itr = tmp.iterator();
                while ((itr.hasNext()))
                    ans.add(itr.next());
            }
            else
            {
                Bucket b = readBucket((String)index.references[i]);
                loop2: for(int k=0;k<b.IndexColumnValues.size();k++)
                {
                    for(SQLTerm term : terms)
                    {
                        if(b.IndexColumnValues.get(k).containsKey(term._strColumnName)) {


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
                for(String ovfName: b.overflow)
                {
                    Bucket ovf = readBucket(ovfName);
                    loop2: for(int k=0;k<ovf.IndexColumnValues.size();k++)
                    {
                        for(SQLTerm term : terms)
                        {
                            if(ovf.IndexColumnValues.get(k).containsKey(term._strColumnName)) {


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

    public Vector<Hashtable<String,Object>> readPage(Table currentTable, int i) throws DBAppException {
        try {
            FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + currentTable.pageNames.get(i) + ".ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Vector<Hashtable<String, Object>> currentPage = (Vector<Hashtable<String,Object>>) in.readObject();
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


        } else if (min instanceof Double) {
            double minDl = (double) min, maxDl = (double) max;
            double range = maxDl - minDl;
            double inc = range / 10.0;
            ans = new Pair[10];
            for (int i = 0; i <= 9; i++) {
                ans[i] = new Pair(minDl + i * inc, minDl + (i + 1) * inc - 1e-6);
            }
            ans[9].max = maxDl;


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
                ans[i] = new Pair(tmp + (char) (begin + (i * inc)), tmp + (char) (begin + ((i + 1) * inc - 1)));
            }


        } else {
            Date minDate = (Date) min;
            Date maxDate = (Date) max;
            LocalDate minLD = LocalDate.of(minDate.getYear() + 1900, minDate.getMonth() + 1, minDate.getDate());
            LocalDate maxLD = LocalDate.of(maxDate.getYear() + 1900, maxDate.getMonth() + 1, maxDate.getDate());
            long range = maxLD.toEpochDay() - minLD.toEpochDay() + 1;
            long inc = (long) Math.ceil(range / 10.0);
            int size = (int) (Math.ceil(range * 1.0 / inc));
            System.out.println(range + " " + inc + " " + size);

            ans = new Pair[size];
            for (int i = 0; i < size; i++) {
                LocalDate tmpMin = minLD.plusDays(i * inc);
                LocalDate tmpMax = minLD.plusDays((i + 1) * inc - 1);
                ans[i] = new Pair(new Date(tmpMin.getYear() - 1900, tmpMin.getMonthValue() - 1, tmpMin.getDayOfMonth()),
                        new Date(tmpMax.getYear() - 1900, tmpMax.getMonthValue() - 1, tmpMax.getDayOfMonth()));
            }


        }

        return ans;
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException, ParseException {
//        DBApp app = new DBApp();
//        Grid g=new Grid(new Pair[]{new Pair(1,3),new Pair(4,6),new Pair(7,9)},"Omar");
//        Pair[][] ranges={{new Pair(1,3),new Pair(4,6),new Pair(7,9)},{new Pair(3,5),new Pair(6,8),new Pair(9,11)},{new Pair(2,5),new Pair(6,9),new Pair(10,13)}};
//        String []colnames={"Omar","Abdallah","Samir"};
//        app.createGrid(g,ranges,1,colnames);
//        for (int i=0;i<3;i++)
//        {
//            for(int j=0;j<3;j++)
//            {
//                Grid zeft=(Grid)((Grid)(g.references[i])).references[j];
//                System.out.println(Arrays.toString(zeft.references));
//            }
//        }


    }
}
