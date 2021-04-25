import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface
{
    Vector<Table> tables;
    int maxPageSize;
    int maxIndexBucket;

    public DBApp() throws IOException, ClassNotFoundException {
        try {
            tables = readTables();

        }
        catch (FileNotFoundException e) {
            tables = new Vector<>();
            writeTables();
        }
        Properties prop = new Properties();
        String propFileName = "config.properties";
        String fileName = "src/main/resources/DBApp.config";
        InputStream is ;
        is = new FileInputStream(fileName);
        prop.load(is);
        maxPageSize=Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
        maxIndexBucket=Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));

    }

    @Override
    public void init() throws IOException {



    }


    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, Object> colNameMin, Hashtable<String, Object> colNameMax) throws DBAppException, IOException, ClassNotFoundException {


        tables = readTables();
        for(Table t : tables)
            if (t.name.equals(tableName)) {
                throw new DBAppException("table is already created");
            }

        String clusterColumn="";
        String csv = "src/main/resources/metadata.csv";
        CSVWriter writer = new CSVWriter(new FileWriter(csv,true));

        //Create record
        Enumeration<String> enumeration = colNameType.keys();
        while(enumeration.hasMoreElements()) {
            String key = enumeration.nextElement();

            //Create record
            String type = colNameType.get(key);
            String min = (String)colNameMin.get(key);
            String max = (String)colNameMax.get(key);
            boolean clust = key.equals(clusteringKey);
            if(clust) clusterColumn = key;
            String [] record = {tableName, key,type, clust+"","false", min, max};
            //Write the record to file
            writer.writeNext(record);

            /* close the writer */

        }
        writer.close();
        tables.add(new Table(tableName,clusterColumn));
        writeTables();
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, IOException, ClassNotFoundException, ParseException {
        validate(tableName, colNameValue);
        Table currentTable = findTable(tableName);

        Object clustObj = colNameValue.get(currentTable.clusteringColumn);


            if(currentTable.pageNames.size()==0)
            {
                currentTable.pageNames.add(tableName+"0");
                currentTable.pageRanges.add(new Table.pair(clustObj,clustObj));
                currentTable.pageSizes.add(0);
                Vector<Hashtable> page = new Vector<Hashtable>();
                page.add(colNameValue);
                updatePageInfo(currentTable, page,0);
            }
            else
            {
                int pageIndex = binarySearchTable(clustObj,currentTable);
                pageIndex=getPageIndex(currentTable,clustObj,pageIndex);
                Vector<Hashtable> currentPage = readPage(currentTable, pageIndex);

                int ind=binarySearchPage(clustObj,currentPage,currentTable);
                if(ind==-1)currentPage.add(colNameValue);
                else currentPage.add(ind,colNameValue);

                updatePageInfo(currentTable,currentPage,pageIndex);

                if(currentTable.pageSizes.get(pageIndex)>maxPageSize)
                    splitPage(currentTable,currentPage,pageIndex);
            }


    }

    public int binarySearchTable(Object clustVal,Table currentTable) {
        int lo = 0;
        int hi = currentTable.pageRanges.size()-1;
        int idx = -1;
        while(lo<=hi)
        {
            int mid = (lo+hi)/2;
            Object min = currentTable.pageRanges.get(mid).min;
            Object max = currentTable.pageRanges.get(mid).max;
            int comMin =compare(clustVal,min);
            int comMax = compare(max,clustVal);
            if(comMin>=0 && comMax>=0)
            {
                idx = mid;
                break;
            }
            else if(comMax<0)
            {
                idx=mid;
                lo = mid +1;
            }
            else {
                idx = mid;
                hi = mid - 1;
            }
        }
        return idx;
    }
    public int binarySearchPage(Object clusterValue,Vector<Hashtable> currentPage,Table currentTable){
        int lo = 0;
        int hi = currentPage.size()-1;
        int ind =-1;
        while(lo<=hi){
            int mid = lo+hi>>2;
            Object V = currentPage.get(mid).get(currentTable.clusteringColumn);
            int ans = compare(V,clusterValue);
            if(ans>=0){
                ind=mid;
                hi=mid-1;
            }
            else
                lo = mid+1;
        }
        return ind;
    }
    public int compare(Object a , Object b){
        int ans;
        if(a instanceof Integer){
            ans=((Integer)a).compareTo((Integer)b);
        }
        else if (a instanceof  Double){
            ans=((Double)a).compareTo((Double)b);
        }
        else if (a instanceof String ) ans = ((String) a).compareTo((String) b);
        else {
            ans=((Date)a).compareTo((Date)b);
        }
        return ans;
    }

    public void splitPage(Table currentTable,Vector<Hashtable> currentPage, int i ) throws IOException {
        String pageName;
        if(currentTable.availableNames.size()>0)
        {
            pageName = currentTable.availableNames.get(0);
            currentTable.availableNames.remove(0);

        }
        else
        {
            pageName = currentTable.name + currentTable.pageSizes.size();
        }
        currentTable.pageNames.add(i+1,pageName); // shouldnt we have called it pageName + i ?
        Vector<Hashtable> newPage = new Vector<>();
        for(int j = maxPageSize/2;j<=maxPageSize;j++)
        {
            Hashtable entry = currentPage.get(j);
            newPage.add(entry);
            currentPage.remove(j);
        }
        updatePageInfo(currentTable,currentPage,i);
        updatePageInfo(currentTable,newPage,i+1);
    }
    public int getPageIndex(Table currentTable, Object clustVal, int i){

        Object min = currentTable.pageRanges.get(i).min;
        Object max = currentTable.pageRanges.get(i).max;
        int ans1 =compare(clustVal,min);
        int ans2 = compare(max,clustVal);
        if(currentTable.pageSizes.get(i)==maxPageSize)
        {
            if(ans2<0)
            {
                int i2 = Math.min(currentTable.pageRanges.size()-1, i+1);
                if(currentTable.pageSizes.get(i2)<maxPageSize)i=i2;
            }
            else if(ans1<0){
                int i2 = Math.max(0, i-1);
                if(currentTable.pageSizes.get(i2)<maxPageSize)i=i2;
            }
        }
        return i;
    }
    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException, IOException, ParseException, ClassNotFoundException {
        validate(tableName, columnNameValue);
        Table currentTable = findTable(tableName);
        int pageNumber = binarySearchTable(clusteringKeyValue, currentTable);
        Object min = currentTable.pageRanges.get(pageNumber).min;
        Object max = currentTable.pageRanges.get(pageNumber).max;
        int compareMin = compare(clusteringKeyValue, min);
        int compareMax = compare(clusteringKeyValue,max);
        String clusteringColumn = currentTable.clusteringColumn;
        if(compareMin <0  || compareMax>0)throw new DBAppException("A record with given clustering key value is not found");
        Vector<Hashtable> currentPage = readPage(currentTable, pageNumber);
        int ind = binarySearchPage(clusteringKeyValue,currentPage,currentTable);
        if( compare(currentPage.get(ind).get(clusteringColumn),(clusteringKeyValue)) == 0) {
            Iterator<String>it = columnNameValue.keySet().iterator();
            while(it.hasNext())
            {
                String key = it.next();
                Object val = columnNameValue.get(key);
                currentPage.get(ind).put(key,val);
            }
            updatePageInfo(currentTable, currentPage, pageNumber);
        }else{
            throw new DBAppException("A record with given clustering key value is not found");
        }
        
    }


    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException, IOException, ClassNotFoundException, ParseException {
        validate(tableName, columnNameValue);
        Table currentTable = findTable(tableName);
        String clusteringColumn = currentTable.clusteringColumn;
        if(columnNameValue.contains(clusteringColumn))
        {
            Object clusteringValue = columnNameValue.get(clusteringColumn);
            int pageNumber = binarySearchTable(clusteringValue, currentTable);
            int compareMin = compare(currentTable.pageRanges.get(pageNumber).min, clusteringValue);
            int compareMax = compare(currentTable.pageRanges.get(pageNumber).max, clusteringValue);
            if(compareMin>0 || compareMax<0) throw new DBAppException("A record with the given values is not found");
            Vector<Hashtable> currentPage = readPage(currentTable, pageNumber);
            int ind = binarySearchPage(clusteringValue, currentPage, currentTable);
            if(compare(currentPage.get(ind).get(clusteringColumn), clusteringValue) !=0) throw new DBAppException("A record with the given values is not found");


            Iterator<String>it = columnNameValue.keySet().iterator();
            while(it.hasNext())
            {
                String key = it.next();
                Object inputVal = columnNameValue.get(key);
                Object recordVal = currentPage.get(ind).get(key);
                if(compare(inputVal, recordVal) !=0)
                    throw new DBAppException("A record with the given values is not found");

            }
            currentPage.remove(ind);
            updatePageInfo(currentTable, currentPage, pageNumber);
        }
        else
        {
            boolean found = false;
            for(int i=0;i<currentTable.pageNames.size();i++)
            {
                boolean updatePage = false;
                Vector<Hashtable> currentPage = readPage(currentTable,i);
               loop: for(Hashtable<String,Object> record : currentPage)
                {
                    Iterator<String>it = columnNameValue.keySet().iterator();
                    while(it.hasNext())
                    {
                        String key = it.next();
                        Object inputVal = columnNameValue.get(key);
                        Object recordVal = currentPage.get(i).get(key);
                        if(compare(inputVal, recordVal) !=0)
                            continue loop;
                    }
                    currentPage.remove(record);
                    updatePage = true;
                    found = true;
                }
                if(updatePage) updatePageInfo(currentTable,currentPage,i);
            }
            if(!found)  throw new DBAppException("A record with the given values is not found");
        }
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }


    public Table findTable(String tableName) throws IOException, ClassNotFoundException {
        Vector<Table> tables = readTables();
        Table currentTable = null;
        for(Table t : tables)
        {
            if(t.name.equals(tableName))
            {
                currentTable = t;
                break;
            }
        }
        return currentTable;
    }


    public Vector<Table> readTables() throws IOException, ClassNotFoundException {
        if(tables!=null)return tables;
        FileInputStream fileIn = new FileInputStream("src/main/resources/tables.ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        tables = (Vector<Table>) in.readObject();
        in.close();
        fileIn.close();
        return tables;
    }

    public void writeTables() throws IOException {
        FileOutputStream fileOut =
                new FileOutputStream("src/main/resources/tables.ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(tables);
        out.close();
        fileOut.close();
    }
    public void writePage(String pageName, Vector<Hashtable> page) throws IOException {
        FileOutputStream fileOut =
                new FileOutputStream("src/main/resources/data/"+ pageName+".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(page);
        out.close();
        fileOut.close();
    }

    public void updatePageInfo(Table currentTable, Vector<Hashtable> currentPage, int i) throws IOException {
        String name = currentTable.pageNames.get(i);
        File file = new File("src/main/resources/data/"+ name+".ser");
        file.delete();
        if(currentPage.size()==0)
        {

            currentTable.availableNames.add(name);
            currentTable.pageNames.remove(i);
            currentTable.pageRanges.remove(i);
            currentTable.pageSizes.remove(i);
            writeTables();
            return;
        }
        Object min = currentPage.get(0).get(currentTable.clusteringColumn);
        Object max = currentPage.get(currentPage.size()-1).get(currentTable.clusteringColumn);
        currentTable.pageSizes.set(i,currentPage.size());
        currentTable.pageRanges.set(i,new Table.pair(min, max));
        writePage(name, currentPage);
        writeTables();
    }

    public void validate(String tableName, Hashtable<String, Object> colNameValue) throws IOException, DBAppException, ParseException {
        boolean found = false;
        CSVReader reader = new CSVReader(new FileReader("src/main/resources/metadata.csv"), ',', '"', 1);

        //Read CSV line by line and use the string array as you want
        String[] record;
        while ((record = reader.readNext()) != null) {
            if (record != null) {

                if (record[0].equals(tableName)) {
                    found = true;
                    String colName = record[1];
                    String colType = record[2];
                    boolean clust = Boolean.parseBoolean(record[3]);
                    String minSt = record[5], maxSt = record[6];
                    Object valobj = colNameValue.get(colName);
                    if(valobj == null)
                        throw new DBAppException("A column entry is missing");
                    if(valobj instanceof Integer)
                    {
                        if(colType.equals("java.lang.Integer"))
                        {
                            int val = (Integer) valobj;
                            int min = Integer.parseInt(minSt), max = Integer.parseInt(maxSt);
                            if(val<min || val > max)
                                throw new DBAppException(colName+ " value out of bound");
                        }
                        else
                            throw new DBAppException("not compatible data type at "+colName);

                    }
                    else if(valobj instanceof Double)
                    {
                        if(colType.equals("java.lang.Double"))
                        {
                            double val = (Double) valobj;
                            double min = Double.parseDouble(minSt), max = Double.parseDouble(maxSt);
                            if(val<min || val > max)
                                throw new DBAppException(colName+ " value out of bound");
                        }
                        else
                            throw new DBAppException("not compatible data type at "+colName);
                    }
                    else if(valobj instanceof Date)
                    {
                        if(colType.equals("java.util.Date"))
                        {
                            SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
                            Date val =(Date) valobj;
                            Date min = formatter.parse(minSt), max =formatter.parse(maxSt);
                            if(val.compareTo(min)<0 || val.compareTo(max)>0)
                                throw new DBAppException(colName+ " value out of bound");
                        }
                        else
                            throw new DBAppException("not compatible data type at "+colName);
                    }
                    else if(valobj instanceof String)
                    {
                        if(colType.equals("java.lang.String"))
                        {
                            String val = (String) valobj;
                            if(val.compareTo(minSt)<0 || val.compareTo(maxSt)>0)
                                throw new DBAppException(colName+ " value out of bound");
                        }
                        else
                            throw new DBAppException("not compatible data type at "+colName);
                    }
                }
            }
        }
        if(!found)
            throw new DBAppException("table not found");
    }

    public Vector<Hashtable> readPage(Table currentTable, int i) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream("src/main/resources/data/"+currentTable.pageNames.get(i) +".ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Vector<Hashtable> currentPage= (Vector<Hashtable>) in.readObject();
        in.close();
        fileIn.close();
        return currentPage;
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException, ParseException {
    DBApp app = new DBApp();

    }
}
