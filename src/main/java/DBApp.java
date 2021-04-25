import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import java.io.*;
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
            this.init();
        }
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        InputStream is ;
        is = new FileInputStream(fileName);
        prop.load(is);
        maxPageSize=Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
        maxIndexBucket=Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));

    }

    @Override
    public void init() throws IOException {

        String csv = "src/main/resources/metadata.csv";
        CSVWriter writer = new CSVWriter(new FileWriter(csv));

        //Create record
        String [] record = "Table Name,Column Name,Column Type,ClusteringKey,Indexed,min,max".split(",");
        //Write the record to file
        writer.writeNext(record);

        /* close the writer */
        writer.close();

    }


    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, Object> colNameMin, Hashtable<String, Object> colNameMax) throws DBAppException, IOException, ClassNotFoundException {


        tables = readTables();
        for(Table t : tables)
        {
            if(t.name.equals(tableName))
            {
                throw new DBAppException("table is already created");
            }
        }

        String clusterColumn="", clusterType = "";
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
            if(clust){
                clusterColumn = key;
                clusterType = type;
            }
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
                Vector<Hashtable> page = new Vector<>();
                page.add(colNameValue);
                FileOutputStream fileOut =
                        new FileOutputStream("src/main/resources/"+tableName+"0.ser");
                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                out.writeObject(tables);
                out.close();
                fileOut.close();
            }
            else
            {
                int lo = 0;
                int hi = currentTable.pageRanges.size()-1;
                int pageIndex = binarySearchTable(hi,lo,clustObj,currentTable);
                pageIndex=getPageIndex(currentTable,clustObj,pageIndex);
                Vector<Hashtable> currentPage = readPage(currentTable, pageIndex);


                lo = 0; hi = currentPage.size()-1;

                int ind=binarySearchPage(hi,lo,clustObj,currentPage,currentTable);
                if(ind==-1)currentPage.add(colNameValue);
                else currentPage.add(ind,colNameValue);

                updatePageInfo(currentTable,currentPage,pageIndex);

                if(currentTable.pageSizes.get(pageIndex)>maxPageSize)
                    splitPage(currentTable,currentPage,pageIndex);
            }


    }

    public int binarySearchTable(int hi, int lo, Object clustVal,Table currentTable) {
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
            else if(comMax<0) {
                idx = mid;
                hi = mid - 1;
            }
        }
        return idx;
    }
    public int binarySearchPage(int hi, int lo, Object clusterValue,Vector<Hashtable> currentPage,Table currentTable){
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
        else if (a instanceof String ){
            ans=((String)a).compareTo((String)b);
        }
        else {
            ans=((Date)a).compareTo((Date)b);
        }
        return ans;
    }

    public void splitPage(Table currentTable,Vector<Hashtable> currentPage, int i ){
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
        int pageNumber = binarySearchTable(currentTable.pageSizes.size()-1,0,clusteringKeyValue, currentTable);
        Object min = currentTable.pageRanges.get(pageNumber).min;
        Object max = currentTable.pageRanges.get(pageNumber).max;
        int compareMin = compare(clusteringKeyValue, min);
        int compareMax = compare(clusteringKeyValue,max);
        String clusteringColumn = currentTable.clusteringColumn;
        if(compareMin <0  || compareMax>0)return;
        Vector<Hashtable> currentPage = readPage(currentTable, pageNumber);
        int ind = binarySearchPage(currentPage.size()-1,0,clusteringKeyValue,currentPage,currentTable);
        if(currentPage.get(ind).get(clusteringColumn).equals(clusteringKeyValue)) {
            currentPage.set(ind, columnNameValue); // comment: subset of the whole record
            updatePageInfo(currentTable, currentPage, pageNumber);
        }else{
            throw new DBAppException("A record with given clustering key value is not found");
        }
        
    }


    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

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

    public void updatePageInfo(Table t, Vector<Hashtable> page, int i)
    {
        Object min = page.get(0).get(t.clusteringColumn);
        Object max = page.get(page.size()-1).get(t.clusteringColumn);
        t.pageSizes.set(i,page.size());
        t.pageRanges.set(i,new Table.pair(min, max));
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
                            Date val = formatter.parse((String)valobj);
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
        FileInputStream fileIn = new FileInputStream("src/main/resources/+"+currentTable.pageNames.get(i) +".ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Vector<Hashtable> currentPage= (Vector<Hashtable>) in.readObject();
        in.close();
        fileIn.close();
        return currentPage;
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException, ParseException {
        String strTableName = "Student";
        DBApp dbApp = new DBApp( );





//        Hashtable htblColNameValue = new Hashtable( );
//        htblColNameValue.put("id", new Integer( 2343432 ));
//        htblColNameValue.put("name", new String("Ahmed Noor" ) );
//        htblColNameValue.put("gpa", new Double( 0.95 ) );
//        dbApp.insertIntoTable( strTableName , htblColNameValue );
    }
}
