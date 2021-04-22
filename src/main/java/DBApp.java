import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface
{
    Vector<Table> tables;

    public DBApp() throws IOException, ClassNotFoundException {
        try {
            tables = (Vector<Table>) readTables();
        }
        catch (FileNotFoundException e) {
            tables = new Vector<Table>();
            writeTables();
            this.init();
        }

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
        tables = readTables();
        Table currentTable = null;
        for(Table t : tables)
        {
            if(t.name.equals(tableName))
            {
                currentTable = t;
                break;
            }
        }

        Object clustObj = colNameValue.get(currentTable.clusteringColumn);


            if(currentTable.pageNames.size()==0)
            {
                currentTable.pageNames.add(tableName+"0");
                currentTable.pageRanges.add(new Table.pair(clustObj,clustObj));
                Vector<Hashtable> page = new Vector<Hashtable>();
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
                int i = binarySearchTable(hi,lo,clustObj,currentTable);
                i=getPageIndex(currentTable,clustObj,i);

                FileInputStream fileIn = new FileInputStream("src/main/resources/+"+currentTable.pageNames.get(i) +".ser");
                ObjectInputStream in = new ObjectInputStream(fileIn);
                Vector<Hashtable> currentPage= (Vector<Hashtable>) in.readObject();
                in.close();
                fileIn.close();

                lo = 0; hi = currentPage.size()-1;

                int ind=binarySearchPage(hi,lo,clustObj,currentPage,currentTable);
                if(ind==-1)currentPage.add(colNameValue);
                else currentPage.add(ind,colNameValue);

                updatePageInfo(currentTable,currentPage,i);

                if(currentTable.pageSizes.get(i)>200)
                    splitPage(currentTable,currentPage,i);
            }


    }

    public int binarySearchTable(int hi, int lo, Object clustVal,Table currentTable) {
        int idx = -1;
        while(lo<=hi)
        {
            int mid = (lo+hi)/2;
            Object min = currentTable.pageRanges.get(mid).min;
            Object max = currentTable.pageRanges.get(mid).max;
            int ans1 =compare(clustVal,min);
            int ans2 = compare(max,clustVal);
            if(ans1>=0 && ans2>=0)
            {
                idx = mid;
                break;
            }
            else if(ans2<0)
            {
                idx=mid;
                lo = mid +1;
            }
            else if(ans1<0) {
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
        String pageName = currentTable.name + currentTable.pageSizes.size();
        currentTable.pageNames.add(i+1,pageName); // shouldnt we have called it pageName + i ?
        Vector<Hashtable> newPage = new Vector<Hashtable>();
        for(int j = 100;j<=200;j++)
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
        if(currentTable.pageSizes.get(i)==200)
        {
            if(ans2<0)
            {
                int i2 = Math.min(currentTable.pageRanges.size()-1, i+1);
                if(currentTable.pageSizes.get(i2)<200)i=i2;
            }
            else if(ans1<0){
                int i2 = Math.max(0, i-1);
                if(currentTable.pageSizes.get(i2)<200)i=i2;
            }
        }
        return i;
    }
    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }





    public Vector<Table> readTables() throws IOException, ClassNotFoundException {
        if(tables!=null)return tables;
        FileInputStream fileIn = new FileInputStream("src/main/resources/tables.ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        tables = (Vector<Table>) in.readObject();
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
        int min = (Integer) page.get(0).get(t.clusteringColumn), max =  (Integer) page.get(page.size()-1).get(t.clusteringColumn);
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
                            SimpleDateFormat formatter=new SimpleDateFormat("YYYY-MM-DD");
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
