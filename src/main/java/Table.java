import java.io.*;
import java.util.*;

public class Table implements Serializable
{
    String name;
    String clusteringColumn;
    Vector<pair> pageRanges;
    Vector<String> pageNames;
    Vector<Integer>pageSizes;
    public Table(String name, String clusteringColumn)
    {
        this.name = name;
        this.clusteringColumn = clusteringColumn;
        pageRanges = new Vector<pair>();
        pageNames = new Vector<String>();
        pageSizes = new Vector<Integer>();
    }


    static class pair implements Serializable
    {
        Object min, max;
        public pair(Object min, Object max)
        {
            this.min = min;
            this.max = max;
        }

    }
}
