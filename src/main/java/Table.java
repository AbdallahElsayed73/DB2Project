import java.io.*;
import java.util.*;

public class Table implements Serializable
{
    String name;
    String clusteringColumn;
    Vector<Pair> pageRanges;
    Vector<String> pageNames;
    Vector<Integer>pageSizes;
    Vector<String> availableNames;
    Vector<String[]> indices;
    public Table(String name, String clusteringColumn)
    {
        this.name = name;
        this.clusteringColumn = clusteringColumn;
        pageRanges = new Vector<Pair>();
        pageNames = new Vector<String>();
        pageSizes = new Vector<Integer>();
        availableNames = new Vector<String>();
        indices = new Vector<String[]>();
    }



}
