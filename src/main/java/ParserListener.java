import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Vector;

public class ParserListener extends SQLiteParserBaseListener {
    public Hashtable<String, Vector<String>> query = new Hashtable<>();
    static Vector<SQLTerm> expValues;
    static Vector <String> operators;

    @Override
    public void enterSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
        if (ctx != null) {
            Vector<String> v = new Vector<>();
            v.add("select");
            query.put("stmt", v);
            if (ctx.select_core().size() > 1) {
                query = new Hashtable<>();
                return;
            }
            SQLiteParser.Select_coreContext core = ctx.select_core(0);
            List<SQLiteParser.Result_columnContext> result = core.result_column();
            v = new Vector<>();
            for (SQLiteParser.Result_columnContext r : result)
                v.add(r.getText());
            query.put("result col", v);
            v = new Vector<>();
            if(core.table_or_subquery().get(0).table_name()==null)
            {
                System.out.println("Unsupported SQL query");
                query=new Hashtable<>();
                return;
            }
            String tname=core.table_or_subquery().get(0).table_name().getText();
            v.add(tname);
            query.put("table", v);
            List<SQLiteParser.ExprContext> exp = core.expr();
            if (exp.size() != 1) {
                query = new Hashtable<>();
                return;
            }
            v = new Vector<>();
            if(exp.get(0).OR_()!=null)
            {
                getExp(exp.get(0).expr().get(0),tname);
                operators.add("OR");
                getExp(exp.get(0).expr().get(1),tname);

            }else if (exp.get(0).AND_()!=null){
                getExp(exp.get(0).expr().get(0),tname);
                operators.add("AND");
                getExp(exp.get(0).expr().get(1),tname);
            }else
            {
                getExp(exp.get(0),tname);

            }
        }
    }
    public void getExp(SQLiteParser.ExprContext core, String tableName){

            if(core.OR_()!=null)
            {
                getExp(core.expr().get(0),tableName);
                operators.add("OR");
                getExp(core.expr().get(1),tableName);


            }else if(core.AND_()!=null)
            {
                getExp(core.expr().get(0),tableName);
                operators.add("AND");
                getExp(core.expr().get(1),tableName);

            }else {
                expValues.add(new SQLTerm(tableName,core.getChild(0).getText(),core.getChild(1).getText(),core.getChild(2).getText()));
        }


    }

    @Override
    public void enterDelete_stmt(SQLiteParser.Delete_stmtContext ctx) {
        if (ctx != null) {
            Vector<String> v = new Vector<>();
            v.add("delete");
            query.put("stmt", v);
            v = new Vector<>();
            String tname=ctx.qualified_table_name().table_name().getText();
            v.add(tname);
            query.put("table", v);
            SQLiteParser.ExprContext exp=ctx.expr();
            if(exp.OR_()!=null)
            {
                System.out.println("Unsupported operators");
                query = new Hashtable<>();
                return;

            }else if (exp.AND_()!=null){
                getExp(exp.expr().get(0),tname);
                operators.add("AND");
                getExp(exp.expr().get(1),tname);
            }else
            {
                getExp(exp,tname);
            }
        }
    }

    @Override
    public void enterUpdate_stmt(SQLiteParser.Update_stmtContext ctx){

        if (ctx != null) {
            Vector<String> v = new Vector<>();
            v.add("update");
            query.put("stmt", v);
            String tName = ctx.qualified_table_name().table_name().getText();
            v = new Vector<>();
            v.add(tName);
            query.put("table", v);
            v=new Vector<>();
            List<SQLiteParser.Column_name_listContext>l=ctx.column_name_list();
            List<SQLiteParser.Column_nameContext> columns=ctx.column_name();
            List<SQLiteParser.ExprContext> exp=ctx.expr();
            Vector<String>updated=new Vector<>();


            for(SQLiteParser.Column_nameContext col:columns)
            {
                v.add(col.getText());
                updated.add(exp.remove(0).getText());
            }

            query.put("columns",v);
            query.put("updated",updated);
            SQLiteParser.ExprContext e =ctx.expr().get(columns.size());
            if(e.OR_()!=null || e.AND_()!=null) {
                query = new Hashtable<>();
                return;

            }else
            {
                if(!e.getChild(1).getText().equals("="))
                {
                    query = new Hashtable<>();
                    return;
                }
                v=new Vector<>();
                v.add(e.getChild(0).getText());
                query.put("clusteringColum", v);
                v=new Vector<>();
                v.add(e.getChild(2).getText());
                query.put("clusteringValue", v);


            }



        }
    }

    public void enterCreate_index_stmt(SQLiteParser.Create_index_stmtContext ctx) {
        if (ctx != null) {
            Vector<String> v = new Vector<>();
            v.add("create index");
            query.put("stmt", v);
            v = new Vector<>();
            List<SQLiteParser.Indexed_columnContext> idx = ctx.indexed_column();
            for (SQLiteParser.Indexed_columnContext i : idx)
                v.add(i.getText());
            query.put("columns", v);
            v = new Vector<>();
            v.add(ctx.table_name().getText());
            query.put("table", v);
        }
    }

    public void enterCreate_table_stmt(SQLiteParser.Create_table_stmtContext ctx) {
        if (ctx != null) {
            boolean foundPk=false;
            Vector<String> v = new Vector<>();
            v.add("create table");
            query.put("stmt", v);
            v = new Vector<>();
            v.add(ctx.table_name().getText());
            query.put("table", v);
            Vector<String>[] col = new Vector[4];
            for (int i = 0; i < 4; i++) col[i] = new Vector();
            List<SQLiteParser.Column_defContext> columns = ctx.column_def();
            for (SQLiteParser.Column_defContext column : columns) {
                boolean checkConst=false;
                String colName=column.column_name().getText();
                col[0].add(colName);
                col[1].add(column.type_name().getText());
                List<SQLiteParser.Column_constraintContext> constraints = column.column_constraint();
                for (SQLiteParser.Column_constraintContext c : constraints) {
                    if (c.PRIMARY_() != null) {
                        foundPk=true;
                        Vector<String> tmp = new Vector<>();
                        tmp.add(column.column_name().getText());
                        query.put("primary key", tmp);
                    }else if (c.CHECK_() != null) {
                        List<ParseTree> l=c.expr().children;
                        if(l.size()!=5)
                        {
                            System.out.println("Invalid constraint");
                            query = new Hashtable<>();
                            return;
                        }else {

                            if(!l.get(0).getText().equals(colName)||!l.get(1).getText().toLowerCase(Locale.ROOT).equals("between")
                            ||!l.get(3).getText().toLowerCase(Locale.ROOT).equals("and"))
                            {
                                System.out.println("Invalid constraint");
                                query = new Hashtable<>();
                                return;
                            }else {
                                String min=l.get(2).getText();
                                String max=l.get(4).getText();
                                min=((min.length())>1&&
                                        ((min.charAt(0)=='"'&&min.charAt(min.length()-1)=='"')||
                                        (min.charAt(0)==("'").charAt(0)&&min.charAt(min.length()-1)==("'").charAt(0))))?
                                        min.substring(1,min.length()-1):min;
                                max=((max.length())>1&&
                                        ((max.charAt(0)=='"'&&max.charAt(max.length()-1)=='"')||
                                                (max.charAt(0)==("'").charAt(0)&&max.charAt(max.length()-1)==("'").charAt(0))))?
                                        max.substring(1,max.length()-1):max;

                                col[2].add(min);
                                col[3].add(max);
                            }
                        }
                        checkConst=true;
                    } else {
                        System.out.println("Missing column range");
                        query = new Hashtable<>();
                        return;
                    }

                }
                if(!checkConst)
                {
                    System.out.println("You should specify the column range");
                    query=new Hashtable<>();
                    return;
                }


            }
            if(!foundPk){
                System.out.println("You should specify the primary key");
                query=new Hashtable<>();
                return;
            }
            query.put("columns", col[0]);
            query.put("types", col[1]);
            query.put("colMin",col[2]);
            query.put("colMax",col[3]);
        }
    }

    @Override
    public void enterInsert_stmt(SQLiteParser.Insert_stmtContext ctx) {
        if (ctx != null) {
            Vector<String> v = new Vector<>();
            v.add("insert");
            query.put("stmt", v);
            v = new Vector<>();
            v.add(ctx.table_name().getText());
            query.put("table", v);
            v=new Vector<>();
            List<SQLiteParser.Column_nameContext> columns=ctx.column_name();
            for(SQLiteParser.Column_nameContext col:columns)
            {
                v.add(col.getText());
            }
            query.put("columns",v);
            v=new Vector<>();
            List<SQLiteParser.ExprContext> exp=ctx.expr();
            for (SQLiteParser.ExprContext e:exp){
                v.add(e.getText());
            }
            query.put("values",v);
        }
    }



}



