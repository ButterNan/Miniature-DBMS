package test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import net.sf.jsqlparser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Table;

public class DbsAss 
{    
	public static List<String> cname=new ArrayList<String>();
	public static Map<String, List<String>> cnam=new HashMap<String, List<String>>();
	public static Map<String, List<String>> cnammeth=new HashMap<String, List<String>>();
	  public static List<String> colname=new ArrayList<String>();
	  public static Map <String, Integer> capturemax=new HashMap<String, Integer>();
	public static void main(String args[])throws JSQLParserException
	{
		Scanner sc= new Scanner(System.in);
		String sqlStatement=sc.nextLine();
      try 
      {  
           // System.out.println("yes");
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
          //  String sqlStatement1 = "SELECT LOCATION_D.REGION_NAME, LOCATION_D.AREA_NAME, COUNT(DISTINCT INCIDENT_FACT.TICKET_ID) FROM LOCATION_D, INCIDENT_FACT WHERE ( LOCATION_D.LOCATION_SK=INCIDENT_FACT.LOCATION_SK ) GROUP BY LOCATION_D.REGION_NAME, LOCATION_D.AREA_NAME HAVING LOCATION_D.AREA_NAME=9 ORDER BY LOCATION_D.AREA_NAME";
           // String sqlStatement = "SELECT ID,REGION_NAME FROM LOCATION_D, INCIDENT_FACT";
            //s=sqlStatement;
            Statement statement = parserManager.parse(new StringReader(sqlStatement));
            Select selectStatement = (Select) statement;
            PlainSelect body = (PlainSelect)selectStatement.getSelectBody();
            Distinct distinct  = body.getDistinct();
            
            
            //System.out.println("dist"+distinct);
           // System.out.println(distinct.getOnSelectItems());
            FromItem fromItem = body.getFromItem();
           //fromItem.toString();
           //fromItem.accept(arg0);
          // System.out.println("I am frm"+fromItem);
            List groupByColumn = body.getGroupByColumnReferences();    //list
            List joins = body.getJoins();       //list
        //    System.out.println("join"+joins);
            List selectItems = body.getSelectItems();        //list
            List orderby=body.getOrderByElements();      //list
            Limit limit = body.getLimit();       //plainselect          
            Expression having=body.getHaving();        //expression    
            //Expression whereExpresions = body.getWhere();     //expression 
            //Class ed=whereExpresions.getClass();
            //System.out.println(ed);
            
            
            Expression whereExpressions = body.getWhere();
            
            //System.out.println("ehr"+whereExpressions);
            wheremeth(whereExpressions);
        /**    if (whereExpressions instanceof Parenthesis) 
            {
                whereExpressions = ((Parenthesis) whereExpressions).getExpression();
              
                if(whereExpressions instanceof EqualsTo)
                {    EqualsTo isEqual=(EqualsTo) whereExpressions;
                	System.out.println("is where left"+isEqual.getLeftExpression());
                	System.out.println(isEqual.getRightExpression());
                }**/
            //  
              // GreaterThanEquals grtthneq=(GreaterThanEquals) whereExpressions;
              
                //System.out.println("where"+whereExpressions);
              //  System.out.println("hehhehe");
            /***    if(whereExpressions instanceof GreaterThan)
                {    GreaterThan grtthn=(GreaterThan) whereExpressions;
                	System.out.println("great");
                	System.out.println(grtthn.getLeftExpression());
                	System.out.println(grtthn.getRightExpression());
                }
                if(whereExpressions instanceof GreaterThanEquals)
                {    GreaterThanEquals grtthneq=(GreaterThanEquals) whereExpressions;
                	
                	System.out.println(grtthneq.getLeftExpression());
                	System.out.println(grtthneq.getRightExpression());
             
                }
                if(whereExpressions instanceof EqualsTo)
                {    EqualsTo isEqual=(EqualsTo) whereExpressions;
                	System.out.println(isEqual.getLeftExpression());
                	System.out.println(isEqual.getRightExpression());
                }
                if(whereExpressions instanceof MinorThan)
                {    MinorThan minorthn=(MinorThan) whereExpressions;
                	
                	System.out.println(minorthn.getLeftExpression());
                	System.out.println(minorthn.getRightExpression());
                }
                if(whereExpressions instanceof MinorThanEquals)
                {    MinorThanEquals minorthneq=(MinorThanEquals) whereExpressions;
                	
                	System.out.println(minorthneq.getLeftExpression());
                	System.out.println(minorthneq.getRightExpression());
                }
                if(whereExpressions instanceof NotEqualsTo)
                {    NotEqualsTo noteq=(NotEqualsTo) whereExpressions;
                	
                	System.out.println(noteq.getLeftExpression());
                	System.out.println(noteq.getRightExpression());
                }
                if(whereExpressions instanceof Matches)
                {    Matches match=(Matches) whereExpressions;
                	
                	System.out.println(match.getLeftExpression());
                	System.out.println(match.getRightExpression());
                }
                if(whereExpressions instanceof Between)
                {    Between betwn=(Between) whereExpressions;
                System.out.println(betwn.getLeftExpression());
                	System.out.println(betwn.getBetweenExpressionStart());
                	System.out.println(betwn.getBetweenExpressionEnd());
                }
                
                if(whereExpressions instanceof LikeExpression)
                {    LikeExpression lyk=(LikeExpression) whereExpressions;
                	
                	System.out.println(lyk.getLeftExpression());
                	System.out.println(lyk.getRightExpression());
                	System.out.println(lyk.getStringExpression());
                }
                if(whereExpressions instanceof ExistsExpression)
                {    ExistsExpression exist=(ExistsExpression) whereExpressions;
                	
                	System.out.println(exist.getRightExpression());
                	
                }
                if(whereExpressions instanceof InExpression)
                {    InExpression in=(InExpression) whereExpressions;
                	
                	System.out.println(in.getLeftExpression());
                	System.out.println(in.getItemsList());
                }
                if(whereExpressions instanceof IsNullExpression)
                {     IsNullExpression is=( IsNullExpression) whereExpressions;
                	
                	System.out.println(is.getLeftExpression());
               
                }
                if(whereExpressions instanceof ItemsList)
                {    ItemsList item=(ItemsList) whereExpressions;
                                	//System.out.println(in.getItemsList());
                }
                if(whereExpressions instanceof ItemsListVisitor)
                {    ItemsListVisitor itemls=(ItemsListVisitor) whereExpressions;
                	
                   }
                
                if(whereExpressions instanceof ExpressionList)
                {    ExpressionList exp=(ExpressionList) whereExpressions;
                	
             
                	System.out.println(exp.getExpressions());
                }
                if(whereExpressions instanceof Addition)
                {    Addition add=(Addition) whereExpressions;        
                	System.out.println(add.getLeftExpression());
                	System.out.println(add.getRightExpression());
                	
                }
                if(whereExpressions instanceof Subtraction)
                {    Subtraction sub=(Subtraction) whereExpressions;        
                	System.out.println(sub.getLeftExpression());  
                	System.out.println(sub.getRightExpression());
                }
                
                if(whereExpressions instanceof Multiplication)
                {    Multiplication mul=(Multiplication) whereExpressions;        
                	System.out.println(mul.getLeftExpression());  
                	System.out.println(mul.getRightExpression());
                }
                if(whereExpressions instanceof Division)
                {    Division div=(Division) whereExpressions;        
                	System.out.println(div.getLeftExpression()); 
                	System.out.println(div.getRightExpression());
                }
                if(whereExpressions instanceof Concat)
                {    Concat conc=(Concat) whereExpressions;        
                	System.out.println(conc.getLeftExpression()); 
                	System.out.println(conc.getRightExpression());
                }
                if(whereExpressions instanceof BitwiseOr)
                {     BitwiseOr bitor=(BitwiseOr) whereExpressions;        
                	System.out.println(bitor.getLeftExpression()); 
                	System.out.println(bitor.getRightExpression());
                }
                if(whereExpressions instanceof BitwiseAnd)
                {     BitwiseAnd bitand=(BitwiseAnd) whereExpressions;        
                	System.out.println(bitand.getLeftExpression()); 
                	System.out.println(bitand.getRightExpression());
                }
                if(whereExpressions instanceof BitwiseXor)
                {     BitwiseXor bitxor=(BitwiseXor) whereExpressions;        
                	System.out.println(bitxor.getLeftExpression()); 
                	System.out.println(bitxor.getRightExpression());
                }
                ***/
              //  System.out.println("where"+whereExpressions);
                
                //		}//if parenthesis
        /**    else
            {
                System.out.println(" no parent where"+whereExpressions);
                
            /***    if(whereExpressions instanceof EqualsTo)
                {    EqualsTo isEqual=(EqualsTo) whereExpressions;
                	System.out.println("where left no per"+isEqual.getLeftExpression());
                	System.out.println(isEqual.getRightExpression().getClass());
                	//wheremeth(isEqual);
                }
            }
            Expression left = null;
            Expression right = null;
            
            if (whereExpressions instanceof AndExpression) {
                left = ((AndExpression) whereExpressions).getLeftExpression();
                right = ((AndExpression) whereExpressions).getLeftExpression();
               // captureleft=where(left);
               // captureright=where(right);
            }
           
            if (whereExpressions instanceof OrExpression) {
                left = ((OrExpression) whereExpressions).getLeftExpression();
                right = ((OrExpression) whereExpressions).getLeftExpression();
            }
            	**/
          
          // System.out.println("wher"+whereExpressions);
           
            Table table=body.getInto(); //table
        
           // System.out.println("wholetable nmE"+table.getWholeTableName());
          //  System.out.println(" nmE"+table.getName());
         //   System.out.println(" schemea nmE"+table.getSchemaName());
       //     System.out.println("aliasnmE"+table.getAlias());
            
            List<String> frm=new ArrayList<String>();
          frm.add(fromItem.toString());
         // if(joins instanceof Expression)
          //{
          
        //  for(int k=0;k<joins.size();k++)
     //     {
       // 	  frm.add(joins.get(k).toString());
        //  }
          //}
         // System.out.println("grrm"+frm);
       //   System.out.println("I m size from"+frm.size()+frm);
          
      /***      for(int i=0; i< frm.size();i++)
            {
            	   frm.add(fromItem.toString());
                   
           System.out.println("frm"+frm.get(i));
            }***/
         //   System.out.println("selectitems"+selectItems);
            
            
  //FILE OPEN
            
            String fileName = "C:\\Users\\NANCY\\Desktop\\metadata.txt";

            // This will reference one line at a time
            String line = null;
            String fm = null;
            
            List <String> meta=new ArrayList<String>();
            BufferedReader bufferedReader=null;
            
            try {
                // FileReader reads text files in the default encoding.
                FileReader fileReader = new FileReader(fileName);

                // Always wrap FileReader in BufferedReader.
              //  Scanner scanner = new Scanner (fileReader);
               bufferedReader =  new BufferedReader(fileReader);
            }
            catch (IOException e) {
                System.out.println("Unable to load file metadata.txt, correct it and re-run.");
                System.exit(1);
            }
              
              try
              {
                while((line = bufferedReader.readLine())!=null)
                {
                   // System.out.println(line);
                  meta.add(line.toString());
               }
  
           
         //  System.out.println("meta");
           
       
                // Always close files.
                bufferedReader.close();            
            }
            catch(FileNotFoundException ex)
            {
                System.out.println(
                    "Unable to open file '" + 
                    fileName + "'");                
            }
            catch(IOException ex) 
            {
                System.out.println(
                    "Error reading file '" 
                    + fileName + "'");                   
                // Or we could just do this: 
                // ex.printStackTrace();
            }
            
           
            HashMap<String,List> l=new HashMap<>();
            List<Integer> dlist=new ArrayList<>();
            
            
            List<String> dlist1=new ArrayList<String>();

            
            	int m=0;
           for(int j=m+1;j<meta.size();j++)
            {
            	if(meta.get(m).equalsIgnoreCase("<begin_table>") && !(meta.get(j).equalsIgnoreCase("<end_table>")) && !((meta.get(m).equalsIgnoreCase("<begin_table>")) && (meta.get(j).equalsIgnoreCase("<begin_table>"))))
            	{
              dlist.add(j+1);
              dlist1.add(meta.get(j).toString());
              //System.out.println(meta.get(m+1)+meta.get(j));
            		
            	}
            	if(meta.get(j).equalsIgnoreCase("<end_table>"))
            	{
            		dlist1.add("endhogya");
            	}
           }
           //System.out.println("dlist"+dlist1);
           List<String> tabname=new ArrayList<String>();
           for(int i=0;i<meta.size();i++)
           {
        	   if(meta.get(i).equalsIgnoreCase("<begin_table>"))
        	   {
        		   String h=meta.get(i+1).toString();
        		  // h=h.replaceAll(">", "");
        		   //h=h.replaceAll("<", "");
        		   tabname.add(h);
        	//	   System.out.println("meta.get(i+1)");
        		//   System.out.println(meta.get(i+1));
        	   }
           }
           
            
       /***     for(int i=0;i<dlist1.size();i++)
            {
            	System.out.println(dlist1.get(i));
            }***/
           
      
           
            int a=0;
            for(int i=1;i<dlist1.size();i++)
            {
            	if(!dlist1.get(i).equalsIgnoreCase("endhogya") && !(dlist1.get(i-1).equalsIgnoreCase("endhogya")) && i>0)
            	{
            		cname.add(dlist1.get(i));
            	}
            else if(dlist1.get(i).equalsIgnoreCase("endhogya"))
            	{
            		cname.add("eng");
            	}
            }
            
            for(int i=0;i<cname.size();i++)
            {
            	//System.out.println(cname.get(i));
            }
        	//System.out.println("cname"+cname); 
        	    	        	      	               
        
        	
        	
           //System.out.println("from"+fromItem.getAlias());
        
            	//	System.out.println(having);
            
            		//System.out.println("having");
       /***     if (having instanceof Column) {
                   Column column = (Column) having;
                   System.out.println(column.getTable() + "," + column.getColumnName());
                   System.out.println("having function");
               } else if (having instanceof Function) {
                   Function fun = (Function)having;
                   System.out.println("Function name : " + fun.getName());
                   System.out.println("Function parameters : " + fun.getParameters());
               }
for(int j=0;j<groupByColumn.size();j++)
{
// Expression expression=((SelectExpressionItem) groupByColumn.get(j)).getExpression();
  //   System.out.println("Expression: " + expression);	
    System.out.println("ju"+groupByColumn.get(j));
    	// Column column = (Column) expression;
      //   System.out.println(column.getTable() + "," + column.getColumnName());
       //  System.out.println("Group by \n");
     
}
          
for(int j=0;j<orderby.size();j++)
{
	  System.out.println("oeder"+groupByColumn.get(j));
}***/
for(int i=0;i<colname.size();i++)
{

	//System.out.println(colname.get(i));
}	
//System.out.println("jdh");
List<String> cname1=new ArrayList<>();
for(int i=0;i<cname.size();i++)
{  String c="";
    
    c=cname.get(i).replaceAll("<","");
    c=c.replaceAll(">","");
    cname1.add(c);
	//System.out.println(cname1.get(i));
	
}	

for(int i=0;i<cname1.size();i++)
{

	//System.out.println(cname1.get(i));
}	
for(int i=0;i<tabname.size();i++)
{  

//	System.out.println("tabname"+tabname.get(i));
}


int count=0;

List<String> coln=new ArrayList<String>();
//System.out.println("cname1"+cname1.size());
for(int i=0;i<cname1.size();i++)
{
 if(!cname1.get(i).equalsIgnoreCase("eng"))
 {
	 
	 coln.add(cname1.get(i));
	// System.out.println(coln.get(i));
	 
 }
 else
 {String h=tabname.get(count).toString();
// String h1=h.replace("<", "");
 //h1=h1.replace(">", "");
 //System.out.println("ghhh");
	  cnam.put(h, coln);
	//System.out.println(cnam);
	 count=count+1;
	coln=new ArrayList<String>();
	 

 }
 
}
//System.out.println("cnam"+cnam);
//System.out.println(cnam.size());
cnammeth=cnam;
           
//System.out.println("colnam");
//System.out.println(tabname);
//System.out.println(frm);
         /***   for(int i=0;i<frm.size();i++)
            { for(int k=0;k<tabname.size();k++)
            {
      //      	System.out.println(tabname.get(k));
        //    	System.out.println(frm.get(i));
            	if(tabname.get(k).toString().equalsIgnoreCase(frm.get(i).toString())==false);
            	{  //System.out.println(tabname.contains(frm.get(i).toString()));
            		System.out.println("error\n");
            		
            		//System.exit(1);
            		break;
            	}
            }
            }**/
        int flgcount=0;
        int flg=0;
        int flgsum=0;
        int flgavg=0;
        int flgmax=0;
        int starcount=0;
        int flgmin=0;
        int flgdis=0;
        int flgevrythng=0;
        int flagcom=0;
          for(int i=0;i<selectItems.size();i++)
          {
          	
        
  //         System.out.println("Expression: " + selectItems.get(i).getClass());
           //Statement st=(Statement)selectItems.get(0);
        	        // Expression  expression=(Expression) selectItems.get(i);
        // Expression expression=((SelectExpression) selectItems.get(i)).getExpression();
        	 if(selectItems.get(i) instanceof SelectExpressionItem)
        	 {
        		// System.out.println("ERROR");
        	  Expression expression=((SelectExpressionItem) selectItems.get(i)).getExpression();
        	 // System.out.println("CLASS"+expression.getClass());
        	  if (expression instanceof Column) {
                  Column column = (Column) expression;
            //      System.out.println(column.getTable() + "," + column.getColumnName());
                  colname.add(column.getColumnName().toString());
                  
                //  System.out.println("col"+column.getColumnName());
                
              } 
        	  
        	  else if(expression instanceof Parenthesis)
        	  {
    //    		  System.out.println("PAREN inside");
        		  Parenthesis par=(Parenthesis)expression;
      //  		  System.out.println("EXP"+par.getExpression());
        		  colname.add(par.getExpression().toString());
        		  flgdis=1;
        	  }
        	  else if (expression instanceof Function) {
                  Function fun = (Function)expression;
        //         System.out.println("Function name : " + fun.getName());
          //       System.out.println("Function parameters : " + fun.getParameters());
                //  colname.add(fun.getParameters().toString());
                 // String f=fun.getParameters().toString().replace("(","");
                 // f=f.replace(")", "");
           //       System.out.println("fff"+f);
              //    String[] f1=f.split(".");
                //System.out.println("func"+fun.getName());
                  if(fun.getName().equalsIgnoreCase("DISTINCT"))
                  {
            //     	 System.out.println("distn");
                 	flgdis=1; 
                  }
                  if(fun.getName().equalsIgnoreCase("COUNT"))
                  {
                 	flg=1; 
              //   	System.out.println("count");
                 	//System.out.println(fun.getParameters().toString());
                 	if(fun.getParameters()==null)
                 	{
                // 		System.out.println("NULL");
                 		starcount=1;
                 		flagcom=1;
                 	}
                 	else
                 	{
                 //	System.out.println("CLAss"+fun.getParameters().getClass());
                 	String h=fun.getParameters().toString().replace("(","");
                 	h=h.replace(")","");
                 	
                 	colname.add(h);
                 	}
                  }
                  if(fun.getName().equalsIgnoreCase("AVG"))
                  {
                 	flg=2; 
                 	//System.out.println("avg");
                 	//System.out.println(fun.getParameters().toString());
                 	String h=fun.getParameters().toString().replace("(","");
                 	h=h.replace(")","");
                 	capturemax=meth(flgmax,h);
                 	colname.add(h);
                  }
                  if(fun.getName().equalsIgnoreCase("MAX"))
                  {
                 	flg=3; 
                 	//System.out.println("max");
                 	//System.out.println(fun.getParameters().toString());
                 	String h=fun.getParameters().toString().replace("(","");
                 	h=h.replace(")","");
                 	//meth(flgmax,h);
                 	capturemax=meth(flgmax,h);
                 	//System.out.println("capturemax="+capturemax+meth(flgmax,h));
                 	colname.add(h);
                  }
                  if(fun.getName().equalsIgnoreCase("MIN"))
                  {
                 	flg=4; 
                 	//System.out.println("min");
                 	//System.out.println(fun.getParameters().toString());
                 	String h=fun.getParameters().toString().replace("(","");
                 	h=h.replace(")","");
                 	capturemax=meth(flgmax,h);
                 	colname.add(h);
                  }
                
                  if(fun.getName().equalsIgnoreCase("SUM"))
                  {
                 	flg=5; 
                 	//System.out.println("sum");
                 	//System.out.println(fun.getParameters().toString());
                 	String h=fun.getParameters().toString().replace("(","");
                 	h=h.replace(")","");
                 	capturemax=meth(flgmax,h);
                 	colname.add(h);
                  }
                  
                  //System.out.println("fff"+f1);
              }
       
        	  
        	  
        	  
        	 }
        	 else
        	 {
        	  AllColumns expression = (AllColumns)selectItems.get(i);
        	  //System.out.println("alll"+expression.toString());
        	  flgevrythng=1;
        	  //colname.add(expression.toString());
        	  //System.out.println("girdl hu"+flgevrythng);
        	  flagcom=1;
        	 }
        	
        	/**  if (expression instanceof Column) {
                 Column column = (Column) expression;
           //      System.out.println(column.getTable() + "," + column.getColumnName());
                 colname.add(column.getColumnName().toString());
                 
                // System.out.println("col"+column.getColumnName());
               
             } **/
        	 
        	  
        	  
        	  
        	 
             
          }
          
          
         // System.out.println("colnm size"+colname.size());
          for(int j=0;j<colname.size();j++)
          {
       //	  System.out.println("I m colname"+colname.get(j));
          }
        
        //  System.out.println("from dat"+colname);
        
//System.out.println("cnammethkkkkk"+cnammeth);
Set setkey=cnam.keySet();
Iterator iter=setkey.iterator();
Map<String,List<Integer>> tableindex=new HashMap<String,List<Integer>>();
List<Integer> ls=new ArrayList<Integer>();
while(iter.hasNext())
{
	String key=(String)iter.next();
	List<String> val=new ArrayList<String>();
	
	val.addAll(cnam.get(key));
	//System.out.println(key+val);
	
		for(int k=0;k<colname.size();k++)
		{
			if(val.contains(colname.get(k)))
			{ int d=val.indexOf(colname.get(k));
			ls.add(d);
				tableindex.put(key,ls);
			//	System.out.println(key+val.indexOf(colname.get(k)));
			}
		}
		ls=new ArrayList<Integer>();
		 
}
//System.out.println("table name and col name needed"+tableindex);
for(int k=0;k<frm.size();k++)
{
//	System.out.println("I AM FROM "+frm.get(k));
}
//data read
//System.out.println("Tab"+tabname);
//  System.out.println("c"+cnam);
  //System.out.println("frm"+frm);



//for(int j=0;j<frm.size();j++)
//{ 
	int flag=tableindex.size();
	//System.out.println("table index"+tableindex);
	
	Set setkey1=tableindex.keySet();
	Iterator iter1=setkey1.iterator();
	int coun=0;
	int sumn=0;
	int maxn=0;
	int minn=-0;
	int avgn=0;
	
	if(flagcom==1)
	{
		String fname=frm.get(0);
		//System.out.println(fname);
		String csvFile = "C:\\Users\\NANCY\\Desktop\\"+fname+".csv";
		//System.out.println("File name * ka"+csvFile);
		BufferedReader br = null;
		int flgstarcount=0;
		try 
		{
		br = new BufferedReader(new FileReader(csvFile));
		while ((line = br.readLine()) != null)
		  {
			// System.out.println("girdl hu file ke"+flgevrythng);
		        // use comma as separator
			 String cvsSplitBy = ",";
			String[] country = line.split(cvsSplitBy);
			if(flgevrythng==1)
			{
			System.out.println("line"+line);
			}
			if(starcount==1)
			{
				flgstarcount=flgstarcount+1;
			}
			
	      }
		
		if(starcount==1)
		{
			System.out.println(flgstarcount);
		}
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		} 
		catch (IOException e)
		{
			e.printStackTrace();
		}  
		finally 
		{
			if (br != null) 
			{
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
			}
		}

		}
	}
	String str=selectItems.toString();
System.out.println(str.replace("[","").replace("]",""));	
while(iter1.hasNext())
{
	String key=(String)iter1.next();     //table name
	String fname=key;
	//System.out.println(fname);
	String csvFile = "C:\\Users\\NANCY\\Desktop\\"+fname+".csv";
	//System.out.println("File name"+csvFile);
	BufferedReader br = null;
	String g="";
	List<Integer> listdis=new ArrayList<Integer>();
	String cvsSplitBy = ",";
	int maxstore1=0;
	int maxstore=0;
	int minstore1=10000000;
	int minstore=0;
	int sumc=0;
	int avgc=0;
	int flagavg=0;
	int countavg=0;
	int sumavgc=0;
	int flgcount1=0;
	int flagsum=0;
	int sum1=0;
	int fmax=0;
	int fmin=0;
	int countdist=-1;
	int flgfor=0;
	try 
	{
	br = new BufferedReader(new FileReader(csvFile));
	while ((line = br.readLine()) != null)
	  {  countdist=countdist+1;
		 //System.out.println("girdl hu file ke"+flgevrythng);
	        // use comma as separator
		String[] country = line.split(cvsSplitBy);
		//System.out.println("line"+line);
		//System.out.println("ans"+country[val.get(i)]);
       //    System.out.println("Key is table name"+key);
          // System.out.println((List<Integer>)tableindex.values().toArray()[0]);
			//if(key.equalsIgnoreCase(fname))
			//{ //System.out.println("Key"+key);
			//System.out.println("fname"+fname);
				List<Integer> val=new ArrayList<Integer>();
				//System.out.println("###############"+(tableindex.get(key)));
				//System.out.println(key);
				String vl=tableindex.get(key).toString();
				val.addAll(tableindex.get(key));
				//System.out.println(key+val+country.length);
				String finl="";
				String finlstr="";
				
				
				for(int j=0;j<country.length;j++)
				{// System.out.println("enty ho plzzz"+flgdis);
				
					
					
					if(flgdis==1)        //disticnt
					{   //System.out.println("DIST ENTRY POINT");
					   
					 //System.out.println("value is"+val);
						 if(j==val.get(0))
					      {  
							/** if(countdist==0)
					        {
							  System.out.println("DIST ENTRY POINT1");
						listdis.add(Integer.parseInt(country[j]));
					       }
					   
					    else
					   {**/
					    	//System.out.println("TRUE"+listdis.contains(Integer.parseInt(country[j])));
						if(listdis.contains(Integer.parseInt(country[j]))==false);
						 {  //System.out.println(Integer.parseInt(country[j]));
							 
							  for(int i=0;i<listdis.size();i++)
							  {    
								  if(listdis.get(i)!=Integer.parseInt(country[j]));
								  {
									  listdis.add(Integer.parseInt(country[j]));
								//	  System.out.println("DIST ENTRY POINT2");
								  }
							  }
							//listdis.add(Integer.parseInt(country[j]));
						}
						 //System.out.println("List"+listdis);
						// System.out.println("HARD"+listdis.contains(2));
				//	}//else
					    }//if
					
					}//if 
					 if(flg==2)   //avg
						{
							//System.out.println("#####file avg entry");
							//System.out.println("#####index avg"+findmeth(fname)+"i"+j);
							if(j==findmeth(fname))
							{
								
									avgc=Integer.parseInt(country[j]);
								//	System.out.println("####avg hai ye"+avgc);
								//System.out.println(country[j].toString());
							//System.out.println("####ye kro avg");	
							flagavg=1;
							//countavg=countavg+1;
							//System.out.println("new store"+maxstore1);
							}
						}
							if(flg==4)   //min
							{
								//System.out.println("file min entry");
								//findmeth(fname);
								if(j==findmeth(fname))
								{
									
										minstore=Integer.parseInt(country[j]);
									//	System.out.println("min dekho toh"+minstore);
								//	System.out.println(country[j].toString());
							//	System.out.println("ye kro min");	
								fmin=1;
								//System.out.println("new store"+maxstore1);
								}
								
							}
							if(flg==3) //max
							{
								//System.out.println("file max entry");
								//findmeth(fname);
								if(j==findmeth(fname))
								{
									
										maxstore=Integer.parseInt(country[j]);
										//System.out.println("max dekho toh"+maxstore);
									//System.out.println(country[j].toString());
								//System.out.println("ye kro max");	
								fmax=1;
								//System.out.println("new store"+maxstore1);
								}
								
							}
							
							 if(flg==1)   //count
								{
									//System.out.println("#####file count entry");
								//	System.out.println("#####index count"+findmeth(fname)+"i"+j);
									if(j==findmeth(fname))
									{
										
										//	countc=Integer.parseInt(country[j]);
											//System.out.println("####count hai ye"+avgc);
										//System.out.println(country[j].toString());
									flgcount1=1;
									
									//countavg=countavg+1;
									//System.out.println("new store"+maxstore1);
									}
								}
							 
							 if(flg==5)   //sum
								{
									//System.out.println("#####file sum entry");
									//System.out.println("#####index sum"+findmeth(fname)+"i"+j);
									if(j==findmeth(fname))
									{
										
											sumc=Integer.parseInt(country[j]);
										//	System.out.println("####avg hai ye"+sumc);
									///	System.out.println(country[j].toString());
								//	System.out.println("####ye kro kro");	
									flgsum=1;
									//countavg=countavg+1;
									//System.out.println("new store"+maxstore1);
									}
								}
							// System.out.println("evrythignnfnfnf"+flgevrythng);
							 if(flgevrythng==1)
							 {
								// System.out.println("evrythng *");
								 finlstr=finlstr+","+country[j].toString(); 
							 }
							 else
							 {//System.out.println("pppp");
								 for(int k=0;k<val.size();k++)
								 {
								 //System.out.println("value is"+val);
									if(j==val.get(k))
								    {
				                      // System.out.println("i"+val.get(k)+"j"+j);
									finl=finl+","+country[val.get(k)].toString();
								    }
								 if(k==0)
								{
									finl=country[val.get(k)].toString();
								}
								 
							 }
								 flgfor=1;
							 }
							
							/**	else
								{
									finl=finl+","+country[val.get(i)].toString();
								}**/
								 
							
						} //for country
		
				
					if(flgfor==1)
					{
						   
						//System.out.println(fname);
				 System.out.println(finl);
					}
		
			
    		
	   if(flgcount1==1)
		{
//		System.out.println("ans"+country[val.get(i)]);
		//System.out.println("count entry");
		coun=coun+1;
		}
	   
	
		
		 if(flgsum==1)   //sum
		{
			sum1=sumc+sum1;
		}
		 if(flagavg==1)
		 {
			 countavg=countavg+1;  //for n
			 sumavgc=sumavgc+avgc;
		//	 System.out.println("entry ho gya yieeee");
			 
		 }
	   if(maxstore1<maxstore)
		{
		maxstore1=maxstore;
		//System.out.println("india hai"+maxstore1);
		}
	//	System.out.println("max aayayaa re"+maxstore1);   
	   
		if(minstore1>minstore)
		{
			minstore1=minstore;
			//System.out.println("minscxz"+minstore1);
		}
		//System.out.println("COUNTDIST"+countdist);
	  }//while for read line
	//System.out.println("DISITCINT"+listdis);
	if(flgsum==1)
	{
		System.out.println("sum hai"+sum1);
	}
	
	if(flagavg==1)
	{
	System.out.println("average nikal gya"+sumavgc/countavg);
	}
	if(flgcount1==1)
	{
		System.out.println("count"+coun);
	}
	if(fmax==1)
	{
		System.out.println("Max aa gya hoga shayad"+maxstore1);
	}
	if(fmin==1)
	{
		System.out.println("Min aa gya hoga shayad"+minstore1);
	}
	flag=flag-1;
  coun=0;
   } //try
catch (FileNotFoundException e)
{
	e.printStackTrace();
} 
catch (IOException e)
{
	e.printStackTrace();
}  
finally 
{
	if (br != null) 
	{
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
	}
}

}
	
//System.out.println("Done");
}






    
        } catch (JSQLParserException e) {
            // TODO Auto-generated catch block
        	//
           // e.printStackTrace();
            System.out.println("JSQLerror\n");   
        }
    
	     
        }
	

	public static  Map<String, Integer> meth(int flag,String fun)
	{ int ct=0;
	int found=0;
	//System.out.println("Meth entry");
	Map<String,Integer> tableindex=new HashMap<String,Integer>();
	List<Integer> ls=new ArrayList<Integer>();
	Set setkey=cnammeth.keySet();
	
	//System.out.println("colname meth"+colname);
	Iterator iter=setkey.iterator();
	
	while(iter.hasNext())
	{
		String key=(String)iter.next();     //table name
		List<String> val=new ArrayList<String>();
		
		val.addAll(cnam.get(key));
	//	System.out.println(key+val);
		//System.out.println("fun"+fun);
		if(val.contains(fun))
		{
			//System.out.println("enrty");
			val.indexOf(fun);
			//System.out.println("index"+val.indexOf(fun));
			int d=val.indexOf(fun);
			  
			tableindex.put(key,d);
		}
			
			//	System.out.println("tablendmm"+tableindex);
				
		//return tableindex;	
	}
	//System.out.println("Table index for meth final"+tableindex);
	if(flag==1)
	{
		//System.out.println("count");
		//findmeth(tableindex);
	}
    if(flag==2)
    {
    	//System.out.println("avg");
    }
    if(flag==3)
    {
    	//System.out.println("max");
    }
    if(flag==4)
    {
    	//System.out.println("min");
    }
    if(flag==5)
    {
    	//System.out.println("sum");
    }
	return tableindex;
	}//method meth

	public static int findmeth(String fname)
	{
		//System.out.println("Entry into findmeth"+fname);
	//System.out.println("max ka captur"+capturemax);
		Set setkeymax=capturemax.keySet();
		
		Iterator itermax=setkeymax.iterator();
		
		int j=0;
		while(itermax.hasNext())
		{
			String key=(String)itermax.next();     //table name
			List<String> val=new ArrayList<String>();
			 j=capturemax.get(key);
		//	System.out.println("capture val hai"+capturemax.get(key));
			//val.add(capturemax.get(key));
			//System.out.println(key+"index hai ye"+j);
			//System.out.println("fun"+fun);
			/***if(val.contains(fun))
			{
				System.out.println("enrty");
				val.indexOf(fun);
				System.out.println("index"+val.indexOf(fun));
				int d=val.indexOf(fun);
				  
				tableindex.put(key,d);
			}***/
		
				}
		return j;
}
	
	public static void wheremeth(Expression whereExpressions)
	{
		//System.out.println("entry of method where"+whereExpressions);
		   Expression left = null;
		   Expression newwhere=null;
	          Expression right = null;
	          String c="";
		  if (whereExpressions instanceof Parenthesis) 
          {
              //whereExpressions = ((Parenthesis) whereExpressions).getExpression();
           ///  System.out.println(((Parenthesis) whereExpressions).getExpression());  
             newwhere=((Parenthesis) whereExpressions).getExpression();
            
          //System.out.println("left "+((Parenthesis) whereExpressions).getExpression());
           
          if(newwhere instanceof EqualsTo)
           {    
            	//EqualsTo isEqual=(EqualsTo) whereExpressions;
            	//System.out.println("is where left"+newwhere.getLeftExpression());
          //	System.out.println(isEqual.getRightExpression());
           }
     
         if(newwhere instanceof GreaterThan)
          {    GreaterThan grtthn=(GreaterThan) whereExpressions;
          	System.out.println("great");
          	System.out.println(grtthn.getLeftExpression());
          	System.out.println(grtthn.getRightExpression());
          }
          if(newwhere instanceof GreaterThanEquals)
          {    GreaterThanEquals grtthneq=(GreaterThanEquals) whereExpressions;
          	
          	System.out.println(grtthneq.getLeftExpression());
          	System.out.println(grtthneq.getRightExpression());
       
          }
          if(newwhere instanceof EqualsTo)
          {    EqualsTo isEqual=(EqualsTo) whereExpressions;
          	//System.out.println(isEqual.getLeftExpression());
          	//System.out.println(isEqual.getRightExpression());
          }
          if(newwhere instanceof MinorThan)
          {    MinorThan minorthn=(MinorThan) whereExpressions;
          	
          	System.out.println(minorthn.getLeftExpression());
          	System.out.println(minorthn.getRightExpression());
          }
          if(newwhere instanceof MinorThanEquals)
          {    MinorThanEquals minorthneq=(MinorThanEquals) whereExpressions;
          	
          	System.out.println(minorthneq.getLeftExpression());
          	System.out.println(minorthneq.getRightExpression());
          }
          if(newwhere instanceof NotEqualsTo)
          {    NotEqualsTo noteq=(NotEqualsTo) whereExpressions;
          	
          	System.out.println(noteq.getLeftExpression());
          	System.out.println(noteq.getRightExpression());
          }
        
          if (newwhere instanceof AndExpression) {
              left = ((AndExpression) whereExpressions).getLeftExpression();
              right = ((AndExpression) whereExpressions).getRightExpression();
          } if (newwhere instanceof OrExpression) {
              left = ((OrExpression) whereExpressions).getLeftExpression();
              right = ((OrExpression) whereExpressions).getRightExpression();
          }
          
          
          
          }
		  
		  if(!(whereExpressions instanceof Parenthesis)) 
		  {// System.out.println("NO Parenthesis");  
		   // left=((Parenthesis) whereExpressions).getExpression();
          //right=((Parenthesis) whereExpressions).getExpression();
 		  
		   if(whereExpressions instanceof EqualsTo)
              {    EqualsTo isEqual=(EqualsTo) whereExpressions;
              	//System.out.println("is where left"+isEqual.getLeftExpression());
              	//System.out.println(isEqual.getRightExpression());
              	c=isEqual.getLeftExpression().toString()+","+"="+","+isEqual.getRightExpression().toString();
              
              }
         
             if(whereExpressions instanceof GreaterThan)
              {    GreaterThan grtthn=(GreaterThan) whereExpressions;
              	System.out.println("great");
              	System.out.println(grtthn.getLeftExpression());
              	System.out.println(grtthn.getRightExpression());
              }
              if(whereExpressions instanceof GreaterThanEquals)
              {    GreaterThanEquals grtthneq=(GreaterThanEquals) whereExpressions;
              System.out.println("greatthn");
              	System.out.println(grtthneq.getLeftExpression());
              	System.out.println(grtthneq.getRightExpression());
           
              }
            
              if(whereExpressions instanceof MinorThan)
              {    MinorThan minorthn=(MinorThan) whereExpressions;
              	
              	System.out.println(minorthn.getLeftExpression());
              	System.out.println(minorthn.getRightExpression());
              }
              if(whereExpressions instanceof MinorThanEquals)
              {    MinorThanEquals minorthneq=(MinorThanEquals) whereExpressions;
              	
              	System.out.println(minorthneq.getLeftExpression());
              	System.out.println(minorthneq.getRightExpression());
              }
              if(whereExpressions instanceof NotEqualsTo)
              {    NotEqualsTo noteq=(NotEqualsTo) whereExpressions;
              	
              	System.out.println(noteq.getLeftExpression());
              	System.out.println(noteq.getRightExpression());
              }
              if (whereExpressions instanceof AndExpression) {
                  left = ((AndExpression) whereExpressions).getLeftExpression();
                  right = ((AndExpression) whereExpressions).getRightExpression();
              } if (whereExpressions instanceof OrExpression) {
                  left = ((OrExpression) whereExpressions).getLeftExpression();
                  right = ((OrExpression) whereExpressions).getRightExpression();
              }
            //  System.out.println("where"+whereExpressions);
              
              		}//if parenthesis
         
          }
       
          
        
	   
    
}