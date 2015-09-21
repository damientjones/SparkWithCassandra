/**
 * Created by damien on 9/13/2015.
 */
package App

import Util.CreateSparkContext
import DAO.{DepartmentDao,EmployeeDao}
import com.datastax.spark.connector.cql.CassandraConnector

class SparkCassandraTest {
  //Constructor setting up cassandra connection
  val url : String = "127.0.0.1"
  val userName : String = "cassandra"
  val password : String = "cassandra"
  val appName : String = "testApp"
  val master : String = "local[2]"
  CreateSparkContext.CreateContext(url,userName,password,appName,master)
  val sc = CreateSparkContext.getSparkContext
  val csc = CreateSparkContext.getCassandraContext
  csc.setCluster("Test Cluster")
  csc.setKeyspace("test")
  val emp = new EmployeeDao(sc,csc)
  val dept = new DepartmentDao(sc,csc)
  val empCols = Array("empid","managerid","deptid","firstname","lastname","salary")
  val deptCols = Array("deptid","department")
  val deptWhere = Array(Array("AND","department","=","'IT'","1","1"),Array("OR","department","=","'HR'","1","2"))
  /* Delete extra record */
  CassandraConnector(CreateSparkContext.getConf).withSessionDo { session =>
    session.execute("DELETE FROM test.department WHERE deptid = 10;")
  }

  private def createDeptRec = {
    dept.createRec(10,"abc")
  }

  def main = {
    val empData = emp.select(empCols).cache
    val deptData = dept.select(deptCols)
    // use where clause
    val itDeptData = dept.select(deptCols,deptWhere)
    itDeptData.foreach(println)
    /* Show count prior to insert */
    println("Department has " + deptData.count + " records")
    dept.insert(Seq(createDeptRec))
    deptData.cache
    /* Show count after insert */
    println("Department now has " + deptData.count + " records")
    val empDeptJoin = empData.join(deptData,"deptid").cache
    println(empDeptJoin.groupBy("department").sum("salary").show)
    val json = empDeptJoin.toJSON.collect
    CreateSparkContext.closeSparkContext
    json
  }
}
