/**
 * Created by damien on 9/13/2015.
 */
package App

import Util.CreateSparkContext
import DAO.{DepartmentDao,EmployeeDao}

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

  private def createDeptRec = {
    dept.createRec(10,"abc")
  }

    /** Gets data from employee and department joins the data output a summary of salaries
      * for each department and then returns a json object of the joined data
       */
  def main = {
    val empData = emp.select("empid,managerid,deptid,firstname,lastname,salary")
    val deptData = dept.select("deptid,department")
    dept.insert(Seq(createDeptRec))
    val empDeptJoin = empData.join(deptData,"deptid")
    println(empDeptJoin.groupBy("department").sum("salary").show)
    val json = empDeptJoin.toJSON.collect
    CreateSparkContext.closeSparkContext
    json
  }
}
