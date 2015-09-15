/**
 * Created by damien on 9/13/2015.
 */
package DAO

import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra.CassandraSQLContext

case class empRec (empId     : BigInt,
                   managerId : BigInt,
                   firstName : String,
                   lastName  : String,
                   deptId    : BigInt,
                   salary    : BigInt,
                   hireDate  : String)

class EmployeeDao (sc:SparkContext,
    override protected val csc:CassandraSQLContext)
  extends BaseDAO[empRec] {
  override protected val tableName = "employee"
  override protected val keySpace : String = "test"
  def createRec(empId:BigInt,
                managerId:BigInt,
                firstName:String,
                lastName:String,
                deptid:BigInt,
                salary:BigInt,
                hiredate:String) : empRec = {
    empRec(empId,managerId,firstName,lastName,deptid,salary,hiredate)
  }
}
