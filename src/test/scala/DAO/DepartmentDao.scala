/**
 * Created by damien on 9/13/2015.
 */
package DAO

import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra.CassandraSQLContext
import com.datastax.spark.connector._

case class deptRec (deptid     : BigInt,
                    department : String)

class DepartmentDao (sc:SparkContext,
    override protected val csc:CassandraSQLContext)
  extends BaseDAO[deptRec] {
  override protected val tableName : String = "department"
  override protected val keySpace : String = "test"
  private val columns = SomeColumns("deptid","department")
  def createRec(deptid:BigInt,department:String) : deptRec = {
    deptRec(deptid,department)
  }
  def insert(recList:Seq[deptRec]){
    val recs = sc.parallelize(recList)
    recs.saveToCassandra(keySpace,tableName,columns)
  }
}
