/**
 * Created by damien on 9/13/2015.
 */
package DAO

import org.apache.spark.sql.cassandra.CassandraSQLContext

class EmployeeDao (cc:CassandraSQLContext) extends BaseDAO {
  override protected val tableName = "employee"
  override protected val csc : CassandraSQLContext = cc
}
