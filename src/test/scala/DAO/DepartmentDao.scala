/**
 * Created by damien on 9/13/2015.
 */
package DAO

import org.apache.spark.sql.cassandra.CassandraSQLContext

class DepartmentDao (cc:CassandraSQLContext) extends BaseDAO {
  override protected val tableName = "department"
  override protected val csc : CassandraSQLContext = cc
}
