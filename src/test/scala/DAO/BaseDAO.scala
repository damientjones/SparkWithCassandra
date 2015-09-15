/**
 * Created by damien on 9/13/2015.
 */
package DAO

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra.CassandraSQLContext

trait BaseDAO[T] {
  protected val tableName : String = ""
  protected val keySpace : String = ""
  private var sql : String = ""
  private var selectCols : String = ""
  private var whereClause : String = ""
  protected val csc : CassandraSQLContext = null

  /** Returns dataframe based on select statement **/
  private def getData : DataFrame = {
    csc.cassandraSql(sql)
  }
  /** String comma delimited list of columns without select keyword **/
  private def setSelect(cols:String) {
    this.selectCols = "SELECT " + cols
  }
  /** Complete where clause without where keyword **/
  private def setWhere(where:String) {
    if (where == null){
      this.whereClause = ""
    }else {
      this.whereClause = "WHERE " + where
    }
  }
  /** Creates sql statement from select and where clauses using table name **/
  private def createSql {
    sql = selectCols + " FROM " + tableName + " " + whereClause
  }
  def select(cols:String,where:String) : DataFrame = {
    setSelect(cols)
    setWhere(where)
    createSql
    getData
  }
  def select(cols:String) : DataFrame = {
    select(cols,null)
  }
}
