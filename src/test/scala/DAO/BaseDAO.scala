/**
 * Created by damien on 9/13/2015.
 */
package DAO

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra.CassandraSQLContext

/**
 * All functions required to select data from a table
 * Generic type T is case class for the cassandra record type
 * @tparam T
 */
trait BaseDAO[T] {
  protected val tableName : String = ""
  protected val keySpace : String = ""
  protected val csc : CassandraSQLContext = null
  private val newLine = sys.props("line.separator")

  /** Returns dataframe based on select statement **/
  private def getData(sql:String) : DataFrame = {
    csc.cassandraSql(sql)
  }

  /**
   * Input array of string is the list of columns to select
   * Will return a comma delimited string of columns with SELECT prepended
   * @param cols
   * @return
   */
  private def getSelectClause(cols:Array[String]) : String = {
    "SELECT " + cols.mkString(",")
  }

  /**
   * Will generate from clause from tableName field
   * @return
   */
  private def getFromClause : String = {
    newLine + "FROM " + tableName
  }

  /**
   * Will generate a where clause line from input Array of string
   * @param where
   * @return
   */
  private def getWhereClauseLine(where:Array[String]) : String = {
    val (cond,string,grp,grpOrd) = (where(0)+ " ",where(1)+where(2)+where(3),where(4).toInt,where(5).toInt)
    if (grp == 1 && grpOrd==1 ) {
      newLine + "WHERE " + "(" + string
    } else if (grp != 1 && grpOrd == 1) {
      ")"+ cond + "(" + string
    } else if (grpOrd != 1) {
      " " + cond + string
    } else {
      ""
    }
  }

  /**
   * Will output a full where clause from an input array of array of strings
   * @param where input array takes the form of:
   *              1) AND/OR condition
   *              2) column
   *              3) comparator (=,>,<, ect)
   *              4) value
   *              5) grouping denotes grouped conditions should have an ascending value starting with 1
   *              6) order of filter in a group should have ascending value starting with 1
   * @return
   */
  private def getWhereClause(where:Array[Array[String]]) : String = {
    val output = where.map(getWhereClauseLine).mkString
    if (output.size > 0) {
      output + ")"
    } else {
      output
    }
  }

  /**
   * Will create a select statement from an input array of cols and
   * an input array of array of strings which denotes a where clause with filters
   * @param cols
   * @param where
   * @return
   */
  private def createSql(cols:Array[String],where:Array[Array[String]]) : String = {
    getSelectClause(cols) + getFromClause + getWhereClause(where)
  }

  /**
   * Outputs a dataframe based on an input array of cols and
   * an input array of array of strings which denotes a where clause with filters
   * @param cols
   * @param where
   * @return
   */
  def select(cols:Array[String],where:Array[Array[String]]) : DataFrame = {
    getData(createSql(cols,where))
  }

  /**
   * Outputs a dataframe based on an input array of cols
   * @param cols
   * @return
   */
  def select(cols:Array[String]) : DataFrame = {
    val where : Array[Array[String]] = Array.empty
    select(cols,where)
  }
}
