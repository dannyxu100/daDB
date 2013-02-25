using System;
using System.Data;
using System.Configuration;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.HtmlControls;
using System.Web.UI.WebControls;
using System.Web.UI.WebControls.WebParts;


//引用
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Text;

namespace daDB
{
    public class DBHelper_SQL
    {

        /// <summary>
        /// 公开属性ErrorMessage，返回错误信息
        /// </summary>
        public string ErrorMessage
        {
            get{return _sError;}
        }
        public string _sError;
        private string _strConnPath;

        //定义全局变量
        private SqlConnection _conn = new SqlConnection();

        /// <summary>
        /// 无参构造函数
        /// </summary>
        public DBHelper_SQL()
        {
            _sError = null;
            _strConnPath = ConfigurationManager.AppSettings["daUser"].ToString();
        }
        /// <summary>
        /// 带数据库链接字符串的 构造函数
        /// </summary>
        public DBHelper_SQL(string sConnName)
        {
            _sError = null;
            if (!Object.Equals(ConfigurationManager.AppSettings[sConnName], null))        //传入webconfig文件AppSettings参数名
            {
                _strConnPath = ConfigurationManager.AppSettings[sConnName].ToString();
            }
            else
            {
                _strConnPath = sConnName;             //直接传入数据库连接字符串
            }
        }

        /// <summary>
        /// 从Web.config中获取连接字符串"
        /// </summary>
        public string GetConnectionString()
        {
            string sConnction = _strConnPath;
            if (sConnction == "")
            {
                _sError = "读取连接字符串失败";
                return null;
            }
            return sConnction;
        }
        /// <summary>
        /// 打开数据库的连接,无参函数.
        /// </summary>
        public bool OpenDB()
        {
            try
            {
                if (_conn.State == ConnectionState.Closed)
                {
                    SqlConnection myconn = new SqlConnection(GetConnectionString());
                    myconn.Open();
                    _conn = myconn;
                    return true;
                }
                else
                {
                    _sError = "打开数据库失败";
                }
                if(_conn.State == ConnectionState.Broken)
                {
                    _conn.Close();
                    _conn.Open();
                    return true;

                }
                else
                {
                    _sError = "打开数据库失败";
                }
                return false;

            }
            catch (Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                return false;
            }
        }
        /// <summary>
        /// 关闭数据库的连接,无参函数.
        /// </summary>
        public bool CloseDB()
        {
            try
            {
                if (_conn.State != ConnectionState.Closed)
                {
                    _conn.Close();
                    return true;
                }
                else
                {
                    _sError = "打开数据库失败";

                }
                return false;
            }
            catch (Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                return false;
            }
        }


        /// <summary>
        /// 执行插入删除修改的sql,只能是insert,update,delete
        /// </summary>
        /// <param name="sql">传入执行insert,update,delete的sql操作语言</param>
        /// <returns>影响行数</returns>
        public int runsql(string sql)
        {
            if (ConnectionState.Closed == _conn.State)
            {
                _sError = "在操作runsql的时候，数据库的连接没有打开";
                return 0;
            }
            SqlCommand comm = _conn.CreateCommand();
            SqlTransaction trans = _conn.BeginTransaction();

            comm.Connection = _conn;
            comm.Transaction = trans;

            int nline = 0;
            string[] arrSQL = sql.Split(';');
            try
            {
                for (int i = 0; i <= arrSQL.Length - 1; i++)
                {
                    if ("" != arrSQL[i].Trim())
                    {
                        comm.CommandText = arrSQL[i].Trim();
                        nline += comm.ExecuteNonQuery();
                    }
                }
                trans.Commit();
                return nline;
            }
            catch (Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                trans.Rollback();
                return 0;
            }
        }
        /// <summary>
        /// 执行插入删除修改的sql,只能是insert,update,delete,
        /// 返回类型:bool
        /// </summary>
        /// <param name="sql">传入要查询的Select语句</param>
        /// <param name="values">SqlParameter[]数组对象</param>
        /// <returns>影响行数</returns>
        public int runsql(string sql, SqlParameter[] values)
        {
            if (ConnectionState.Closed == _conn.State)
            {
                _sError = "在操作runsql的时候，数据库的连接没有打开";
                return 0;
            }

            SqlCommand comm = _conn.CreateCommand();
            SqlTransaction trans = _conn.BeginTransaction();

            comm.Connection = _conn;
            comm.Transaction = trans;

            int nline = 0;
            string[] arrSQL = sql.Split(';');
            try
            {
                for (int i = 0; i <= arrSQL.Length - 1; i++)
                {
                    if ("" != arrSQL[i].Trim())
                    {
                        comm.CommandText = arrSQL[i].Trim();
                        comm.Parameters.AddRange(values);
                        nline += comm.ExecuteNonQuery();
                    }
                }
                trans.Commit();

                return nline;
            }
            catch (Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                return 0;
            }
        }


        /// <summary>
        /// 执行数据库的查询
        /// </summary>
        /// <param name="sql">传入要查询的Select语句</param>
        /// <returns>返回数据集</returns>
        public DataSet getDataSet(string sql)
        {
            if (ConnectionState.Closed == _conn.State)
            {
                _sError = "在操作getDataSet的时候，数据库的连接没有打开";
                return null;
            }
            if (sql == "")
            {
                _sError = "查询语句为空";
                return null;
            }

            try
            {
                SqlDataAdapter adpt = new SqlDataAdapter(sql, _conn);
                DataSet ds = new DataSet();
                adpt.Fill(ds, "ds1");
                return ds;
            }
            catch (Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                return null;

            }

        }
        /// <summary>
        /// 执行数据库的查询
        /// </summary>
        /// <param name="sql">传入要查询的Select语句</param>
        ///  <param name="values">SqlParameter[]数组对象</param>
        /// <returns>返回数据集</returns>
        public DataSet getDataSet(string sql, SqlParameter[] values)
        {
            if (ConnectionState.Closed == _conn.State)
            {
                _sError = "在操作getDataSet的时候，数据库的连接没有打开";
                return null;
            }
            if (sql == "")
            {
                _sError = "查询语句为空";
                return null;
            }
            try
            {
                DataSet ds = new DataSet();
                SqlCommand comm = new SqlCommand(sql, _conn);
                SqlDataAdapter adpt = new SqlDataAdapter();
                comm.Parameters.AddRange(values);
                adpt.SelectCommand = comm;

                adpt.Fill(ds, "ds1");
                return ds;
            }
            catch (Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                return null;
            }
        }
        /// <summary>
        /// 查询分页数据
        /// </summary>
        /// <param name="sql">传入要查询的Select语句</param>
        /// <param name="values">sql参数</param>
        /// <param name="count">是否统计总记录数</param>
        /// <param name="pagesize">分页记录数</param>
        /// <param name="pageindex">第几页</param>
        /// <returns>返回数据集</returns>
        public DataSet getPage(string sql, string order, List<SqlParameter> values, bool count, int pagesize, int pageindex)
        {
            if (ConnectionState.Closed == _conn.State)
            {
                _sError = "在操作getPage的时候，数据库的连接没有打开";
                return null;
            }
            if (sql == "")
            {
                _sError = "查询语句为空";
                return null;
            }

            if (order == "")
            {
                _sError = "分页查询必须要提交order by排序字段";
                return null;
            }

            StringBuilder sqlfix = new StringBuilder();             //包裹后sql代码
            int start = (pageindex - 1) * pagesize;                 //计算当前页记录的起点和终点
            int end = start + pagesize;

            if (sql.LastIndexOf("order by") > 0)                    //去掉尾部order by代码
                sql = sql.Substring(0, sql.LastIndexOf("order by"));

            //sqlfix.Append("set rowcount @pageUpperBound;");
            if (count)                                              //需要统计总记录数
            {
                sqlfix.Append("select count(*) from(" + sql + ") pdt;");
            }

            sqlfix.Append("select * from (");                       //嵌入row_number排序Id

            sqlfix.Append(sql.Substring(0, sql.IndexOf(",")));
            sqlfix.Append(",row_number()over(order by " + order + ") RowId");
            sqlfix.Append(sql.Substring(sql.IndexOf(",")));

            sqlfix.Append(")pgt where RowId>@_PAGESTART and RowId<=@_PAGEEND;");      //通过row_number排序Id和 起点、终点值筛选分页记录

            SqlParameter param = new SqlParameter();                //追加起点和终点参数值
            param.Direction = ParameterDirection.Input;
            param.SqlDbType = SqlDbType.Int;
            param.ParameterName = "@_PAGESTART";
            param.Value = start;
            values.Add(param);

            param = new SqlParameter();
            param.Direction = ParameterDirection.Input;
            param.SqlDbType = SqlDbType.Int;
            param.ParameterName = "@_PAGEEND";
            param.Value = end;
            values.Add(param);

            try
            {
                DataSet ds = new DataSet();
                SqlCommand comm = new SqlCommand(sqlfix.ToString(), _conn);
                SqlDataAdapter adpt = new SqlDataAdapter();
                comm.Parameters.AddRange(values.ToArray());
                adpt.SelectCommand = comm;

                adpt.Fill(ds, "ds1");
                return ds;
            }
            catch (Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                return null;
            }
        }


        /// <summary>
         /// 返回datatable
         /// </summary>
         /// <param name="sql"></param>
         /// <returns></returns>
        public DataTable getTable(string sql)
        {
            if (ConnectionState.Closed == _conn.State)
            {
                _sError = "在操作getTable的时候，数据库的连接没有打开";
                return null;
            }
            if(sql == "")
            {
                _sError = "查询语句为空";
                 return null;
            }
            try
            {
                DataSet ds = new DataSet();
                SqlCommand comm = new SqlCommand(sql, _conn);
                SqlDataAdapter adpt = new SqlDataAdapter(comm);
                adpt.Fill(ds, "ds1");
                return ds.Tables[0];
            }
            catch(Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                return null;
            }
          
         }
        /// <summary>
        /// 返回datatable
        /// </summary>
        /// <param name="sql">select语句</param>
        /// <param name="values">SqlParameter[]数组对象</param>
        /// <returns></returns>
        public DataTable getTable(string sql, SqlParameter[] values)
        {
            if (ConnectionState.Closed == _conn.State)
            {
                _sError = "在操作getTable的时候，数据库的连接没有打开";
                return null;
            }
            if (sql == "")
            {
                _sError = "查询语句为空";
                return null;
            }
            try
            {
                DataSet ds = new DataSet();
                SqlCommand comm = new SqlCommand(sql, _conn);
                SqlDataAdapter dapt = new SqlDataAdapter(comm);

                dapt.Fill(ds, "ds1");
                comm.Parameters.AddRange(values);
                return ds.Tables[0];
            }
            catch (Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                return null;
            }

        }

        /// <summary>
        /// 返回数据集第一个值
        /// </summary>
        /// <param name="sql">select语句</param>
        /// <returns>第一个值</returns>
        public object getObject(string sql)
        {
            if ("Closed" == _conn.State.ToString())
            {
                _sError = "在操作runsql的时候，数据库的连接没有打开";
                return false;
            }
            SqlCommand comm = _conn.CreateCommand();
            SqlTransaction trans = _conn.BeginTransaction();

            comm.Connection = _conn;
            comm.Transaction = trans;

            object obj = null;
            string[] arrSQL = sql.Split(';');
            try
            {
                for (int i = 0; i <= arrSQL.Length - 1; i++)
                {
                    if ("" != arrSQL[i].Trim())
                    {
                        comm.CommandText = arrSQL[i].Trim();
                        obj = comm.ExecuteScalar();
                    }
                }
                trans.Commit();
                return obj;
            }
            catch (Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                trans.Rollback();
                return null;
            }

        }

        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="spname">存储过程名</param>
        /// <param name="values">参数列表</param>
        /// <returns></returns>
        public SqlCommand runsp(string spname, SqlParameter[] values)
        {
            if ("Closed" == _conn.State.ToString())
            {
                _sError = "在操作runsp的时候，数据库的连接没有打开";
                return null;
            }

            SqlCommand comm = _conn.CreateCommand();
            SqlTransaction trans = _conn.BeginTransaction();

            comm.Connection = _conn;
            comm.Transaction = trans;
            comm.CommandType = CommandType.StoredProcedure;

            try
            {
                comm.CommandText = spname;
                comm.Parameters.AddRange(values);
                int tmp = comm.ExecuteNonQuery();
                trans.Commit();

                return comm;
            }
            catch (Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                return null;
            }
        }



        /// <summary>
        /// 获得数据库对象信息
        /// </summary>
        /// <param name="name">对象名</param>
        /// <returns>对象信息数据集</returns>
        public DataSet getDBObject(string name)
        {
            if ("Closed" == _conn.State.ToString())
            {
                _sError = "在操作getDBObject的时候，数据库的连接没有打开";
                return null;
            }

            StringBuilder sqlText = new StringBuilder();
            DataSet ds;

            string objectName = name;
            string usedb = "";

            if (0 < name.LastIndexOf('.'))          //获得引用数据库名。
            {
                usedb = name.Substring(0, name.IndexOf('.'));
                objectName = name.Substring(name.LastIndexOf('.') + 1, name.Length - name.LastIndexOf('.') - 1);
            }

            if ("" != usedb)                        //设置引用数据库。
            {
                sqlText.Append(" use " + usedb + ";");
            }

            sqlText.Append("select syscolumns.name, systypes.name dtype, syscolumns.length,isoutparam,sysobjects.Type tbtype,syscolumns.scale,");
            sqlText.Append("syscolumns.xscale,syscolumns.prec,syscolumns.xprec,");
            sqlText.Append("(case when columnproperty( syscolumns.id,syscolumns.name,'IsIdentity')=1 then '√'else '' end) ident ");
            sqlText.Append("from syscolumns ,systypes ,sysobjects where syscolumns.xusertype=systypes.xusertype  ");
            sqlText.Append("and  syscolumns.id= sysobjects.id  ");
            sqlText.Append("and  sysobjects.name='" + objectName + "' ");
            sqlText.Append("order by colorder;");

            ds = getDataSet(sqlText.ToString());                //获得的对象结构信息。

            if (0 >= ds.Tables[0].Rows.Count) return null;      //没有找到对象信息。

            if ("u" == ds.Tables[0].Rows[0]["tbtype"].ToString().ToLower().Trim())  //表明这是一个数据表，再并入主键信息。
            {
                DataSet dsPK = getDataSet("exec sys.sp_pkeys '" + name + "'");      //获得的表主键信息。

                DataColumn col = new DataColumn();
                col.ColumnName = "pk";
                col.DefaultValue = 1;
                //col.DataType = System.Type.GetType("System.int");
                ds.Tables[0].Columns.Add(col);

                foreach (DataRow row in dsPK.Tables[0].Rows)
                {
                    ds.Tables[0].Select("name='" + row["column_name"] + "'")[0]["pk"] = 0;
                }
            }

            return ds;
        }

        /// <summary>
        /// 查找数据库数据类型枚举
        /// </summary>
        /// <param name="stype">类型名</param>
        /// <returns>数据库数据类型枚举</returns>
        public SqlDbType mapDBType(string stype)
        {
            SqlDbType dbType = SqlDbType.Variant;       //默认为Object

            switch (stype.ToLower())
            {
                case "int":
                    dbType = SqlDbType.Int;
                    break;
                case "varchar":
                    dbType = SqlDbType.VarChar;
                    break;
                case "bit":
                    dbType = SqlDbType.Bit;
                    break;
                case "datetime":
                    dbType = SqlDbType.DateTime;
                    break;
                case "decimal":
                    dbType = SqlDbType.Decimal;
                    break;
                case "float":
                    dbType = SqlDbType.Float;
                    break;
                case "image":
                    dbType = SqlDbType.Image;
                    break;
                case "money":
                    dbType = SqlDbType.Money;
                    break;
                case "ntext":
                    dbType = SqlDbType.NText;
                    break;
                case "nvarchar":
                    dbType = SqlDbType.NVarChar;
                    break;
                case "smalldatetime":
                    dbType = SqlDbType.SmallDateTime;
                    break;
                case "smallint":
                    dbType = SqlDbType.SmallInt;
                    break;
                case "text":
                    dbType = SqlDbType.Text;
                    break;
                case "bigint":
                    dbType = SqlDbType.BigInt;
                    break;
                case "binary":
                    dbType = SqlDbType.Binary;
                    break;
                case "char":
                    dbType = SqlDbType.Char;
                    break;
                case "nchar":
                    dbType = SqlDbType.NChar;
                    break;
                case "numeric":
                    dbType = SqlDbType.Decimal;
                    break;
                case "real":
                    dbType = SqlDbType.Real;
                    break;
                case "smallmoney":
                    dbType = SqlDbType.SmallMoney;
                    break;
                case "sql_variant":
                    dbType = SqlDbType.Variant;
                    break;
                case "timestamp":
                    dbType = SqlDbType.Timestamp;
                    break;
                case "tinyint":
                    dbType = SqlDbType.TinyInt;
                    break;
                case "uniqueidentifier":
                    dbType = SqlDbType.UniqueIdentifier;
                    break;
                case "varbinary":
                    dbType = SqlDbType.VarBinary;
                    break;
                case "xml":
                    dbType = SqlDbType.Xml;
                    break;
            }
            return dbType;
        }

        /// <summary>
        /// 通过数据库对象信息，创建SqlParameter
        /// </summary>
        /// <param name="row">数据库对象信息</param>
        /// <param name="value">参数值</param>
        /// <returns>SqlParameter对象</returns>
        public SqlParameter getSqlParam(DataRow row, string value)
        {
            SqlParameter param = new SqlParameter();

            param.ParameterName = "@" + row["name"].ToString();

            string type = row["dtype"].ToString().Trim().ToLower();
            if (type != "ntext" &&
                type != "text" &&
                type != "uniqueidentifier" &&
                type != "image" &&
                type != "sql_variant" &&
                type != "xml")                             //设置参数长度
            {
                param.Size = Convert.ToInt32(row["length"]);
            }
            param.SqlDbType = mapDBType(type);


            if (value == "" &&
                type != "varchar" &&
                type != "nvarchar" &&
                type != "text" &&
                type != "ntext" &&
                type != "char" &&
                type != "nchar")                           //空数据
            {
                param.Value = System.DBNull.Value;
            }
            else
            {
                param.Value = value;
            }


            if (row["isoutparam"].ToString() == "0")        //存储过程输出参数
            {
                param.Direction = ParameterDirection.Input;
            }
            else                                            //输入参数
            {
                param.Direction = ParameterDirection.Output;
            }

            return param;
        }

        /// <summary>
        /// 获取一个新的流水号
        /// </summary>
        /// <param name="tablename">表名</param>
        /// <returns>新流水号</returns>
        public string getNewPK(string tablename)
        {
            if ("Closed" == _conn.State.ToString())
            {
                _sError = "在操作getNewPK的时候，数据库的连接没有打开";
                return "";
            }

            List<SqlParameter> list = new List<SqlParameter>();
            SqlParameter p = new SqlParameter();
            p.ParameterName = "@tablename";
            p.Value = tablename;
            p.Direction = ParameterDirection.Input;
            p.SqlDbType = SqlDbType.VarChar;
            p.Size = 50;
            list.Add(p);

            p = new SqlParameter();
            p.ParameterName = "@newid";
            p.Direction = ParameterDirection.Output;
            p.SqlDbType = SqlDbType.VarChar;
            p.Size = 20;
            list.Add(p);

            SqlCommand comm = runsp("getNewPK", list.ToArray());

            return comm.Parameters["@newid"].Value.ToString();

        }

    }
}
