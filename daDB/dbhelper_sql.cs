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
        /// 执行插入删除修改的sql,只能是insert,update,delete,
        /// 参数为string数据类型
        /// </summary>
        /// <param name="sql">传入执行insert,update,delete的sql操作语言</param>
        public bool runsql(string sql)
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

            string[] arrSQL = sql.Split(';');
            try
            {
                for (int i = 0; i <= arrSQL.Length - 1; i++)
                {
                    if ("" != arrSQL[i].Trim())
                    {
                        comm.CommandText = arrSQL[i].Trim();
                        int tmp = comm.ExecuteNonQuery();
                    }
                }
                trans.Commit();
                return true;
            }
            catch (Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                trans.Rollback();
                return false;
            }
        }
        /// <summary>
        /// 执行插入删除修改的sql,只能是insert,update,delete,
        /// 返回类型:bool
        /// </summary>
        /// <param name="sql">传入要查询的Select语句</param>
        ///  <param name="values">SqlParameter[]数组对象</param>
        public bool runsql(string sql, params SqlParameter[] values)
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

            string[] arrSQL = sql.Split(';');
            try
            {
                for (int i = 0; i <= arrSQL.Length - 1; i++)
                {
                    if ("" != arrSQL[i].Trim())
                    {
                        comm.CommandText = arrSQL[i].Trim();
                        comm.Parameters.AddRange(values);
                        int tmp = comm.ExecuteNonQuery();
                    }
                }
                trans.Commit();

                return true;
            }
            catch (Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                return false;
            }
        }

        /// <summary>
        /// 执行数据库的查询
        /// 返回类型：DataSet
        /// 传入参数:string
        /// </summary>
        /// <param name="sql">传入要查询的Select语句</param>
        public DataSet getDataSet(string sql)
        {
            if (sql == "")
            {
                _sError = "查询语句为空";
                return null;
            }
            try
            {
                if (_conn.State != ConnectionState.Closed)
                {
                    SqlDataAdapter adpt = new SqlDataAdapter(sql, _conn);
                    DataSet ds = new DataSet();
                    adpt.Fill(ds);
                    return ds;
                }
                return null;

            }
            catch (Exception ex)
            {
                _sError = "温馨提示：" + ex.Message;
                return null;

            }

        }
        /// <summary>
        /// 执行数据库的查询
        /// 返回类型：DataSet
        /// 传入参数:string
        /// </summary>
        /// <param name="sql">传入要查询的Select语句</param>
        ///  <param name="values">SqlParameter[]数组对象</param>
        public DataSet getDataSet(string sql, params SqlParameter[] values)
        {
            if (sql == "")
            {
                _sError = "查询语句为空";
                return null;
            }
            try
            {
                if (_conn.State == ConnectionState.Open)
                {
                    DataSet ds = new DataSet();
                    SqlCommand comm = new SqlCommand(sql, _conn);
                    SqlDataAdapter adpt = new SqlDataAdapter();
                    comm.Parameters.AddRange(values);
                    adpt.SelectCommand = comm;

                    adpt.Fill(ds);
                    comm.ExecuteNonQuery();
                    return ds;
                }
                else
                {
                    _sError = "数据库连接失败";
                    return null;
                }
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
             if(sql == "")
             {
                _sError = "查询语句为空";
                 return null;
             }
             try
             {
                 if(_conn.State != ConnectionState.Closed)
                 {
                     DataSet ds = new DataSet();
                     SqlCommand comm = new SqlCommand(sql, _conn);
                     SqlDataAdapter adpt = new SqlDataAdapter(comm);
                     adpt.Fill(ds);
                     return ds.Tables[0];
                 }
                 return null;

                
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
        public DataTable getTable(string sql,params SqlParameter[] values)
        {
            if (sql == "")
            {
                return null;
            }
            try
            {
                if (_conn.State != ConnectionState.Closed)
                {
                    DataSet ds = new DataSet();
                    SqlCommand comm = new SqlCommand(sql, _conn);
                    SqlDataAdapter dapt = new SqlDataAdapter(comm);

                    dapt.Fill(ds);
                    comm.Parameters.AddRange(values);
                    return ds.Tables[0];
                }
                return null;


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
        public SqlCommand runsp(string spname, params SqlParameter[] values)
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
                DataSet dsPK = getDataSet("exec  sys.sp_pkeys '" + name + "'");     //获得的表主键信息。

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
        /// 中对应表名的流水号
        /// </summary>
        /// <param name="tablename">表名</param>
        /// <returns>新流水号</returns>
        public string getNewPK(string tablename)
        {
            if ("Closed" == _conn.State.ToString())
            {
                _sError = "在操作getPKey的时候，数据库的连接没有打开";
                return null;
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
