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
        private string _strConnectPath;

        //定义全局变量
        private SqlConnection _conn = new SqlConnection();

        /// <summary>
        /// 无参构造函数
        /// </summary>
        public DBHelper_SQL()
        {
            _sError = null;
            _strConnectPath = ConfigurationManager.AppSettings["connect"].ToString();
        }
        /// <summary>
        /// 带数据库链接字符串的 构造函数
        /// </summary>
        public DBHelper_SQL(string sConnectName)
        {
            _sError = null;
            if (!Object.Equals(ConfigurationManager.AppSettings[sConnectName], null))        //传入webconfig文件AppSettings参数名
            {
                _strConnectPath = ConfigurationManager.AppSettings[sConnectName].ToString();
            }
            else
            {
                _strConnectPath = sConnectName;             //直接传入数据库连接字符串
            }
        }

        /// <summary>
        /// 从Web.config中获取连接字符串"
        /// </summary>
        public string GetConnectionString()
        {
            string sConnction = _strConnectPath;
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
        /// 执行插入删除修改的存储过程,只能是insert,update,delete,
        /// 参数为string数据类型
        /// </summary>
        /// <param name="sql">传入执行insert,update,delete的SQL操作语言</param>
        public bool runsql(string sql)
        {
            if ("Closed" == _conn.State.ToString())
            {
                _sError = "在操作DBExcuteSQL模块的时候，数据库的连接没有打开";
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
        /// 执行插入删除修改的存储过程,只能是insert,update,delete,
        /// 返回类型:bool
        /// </summary>
        /// <param name="sql">传入要查询的Select语句</param>
        ///  <param name="values">SqlParameter[]数组对象</param>
        public bool runsql(string sql, params SqlParameter[] values)
        {
            if ("Closed" == _conn.State.ToString())
            {
                _sError = "在操作params_DBExcuteSQL模块的时候，数据库的连接没有打开";
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
                _sError = _sError + "温馨提示2：" + ex.Message;
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

    }
}
