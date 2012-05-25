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
using System.Text;
using System.Collections.Generic;

namespace daDB
{
    public class Functions
    {
        /// <summary>
        /// 转码
        /// </summary>
        /// <param name="str">待转码串</param>
        /// <returns>编码串</returns>
        public static string tohex(string str)
        {
            if (str.IndexOf("~h`") == 0)
            {
                return str;
            };
            int i = str.Length;
            string temp;
            string end = "", endt = "";
            byte[] array = new byte[2];
            int i1, i2;
            for (int j = 0; j < i; j++)
            {
                temp = str.Substring(j, 1);
                array = System.Text.Encoding.Unicode.GetBytes(temp);
                if (array.Length.ToString() == "1")
                {
                    i1 = Convert.ToInt32(array[0]);
                    endt = Convert.ToString(i1, 16);
                    if (endt.Length < 2)
                        while (endt.Length < 2) endt = "0" + endt;
                    end += endt;
                }
                else
                {
                    i1 = Convert.ToInt32(array[0]);
                    i2 = Convert.ToInt32(array[1]);
                    endt = Convert.ToString(i1, 16);
                    if (endt.Length < 2)
                        while (endt.Length < 2) endt = "0" + endt;
                    end += endt;

                    endt = Convert.ToString(i2, 16);
                    if (endt.Length < 2)
                        while (endt.Length < 2) endt = "0" + endt;
                    end += endt;
                }
            }
            return "~h`" + end.ToUpper();
        }

        /// <summary>
        /// 解码
        /// </summary>
        /// <param name="HexStr">转码串</param>
        /// <returns>文本</returns>
        public static string tostr(string HexStr)
        {
            string str2 = HexStr;
            if (str2.IndexOf("~h`") == 0)
            {
                str2 = str2.Substring(3);
                if (str2 == "")
                {
                    return "";
                }
                else
                {
                    byte[] oribyte = new byte[str2.Length / 2];
                    for (int i = 0; i < str2.Length; i += 2)
                    {
                        string str = Convert.ToInt32(str2.Substring(i, 2), 16).ToString();
                        oribyte[i / 2] = Convert.ToByte(str2.Substring(i, 2), 16);
                    }
                    return System.Text.Encoding.Unicode.GetString(oribyte);
                };
            }
            else return str2;
        }


        public static SqlParameter getSqlParam(DataRow row, string value)
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
            param.SqlDbType = Functions.mapDBType(type);


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
        /// 查找数据库数据类型枚举
        /// </summary>
        /// <param name="stype">类型名</param>
        /// <returns>数据库数据类型枚举</returns>
        public static SqlDbType mapDBType(string stype )
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

    }
}
