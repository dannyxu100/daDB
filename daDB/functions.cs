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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dscmp"></param>
        public static void nonull2(DataSet dscmp)
        {
            foreach (DataTable t1 in dscmp.Tables)
            {
                for (int i = 0; i < t1.Rows.Count; i++)
                {
                    DataRow tr1 = t1.Rows[i];
                    for (int ti1 = 0; ti1 < t1.Columns.Count; ti1++)
                    {
                        if (tr1[ti1].Equals(System.DBNull.Value))
                        {
                            if (t1.Columns[ti1].DataType.Equals(System.Type.GetType("System.DateTime")))
                                tr1[ti1] = DateTime.Parse("1900-1-1");  //DateTime.Today ;
                            else
                                if ((t1.Columns[ti1].DataType.Equals(System.Type.GetType("System.Boolean"))))
                                    tr1[ti1] = false;
                                else
                                    if ((t1.Columns[ti1].DataType.Equals(System.Type.GetType("System.Int32"))) ||
                                    (t1.Columns[ti1].DataType.Equals(System.Type.GetType("System.Double"))) ||
                                    (t1.Columns[ti1].DataType.Equals(System.Type.GetType("System.Decimal"))) ||
                                    (t1.Columns[ti1].DataType.Equals(System.Type.GetType("System.Int16"))) ||
                                    (t1.Columns[ti1].DataType.Equals(System.Type.GetType("System.Int64"))) ||
                                    (t1.Columns[ti1].DataType.Equals(System.Type.GetType("System.SByte"))) ||
                                    (t1.Columns[ti1].DataType.Equals(System.Type.GetType("System.Byte"))) ||
                                    (t1.Columns[ti1].DataType.Equals(System.Type.GetType("System.Single"))) ||
                                    (t1.Columns[ti1].DataType.Equals(System.Type.GetType("System.UInt16"))) ||
                                    (t1.Columns[ti1].DataType.Equals(System.Type.GetType("System.UInt32"))) ||
                                    (t1.Columns[ti1].DataType.Equals(System.Type.GetType("System.UInt64"))))
                                        tr1[ti1] = 0;
                                    else tr1[ti1] = "";
                        }
                    }
                }
            }
        }
    }
}
