using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.OracleClient;
using log4net;

namespace BI_Incremental
{
    public class LoggingHelper
    {
        public static int LogErrorToDb(string query, string errorMessage)
        {
            string conString = "Data Source=singer;User ID=manthan;Password=mant#456;Unicode=True";

            using (OracleConnection connection = new OracleConnection())
            {
                connection.ConnectionString = conString;
                connection.Open();
                //mant_STG_SALE_LINE_exception
                try
                {
                    string recordValues = GetRecordValue(query); // errorCode, ErrorDate, tableName,record
                    string tableName = GetTableNameFromQuery(query);
                    string formattedErrorMessage = errorMessage.Replace("'", "");
                    string errorQuery = "INSERT INTO ifsapp.mant_exception VALUES('" + formattedErrorMessage + "',to_date('" + HelperClass.DateManipulationWithTime(DateTime.Now) + "', 'yyyy/MM/dd HH24:MI:SS'),'" + tableName + "','" + recordValues + "')";
                    OracleCommand oOracleCommand = new OracleCommand(errorQuery, connection);
                    return oOracleCommand.ExecuteNonQuery();
                }
                catch (Exception ee)
                {
                    WriteErrorToFile(string.Format("{0} - {1}", ee.Message, ee.StackTrace), "--- Error when writing error to DB. ---", System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.ToString());
                    return 0;
                }
                finally
                {
                    connection.Close();
                }
            }
        }

        public static void WriteErrorToFile(string ErrorMessage, string query, string ErrorMethod)
        {
            ILog log = log4net.LogManager.GetLogger(ErrorMethod);
            log.Error(string.Format("{0} \r {1} ", ErrorMessage, query + "\n"));
        }


        public static string GetRecordValue(string query)
        {
            if (query.ToLower().Contains("select"))
            {
                return "";
            }
            else if (query.ToLower().Contains("insert"))
            {
                var Startindex = query.IndexOf("VALUES");
                var value = query.Substring(Startindex + 6);

                query = value.Replace("'", "");
            }
            return query;
        }

        public static string GetTableNameFromQuery(string query)
        {
            var strarray = query.Split('(')[0];
            var tableName = strarray.Split(' ')[2];
            return tableName;
        }
    }
}
