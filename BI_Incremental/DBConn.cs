using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.OracleClient;
using System.Windows.Forms;
using System.Data;

namespace BI_Incremental
{
    public class DBConn
    {
        OracleConnection oOracleConnection;
        OracleCommand oOracleCommand;
        OracleDataAdapter oOracleAdapter;

        public OracleConnection GetConnection()
        {

            try
            {
                //Live
                 string conString = "Data Source=SSLPDB;User ID=neenopal;Password=neen#456;Unicode=True";
               
                //DR
                //string conString = "Data Source=SINGERDR;User ID=NEENOPAL;Password=NEEN#456;Unicode=True";
                oOracleConnection = new OracleConnection();
                oOracleConnection.ConnectionString = conString;
                oOracleConnection.Open();
                return oOracleConnection;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "**** Could not create connection *****", System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.ToString());
                Application.Exit();
                return null;
            }
            finally
            {
                //oOracleConnection.Close();
            }

        }

        public OracleConnection GetConnectionDMD()
        {
            try
            {
                //Live
                string conString = "Data Source=DMD;User ID=DMNEENOPAL;Password=NEEN#456;Unicode=True";

                //DR
                //string conString = "Data Source=DMDDR;User ID=DMNEENOPAL;Password=NEEN#456;Unicode=True";
                oOracleConnection = new OracleConnection();
                oOracleConnection.ConnectionString = conString;
                oOracleConnection.Open();
                return oOracleConnection;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "**** Could not create connection *****", System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.ToString());
                Application.Exit();
                return null;
            }
            finally
            {

            }
        }

        public OracleConnection GetConnectionnewForNRTAN()
        {
            try
            {
                //LIVE
                string conString = "Data Source=SINGEXT;User ID=NRTAN;Password=NRTAN;Unicode=True";

                //DR
                //string conString = "Data Source=SINSTBY;User ID=NRTAN;Password=NRTAN;Unicode=True";

                oOracleConnection = new OracleConnection();
                oOracleConnection.ConnectionString = conString;
                oOracleConnection.Open();
                return oOracleConnection;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "**** Could not create connection *****", System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.ToString());
                Application.Exit();
                return null;
            }
            finally
            {
                //oOracleConnection.Close();
            }

        }

        public OracleConnection GetConnectionnRENT()
        {
            try
            {
                //Live
                //string conString = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=singersl )(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=SINGER)));User ID=singerapp;Password=0r@10$in9;";
                string conString = "Data Source=singer;User ID=singerapp;Password=0r@10$in9;Unicode=True";

                //dr
                //string conString = "Data Source=singerdr;User ID=singerapp;Password=0r@10$in9;Unicode=True";

                oOracleConnection = new OracleConnection();
                oOracleConnection.ConnectionString = conString;
                oOracleConnection.Open();
                return oOracleConnection;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "**** Could not create connection *****", System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.ToString());
                Application.Exit();
                return null;
            }
            finally
            {
                //oOracleConnection.Close();
            }

        }

        public OracleConnection GetConnectionRTAN()
        {
            try
            {

                //live 
                string conString = "Data Source=SINGEXT;User ID=RTAN;Password=RTAN#121;Unicode=True";

                oOracleConnection = new OracleConnection();
                oOracleConnection.ConnectionString = conString;
                oOracleConnection.Open();
                return oOracleConnection;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "**** Could not create connection *****", System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.ToString());
                Application.Exit();
                return null;
            }
            finally
            {
                //oOracleConnection.Close();
            }

        }

        public OracleConnection GetConnectionDMD_MANTHAN()
        {
            try
            {

                //live 
                string conString = "Data Source=DMD;User ID=dmmanthan;Password=mant#456;Unicode=True";

                oOracleConnection = new OracleConnection();
                oOracleConnection.ConnectionString = conString;
                oOracleConnection.Open();
                return oOracleConnection;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "**** Could not create connection *****", System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.ToString());
                Application.Exit();
                return null;
            }
            finally
            {
                //oOracleConnection.Close();
            }

        }

        //newly added on 2018-03-27
        /// <summary>
        ///  execute querries with patameters
        /// </summary>
        /// <param name="SelectQuery"></param>
        /// <param name="ReturnType"></param>
        /// <param name="_OraParameter"></param>
        /// <param name="cmdType"></param>
        /// <param name="orConnection"></param>
        /// <returns>objects</returns>
        public object Executes(string SelectQuery, ReturnType ReturnType, OracleParameter[] _OraParameter, CommandType cmdType, OracleConnection orConnection)
        {
            try
            {
                Object objValue = new object();
                switch (ReturnType)
                {
                    case (ReturnType.DataTable):
                        {
                            objValue = ReturnDataTable(SelectQuery, _OraParameter, cmdType, orConnection);
                        }
                        break;

                    case (ReturnType.DataSet):
                        {
                            objValue = ReturnDataSet(SelectQuery, _OraParameter, cmdType, orConnection);
                        }
                        break;

                    case (ReturnType.DataRow):
                        {
                            objValue = ReturnDataRow(SelectQuery, _OraParameter, cmdType, orConnection);
                        }
                        break;
                }
                return objValue;
            }

            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="SelectQuery"></param>
        /// <param name="ReturnType"></param>
        /// <param name="cmdType"></param>
        /// <param name="orConnection"></param>
        /// <returns></returns>
        public object Executes(string SelectQuery, ReturnType ReturnType, CommandType cmdType, OracleConnection orConnection)
        {
            try
            {
                Object objValue = new object();
                switch (ReturnType)
                {
                    case (ReturnType.DataTable):
                        {
                            objValue = ReturnDataTable(SelectQuery, cmdType, orConnection);
                        }
                        break;

                    case (ReturnType.DataSet):
                        {
                            objValue = ReturnDataSet(SelectQuery, cmdType, orConnection);
                        }
                        break;

                    case (ReturnType.DataRow):
                        {
                            objValue = ReturnDataRow(SelectQuery, cmdType, orConnection);
                        }
                        break;
                }
                return objValue;
            }

            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// With parameters return Data Table
        /// </summary>
        /// <param name="SelectQuery"></param>
        /// <param name="_oOracleParameter"></param>
        /// <param name="cmdType"></param>
        /// <param name="oOracleConnection"></param>
        /// <returns></returns>
        private DataTable ReturnDataTable(string SelectQuery, OracleParameter[] _oOracleParameter, CommandType cmdType, OracleConnection oOracleConnection)
        {
            DataTable dt;
            OracleConnection conn = null;

            oOracleCommand = new OracleCommand();
            dt = new DataTable();
            conn = oOracleConnection;
            oOracleCommand.Connection = conn;
            oOracleCommand.CommandText = SelectQuery;
            oOracleCommand.CommandType = cmdType;

            for (int i = 0; i < _oOracleParameter.Length; i++)
            {
                oOracleCommand.Parameters.Add(_oOracleParameter[i]);
            }

            oOracleAdapter = new OracleDataAdapter(oOracleCommand);
            oOracleAdapter.Fill(dt);
            return dt;

        }

        /// <summary>
        /// With out parameters return Data Table
        /// </summary>
        /// <param name="SelectQuery"></param>
        /// <param name="cmdType"></param>
        /// <param name="oOracleConnection"></param>
        /// <returns></returns>
        private DataTable ReturnDataTable(string SelectQuery, CommandType cmdType, OracleConnection oOracleConnection)
        {
            DataTable dt;
            OracleConnection conn = null;

            oOracleCommand = new OracleCommand();
            dt = new DataTable();
            conn = oOracleConnection;
            oOracleCommand.Connection = conn;
            oOracleCommand.CommandText = SelectQuery;
            oOracleCommand.CommandType = cmdType;


            oOracleAdapter = new OracleDataAdapter(oOracleCommand);
            oOracleAdapter.Fill(dt);
            return dt;

        }

        /// <summary>
        /// With parameters return Data Set
        /// </summary>
        /// <param name="SelectQuery"></param>
        /// <param name="_oOracleParameter"></param>
        /// <param name="cmdType"></param>
        /// <param name="oOracleConnection"></param>
        /// <returns></returns>
        private DataSet ReturnDataSet(string SelectQuery, OracleParameter[] _oOracleParameter, CommandType cmdType, OracleConnection oOracleConnection)
        {
            DataSet ds;
            OracleConnection conn = null;

            oOracleCommand = new OracleCommand();
            ds = new DataSet();

            conn = oOracleConnection;
            oOracleCommand.Connection = conn;
            oOracleCommand.CommandText = SelectQuery;
            oOracleCommand.CommandType = cmdType;
            for (int i = 0; i < _oOracleParameter.Length; i++)
            {
                oOracleCommand.Parameters.Add(_oOracleParameter[i]);
            }

            oOracleAdapter = new OracleDataAdapter(oOracleCommand);
            oOracleAdapter.Fill(ds);

            conn.Close();
            return ds;

        }

        /// <summary>
        /// With out parameters return Data Table
        /// </summary>
        /// <param name="SelectQuery"></param>
        /// <param name="cmdType"></param>
        /// <param name="oOracleConnection"></param>
        /// <returns></returns>
        private DataSet ReturnDataSet(string SelectQuery, CommandType cmdType, OracleConnection oOracleConnection)
        {
            DataSet ds;
            OracleConnection conn = null;

            oOracleCommand = new OracleCommand();
            ds = new DataSet();

            conn = oOracleConnection;
            oOracleCommand.Connection = conn;
            oOracleCommand.CommandText = SelectQuery;
            oOracleCommand.CommandType = cmdType;

            oOracleAdapter = new OracleDataAdapter(oOracleCommand);
            oOracleAdapter.Fill(ds);

            conn.Close();
            return ds;

        }


        private DataRow ReturnDataRow(string SelectQuery, OracleParameter[] _oOracleParameter, CommandType cmdType, OracleConnection oOracleConnection)
        {

            DataTable dt;
            OracleConnection conn = null;

            oOracleCommand = new OracleCommand();
            dt = new DataTable();
            conn = oOracleConnection;

            oOracleCommand.Connection = conn;
            oOracleCommand.CommandText = SelectQuery;
            oOracleCommand.CommandType = cmdType;
            for (int i = 0; i < _oOracleParameter.Length; i++)
            {
                oOracleCommand.Parameters.Add(_oOracleParameter[i]);
            }

            oOracleAdapter = new OracleDataAdapter(oOracleCommand);
            oOracleAdapter.Fill(dt);
            if (dt.Rows.Count == 0)
                return null;

            return dt.Rows[0];

        }

        private DataRow ReturnDataRow(string SelectQuery, CommandType cmdType, OracleConnection oOracleConnection)
        {

            DataTable dt;
            OracleConnection conn = null;

            oOracleCommand = new OracleCommand();
            dt = new DataTable();
            conn = oOracleConnection;

            oOracleCommand.Connection = conn;
            oOracleCommand.CommandText = SelectQuery;
            oOracleCommand.CommandType = cmdType;

            oOracleAdapter = new OracleDataAdapter(oOracleCommand);
            oOracleAdapter.Fill(dt);
            if (dt.Rows.Count == 0)
                return null;

            return dt.Rows[0];

        }


        public static OracleParameter AddParameter(string Name, object Value)
        {
            OracleParameter Parm;
            try
            {
                Parm = new OracleParameter();
                Parm.ParameterName = Name;
                Parm.Value = Value;

                return Parm;
            }

            catch (Exception ex)
            {
                throw ex;
            }
        }



    }
}
