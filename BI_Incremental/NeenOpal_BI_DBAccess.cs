using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.OracleClient;
using System.Transactions;
using BI_Incremental.Objects;

namespace BI_Incremental
{
    public class NeenOpal_BI_DBAccess
    {
        #region Private Variables
        private DBConn dbConnection = new DBConn();
        private DataSet oDataSet;
        private DataTable oDataTable = null;
        private DataRow O_dRow = null;
        #endregion

        #region Public Methods

        public DataRow GetSINGERServerCurrentTime()
        {

            try
            {

                OracleConnection oOracleConnection = dbConnection.GetConnection();
                string stringQuerry = @"select TRUNC(CURRENT_DATE)   AS CURRENTDATE from DUAL";

                O_dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return O_dRow;
            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In GetSINGERServerCurrentTime ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return O_dRow;
        }

        public DataRow GetMethodRelatedTableIDDetails(BIAdminControlETITableTypes intTypeID)
        {
            string stringQuerry = string.Empty;

            try
            {

                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
                stringQuerry = @" SELECT C.BATCHID,C.BATCHTYPE,C.TABLEID,C.TABLENAME,C.TABLETYPE 
                from NRTAN.BI_ADMIN_ETL_CONTROL_TABLE C 
                WHERE C.TABLEID = :TABLEID  ";

                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("TABLEID",intTypeID ) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return dRow;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), stringQuerry, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ee;
            }
        }

        public bool UpdateBIAdminETLExtractionLogFile(int intPassTableID, DateTime dtTaskDate, DateTime dtUpdateOn, int intRowCount)
        {
            try
            {
                bool isSave = false;

                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
                string stringQuerry = @"
                UPDATE   NRTAN.BI_ADMIN_ETL_EXTRACTION_LOG  SET ROW_COUNT = ROW_COUNT + :ROW_COUNT  , EXTRACTION_END_DATE_TIME = TO_DATE(:EXTRACTION_END_DATE_TIME,'yyyy-MM-dd HH24:MI:SS')  WHERE TABLE_ID = :TABLE_ID AND    TASK_DATE = TO_DATE(:TASK_DATE,'yyyy-MM-dd')
                ";

                OracleParameter[] param = new OracleParameter[] 
                   { 
                    DBConn.AddParameter("ROW_COUNT",intRowCount ) 
                   ,DBConn.AddParameter("TASK_DATE",dtTaskDate.ToString("yyyy/MM/dd"))
                   ,DBConn.AddParameter("EXTRACTION_END_DATE_TIME",dtUpdateOn.ToString("yyyy/MM/dd hh:mm:ss"))  
                   ,DBConn.AddParameter("TABLE_ID",intPassTableID)
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        public bool CloseStatusInBIAdminETLExtractionLogFile(int PassTableID, DateTime dtTaskDate, DateTime dtUpdateOn, int intStatus)
        {
            string stringQuerry = string.Empty;
            try
            {
                bool isSave = false;

                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
                stringQuerry = @"
                UPDATE   NRTAN.BI_ADMIN_ETL_EXTRACTION_LOG  SET EXTRACTION_STATUS_FLAG = :EXTRACTION_STATUS_FLAG , EXTRACTION_END_DATE_TIME = TO_DATE(:EXTRACTION_END_DATE_TIME,'yyyy-MM-dd HH24:MI:SS') WHERE  TABLE_ID = :TABLE_ID AND TASK_DATE = TO_DATE(:TASK_DATE,'yyyy-MM-dd') 
                ";

                OracleParameter[] param = new OracleParameter[] 
                   { 
                    DBConn.AddParameter("TASK_DATE",dtTaskDate.ToString("yyyy/MM/dd"))
                   ,DBConn.AddParameter("EXTRACTION_END_DATE_TIME",dtUpdateOn.ToString("yyyy/MM/dd HH:mm:ss"))  
                   ,DBConn.AddParameter("EXTRACTION_STATUS_FLAG",intStatus)  
                   ,DBConn.AddParameter("TABLE_ID",PassTableID)
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), stringQuerry, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ee;
            }
        }

        public DataRow CheckTableRecordInBIExtractionLogFileAlreadyExsists(int PassTableID, DateTime dtTaskDate)
        {
            string stringQuerry = string.Empty;

            try
            {

                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
                stringQuerry = @"
                SELECT COUNT(*) AS RowCount FROM NRTAN.BI_ADMIN_ETL_EXTRACTION_LOG  WHERE  TABLE_ID = :TABLE_ID AND TASK_DATE = TO_DATE(:TASK_DATE,'yyyy-MM-dd') 
                ";

                OracleParameter[] param = new OracleParameter[] 
                   { 
                    DBConn.AddParameter("TASK_DATE",dtTaskDate.ToString("yyyy/MM/dd"))
                   ,DBConn.AddParameter("TABLE_ID",PassTableID)
                };

                using (TransactionScope ts = new TransactionScope())
                {
                    O_dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                    ts.Complete();
                }
                oOracleConnection.Close();
                return O_dRow;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), stringQuerry, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return O_dRow;
        }

        public void DeleteRecordsIn_BI_NRTAN_ServerTables(DateTime dtTaskDate, int intRelatedTblID)
        {
            string stringQuerry = string.Empty;
            
            try
            {
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_HPC_CCD))
                {
                    stringQuerry = @"
                    DELETE from NRTAN.BI_HPC_CCD    
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_ONLINE_HP_ACC))
                {
                    stringQuerry = @"
                    DELETE from NRTAN.BI_ONLINE_HP_ACC    
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_HP_WALK_MEGA))
                {
                    stringQuerry = @"
                    DELETE from NRTAN.BI_HP_WALK_MEGA    
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_WRITE_OFF_AMT))
                {
                    stringQuerry = @"
                    DELETE from NRTAN.BI_WRITE_OFF_AMT    
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_BLACKLIST_CUST))
                {
                    stringQuerry = @"
                    DELETE from NRTAN.BI_BLACKLIST_CUST    
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_BR_MGR_INCEN))
                {
                    stringQuerry = @"
                    DELETE from NRTAN.BI_BR_MGR_INCEN    
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_RESALE_VAL_ITEM))
                {
                    stringQuerry = @"
                    DELETE from NRTAN.BI_RESALE_VAL_ITEM    
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_GUARANTER_INFO))
                {
                    stringQuerry = @"
                    DELETE from NRTAN.BI_GUARANTER_INFO    
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_REVERT_INFO))
                {
                    stringQuerry = @"
                    DELETE from NRTAN.BI_REVERT_INFO    
                    ";
                }


                using (TransactionScope ts = new TransactionScope())
                {
                    DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, CommandType.Text, oOracleConnection);
                    ts.Complete();
                }
                oOracleConnection.Close();
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), stringQuerry, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ee;
            }
        }

        public void DeleteRecordsIn_BI_SSLPDB_ServerTables(DateTime dtTaskDate, int intRelatedTblID)
        {
            string stringQuerry = string.Empty;

            try
            {
                OracleConnection oOracleConnection = dbConnection.GetConnection();

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_HPC_CCD))
                {
                    stringQuerry = @"
                    DELETE from IFSAPP.SIN_HPC_CCD  WHERE MONTH = :current_month  
                    ";
                }

                var today = DateTime.Today;
                var month = new DateTime(today.Year, today.Month, today.Day);

                var current_month = month.AddDays(-4).Month;

                OracleParameter[] param = new OracleParameter[]
                {
                    DBConn.AddParameter("current_month",current_month)
                };

                using (TransactionScope ts = new TransactionScope())
                {
                    DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                    ts.Complete();
                }
                oOracleConnection.Close();
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), stringQuerry, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ee;
            }

        }

        public bool InsertStartProcessToBIAdminETLExtractionLogFile(int intLogTableID, int intRowCount, DateTime dtTaskDate, int intStatus, DateTime dtStartOn, string strUserName, string strMachineName)
        {
            string stringQuerry = string.Empty;

            try
            {
                bool isSave = false;

                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
                stringQuerry = @"
                INSERT INTO NRTAN.BI_ADMIN_ETL_EXTRACTION_LOG (TABLE_ID,ROW_COUNT,TASK_DATE,EXTRACTION_START_DATE_TIME,EXTRACTION_END_DATE_TIME,EXTRACTION_STATUS_FLAG,MACHINENAME,USERNAME) 
                values 
                ( :TABLE_ID, :ROW_COUNT,TO_DATE(:TASK_DATE,'yyyy-MM-dd'), TO_DATE(:Extraction_Start_Date_Time,'yyyy/mm/dd hh24:mi:ss'),null,:Extraction_Status_Flag,:MACHINENAME,:USERNAME )
                ";

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("TABLE_ID",intLogTableID) 
                   ,DBConn.AddParameter("Row_COUNT",intRowCount ) 
                   ,DBConn.AddParameter("TASK_DATE",dtTaskDate.ToString("yyyy/MM/dd"))
                   ,DBConn.AddParameter("Extraction_Start_Date_Time",dtStartOn.ToString("yyyy/MM/dd HH:mm:ss"))  
                   ,DBConn.AddParameter("Extraction_Status_Flag",intStatus ) 
                   ,DBConn.AddParameter("USERNAME", strUserName) 
                   ,DBConn.AddParameter("MACHINENAME",strMachineName ) 
                   
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), stringQuerry, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ee;
            }
        }

        public DataRow Get_BI_NRTAN_MasterTableInsertedRecordCount(int intRelatedTblID)
        {
            string stringQuerry = string.Empty;

            try
            {
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_Store))//1
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_DIM_STORE
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_Product))//2
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.bi_dim_product
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_STATUS))//3
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.bi_dim_status
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_SALES_TYPE))//4
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.bi_dim_sales_type
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_SALES_PROMOTER))//5
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_SALES_PROMOTER
                    ";
                }
                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_SALES_STATUS))//6
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_SALES_STATUS
                    ";
                }
                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_INVENTORY_LOCATION))//7
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_DIM_INVENTORY_LOCATION
                    ";
                }
                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_SALES_LOCATION))//8
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_DIM_SALES_LOCATION
                    ";
                }
                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_RELAVENT_TYPE))//9
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_DIM_RELAVENT_TYPE
                    ";
                }
                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_PROMOTION))//10
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_DIM_PROMOTION
                    ";
                }
                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_DISCOUNT))//11
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_DIM_DISCOUNT
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_SALESMAN))//12
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_DIM_SALESMAN
                    ";
                }
                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_RETURN))//13
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_DIM_RETURN
                    ";
                }
                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_CUSTOMER))//14
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_DIM_CUSTOMER
                    ";
                }
                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_MARKETING_BUDGET))
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_MARKETING_BUDGET
                    ";
                }
                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_BRANCH_EXPOSURE))
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_BRANCH_EXPOSURE
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_586P_PLANNED_SALES))
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_586P_PLANNED_SALES
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_PRODUCT_IV))
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_DIM_PRODUCT_IV
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_STORE_RENT))
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_DIM_STORE_RENT
                    ";
                }

                if (intRelatedTblID == Convert.ToInt32(BIAdminControlETITableTypes.BI_USER_RLS))
                {
                    stringQuerry = @"
                    SELECT COUNT(*) AS RecCount from NRTAN.BI_USER_RLS
                    ";
                }

                DataRow dRow;
                using (TransactionScope ts = new TransactionScope())
                {
                    dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, CommandType.Text, oOracleConnection);
                    ts.Complete();
                }
                oOracleConnection.Close();
                return dRow;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), stringQuerry, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ee;
            }
        }

        public bool InsertToETLSalesLineLogFile(int intPassTableID, DateTime dtTaskDate, DateTime dtUpdateOn, int intRowCount, string MethodName)
        {
            try
            {
                bool isSave = false;

                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
                string stringQuerry = @"
                

                    INSERT INTO NRTAN.BI_ETL_SALE_LINE_LOG 
                    (
                    TASKDATE
                    ,METHODNAME
                    ,ROWCOUNT
                    ,TABLEID
                    ,ENTEREDON
                    )
                    values
                    (
                    TO_DATE(:TASK_DATE,'yyyy-MM-dd')
                    ,:METHOD_NAME
                    ,:ROW_COUNT
                    ,:TABLE_ID
                    , TO_DATE(:ENTERED_ON,'yyyy-MM-dd HH24:MI:SS') 
                    )

                ";

                OracleParameter[] param = new OracleParameter[] 
                   { 
                    DBConn.AddParameter("ROW_COUNT",intRowCount ) 
                   ,DBConn.AddParameter("TASK_DATE",dtTaskDate.ToString("yyyy/MM/dd"))
                   ,DBConn.AddParameter("ENTERED_ON",dtUpdateOn.ToString("yyyy/MM/dd hh:mm:ss"))  
                   ,DBConn.AddParameter("TABLE_ID",intPassTableID)
                   ,DBConn.AddParameter("METHOD_NAME",MethodName)
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In InsertToETLSalesLineLogFile ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                throw ex;
            }
        }


        #region BI_DIM_STORE

        public DataTable GetStore(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region Sql
//                selectQuery = @"
//       select aa.contract as SITE_ID,
//       pp.description as SITE_NAME,
//       hh.Channel_id AS CHANNEL_ID,
//       hh.Channel AS CHANNEL_NAME,
//       hh.Area_id AS AREA_ID,
//       hh.Area AS AREA_NAME,
//       hh.District_id AS DISTRICT_ID,
//       hh.District AS DISTRICT_NAME,
//       'DIRECT' AS AGENCY_ID,
//       'SSL' as COMPANY_ID,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   aa.contract,
//                                                   'Customer',
//                                                   'LATITUDE') as STORE_LATITUDE,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   aa.contract,
//                                                   'Customer',
//                                                   'LONGITUDE') as STORE_LONGITUDE,
//       case
//         when LL.hpnret_level_type = 'Branch' then
//          'BM'
//         when LL.hpnret_level_type = 'Approve Dealer' then
//          'AD'
//       end as TYPE_ID,
//       LL.hpnret_level_type AS TYPE_NAME,
//       ll.provincial_council_id as PROVINCE_ID,
//       IFSAPP.HPNRET_PVN_COUNCIL_api.Get_Description(ll.provincial_council_id) AS PROVINCE_NAME,
//       mm.value AS MOBILE_NO,
//       '' as STORE_PROVINCE,
//       '' AS QOS,
//       (SELECT SUM(B.SQUAREAREA) AS CAPACITIES
//          FROM ifsapp.SIN_RENT_BUILDINGMAIN B
//         WHERE B.STATUS = 'A'
//           AND B.SITEID = aa.contract) AS CAPACITIES,
//       '' AS AUDIT_GRADING,
//       '' AS STORE_ATTRIBUTE_1,
//       '' AS STORE_ATTRIBUTE_2,
//       '' AS STORE_ATTRIBUTE_3,
//       '' AS STORE_ATTRIBUTE_4,
//       '' AS STORE_ATTRIBUTE_5,
//       PP.primary_supp as PRIMARY_WAREHOUSE,
//       
//        (
//          select sum(nn.reserve_open_amount) AS FLOAT_AMT 
//           from ifsapp.ssl_open_amt_res_dtl nn 
//           where nn.site_code = aa.contract 
//           AND to_date(nn.valid_to, 'YYYY/MM/DD') >= trunc(sysdate)
//        ) AS FLOAT_AMT ,
//       
//       ll.district_id as SL_DISTRICT_ID,
//       IFSAPP.HPNRET_S_L_DISTRICTS_api.Get_Description(ll.provincial_council_id,
//                                                       ll.district_id) as SL_DISTRICT_NAME,
//       pp.site_type as STORE_TYPE,
//       1 AS TABLETYPE_ID -- 1= for if it is a dealer querry
//
//  from IFSAPP.INVENTORY_LOCATION aa
// inner join ifsapp.hpnret_levels_overview hh on aa.contract = hh.site_id
//  left join IFSAPP.SIN_STORE_MASTER_TAB bb on hh.site_id = bb.site_id
//  left join IFSAPP.SITE pp on hh.site_id = pp.contract
//  left join ifsapp.company_address cc on pp.delivery_address =
//                                         cc.address_id
//  left join IFSAPP.HPNRET_LEVEL_HIERARCHY yy on hh.site_id = yy.site_id
//  left join ifsapp.HPNRET_LEVEL ll on LL.level_id = YY.level_id
//  left join IFSAPP.COMPANY_COMM_METHOD mm on cc.address_id = mm.address_id  and mm.method_id = 'Mobile'
// --where aa.contract not in ('BDM01', 'BEJ01', 'BFS01', 'BIB01', 'BLP01',
//        --'FAB01', 'SAN01', 'BMJ01')
//--AND  SUBSTR(AA.objversion,1,4 ) = :PassYear
//--AND SUBSTR(AA.objversion,5,2 ) = :PassMonth
//--AND SUBSTR(AA.objversion,7,2 ) = :PassDate
// group by aa.contract,
//          
//          pp.description,
//          hh.Channel_id,
//          hh.Channel,
//          hh.Area_id,
//          hh.Area,
//          hh.District_id,
//          hh.District,
//          hh.site_id,
//          hh.branch,
//          cc.address1,
//          cc.address2,
//          LL.hpnret_level_type,
//          BB.STORE_OPEN_DATE,
//          bb.STORE_OPEN_HOURS,
//          bb.STORE_CLOSE_HOURS,
//          bb.STORE_CLOSE_DATE,
//          hh.channel,
//          bb.store_latitude,
//          bb.store_longitude,
//          bb.store_province,
//          bb.store_district,
//          bb.store_last_renovated_dt,
//          hh.site_id,
//          hh.site_id,
//          mm.value,
//          bb.store_group,
//          bb.store_default_dc_code,
//          bb.store_no_parking,
//          bb.store_no_floors,
//          bb.store_sequre_feet,
//          ll.provincial_council_id,
//          ll.district_id,
//          pp.site_type,
//          PP.primary_supp
//
//UNION
//
//SELECT KK.customer_id as SITE_ID,
//       IFSAPP.CUSTOMER_INFO_API.GET_NAME(KK.customer_id) as SITE_NAME,
//       IndirectOverV.Channel_Id AS CHANNEL_ID,
//       IndirectOverV.Channel AS CHANNEL_NAME,
//       IndirectOverV.Area_Id AS AREA_ID,
//       IndirectOverV.Area AS AREA_NAME,
//       IndirectOverV.Zone_Id as DISTRICT_ID,
//       IndirectOverV.Zone as DISTRICT_NAME,
//       'INDIRECT' AS AGENCY_ID,
//       'SSL' as COMPANY_ID,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   KK.customer_id,
//                                                   'Customer',
//                                                   'LATITUDE') as STORE_LATITUDE,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   KK.customer_id,
//                                                   'Customer',
//                                                   'LONGITUDE') as STORE_LONGITUDE,
//       'DE' as TYPE_ID,
//       'Dealer' AS TYPE_NAME,
//       '' as PROVINCE_ID,
//       '' AS PROVINCE_NAME,
//       '0' AS MOBILE_NO,
//       '' as STORE_PROVINCE,
//       '' AS QOS,
//       0 AS CAPACITIES,
//       '' AS AUDIT_GRADING,
//       '' AS STORE_ATTRIBUTE_1,
//       '' AS STORE_ATTRIBUTE_2,
//       '' AS STORE_ATTRIBUTE_3,
//       '' AS STORE_ATTRIBUTE_4,
//       '' AS STORE_ATTRIBUTE_5,
//       '' as PRIMARY_WAREHOUSE,
//       0 as FLOAT_AMT,
//       '' as SL_DISTRICT_ID,
//       '' as SL_DISTRICT_NAME,
//       'DEALER' as STORE_TYPE,
//       2 AS TABLETYPE_ID -- 1= for if it is a dealer querry
//  FROM IFSAPP.HPNRET_LEVEL_OVERVIEW_ZONE IndirectOverV
// inner join IFSAPP.CUST_ORD_CUSTOMER_ENT KK ON KK.MARKET_CODE =
//                                               IndirectOverV.ZONE_ID
// --WHERE --SUBSTR(KK.objversion, 1, 4) =  :PassYear
//--AND SUBSTR(KK.objversion, 5, 2) =  :PassMonth
//--AND SUBSTR(KK.objversion, 7, 2) = :PassDate
//--AND 
//---- SUBSTR(KK.customer_id, 6, 1) NOT IN ('P') --NEWLY ADDED 2019-05-17  ----commented said by chathura 17th Oct
//--AND 
//--SUBSTR(KK.customer_id,1, 2) NOT IN ('DM')  ---remove dmd dealers from here 
//                ";
                #endregion

                #region Sql 2020/03/06

//                selectQuery = @"
//       select aa.contract as SITE_ID,
//       pp.description as SITE_NAME,
//       hh.Channel_id AS CHANNEL_ID,
//       hh.Channel AS CHANNEL_NAME,
//       hh.Area_id AS AREA_ID,
//       hh.Area AS AREA_NAME,
//       hh.District_id AS DISTRICT_ID,
//       hh.District AS DISTRICT_NAME,
//       'DIRECT' AS AGENCY_ID,
//       'SSL' as COMPANY_ID,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   aa.contract,
//                                                   'Customer',
//                                                   'LATITUDE') as STORE_LATITUDE,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   aa.contract,
//                                                   'Customer',
//                                                   'LONGITUDE') as STORE_LONGITUDE,
//       case
//         when LL.hpnret_level_type = 'Branch' then
//          'BM'
//         when LL.hpnret_level_type = 'Approve Dealer' then
//          'AD'
//       end as TYPE_ID,
//       LL.hpnret_level_type AS TYPE_NAME,
//       ll.provincial_council_id as PROVINCE_ID,
//       IFSAPP.HPNRET_PVN_COUNCIL_api.Get_Description(ll.provincial_council_id) AS PROVINCE_NAME,
//       mm.value AS MOBILE_NO,
//       '' as STORE_PROVINCE,
//       '' AS QOS,
//       (SELECT SUM(B.SQUAREAREA) AS CAPACITIES
//          FROM ifsapp.SIN_RENT_BUILDINGMAIN B
//         WHERE B.STATUS = 'A'
//           AND B.SITEID = aa.contract) AS CAPACITIES,
//       '' AS AUDIT_GRADING,
//       
//       CASE 
//           WHEN (SELECT COUNT(*) FROM IFSAPP.SIN_CLOSED_SHOPS CS WHERE CS.SITE_ID = aa.contract) > 0 THEN 'INACTIVE'
//       END AS STORE_ATTRIBUTE_1,
//       
//       '' AS STORE_ATTRIBUTE_2,
//       '' AS STORE_ATTRIBUTE_3,
//       '' AS STORE_ATTRIBUTE_4,
//       '' AS STORE_ATTRIBUTE_5,
//       PP.primary_supp as PRIMARY_WAREHOUSE,
//       
//        (
//          select sum(nn.reserve_open_amount) AS FLOAT_AMT 
//           from ifsapp.ssl_open_amt_res_dtl nn 
//           where nn.site_code = aa.contract 
//           AND to_date(nn.valid_to, 'YYYY/MM/DD') >= trunc(sysdate)
//        ) AS FLOAT_AMT ,
//       
//       ll.district_id as SL_DISTRICT_ID,
//       IFSAPP.HPNRET_S_L_DISTRICTS_api.Get_Description(ll.provincial_council_id,
//                                                       ll.district_id) as SL_DISTRICT_NAME,
//       pp.site_type as STORE_TYPE,
//       1 AS TABLETYPE_ID -- 1= for if it is a dealer querry
//
//  from IFSAPP.INVENTORY_LOCATION aa
// inner join ifsapp.hpnret_levels_overview hh on aa.contract = hh.site_id
//  left join IFSAPP.SIN_STORE_MASTER_TAB bb on hh.site_id = bb.site_id
//  left join IFSAPP.SITE pp on hh.site_id = pp.contract
//  left join ifsapp.company_address cc on pp.delivery_address =
//                                         cc.address_id
//  left join IFSAPP.HPNRET_LEVEL_HIERARCHY yy on hh.site_id = yy.site_id
//  left join ifsapp.HPNRET_LEVEL ll on LL.level_id = YY.level_id
//  left join IFSAPP.COMPANY_COMM_METHOD mm on cc.address_id = mm.address_id  and mm.method_id = 'Mobile'
// --where aa.contract not in ('BDM01', 'BEJ01', 'BFS01', 'BIB01', 'BLP01',
//        --'FAB01', 'SAN01', 'BMJ01')
//--AND  SUBSTR(AA.objversion,1,4 ) = :PassYear
//--AND SUBSTR(AA.objversion,5,2 ) = :PassMonth
//--AND SUBSTR(AA.objversion,7,2 ) = :PassDate
// group by aa.contract,
//          
//          pp.description,
//          hh.Channel_id,
//          hh.Channel,
//          hh.Area_id,
//          hh.Area,
//          hh.District_id,
//          hh.District,
//          hh.site_id,
//          hh.branch,
//          cc.address1,
//          cc.address2,
//          LL.hpnret_level_type,
//          BB.STORE_OPEN_DATE,
//          bb.STORE_OPEN_HOURS,
//          bb.STORE_CLOSE_HOURS,
//          bb.STORE_CLOSE_DATE,
//          hh.channel,
//          bb.store_latitude,
//          bb.store_longitude,
//          bb.store_province,
//          bb.store_district,
//          bb.store_last_renovated_dt,
//          hh.site_id,
//          hh.site_id,
//          mm.value,
//          bb.store_group,
//          bb.store_default_dc_code,
//          bb.store_no_parking,
//          bb.store_no_floors,
//          bb.store_sequre_feet,
//          ll.provincial_council_id,
//          ll.district_id,
//          pp.site_type,
//          PP.primary_supp
//
//UNION
//
//SELECT KK.customer_id as SITE_ID,
//       IFSAPP.CUSTOMER_INFO_API.GET_NAME(KK.customer_id) as SITE_NAME,
//       IndirectOverV.Channel_Id AS CHANNEL_ID,
//       IndirectOverV.Channel AS CHANNEL_NAME,
//       IndirectOverV.Area_Id AS AREA_ID,
//       IndirectOverV.Area AS AREA_NAME,
//       IndirectOverV.Zone_Id as DISTRICT_ID,
//       IndirectOverV.Zone as DISTRICT_NAME,
//       'INDIRECT' AS AGENCY_ID,
//       'SSL' as COMPANY_ID,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   KK.customer_id,
//                                                   'Customer',
//                                                   'LATITUDE') as STORE_LATITUDE,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   KK.customer_id,
//                                                   'Customer',
//                                                   'LONGITUDE') as STORE_LONGITUDE,
//       'DE' as TYPE_ID,
//       'Dealer' AS TYPE_NAME,
//       '' as PROVINCE_ID,
//       '' AS PROVINCE_NAME,
//       '0' AS MOBILE_NO,
//       '' as STORE_PROVINCE,
//       '' AS QOS,
//       0 AS CAPACITIES,
//       '' AS AUDIT_GRADING,
//       '' AS STORE_ATTRIBUTE_1,
//       '' AS STORE_ATTRIBUTE_2,
//       '' AS STORE_ATTRIBUTE_3,
//       '' AS STORE_ATTRIBUTE_4,
//       '' AS STORE_ATTRIBUTE_5,
//       '' as PRIMARY_WAREHOUSE,
//       0 as FLOAT_AMT,
//       '' as SL_DISTRICT_ID,
//       '' as SL_DISTRICT_NAME,
//       'DEALER' as STORE_TYPE,
//       2 AS TABLETYPE_ID -- 1= for if it is a dealer querry
//  FROM IFSAPP.HPNRET_LEVEL_OVERVIEW_ZONE IndirectOverV
// inner join IFSAPP.CUST_ORD_CUSTOMER_ENT KK ON KK.MARKET_CODE =
//                                               IndirectOverV.ZONE_ID
// --WHERE --SUBSTR(KK.objversion, 1, 4) =  :PassYear
//--AND SUBSTR(KK.objversion, 5, 2) =  :PassMonth
//--AND SUBSTR(KK.objversion, 7, 2) = :PassDate
//--AND 
//---- SUBSTR(KK.customer_id, 6, 1) NOT IN ('P') --NEWLY ADDED 2019-05-17  ----commented said by chathura 17th Oct
//--AND 
//--SUBSTR(KK.customer_id,1, 2) NOT IN ('DM')  ---remove dmd dealers from here 
//               ";

                #endregion

                #region Sql 2020/05/29

//                selectQuery = @"
//       select aa.contract as SITE_ID,
//       pp.description as SITE_NAME,
//       hh.Channel_id AS CHANNEL_ID,
//       hh.Channel AS CHANNEL_NAME,
//       hh.Area_id AS AREA_ID,
//       hh.Area AS AREA_NAME,
//       hh.District_id AS DISTRICT_ID,
//       hh.District AS DISTRICT_NAME,
//       'DIRECT' AS AGENCY_ID,
//       'SSL' as COMPANY_ID,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   aa.contract,
//                                                   'Customer',
//                                                   'LATITUDE') as STORE_LATITUDE,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   aa.contract,
//                                                   'Customer',
//                                                   'LONGITUDE') as STORE_LONGITUDE,
//       case
//         when LL.hpnret_level_type = 'Branch' then
//          'BM'
//         when LL.hpnret_level_type = 'Approve Dealer' then
//          'AD'
//       end as TYPE_ID,
//       LL.hpnret_level_type AS TYPE_NAME,
//       ll.provincial_council_id as PROVINCE_ID,
//       IFSAPP.HPNRET_PVN_COUNCIL_api.Get_Description(ll.provincial_council_id) AS PROVINCE_NAME,
//       mm.value AS MOBILE_NO,
//       '' as STORE_PROVINCE,
//       '' AS QOS,
//       (SELECT SUM(B.SQUAREAREA) AS CAPACITIES
//          FROM ifsapp.SIN_RENT_BUILDINGMAIN B
//         WHERE B.STATUS = 'A'
//           AND B.SITEID = aa.contract) AS CAPACITIES,
//       '' AS AUDIT_GRADING,
//       
//       CASE 
//           WHEN (SELECT COUNT(*) FROM IFSAPP.SIN_CLOSED_SHOPS CS WHERE CS.SITE_ID = aa.contract) > 0 THEN 'INACTIVE'
//       END AS STORE_ATTRIBUTE_1,
//       
//       '' AS STORE_ATTRIBUTE_2,
//       '' AS STORE_ATTRIBUTE_3,
//       '' AS STORE_ATTRIBUTE_4,
//       '' AS STORE_ATTRIBUTE_5,
//       PP.primary_supp as PRIMARY_WAREHOUSE,
//       
//        (
//          select sum(nn.reserve_open_amount) AS FLOAT_AMT 
//           from ifsapp.ssl_open_amt_res_dtl nn 
//           where nn.site_code = aa.contract 
//           AND to_date(nn.valid_to, 'YYYY/MM/DD') >= trunc(sysdate)
//        ) AS FLOAT_AMT ,
//       
//       ll.district_id as SL_DISTRICT_ID,
//       IFSAPP.HPNRET_S_L_DISTRICTS_api.Get_Description(ll.provincial_council_id,
//                                                       ll.district_id) as SL_DISTRICT_NAME,
//       pp.site_type as STORE_TYPE,
//       1 AS TABLETYPE_ID -- 1= for if it is a dealer querry
//
//  from IFSAPP.INVENTORY_LOCATION aa
// inner join ifsapp.hpnret_levels_overview hh on aa.contract = hh.site_id
//  left join IFSAPP.SIN_STORE_MASTER_TAB bb on hh.site_id = bb.site_id
//  left join IFSAPP.SITE pp on hh.site_id = pp.contract
//  left join ifsapp.company_address cc on pp.delivery_address =
//                                         cc.address_id
//  left join IFSAPP.HPNRET_LEVEL_HIERARCHY yy on hh.site_id = yy.site_id
//  left join ifsapp.HPNRET_LEVEL ll on LL.level_id = YY.level_id
//  left join IFSAPP.COMPANY_COMM_METHOD mm on cc.address_id = mm.address_id  and mm.method_id = 'Mobile'
// --where aa.contract not in ('BDM01', 'BEJ01', 'BFS01', 'BIB01', 'BLP01',
//        --'FAB01', 'SAN01', 'BMJ01')
//--AND  SUBSTR(AA.objversion,1,4 ) = :PassYear
//--AND SUBSTR(AA.objversion,5,2 ) = :PassMonth
//--AND SUBSTR(AA.objversion,7,2 ) = :PassDate
// group by aa.contract,
//          
//          pp.description,
//          hh.Channel_id,
//          hh.Channel,
//          hh.Area_id,
//          hh.Area,
//          hh.District_id,
//          hh.District,
//          hh.site_id,
//          hh.branch,
//          cc.address1,
//          cc.address2,
//          LL.hpnret_level_type,
//          BB.STORE_OPEN_DATE,
//          bb.STORE_OPEN_HOURS,
//          bb.STORE_CLOSE_HOURS,
//          bb.STORE_CLOSE_DATE,
//          hh.channel,
//          bb.store_latitude,
//          bb.store_longitude,
//          bb.store_province,
//          bb.store_district,
//          bb.store_last_renovated_dt,
//          hh.site_id,
//          hh.site_id,
//          mm.value,
//          bb.store_group,
//          bb.store_default_dc_code,
//          bb.store_no_parking,
//          bb.store_no_floors,
//          bb.store_sequre_feet,
//          ll.provincial_council_id,
//          ll.district_id,
//          pp.site_type,
//          PP.primary_supp
//
//UNION
//
//SELECT KK.customer_id as SITE_ID,
//       IFSAPP.CUSTOMER_INFO_API.GET_NAME(KK.customer_id) as SITE_NAME,
//       IndirectOverV.Channel_Id AS CHANNEL_ID,
//       IndirectOverV.Channel AS CHANNEL_NAME,
//       IndirectOverV.Area_Id AS AREA_ID,
//       IndirectOverV.Area AS AREA_NAME,
//       IndirectOverV.Zone_Id as DISTRICT_ID,
//       IndirectOverV.Zone as DISTRICT_NAME,
//       'INDIRECT' AS AGENCY_ID,
//       'SSL' as COMPANY_ID,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   KK.customer_id,
//                                                   'Customer',
//                                                   'LATITUDE') as STORE_LATITUDE,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   KK.customer_id,
//                                                   'Customer',
//                                                   'LONGITUDE') as STORE_LONGITUDE,
//       'DE' as TYPE_ID,
//       'Dealer' AS TYPE_NAME,
//       '' as PROVINCE_ID,
//       '' AS PROVINCE_NAME,
//       '0' AS MOBILE_NO,
//       '' as STORE_PROVINCE,
//       '' AS QOS,
//       0 AS CAPACITIES,
//       '' AS AUDIT_GRADING,
//       '' AS STORE_ATTRIBUTE_1,
//       '' AS STORE_ATTRIBUTE_2,
//       '' AS STORE_ATTRIBUTE_3,
//       '' AS STORE_ATTRIBUTE_4,
//       '' AS STORE_ATTRIBUTE_5,
//       NVL((SELECT W.PRIMARY_WH FROM IFSAPP.SIN_WH_FOR_STORE_DEALER W WHERE W.DEALER = KK.customer_id), '') as PRIMARY_WAREHOUSE,
//       0 as FLOAT_AMT,
//       '' as SL_DISTRICT_ID,
//       '' as SL_DISTRICT_NAME,
//       'DEALER' as STORE_TYPE,
//       2 AS TABLETYPE_ID -- 1= for if it is a dealer querry
//  FROM IFSAPP.HPNRET_LEVEL_OVERVIEW_ZONE IndirectOverV
// inner join IFSAPP.CUST_ORD_CUSTOMER_ENT KK ON KK.MARKET_CODE =
//                                               IndirectOverV.ZONE_ID
// --WHERE --SUBSTR(KK.objversion, 1, 4) =  :PassYear
//--AND SUBSTR(KK.objversion, 5, 2) =  :PassMonth
//--AND SUBSTR(KK.objversion, 7, 2) = :PassDate
//--AND 
//---- SUBSTR(KK.customer_id, 6, 1) NOT IN ('P') --NEWLY ADDED 2019-05-17  ----commented said by chathura 17th Oct
//--AND 
//--SUBSTR(KK.customer_id,1, 2) NOT IN ('DM')  ---remove dmd dealers from here 
//               ";

                #endregion

                #region Sql 2020/07/29

//                selectQuery = @"
//       select aa.contract as SITE_ID,
//       pp.description as SITE_NAME,
//       hh.Channel_id AS CHANNEL_ID,
//       hh.Channel AS CHANNEL_NAME,
//       hh.Area_id AS AREA_ID,
//       hh.Area AS AREA_NAME,
//       hh.District_id AS DISTRICT_ID,
//       hh.District AS DISTRICT_NAME,
//       'DIRECT' AS AGENCY_ID,
//       'SSL' as COMPANY_ID,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   aa.contract,
//                                                   'Customer',
//                                                   'LATITUDE') as STORE_LATITUDE,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   aa.contract,
//                                                   'Customer',
//                                                   'LONGITUDE') as STORE_LONGITUDE,
//       case
//         when LL.hpnret_level_type = 'Branch' then
//          'BM'
//         when LL.hpnret_level_type = 'Approve Dealer' then
//          'AD'
//       end as TYPE_ID,
//       LL.hpnret_level_type AS TYPE_NAME,
//       ll.provincial_council_id as PROVINCE_ID,
//       IFSAPP.HPNRET_PVN_COUNCIL_api.Get_Description(ll.provincial_council_id) AS PROVINCE_NAME,
//       mm.value AS MOBILE_NO,
//       '' as STORE_PROVINCE,
//       '' AS QOS,
       
//       (SELECT SUM(B.SQUAREAREA) AS CAPACITIES
//          FROM ifsapp.SIN_RENT_BUILDINGMAIN B
//         WHERE B.STATUS = 'A'
//           AND B.SITEID = aa.contract
//           AND B.CONTRACTEND >= to_char(SYSDATE,'yyyy/mm/dd')) AS CAPACITIES,
           
//       '' AS AUDIT_GRADING,
       
//       CASE 
//           WHEN (SELECT COUNT(*) FROM IFSAPP.SIN_CLOSED_SHOPS CS WHERE CS.SITE_ID = aa.contract) > 0 THEN 'INACTIVE'
//       END AS STORE_ATTRIBUTE_1,
       
//       '' AS STORE_ATTRIBUTE_2,
//       '' AS STORE_ATTRIBUTE_3,
//       '' AS STORE_ATTRIBUTE_4,
//       '' AS STORE_ATTRIBUTE_5,
//       PP.primary_supp as PRIMARY_WAREHOUSE,
       
//        (
//          select sum(nn.reserve_open_amount) AS FLOAT_AMT 
//           from ifsapp.ssl_open_amt_res_dtl nn 
//           where nn.site_code = aa.contract 
//           AND to_date(nn.valid_to, 'YYYY/MM/DD') >= trunc(sysdate)
//        ) AS FLOAT_AMT ,
       
//       ll.district_id as SL_DISTRICT_ID,
//       IFSAPP.HPNRET_S_L_DISTRICTS_api.Get_Description(ll.provincial_council_id,
//                                                       ll.district_id) as SL_DISTRICT_NAME,
//       pp.site_type as STORE_TYPE,
//       1 AS TABLETYPE_ID -- 1= for if it is a dealer querry

//  from IFSAPP.INVENTORY_LOCATION aa
// inner join ifsapp.hpnret_levels_overview hh on aa.contract = hh.site_id
//  left join IFSAPP.SIN_STORE_MASTER_TAB bb on hh.site_id = bb.site_id
//  left join IFSAPP.SITE pp on hh.site_id = pp.contract
//  left join ifsapp.company_address cc on pp.delivery_address =
//                                         cc.address_id
//  left join IFSAPP.HPNRET_LEVEL_HIERARCHY yy on hh.site_id = yy.site_id
//  left join ifsapp.HPNRET_LEVEL ll on LL.level_id = YY.level_id
//  left join IFSAPP.COMPANY_COMM_METHOD mm on cc.address_id = mm.address_id  and mm.method_id = 'Mobile'
// --where aa.contract not in ('BDM01', 'BEJ01', 'BFS01', 'BIB01', 'BLP01',
//        --'FAB01', 'SAN01', 'BMJ01')
//--AND  SUBSTR(AA.objversion,1,4 ) = :PassYear
//--AND SUBSTR(AA.objversion,5,2 ) = :PassMonth
//--AND SUBSTR(AA.objversion,7,2 ) = :PassDate
// group by aa.contract,
          
//          pp.description,
//          hh.Channel_id,
//          hh.Channel,
//          hh.Area_id,
//          hh.Area,
//          hh.District_id,
//          hh.District,
//          hh.site_id,
//          hh.branch,
//          cc.address1,
//          cc.address2,
//          LL.hpnret_level_type,
//          BB.STORE_OPEN_DATE,
//          bb.STORE_OPEN_HOURS,
//          bb.STORE_CLOSE_HOURS,
//          bb.STORE_CLOSE_DATE,
//          hh.channel,
//          bb.store_latitude,
//          bb.store_longitude,
//          bb.store_province,
//          bb.store_district,
//          bb.store_last_renovated_dt,
//          hh.site_id,
//          hh.site_id,
//          mm.value,
//          bb.store_group,
//          bb.store_default_dc_code,
//          bb.store_no_parking,
//          bb.store_no_floors,
//          bb.store_sequre_feet,
//          ll.provincial_council_id,
//          ll.district_id,
//          pp.site_type,
//          PP.primary_supp

//UNION

//SELECT KK.customer_id as SITE_ID,
//       IFSAPP.CUSTOMER_INFO_API.GET_NAME(KK.customer_id) as SITE_NAME,
//       IndirectOverV.Channel_Id AS CHANNEL_ID,
//       IndirectOverV.Channel AS CHANNEL_NAME,
//       IndirectOverV.Area_Id AS AREA_ID,
//       IndirectOverV.Area AS AREA_NAME,
//       IndirectOverV.Zone_Id as DISTRICT_ID,
//       IndirectOverV.Zone as DISTRICT_NAME,
//       'INDIRECT' AS AGENCY_ID,
//       'SSL' as COMPANY_ID,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   KK.customer_id,
//                                                   'Customer',
//                                                   'LATITUDE') as STORE_LATITUDE,
//       IFSAPP.party_type_id_property_api.get_value('SSL',
//                                                   KK.customer_id,
//                                                   'Customer',
//                                                   'LONGITUDE') as STORE_LONGITUDE,
//       'DE' as TYPE_ID,
//       'Dealer' AS TYPE_NAME,
//       '' as PROVINCE_ID,
//       '' AS PROVINCE_NAME,
//       '0' AS MOBILE_NO,
//       '' as STORE_PROVINCE,
//       '' AS QOS,
//       0 AS CAPACITIES,
//       '' AS AUDIT_GRADING,
//       '' AS STORE_ATTRIBUTE_1,
//       '' AS STORE_ATTRIBUTE_2,
//       '' AS STORE_ATTRIBUTE_3,
//       '' AS STORE_ATTRIBUTE_4,
//       '' AS STORE_ATTRIBUTE_5,
//       NVL((SELECT W.PRIMARY_WH FROM IFSAPP.SIN_WH_FOR_STORE_DEALER W WHERE W.DEALER = KK.customer_id), '') as PRIMARY_WAREHOUSE,
//       0 as FLOAT_AMT,
//       '' as SL_DISTRICT_ID,
//       '' as SL_DISTRICT_NAME,
//       'DEALER' as STORE_TYPE,
//       2 AS TABLETYPE_ID -- 1= for if it is a dealer querry
//  FROM IFSAPP.HPNRET_LEVEL_OVERVIEW_ZONE IndirectOverV
// inner join IFSAPP.CUST_ORD_CUSTOMER_ENT KK ON KK.MARKET_CODE =
//                                               IndirectOverV.ZONE_ID
// --WHERE --SUBSTR(KK.objversion, 1, 4) =  :PassYear
//--AND SUBSTR(KK.objversion, 5, 2) =  :PassMonth
//--AND SUBSTR(KK.objversion, 7, 2) = :PassDate
//--AND 
//---- SUBSTR(KK.customer_id, 6, 1) NOT IN ('P') --NEWLY ADDED 2019-05-17  ----commented said by chathura 17th Oct
//--AND 
//--SUBSTR(KK.customer_id,1, 2) NOT IN ('DM')  ---remove dmd dealers from here 
//               ";

                #endregion

                #region Sql 2020/10/05

                selectQuery = @"
 --MODULE NAME:  BI_DIM_STORE
--DESCRIPTION:  'BI_DIM_STORE' SINGER SELECT QUERY
--CREATED BY:   DINIDUR
--CREATED DATE: 2020/10/05
--UPDATED BY:   DINIDUR
--UPDATED DATE: 2020/10/05

      select aa.contract as SITE_ID,
       pp.description as SITE_NAME,
       hh.Channel_id AS CHANNEL_ID,
       hh.Channel AS CHANNEL_NAME,
       hh.Area_id AS AREA_ID,
       hh.Area AS AREA_NAME,
       hh.District_id AS DISTRICT_ID,
       hh.District AS DISTRICT_NAME,
       'DIRECT' AS AGENCY_ID,
       'SSL' as COMPANY_ID,
       IFSAPP.party_type_id_property_api.get_value('SSL',
                                                   aa.contract,
                                                   'Customer',
                                                   'LATITUDE') as STORE_LATITUDE,
       IFSAPP.party_type_id_property_api.get_value('SSL',
                                                   aa.contract,
                                                   'Customer',
                                                   'LONGITUDE') as STORE_LONGITUDE,
       case
         when LL.hpnret_level_type = 'Branch' then
          'BM'
         when LL.hpnret_level_type = 'Approve Dealer' then
          'AD'
       end as TYPE_ID,
       LL.hpnret_level_type AS TYPE_NAME,
       ll.provincial_council_id as PROVINCE_ID,
       IFSAPP.HPNRET_PVN_COUNCIL_api.Get_Description(ll.provincial_council_id) AS PROVINCE_NAME,
       mm.value AS MOBILE_NO,
       '' as STORE_PROVINCE,
       '' AS QOS,
       
       (SELECT SUM(B.SQUAREAREA) AS CAPACITIES
          FROM ifsapp.SIN_RENT_BUILDINGMAIN B
         WHERE B.STATUS = 'A'
           AND B.SITEID = aa.contract
           AND B.CONTRACTEND >= to_char(SYSDATE,'yyyy/mm/dd')) AS CAPACITIES,
           
       '' AS AUDIT_GRADING,
       
       CASE 
           WHEN (SELECT COUNT(*) FROM IFSAPP.SIN_CLOSED_SHOPS CS WHERE CS.SITE_ID = aa.contract) > 0 THEN 'INACTIVE'
       END AS STORE_ATTRIBUTE_1,
       
       '' AS STORE_ATTRIBUTE_2,
       '' AS STORE_ATTRIBUTE_3,
       '' AS STORE_ATTRIBUTE_4,
       '' AS STORE_ATTRIBUTE_5,
       PP.primary_supp as PRIMARY_WAREHOUSE,
       
        (
          select sum(nn.reserve_open_amount) AS FLOAT_AMT 
           from ifsapp.ssl_open_amt_res_dtl nn 
           where nn.site_code = aa.contract 
           AND to_date(nn.valid_to, 'YYYY/MM/DD') >= trunc(sysdate)
        ) AS FLOAT_AMT ,
       
       ll.district_id as SL_DISTRICT_ID,
       IFSAPP.HPNRET_S_L_DISTRICTS_api.Get_Description(ll.provincial_council_id,
                                                       ll.district_id) as SL_DISTRICT_NAME,
   
        case
         when SUBSTR(aa.contract,0,1)='K' then 'BRANCH'
        else pp.site_type 
       end as STORE_TYPE,
       
      
       
       1 AS TABLETYPE_ID -- 1= for if it is a dealer querry

  from IFSAPP.INVENTORY_LOCATION aa
 inner join ifsapp.hpnret_levels_overview hh on aa.contract = hh.site_id
  left join IFSAPP.SIN_STORE_MASTER_TAB bb on hh.site_id = bb.site_id
  left join IFSAPP.SITE pp on hh.site_id = pp.contract
  left join ifsapp.company_address cc on pp.delivery_address =
                                         cc.address_id
  left join IFSAPP.HPNRET_LEVEL_HIERARCHY yy on hh.site_id = yy.site_id
  left join ifsapp.HPNRET_LEVEL ll on LL.level_id = YY.level_id
  left join IFSAPP.COMPANY_COMM_METHOD mm on cc.address_id = mm.address_id  and mm.method_id = 'Mobile'
 --where aa.contract not in ('BDM01', 'BEJ01', 'BFS01', 'BIB01', 'BLP01',
        --'FAB01', 'SAN01', 'BMJ01')
--AND  SUBSTR(AA.objversion,1,4 ) = :PassYear
--AND SUBSTR(AA.objversion,5,2 ) = :PassMonth
--AND SUBSTR(AA.objversion,7,2 ) = :PassDate
 group by aa.contract,
          
          pp.description,
          hh.Channel_id,
          hh.Channel,
          hh.Area_id,
          hh.Area,
          hh.District_id,
          hh.District,
          hh.site_id,
          hh.branch,
          cc.address1,
          cc.address2,
          LL.hpnret_level_type,
          BB.STORE_OPEN_DATE,
          bb.STORE_OPEN_HOURS,
          bb.STORE_CLOSE_HOURS,
          bb.STORE_CLOSE_DATE,
          hh.channel,
          bb.store_latitude,
          bb.store_longitude,
          bb.store_province,
          bb.store_district,
          bb.store_last_renovated_dt,
          hh.site_id,
          hh.site_id,
          mm.value,
          bb.store_group,
          bb.store_default_dc_code,
          bb.store_no_parking,
          bb.store_no_floors,
          bb.store_sequre_feet,
          ll.provincial_council_id,
          ll.district_id,
          pp.site_type,
          PP.primary_supp

UNION

SELECT KK.customer_id as SITE_ID,
       IFSAPP.CUSTOMER_INFO_API.GET_NAME(KK.customer_id) as SITE_NAME,
       IndirectOverV.Channel_Id AS CHANNEL_ID,
       IndirectOverV.Channel AS CHANNEL_NAME,
       IndirectOverV.Area_Id AS AREA_ID,
       IndirectOverV.Area AS AREA_NAME,
       IndirectOverV.Zone_Id as DISTRICT_ID,
       IndirectOverV.Zone as DISTRICT_NAME,
       'INDIRECT' AS AGENCY_ID,
       'SSL' as COMPANY_ID,
       IFSAPP.party_type_id_property_api.get_value('SSL',
                                                   KK.customer_id,
                                                   'Customer',
                                                   'LATITUDE') as STORE_LATITUDE,
       IFSAPP.party_type_id_property_api.get_value('SSL',
                                                   KK.customer_id,
                                                   'Customer',
                                                   'LONGITUDE') as STORE_LONGITUDE,
       'DE' as TYPE_ID,
       'Dealer' AS TYPE_NAME,
       '' as PROVINCE_ID,
       '' AS PROVINCE_NAME,
       '0' AS MOBILE_NO,
       '' as STORE_PROVINCE,
       '' AS QOS,
       0 AS CAPACITIES,
       '' AS AUDIT_GRADING,
       '' AS STORE_ATTRIBUTE_1,
       '' AS STORE_ATTRIBUTE_2,
       '' AS STORE_ATTRIBUTE_3,
       '' AS STORE_ATTRIBUTE_4,
       '' AS STORE_ATTRIBUTE_5,
       NVL((SELECT W.PRIMARY_WH FROM IFSAPP.SIN_WH_FOR_STORE_DEALER W WHERE W.DEALER = KK.customer_id), '') as PRIMARY_WAREHOUSE,
       0 as FLOAT_AMT,
       '' as SL_DISTRICT_ID,
       '' as SL_DISTRICT_NAME,
       'DEALER' as STORE_TYPE,
       2 AS TABLETYPE_ID -- 1= for if it is a dealer querry
  FROM IFSAPP.HPNRET_LEVEL_OVERVIEW_ZONE IndirectOverV
 inner join IFSAPP.CUST_ORD_CUSTOMER_ENT KK ON KK.MARKET_CODE =
                                               IndirectOverV.ZONE_ID
 --WHERE --SUBSTR(KK.objversion, 1, 4) =  :PassYear
--AND SUBSTR(KK.objversion, 5, 2) =  :PassMonth
--AND SUBSTR(KK.objversion, 7, 2) = :PassDate
--AND 
---- SUBSTR(KK.customer_id, 6, 1) NOT IN ('P') --NEWLY ADDED 2019-05-17  ----commented said by chathura 17th Oct
--AND 
--SUBSTR(KK.customer_id,1, 2) NOT IN ('DM')  ---remove dmd dealers from here 
               
               ";

                #endregion

                oDataTable = new DataTable();
                //OracleParameter[] param = new OracleParameter[] 
                //{ 
                //   DBConn.AddParameter("PassYear",dtPreviousDate.Year)
                //   ,DBConn.AddParameter("PassMonth",dtPreviousDate.Month)
                //   ,DBConn.AddParameter("PassDate",dtPreviousDate.Day)
                //};

                //oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);

                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable GetStore_DMD(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionDMD_MANTHAN();
            string selectQuery = string.Empty;

            try
            {

                #region Sql
                selectQuery = @"
       select aa.contract as SITE_ID,
       pp.description as SITE_NAME,
       hh.Channel_id AS CHANNEL_ID,
       hh.Channel AS CHANNEL_NAME,
       hh.Area_id AS AREA_ID,
       hh.Area AS AREA_NAME,
       hh.District_id AS DISTRICT_ID,
       hh.District AS DISTRICT_NAME,
       'INDIRECT' AS AGENCY_ID,
       'DGM' as COMPANY_ID,
       IFSAPP.party_type_id_property_api.get_value('SSL',
                                                   aa.contract,
                                                   'Customer',
                                                   'LATITUDE') as STORE_LATITUDE,
       IFSAPP.party_type_id_property_api.get_value('SSL',
                                                   aa.contract,
                                                   'Customer',
                                                   'LONGITUDE') as STORE_LONGITUDE,
       case
         when LL.hpnret_level_type = 'Branch' then
          'BM'
         when LL.hpnret_level_type = 'Approve Dealer' then
          'AD'
       end as TYPE_ID,
       LL.hpnret_level_type AS TYPE_NAME,
       ll.provincial_council_id as PROVINCE_ID,
       IFSAPP.HPNRET_PVN_COUNCIL_api.Get_Description(ll.provincial_council_id) AS PROVINCE_NAME,
       mm.value AS MOBILE_NO,
       '' as STORE_PROVINCE,
       '' AS QOS,
       /*(SELECT SUM(B.SQUAREAREA) AS CAPACITIES
          FROM ifsapp.SIN_RENT_BUILDINGMAIN B
         WHERE B.STATUS = 'A'
           AND B.SITEID = aa.contract) AS CAPACITIES,*/
           0 AS CAPACITIES,
       '' AS AUDIT_GRADING,
       '' AS STORE_ATTRIBUTE_1,
       '' AS STORE_ATTRIBUTE_2,
       '' AS STORE_ATTRIBUTE_3,
       '' AS STORE_ATTRIBUTE_4,
       '' AS STORE_ATTRIBUTE_5,
       PP.primary_supp as PRIMARY_WAREHOUSE,
       (
          select sum(nn.reserve_open_amount) AS FLOAT_AMT 
           from ifsapp.ssl_open_amt_res_dtl nn 
           where nn.site_code = aa.contract 
           AND to_date(nn.valid_to, 'YYYY/MM/DD') >= trunc(sysdate)
        ) AS FLOAT_AMT ,
       ll.district_id as SL_DISTRICT_ID,
       IFSAPP.HPNRET_S_L_DISTRICTS_api.Get_Description(ll.provincial_council_id,
                                                       ll.district_id) as SL_DISTRICT_NAME,
       pp.site_type as STORE_TYPE,
       1 AS TABLETYPE_ID -- 1= for if it is a dealer querry

  from IFSAPP.INVENTORY_LOCATION aa
 inner join ifsapp.hpnret_levels_overview hh on aa.contract = hh.site_id
  left join IFSAPP.SIN_STORE_MASTER_TAB bb on hh.site_id = bb.site_id
  left join IFSAPP.SITE pp on hh.site_id = pp.contract
  left join ifsapp.company_address cc on pp.delivery_address =
                                         cc.address_id
  left join IFSAPP.HPNRET_LEVEL_HIERARCHY yy on hh.site_id = yy.site_id
  left join ifsapp.HPNRET_LEVEL ll on LL.level_id = YY.level_id
  left join IFSAPP.COMPANY_COMM_METHOD mm on cc.address_id = mm.address_id and mm.method_id = 'Mobile'
 where aa.contract not in ('BDM01', 'BEJ01', 'BFS01', 'BIB01', 'BLP01',
        'FAB01', 'SAN01', 'BMJ01')
--AND  SUBSTR(AA.objversion,1,4 ) = :PassYear
--AND SUBSTR(AA.objversion,5,2 ) = :PassMonth
--AND SUBSTR(AA.objversion,7,2 ) = :PassDate
 group by aa.contract,
          
          pp.description,
          hh.Channel_id,
          hh.Channel,
          hh.Area_id,
          hh.Area,
          hh.District_id,
          hh.District,
          hh.site_id,
          hh.branch,
          cc.address1,
          cc.address2,
          LL.hpnret_level_type,
          BB.STORE_OPEN_DATE,
          bb.STORE_OPEN_HOURS,
          bb.STORE_CLOSE_HOURS,
          bb.STORE_CLOSE_DATE,
          hh.channel,
          bb.store_latitude,
          bb.store_longitude,
          bb.store_province,
          bb.store_district,
          bb.store_last_renovated_dt,
          hh.site_id,
          hh.site_id,
          mm.value,
          bb.store_group,
          bb.store_default_dc_code,
          bb.store_no_parking,
          bb.store_no_floors,
          bb.store_sequre_feet,
          ll.provincial_council_id,
          ll.district_id,
          pp.site_type,
          PP.primary_supp

                UNION
                
                
SELECT KK.customer_id as SITE_ID,
       IFSAPP.CUSTOMER_INFO_API.GET_NAME(KK.customer_id) as SITE_NAME,
       IndirectOverV.Channel_Id AS CHANNEL_ID,
       IndirectOverV.Channel AS CHANNEL_NAME,
       IndirectOverV.Area_Id AS AREA_ID,
       IndirectOverV.Area AS AREA_NAME,
       IndirectOverV.Zone_Id as DISTRICT_ID,
       IndirectOverV.Zone as DISTRICT_NAME,
       'INDIRECT' AS AGENCY_ID,
       'DGM' as COMPANY_ID,
       IFSAPP.party_type_id_property_api.get_value('SSL',
                                                   KK.customer_id,
                                                   'Customer',
                                                   'LATITUDE') as STORE_LATITUDE,
       IFSAPP.party_type_id_property_api.get_value('SSL',
                                                   KK.customer_id,
                                                   'Customer',
                                                   'LONGITUDE') as STORE_LONGITUDE,
       'DE' as TYPE_ID,
       'Dealer' AS TYPE_NAME,
       '' as PROVINCE_ID,
       '' AS PROVINCE_NAME,
       '0' AS MOBILE_NO,
       '' as STORE_PROVINCE,
       '' AS QOS,
       0 AS CAPACITIES,
       '' AS AUDIT_GRADING,
       '' AS STORE_ATTRIBUTE_1,
       '' AS STORE_ATTRIBUTE_2,
       '' AS STORE_ATTRIBUTE_3,
       '' AS STORE_ATTRIBUTE_4,
       '' AS STORE_ATTRIBUTE_5,
       '' as PRIMARY_WAREHOUSE,
       0 as FLOAT_AMT,
       '' as SL_DISTRICT_ID,
       '' as SL_DISTRICT_NAME,
       'DEALER' as STORE_TYPE,
       2 AS TABLETYPE_ID -- 1= for if it is a dealer querry
  FROM IFSAPP.HPNRET_LEVEL_OVERVIEW_ZONE IndirectOverV
 inner join IFSAPP.CUST_ORD_CUSTOMER_ENT KK ON KK.MARKET_CODE =
                                               IndirectOverV.ZONE_ID
 --WHERE  SUBSTR(KK.customer_id, 6, 1) NOT IN ('P') --NEWLY ADDED 2019-05-17  --Said by chathura

                ";
                #endregion

                oDataTable = new DataTable();
                //OracleParameter[] param = new OracleParameter[] 
                //{ 
                //   DBConn.AddParameter("PassYear",dtPreviousDate.Year)
                //   ,DBConn.AddParameter("PassMonth",dtPreviousDate.Month)
                //   ,DBConn.AddParameter("PassDate",dtPreviousDate.Day)
                //};

                //oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);

                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool CheckStoreCodeExsisIN_BI_NRTAN_ServerAndInsertTo_NRTAN_Server(Store objStore)
        {
            bool IsSave = false;
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            #region Sql
            string stringQuerry = @"
                    SELECT COUNT(t1.SITE_ID) AS CheckCount 
                    FROM NRTAN.BI_DIM_STORE  t1 
                    where t1.SITE_ID = :SITE_ID
                ";
            #endregion
            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("SITE_ID",objStore.SITE_ID) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertStoreTo_BI_NRTANServerIncrementalProcess(objStore);

                    }
                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objStore, ex.StackTrace), "Error - In CheckStoreCodeExsisIN_BI_NRTAN_ServerAndInsertTo_NRTAN_Server()", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            return IsSave;
        }

        public bool InsertStoreTo_BI_NRTANServerIncrementalProcess(Store objStore)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;
            #region Sql
            string stringQuerry = @"
                 
                 insert into NRTAN.bi_dim_store
  (
   site_id,
   site_name,
   channel_id,
   channel_name,
   area_id,
   area_name,
   DISTRICT_ID,
   DISTRICT_NAME,
   agency_id,
   company_id,
   store_latitude,
   store_longitude,
   BRANCH_TYPE,
   province_id,
   province_name,
   mobile_no,
   qos,
   capacities,
AUDIT_GRADING,
STORE_ATTRIBUTE_1,
STORE_ATTRIBUTE_2,
STORE_ATTRIBUTE_3,
STORE_ATTRIBUTE_4,
STORE_ATTRIBUTE_5
,PRIMARY_WAREHOUSE
,FLOAT_AMT
,SL_DISTRICT_ID
,SL_DISTRICT_NAME
,STORE_TYPE
    )
values
  (
   :SITE_ID,
   :SITE_NAME,
   :CHANNEL_ID,
   :CHANNEL_NAME,
   :AREA_ID,
   :AREA_NAME,
   :DISTRICT_ID,
   :DISTRICT_NAME,
   :AGENCY_ID,
   :COMPANY_ID,
   :STORE_LATITUDE,
   :STORE_LONGITUDE,
   :TYPE_NAME,
   
   :PROVINCE_ID,
   :PROVINCE_NAME,
   :MOBILE_NO,
   :QOS,
   :CAPACITIES,
:AUDIT_GRADING,
:STORE_ATTRIBUTE_1,
:STORE_ATTRIBUTE_2,
:STORE_ATTRIBUTE_3,
:STORE_ATTRIBUTE_4,
:STORE_ATTRIBUTE_5
,:PRIMARY_WAREHOUSE
,:FLOAT_AMT
,:SL_DISTRICT_ID
,:SL_DISTRICT_NAME
,:STORE_TYPE
   )
        
                ";
            #endregion


            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("SITE_ID",objStore.SITE_ID ) 
                        ,DBConn.AddParameter("SITE_NAME",objStore.SITE_NAME ) 
                        ,DBConn.AddParameter("CHANNEL_ID",objStore.CHANNEL_ID ) 
                        ,DBConn.AddParameter("CHANNEL_NAME",objStore.CHANNEL_NAME ) 
                        ,DBConn.AddParameter("AREA_ID",objStore.AREA_ID ) 
                        ,DBConn.AddParameter("AREA_NAME",objStore.AREA_NAME )
                        ,DBConn.AddParameter("DISTRICT_ID",objStore.DISTRICT_ID ) 
                        ,DBConn.AddParameter("DISTRICT_NAME",objStore.DISTRICT_NAME )
                        ,DBConn.AddParameter("AGENCY_ID",objStore.AGENCY_ID )
                        ,DBConn.AddParameter("COMPANY_ID",objStore.COMPANY_ID )
                        ,DBConn.AddParameter("STORE_LATITUDE",objStore.STORE_LATITUDE )
                        ,DBConn.AddParameter("STORE_LONGITUDE",objStore.STORE_LONGITUDE )
                        //,DBConn.AddParameter("TYPE_ID",objStore.TYPE_ID )
                        ,DBConn.AddParameter("TYPE_NAME",objStore.TYPE_NAME )
                        ,DBConn.AddParameter("PROVINCE_ID",objStore.PROVINCE_ID )
                        ,DBConn.AddParameter("PROVINCE_NAME",objStore.PROVINCE_NAME )
                        
                        ,DBConn.AddParameter("MOBILE_NO",objStore.MOBILE_NO )
                        ,DBConn.AddParameter("QOS",objStore.QOS )
                        ,DBConn.AddParameter("CAPACITIES",objStore.CAPACITIES )
                        ,DBConn.AddParameter("AUDIT_GRADING",objStore.AUDIT_GRADING )
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_1",objStore.STORE_ATTRIBUTE_1 )
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_2",objStore.STORE_ATTRIBUTE_2 )
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_3",objStore.STORE_ATTRIBUTE_3 )
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_4",objStore.STORE_ATTRIBUTE_4 )
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_5",objStore.STORE_ATTRIBUTE_5 )
                        ,DBConn.AddParameter("PRIMARY_WAREHOUSE",objStore.PRIMARY_WAREHOUSE )
                        
                         ,DBConn.AddParameter("FLOAT_AMT",objStore.FLOAT_AMT )
                         ,DBConn.AddParameter("SL_DISTRICT_ID",objStore.SL_DISTRICT_ID )
                         ,DBConn.AddParameter("SL_DISTRICT_NAME",objStore.SL_DISTRICT_NAME )
                         ,DBConn.AddParameter("STORE_TYPE",objStore.STORE_TYPE )
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objStore, ex.StackTrace), "Error - In InsertStoreTo_BI_NRTANServerIncrementalProcess()", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            return isSave;
        }

        public int InsertStoreTo_BI_NRTANServerIncrementalProcess(List<Store> lstStore)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;
            int intRecordCount = 0;

            #region Sql
            string stringQuerry = @"
                 
                 insert into NRTAN.bi_dim_store
  (
   site_id,
   site_name,
   channel_id,
   channel_name,
   area_id,
   area_name,
   DISTRICT_ID,
   DISTRICT_NAME,
   agency_id,
   company_id,
   store_latitude,
   store_longitude,
   BRANCH_TYPE,
   province_id,
   province_name,
   mobile_no,
   qos,
   capacities,
AUDIT_GRADING,
STORE_ATTRIBUTE_1,
STORE_ATTRIBUTE_2,
STORE_ATTRIBUTE_3,
STORE_ATTRIBUTE_4,
STORE_ATTRIBUTE_5
,PRIMARY_WAREHOUSE
,FLOAT_AMT
,SL_DISTRICT_ID
,SL_DISTRICT_NAME
,STORE_TYPE
    )
values
  (
   :SITE_ID,
   :SITE_NAME,
   :CHANNEL_ID,
   :CHANNEL_NAME,
   :AREA_ID,
   :AREA_NAME,
   :DISTRICT_ID,
   :DISTRICT_NAME,
   :AGENCY_ID,
   :COMPANY_ID,
   :STORE_LATITUDE,
   :STORE_LONGITUDE,
   :TYPE_NAME,
   
   :PROVINCE_ID,
   :PROVINCE_NAME,
   :MOBILE_NO,
   :QOS,
   :CAPACITIES,
:AUDIT_GRADING,
:STORE_ATTRIBUTE_1,
:STORE_ATTRIBUTE_2,
:STORE_ATTRIBUTE_3,
:STORE_ATTRIBUTE_4,
:STORE_ATTRIBUTE_5
,:PRIMARY_WAREHOUSE
,:FLOAT_AMT
,:SL_DISTRICT_ID
,:SL_DISTRICT_NAME
,:STORE_TYPE
   )
        
                ";
            #endregion

            try
            {
                foreach (Store objStore in lstStore)
                {

                    try
                    {
                        OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("SITE_ID",objStore.SITE_ID ) 
                        ,DBConn.AddParameter("SITE_NAME",objStore.SITE_NAME ) 
                        ,DBConn.AddParameter("CHANNEL_ID",objStore.CHANNEL_ID ) 
                        ,DBConn.AddParameter("CHANNEL_NAME",objStore.CHANNEL_NAME ) 
                        ,DBConn.AddParameter("AREA_ID",objStore.AREA_ID ) 
                        ,DBConn.AddParameter("AREA_NAME",objStore.AREA_NAME )
                        ,DBConn.AddParameter("DISTRICT_ID",objStore.DISTRICT_ID ) 
                        ,DBConn.AddParameter("DISTRICT_NAME",objStore.DISTRICT_NAME )
                        ,DBConn.AddParameter("AGENCY_ID",objStore.AGENCY_ID )
                        ,DBConn.AddParameter("COMPANY_ID",objStore.COMPANY_ID )
                        ,DBConn.AddParameter("STORE_LATITUDE",objStore.STORE_LATITUDE )
                        ,DBConn.AddParameter("STORE_LONGITUDE",objStore.STORE_LONGITUDE )
                        //,DBConn.AddParameter("TYPE_ID",objStore.TYPE_ID )
                        ,DBConn.AddParameter("TYPE_NAME",objStore.TYPE_NAME )
                        ,DBConn.AddParameter("PROVINCE_ID",objStore.PROVINCE_ID )
                        ,DBConn.AddParameter("PROVINCE_NAME",objStore.PROVINCE_NAME )
                        
                        ,DBConn.AddParameter("MOBILE_NO",objStore.MOBILE_NO )
                        ,DBConn.AddParameter("QOS",objStore.QOS )
                        ,DBConn.AddParameter("CAPACITIES",objStore.CAPACITIES )
                        ,DBConn.AddParameter("AUDIT_GRADING",objStore.AUDIT_GRADING )
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_1",objStore.STORE_ATTRIBUTE_1 )
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_2",objStore.STORE_ATTRIBUTE_2 )
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_3",objStore.STORE_ATTRIBUTE_3 )
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_4",objStore.STORE_ATTRIBUTE_4 )
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_5",objStore.STORE_ATTRIBUTE_5 )
                        ,DBConn.AddParameter("PRIMARY_WAREHOUSE",objStore.PRIMARY_WAREHOUSE )
                        
                         ,DBConn.AddParameter("FLOAT_AMT",objStore.FLOAT_AMT )
                         ,DBConn.AddParameter("SL_DISTRICT_ID",objStore.SL_DISTRICT_ID )
                         ,DBConn.AddParameter("SL_DISTRICT_NAME",objStore.SL_DISTRICT_NAME )
                         ,DBConn.AddParameter("STORE_TYPE",objStore.STORE_TYPE )
                };

                        DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objStore, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }

                }

                oOracleConnection.Close();
                return intRecordCount;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            return intRecordCount;
        }

        #endregion

        #region BI_DIM_STORE_RENT

        public DataTable Get_BI_DIM_STORE_RENT(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region Sql 2020/07/29

                selectQuery = @"
--MODULE NAME:  BI_DIM_STORE_RENT
--DESCRIPTION:  'BI_DIM_STORE_RENT' SELECT QUERY
--CREATED BY:   ANUSHKAR
--CREATED DATE: 2020/07/29
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2020/07/29

SELECT aa.contract AS SITE_ID,
       
       (SELECT SUM(B.CURRENTAMOUNT)
          FROM ifsapp.SIN_RENT_BUILDINGMAIN B
         WHERE B.STATUS = 'A'
               AND B.SITEID = aa.contract
               AND B.CONTRACTEND >= to_char(SYSDATE, 'yyyy/mm/dd')) AS RENT_AMOUNT,
               
        NULL AS STORE_ATTRIBUTE_6,
        NULL AS STORE_ATTRIBUTE_7,
        NULL AS STORE_ATTRIBUTE_8,
        NULL AS STORE_ATTRIBUTE_9

  FROM IFSAPP.INVENTORY_LOCATION aa
 INNER JOIN ifsapp.hpnret_levels_overview hh ON aa.contract = hh.site_id
  LEFT JOIN IFSAPP.SIN_STORE_MASTER_TAB bb ON hh.site_id = bb.site_id
  LEFT JOIN IFSAPP.SITE pp ON hh.site_id = pp.contract
  LEFT JOIN ifsapp.company_address cc ON pp.delivery_address = cc.address_id
  LEFT JOIN IFSAPP.HPNRET_LEVEL_HIERARCHY yy ON hh.site_id = yy.site_id
  LEFT JOIN ifsapp.HPNRET_LEVEL ll ON LL.level_id = YY.level_id
  LEFT JOIN IFSAPP.COMPANY_COMM_METHOD mm ON cc.address_id = mm.address_id
                                             AND mm.method_id = 'Mobile'

 GROUP BY aa.contract,
          
          pp.description,
          hh.Channel_id,
          hh.Channel,
          hh.Area_id,
          hh.Area,
          hh.District_id,
          hh.District,
          hh.site_id,
          hh.branch,
          cc.address1,
          cc.address2,
          LL.hpnret_level_type,
          BB.STORE_OPEN_DATE,
          bb.STORE_OPEN_HOURS,
          bb.STORE_CLOSE_HOURS,
          bb.STORE_CLOSE_DATE,
          hh.channel,
          bb.store_latitude,
          bb.store_longitude,
          bb.store_province,
          bb.store_district,
          bb.store_last_renovated_dt,
          hh.site_id,
          hh.site_id,
          mm.VALUE,
          bb.store_group,
          bb.store_default_dc_code,
          bb.store_no_parking,
          bb.store_no_floors,
          bb.store_sequre_feet,
          ll.provincial_council_id,
          ll.district_id,
          pp.site_type,
          PP.primary_supp

";

                #endregion

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);

                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool Check_BI_DIM_STORE_RENT_ExsisIN_BI_NRTAN_ServerAndInsertTo_NRTAN_Server(Store objStore)
        {
            bool IsSave = false;
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

            #region Sql

            string stringQuerry = @"
                    SELECT COUNT(t1.SITE_ID) AS CheckCount 
                    FROM NRTAN.BI_DIM_STORE_RENT  t1 
                    where t1.SITE_ID = :SITE_ID
                ";

            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("SITE_ID",objStore.SITE_ID) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.Insert_BI_DIM_STORE_RENT_To_BI_NRTANServerIncrementalProcess(objStore);

                    }
                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objStore, ex.StackTrace), "Error - In Check_BI_DIM_STORE_RENT_CodeExsisIN_BI_NRTAN_ServerAndInsertTo_NRTAN_Server()", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            return IsSave;
        }

        public bool Insert_BI_DIM_STORE_RENT_To_BI_NRTANServerIncrementalProcess(Store objStore)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;

            #region Sql 2020/07/29

            string stringQuerry = @"
--Module Name:  BI_DIM_STORE_RENT
--Description:  'BI_DIM_STORE_RENT' INSERT QUERY
--Created By:   AnushkaR
--Created Date: 2020/07/29
--Updated By:   AnushkaR
--Updated Date: 2020/07/29

INSERT INTO NRTAN.BI_DIM_STORE_RENT
   (SITE_ID, RENT_AMOUNT, STORE_ATTRIBUTE_6, STORE_ATTRIBUTE_7, STORE_ATTRIBUTE_8, STORE_ATTRIBUTE_9)

VALUES
   (:SITE_ID, :RENT_AMOUNT, :STORE_ATTRIBUTE_6, :STORE_ATTRIBUTE_7, :STORE_ATTRIBUTE_8, :STORE_ATTRIBUTE_9)
   ";

            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("SITE_ID",objStore.SITE_ID ) 
                        ,DBConn.AddParameter("RENT_AMOUNT",objStore.RENT_AMOUNT ) 
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_6",objStore.STORE_ATTRIBUTE_6 ) 
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_7",objStore.STORE_ATTRIBUTE_7 ) 
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_8",objStore.STORE_ATTRIBUTE_8 ) 
                        ,DBConn.AddParameter("STORE_ATTRIBUTE_9",objStore.STORE_ATTRIBUTE_9 )
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objStore, ex.StackTrace), "Error - In Insert_BI_DIM_STORE_RENT_To_BI_NRTANServerIncrementalProcess()", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            return isSave;
        }

        public int Insert_BI_DIM_STORE_RENT_To_BI_NRTANServerIncrementalProcess(List<Store> lstStore)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;
            int intRecordCount = 0;

            #region Sql 2020/07/29

            string stringQuerry = @"
--Module Name:  BI_DIM_STORE_RENT
--Description:  'BI_DIM_STORE_RENT' INSERT QUERY
--Created By:   AnushkaR
--Created Date: 2020/07/29
--Updated By:   AnushkaR
--Updated Date: 2020/07/29

INSERT INTO NRTAN.BI_DIM_STORE_RENT
   (SITE_ID, RENT_AMOUNT, STORE_ATTRIBUTE_6, STORE_ATTRIBUTE_7, STORE_ATTRIBUTE_8, STORE_ATTRIBUTE_9)

VALUES
   (:SITE_ID, :RENT_AMOUNT, :STORE_ATTRIBUTE_6, :STORE_ATTRIBUTE_7, :STORE_ATTRIBUTE_8, :STORE_ATTRIBUTE_9)
   ";

            #endregion

            try
            {
                foreach (Store objStore in lstStore)
                {

                    try
                    {
                        OracleParameter[] param = new OracleParameter[] 
                        { 
                            DBConn.AddParameter("SITE_ID",objStore.SITE_ID ) 
                            ,DBConn.AddParameter("RENT_AMOUNT",objStore.RENT_AMOUNT ) 
                            ,DBConn.AddParameter("STORE_ATTRIBUTE_6",objStore.STORE_ATTRIBUTE_6 ) 
                            ,DBConn.AddParameter("STORE_ATTRIBUTE_7",objStore.STORE_ATTRIBUTE_7 ) 
                            ,DBConn.AddParameter("STORE_ATTRIBUTE_8",objStore.STORE_ATTRIBUTE_8 ) 
                            ,DBConn.AddParameter("STORE_ATTRIBUTE_9",objStore.STORE_ATTRIBUTE_9 )
                        };

                        DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objStore, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }

                }

                oOracleConnection.Close();
                return intRecordCount;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            return intRecordCount;
        }

        #endregion


        public DataTable GetProduct(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;
            try
            {

                #region new sql.old
                //                selectQuery = @"
                //                
                //    select DISTINCT i.part_no AS PRODUCT_CODE,
                //    i.description as PRODUCT_DESC,
                //    i.accounting_group AS ACCOUNTING_GROUP_ID,
                //    a.description as ACCOUNTING_GROUP_DESC,
                //    i.part_product_family AS PRODUCT_FAMILY_ID,
                //    f.description as PRODUCT_FAMILY_DESC,
                //    i.second_commodity COMMODITY_ID,
                //    g.description as COMMODITY_DESC , --pfdescription,
                //    b.ownership AS OWNER_ID,
                //    b.ownership_des AS OWNER_DESC,
                //    i.part_product_code AS BRAND_ID,
                //    p.description as BRAND_DESC,
                //    i.part_status  
                //    ,trunc(i.create_date) as  DATE_OF_INTRODUCTION
                //    ,RRD.sup_part_no as REPLACEMENT_PRODUCT
                //    ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no),0) as CBM
                //    ,'' as PRODUCT_ATTRIBUTE_1
                //    ,'' as PRODUCT_ATTRIBUTE_2
                //    ,'' as PRODUCT_ATTRIBUTE_3
                //    ,'' as PRODUCT_ATTRIBUTE_4
                //    ,'' as PRODUCT_ATTRIBUTE_5
                //    from ifsapp.inventory_part i
                //left join ifsapp.accounting_group a on i.accounting_group =
                //                                      a.ACCOUNTING_GROUP
                //left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
                //left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
                //                                              f.part_product_family
                //left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
                //                                     i.second_commodity
                //left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
                //                                             i.part_product_code
                //left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
                //left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
                //left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
                //                                            p.part_product_code
                //where I.contract = 'BAD01'
                //and trunc(i.create_date)  >= to_date(:PreviousDate_from, 'YYYY/MM/DD')
                //and trunc(i.create_date)  <= to_date(:PreviousDate, 'YYYY/MM/DD')
                //and i.description not in ('DEMO SAMET HOOD')
                //and i.part_no not in
                //    ('29M63STIF6', 'DELL-3568-I5NB', 'EMPL-HOME-02', 'EMPL-CHILFUR-08',
                //     'AGC-CLS-W7', 'Y-RXV357', 'AG-GRS-07', '29M63SXIF6',
                //     'SPM-75X75-IF6', 'Y-NSP60', 'HU-P9-G-PROMO', 'MCKM757C')
                //and substr(e.vendor_no, 1, 2) in ('FS', 'LS', 'IT', 'CA','AC','WA','FA','MA','WB','MK')
                //and e.primary_vendor_db = 'Y'
                //and substr(e.CONTRACT, 1, 1) in ('W', 'F', 'P','D','B','M','S')
                //AND  SUBSTR(i.second_commodity,1,2) <> '2P' 
                // 
                //union
                //
                //select DISTINCT s.catalog_no as PRODUCT_CODE,
                //    s.catalog_desc as PRODUCT_DESC,
                //    case
                //      when s.part_product_family = 'PF025' then
                //       'AG001'
                //      else
                //       'AG002'
                //    end AS ACCOUNTING_GROUP_ID,
                //    case
                //      when s.part_product_family = 'PF025' then
                //       'MACHINES'
                //      else
                //       'CONSUMER DURABLES'
                //    end as ACCOUNTING_GROUP_DESC,
                //    s.part_product_family AS PRODUCT_FAMILY_ID,
                //    f.description as PRODUCT_FAMILY_DESC,
                //    s.catalog_group as COMMODITY_ID,
                //    g.description COMMODITY_DESC , --as pfdescription,
                //    '' AS OWNER_ID,
                //    '' AS OWNER_DESC,
                //    '' AS BRAND_ID,
                //    '' AS BRAND_DESC,
                //    '' AS part_status
                //,
                //    trunc(s.date_entered) as  DATE_OF_INTRODUCTION
                //    ,''  as REPLACEMENT_PRODUCT
                //    ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no),0) as CBM
                //    ,'' as PRODUCT_ATTRIBUTE_1
                //    ,'' as PRODUCT_ATTRIBUTE_2
                //    ,'' as PRODUCT_ATTRIBUTE_3
                //    ,'' as PRODUCT_ATTRIBUTE_4
                //    ,'' as PRODUCT_ATTRIBUTE_5
                //    
                //from IFSAPP.SALES_PART s
                //left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on s.part_product_family =
                //                                              f.part_product_family
                //left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
                //                                     s.catalog_group
                //where s.contract = 'BAD01'
                //and s.catalog_type_db = 'PKG'
                //and  trunc(s.date_entered)  >=  to_date(:PreviousDate_from, 'YYYY/MM/DD')
                //and  trunc(s.date_entered)  <=  to_date(:PreviousDate, 'YYYY/MM/DD')
                //AND SUBSTR(s.catalog_group,1,2) <> '2P'
                //
                //
                //UNION
                //
                //select DISTINCT i.part_no AS PRODUCT_CODE,
                //    i.description as PRODUCT_DESC,
                //    i.accounting_group AS ACCOUNTING_GROUP_ID,
                //    a.description as ACCOUNTING_GROUP_DESC,
                //    i.part_product_family AS PRODUCT_FAMILY_ID,
                //    f.description as PRODUCT_FAMILY_DESC,
                //    i.second_commodity AS COMMODITY_ID,
                //    g.description as COMMODITY_DESC , --pfdescription,
                //    b.ownership AS OWNER_ID,
                //    b.ownership_des AS OWNER_DESC,
                //    i.part_product_code AS BRAND_ID,
                //    p.description as BRAND_DESC,
                //    i.part_status 
                //,trunc(i.create_date) as  DATE_OF_INTRODUCTION
                //    ,RRD.sup_part_no as REPLACEMENT_PRODUCT
                //    ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no),0) as CBM
                //    ,'' as PRODUCT_ATTRIBUTE_1
                //    ,'' as PRODUCT_ATTRIBUTE_2
                //    ,'' as PRODUCT_ATTRIBUTE_3
                //    ,'' as PRODUCT_ATTRIBUTE_4
                //    ,'' as PRODUCT_ATTRIBUTE_5
                //       
                //from ifsapp.inventory_part i
                //left join ifsapp.accounting_group a on i.accounting_group =
                //                                      a.ACCOUNTING_GROUP
                //left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
                //left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
                //                                              f.part_product_family
                //left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
                //                                     i.second_commodity
                //left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
                //                                             i.part_product_code
                //left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
                //left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
                //left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
                //                                            p.part_product_code
                //where I.contract = 'BAD01'
                //and trunc(i.create_date)  >= to_date(:PreviousDate_from, 'YYYY/MM/DD')
                //and trunc(i.create_date)  <= to_date(:PreviousDate, 'YYYY/MM/DD')
                //and i.description not in ('DEMO SAMET HOOD')
                //and i.part_no not in
                //    ('29M63STIF6', 'DELL-3568-I5NB', 'EMPL-HOME-02', 'EMPL-CHILFUR-08',
                //     'AGC-CLS-W7', 'Y-RXV357', 'AG-GRS-07', '29M63SXIF6',
                //     'SPM-75X75-IF6', 'Y-NSP60', 'HU-P9-G-PROMO', 'MCKM757C')
                // AND (e.vendor_no IS NULL)
                // AND  SUBSTR(i.second_commodity,1,2) <> '2P' 
                // 
                //
                //                ";
                #endregion

                #region New on 2019-08-15
//                selectQuery = @"
//                                
//                select DISTINCT i.part_no AS PRODUCT_CODE,
//                i.description as PRODUCT_DESC,
//                i.accounting_group AS ACCOUNTING_GROUP_ID,
//                a.description as ACCOUNTING_GROUP_DESC,
//                i.part_product_family AS PRODUCT_FAMILY_ID,
//                f.description as PRODUCT_FAMILY_DESC,
//                i.second_commodity COMMODITY_ID,
//                g.description as COMMODITY_DESC , --pfdescription,
//                b.ownership AS OWNER_ID,
//                b.ownership_des AS OWNER_DESC,
//                i.part_product_code AS BRAND_ID,
//                p.description as BRAND_DESC,
//                upper( ifsapp.Inventory_Part_Status_Par_API.Get_Description(i.part_status) )     as part_status   
//                ,trunc(i.create_date) as  DATE_OF_INTRODUCTION
//                ,RRD.sup_part_no as REPLACEMENT_PRODUCT
//                ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no),0) as CBM
//                ,'' as PRODUCT_ATTRIBUTE_1
//                ,'' as PRODUCT_ATTRIBUTE_2
//                ,'' as PRODUCT_ATTRIBUTE_3
//                ,'' as PRODUCT_ATTRIBUTE_4
//                ,'' as PRODUCT_ATTRIBUTE_5
//                ,'INV' AS CATALOG_TYPE
//                , trunc(IFSAPP.SALES_PRICE_LIST_PART_api.Get_Sales_Price('1',i.part_no),4) as Sales_Price
//                from ifsapp.inventory_part i
//            left join ifsapp.accounting_group a on i.accounting_group =
//                                                  a.ACCOUNTING_GROUP
//            left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
//            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
//                                                          f.part_product_family
//            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
//                                                 i.second_commodity
//            left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
//                                                         i.part_product_code
//            left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
//            left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
//            left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
//                                                        p.part_product_code
//            where i.contract = 'BAD01'
//            and trunc(i.create_date)  = to_date(:PreviousDate, 'YYYY/MM/DD')
//
//            UNION
//
//                select DISTINCT i.part_no AS PRODUCT_CODE,
//                i.description as PRODUCT_DESC,
//                i.accounting_group AS ACCOUNTING_GROUP_ID,
//                a.description as ACCOUNTING_GROUP_DESC,
//                i.part_product_family AS PRODUCT_FAMILY_ID,
//                f.description as PRODUCT_FAMILY_DESC,
//                i.second_commodity COMMODITY_ID,
//                g.description as COMMODITY_DESC , --pfdescription,
//                b.ownership AS OWNER_ID,
//                b.ownership_des AS OWNER_DESC,
//                i.part_product_code AS BRAND_ID,
//                p.description as BRAND_DESC,
//                upper( ifsapp.Inventory_Part_Status_Par_API.Get_Description(i.part_status) )     as part_status   
//                ,trunc(i.create_date) as  DATE_OF_INTRODUCTION
//                ,RRD.sup_part_no as REPLACEMENT_PRODUCT
//                ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no),0) as CBM
//                ,'' as PRODUCT_ATTRIBUTE_1
//                ,'' as PRODUCT_ATTRIBUTE_2
//                ,'' as PRODUCT_ATTRIBUTE_3
//                ,'' as PRODUCT_ATTRIBUTE_4
//                ,'' as PRODUCT_ATTRIBUTE_5
//                ,'INV' AS CATALOG_TYPE
//                , trunc(IFSAPP.SALES_PRICE_LIST_PART_api.Get_Sales_Price('1',i.part_no),4) as Sales_Price
//                from ifsapp.inventory_part i
//            left join ifsapp.accounting_group a on i.accounting_group =
//                                                  a.ACCOUNTING_GROUP
//            left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
//            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
//                                                          f.part_product_family
//            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
//                                                 i.second_commodity
//            left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
//                                                         i.part_product_code
//            left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
//            left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
//            left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
//                                                        p.part_product_code
//            where SUBSTR(i.contract,1,1) IN ('R','W','P')
//            and trunc(i.create_date)  = to_date(:PreviousDate, 'YYYY/MM/DD')
//            and SUBSTR(I.second_commodity,1,2) IN ( '2P','2C','GV')
//
//            UNION
//
//            select DISTINCT s.catalog_no as PRODUCT_CODE,
//                s.catalog_desc as PRODUCT_DESC,
//                case
//                  when s.part_product_family = 'PF025' then
//                   'AG001'
//                  else
//                   'AG002'
//                end AS ACCOUNTING_GROUP_ID,
//                case
//                  when s.part_product_family = 'PF025' then
//                   'MACHINES'
//                  else
//                   'CONSUMER DURABLES'
//                end as ACCOUNTING_GROUP_DESC,
//                nvl(s.part_product_family,'N/A') AS PRODUCT_FAMILY_ID,
//                NVL(f.description,'N/A') as PRODUCT_FAMILY_DESC,
//                s.catalog_group as COMMODITY_ID,
//                g.description COMMODITY_DESC , --as pfdescription,
//                '' AS OWNER_ID,
//                '' AS OWNER_DESC,
//                '' AS BRAND_ID,
//                '' AS BRAND_DESC,
//                '' AS part_status
//            ,
//                trunc(s.date_entered) as  DATE_OF_INTRODUCTION
//                ,''  as REPLACEMENT_PRODUCT
//                ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no),0) as CBM
//                ,'' as PRODUCT_ATTRIBUTE_1
//                ,'' as PRODUCT_ATTRIBUTE_2
//                ,'' as PRODUCT_ATTRIBUTE_3
//                ,'' as PRODUCT_ATTRIBUTE_4
//                ,'' as PRODUCT_ATTRIBUTE_5
//                ,'NON' AS CATALOG_TYPE
//                , trunc(IFSAPP.SALES_PRICE_LIST_PART_api.Get_Sales_Price('1',s.catalog_no),4) as Sales_Price
//            from IFSAPP.SALES_PART s
//            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on s.part_product_family =
//                                                          f.part_product_family
//            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
//                                                 s.catalog_group
//            where  s.catalog_type_db = 'NON'  ---NON INVENTORY PART
//            and  trunc(s.date_entered)  =  to_date(:PreviousDate, 'YYYY/MM/DD')
//            
//                ";
                #endregion

                #region SQL 2020-06-06

                selectQuery = @"
                                
                select DISTINCT i.part_no AS PRODUCT_CODE,
                i.description as PRODUCT_DESC,
                i.accounting_group AS ACCOUNTING_GROUP_ID,
                a.description as ACCOUNTING_GROUP_DESC,
                i.part_product_family AS PRODUCT_FAMILY_ID,
                f.description as PRODUCT_FAMILY_DESC,
                i.second_commodity COMMODITY_ID,
                g.description as COMMODITY_DESC , --pfdescription,
                b.ownership AS OWNER_ID,
                b.ownership_des AS OWNER_DESC,
                i.part_product_code AS BRAND_ID,
                p.description as BRAND_DESC,
                upper( ifsapp.Inventory_Part_Status_Par_API.Get_Description(i.part_status) )     as part_status   
                ,trunc(i.create_date) as  DATE_OF_INTRODUCTION
                ,RRD.sup_part_no as REPLACEMENT_PRODUCT
                ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no),0) as CBM
                ,'' as PRODUCT_ATTRIBUTE_1
                ,'' as PRODUCT_ATTRIBUTE_2
                ,'' as PRODUCT_ATTRIBUTE_3
                ,'' as PRODUCT_ATTRIBUTE_4
                ,'' as PRODUCT_ATTRIBUTE_5
                ,'INV' AS CATALOG_TYPE
                , trunc(IFSAPP.SALES_PRICE_LIST_PART_api.Get_Sales_Price('1',i.part_no),4) as Sales_Price

                ,CASE
                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF035', 'PF058') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01',i.part_no), 0) * 1.25, 10) --Refrigerators
                    WHEN ifsapp.inventory_part_api.get_second_commodity(i.contract, i.part_no) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Sofa
                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF003') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Foot Bicycle
                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF124') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 9, 10) --Wood Working Matchine
                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF151') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 3.5, 10) --Zoje Matchine
                    ELSE
                        TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0), 10)
                END                                                                   AS INCREMENTAL_VOLUMN
                
                from ifsapp.inventory_part i
            left join ifsapp.accounting_group a on i.accounting_group =
                                                  a.ACCOUNTING_GROUP
            left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
                                                          f.part_product_family
            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
                                                 i.second_commodity
            left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
                                                         i.part_product_code
            left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
            left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
            left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
                                                        p.part_product_code
            where i.contract = 'BAD01'
            and trunc(i.create_date)  = to_date(:PreviousDate, 'YYYY/MM/DD')

            UNION

                select DISTINCT i.part_no AS PRODUCT_CODE,
                i.description as PRODUCT_DESC,
                i.accounting_group AS ACCOUNTING_GROUP_ID,
                a.description as ACCOUNTING_GROUP_DESC,
                i.part_product_family AS PRODUCT_FAMILY_ID,
                f.description as PRODUCT_FAMILY_DESC,
                i.second_commodity COMMODITY_ID,
                g.description as COMMODITY_DESC , --pfdescription,
                b.ownership AS OWNER_ID,
                b.ownership_des AS OWNER_DESC,
                i.part_product_code AS BRAND_ID,
                p.description as BRAND_DESC,
                upper( ifsapp.Inventory_Part_Status_Par_API.Get_Description(i.part_status) )     as part_status   
                ,trunc(i.create_date) as  DATE_OF_INTRODUCTION
                ,RRD.sup_part_no as REPLACEMENT_PRODUCT
                ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no),0) as CBM
                ,'' as PRODUCT_ATTRIBUTE_1
                ,'' as PRODUCT_ATTRIBUTE_2
                ,'' as PRODUCT_ATTRIBUTE_3
                ,'' as PRODUCT_ATTRIBUTE_4
                ,'' as PRODUCT_ATTRIBUTE_5
                ,'INV' AS CATALOG_TYPE
                , trunc(IFSAPP.SALES_PRICE_LIST_PART_api.Get_Sales_Price('1',i.part_no),4) as Sales_Price
                
                ,CASE
                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF035', 'PF058') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01',i.part_no), 0) * 1.25, 10) --Refrigerators
                    WHEN ifsapp.inventory_part_api.get_second_commodity(i.contract, i.part_no) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Sofa
                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF003') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Foot Bicycle
                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF124') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 9, 10) --Wood Working Matchine
                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF151') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 3.5, 10) --Zoje Matchine
                    ELSE
                        TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0), 10)
                END                                                                   AS INCREMENTAL_VOLUMN
                
                from ifsapp.inventory_part i
            left join ifsapp.accounting_group a on i.accounting_group =
                                                  a.ACCOUNTING_GROUP
            left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
                                                          f.part_product_family
            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
                                                 i.second_commodity
            left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
                                                         i.part_product_code
            left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
            left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
            left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
                                                        p.part_product_code
            where SUBSTR(i.contract,1,1) IN ('R','W','P')
            and trunc(i.create_date)  = to_date(:PreviousDate, 'YYYY/MM/DD')
            and SUBSTR(I.second_commodity,1,2) IN ( '2P','2C','GV')

            UNION

            select DISTINCT s.catalog_no as PRODUCT_CODE,
                s.catalog_desc as PRODUCT_DESC,
                case
                  when s.part_product_family = 'PF025' then
                   'AG001'
                  else
                   'AG002'
                end AS ACCOUNTING_GROUP_ID,
                case
                  when s.part_product_family = 'PF025' then
                   'MACHINES'
                  else
                   'CONSUMER DURABLES'
                end as ACCOUNTING_GROUP_DESC,
                nvl(s.part_product_family,'N/A') AS PRODUCT_FAMILY_ID,
                NVL(f.description,'N/A') as PRODUCT_FAMILY_DESC,
                s.catalog_group as COMMODITY_ID,
                g.description COMMODITY_DESC , --as pfdescription,
                '' AS OWNER_ID,
                '' AS OWNER_DESC,
                '' AS BRAND_ID,
                '' AS BRAND_DESC,
                '' AS part_status
            ,
                trunc(s.date_entered) as  DATE_OF_INTRODUCTION
                ,''  as REPLACEMENT_PRODUCT
                ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no),0) as CBM
                ,'' as PRODUCT_ATTRIBUTE_1
                ,'' as PRODUCT_ATTRIBUTE_2
                ,'' as PRODUCT_ATTRIBUTE_3
                ,'' as PRODUCT_ATTRIBUTE_4
                ,'' as PRODUCT_ATTRIBUTE_5
                ,'NON' AS CATALOG_TYPE
                , trunc(IFSAPP.SALES_PRICE_LIST_PART_api.Get_Sales_Price('1',s.catalog_no),4) as Sales_Price
                
                ,CASE
                    WHEN ifsapp.inventory_part_api.get_part_product_family(s.contract, s.catalog_no) IN ('PF035', 'PF058') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01',s.catalog_no), 0) * 1.25, 10) --Refrigerators
                    WHEN ifsapp.inventory_part_api.get_second_commodity(s.contract, s.catalog_no) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no), 0) * 1.75, 10) --Sofa
                    WHEN ifsapp.inventory_part_api.get_part_product_family(s.contract, s.catalog_no) IN ('PF003') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no), 0) * 1.75, 10) --Foot Bicycle
                    WHEN ifsapp.inventory_part_api.get_part_product_family(s.contract, s.catalog_no) IN ('PF124') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no), 0) * 9, 10) --Wood Working Matchine
                    WHEN ifsapp.inventory_part_api.get_part_product_family(s.contract, s.catalog_no) IN ('PF151') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no), 0) * 3.5, 10) --Zoje Matchine
                    ELSE
                        TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no), 0), 10)
                END                                                                   AS INCREMENTAL_VOLUMN
                
            from IFSAPP.SALES_PART s
            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on s.part_product_family =
                                                          f.part_product_family
            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
                                                 s.catalog_group
            where  s.catalog_type_db = 'NON'  ---NON INVENTORY PART
            and  trunc(s.date_entered)  =  to_date(:PreviousDate, 'YYYY/MM/DD')
            
                ";

                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                   //,DBConn.AddParameter("PreviousDate_from", dtPreviousDate.AddDays(-10).ToString("yyyy/MM/dd"))
                };

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                //oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable GetProduct_DMD(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionDMD_MANTHAN();
            string selectQuery = string.Empty;
            try
            {

                #region New on 2019-08-15
//                selectQuery = @"
//                select DISTINCT i.part_no AS PRODUCT_CODE,
//                i.description as PRODUCT_DESC,
//                i.accounting_group AS ACCOUNTING_GROUP_ID,
//                a.description as ACCOUNTING_GROUP_DESC,
//                i.part_product_family AS PRODUCT_FAMILY_ID,
//                f.description as PRODUCT_FAMILY_DESC,
//                i.second_commodity COMMODITY_ID,
//                g.description as COMMODITY_DESC , --pfdescription,
//                '' AS OWNER_ID,
//                ''  AS OWNER_DESC,
//                i.part_product_code AS BRAND_ID,
//                p.description as BRAND_DESC,
//                upper( ifsapp.Inventory_Part_Status_Par_API.Get_Description(i.part_status) )     as part_status   
//                ,trunc(i.create_date) as  DATE_OF_INTRODUCTION
//                ,RRD.sup_part_no as REPLACEMENT_PRODUCT
//                ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no),0) as CBM
//                ,'' as PRODUCT_ATTRIBUTE_1
//                ,'' as PRODUCT_ATTRIBUTE_2
//                ,'' as PRODUCT_ATTRIBUTE_3
//                ,'' as PRODUCT_ATTRIBUTE_4
//                ,'' as PRODUCT_ATTRIBUTE_5
//                ,'INV' AS CATALOG_TYPE
//                , trunc(IFSAPP.SALES_PRICE_LIST_PART_api.Get_Sales_Price('1',i.part_no),4) as Sales_Price
//           
//                from ifsapp.inventory_part i
//            left join ifsapp.accounting_group a on i.accounting_group =
//                                                  a.ACCOUNTING_GROUP
//            left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
//            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
//                                                          f.part_product_family
//            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
//                                                 i.second_commodity
//            /*left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
//                                                         i.part_product_code*/
//            left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
//            left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
//            left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
//                                                        p.part_product_code
//            where  i.contract = 'WDA01'
//            and trunc(i.create_date)  = to_date(:PreviousDate, 'YYYY/MM/DD')
//                ";
                #endregion

                #region SQL 2020-06-06

                selectQuery = @"
                select DISTINCT i.part_no AS PRODUCT_CODE,
                i.description as PRODUCT_DESC,
                i.accounting_group AS ACCOUNTING_GROUP_ID,
                a.description as ACCOUNTING_GROUP_DESC,
                i.part_product_family AS PRODUCT_FAMILY_ID,
                f.description as PRODUCT_FAMILY_DESC,
                i.second_commodity COMMODITY_ID,
                g.description as COMMODITY_DESC , --pfdescription,
                '' AS OWNER_ID,
                ''  AS OWNER_DESC,
                i.part_product_code AS BRAND_ID,
                p.description as BRAND_DESC,
                upper( ifsapp.Inventory_Part_Status_Par_API.Get_Description(i.part_status) )     as part_status   
                ,trunc(i.create_date) as  DATE_OF_INTRODUCTION
                ,RRD.sup_part_no as REPLACEMENT_PRODUCT
                ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no),0) as CBM
                ,'' as PRODUCT_ATTRIBUTE_1
                ,'' as PRODUCT_ATTRIBUTE_2
                ,'' as PRODUCT_ATTRIBUTE_3
                ,'' as PRODUCT_ATTRIBUTE_4
                ,'' as PRODUCT_ATTRIBUTE_5
                ,'INV' AS CATALOG_TYPE
                , trunc(IFSAPP.SALES_PRICE_LIST_PART_api.Get_Sales_Price('1',i.part_no),4) as Sales_Price
                
                ,CASE
                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF035', 'PF058') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01',i.part_no), 0) * 1.25, 10) --Refrigerators
                    WHEN ifsapp.inventory_part_api.get_second_commodity(i.contract, i.part_no) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Sofa
                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF003') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Foot Bicycle
                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF124') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 9, 10) --Wood Working Matchine
                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF151') 
                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 3.5, 10) --Zoje Matchine
                    ELSE
                        TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0), 10)
                END                                                                   AS INCREMENTAL_VOLUMN
           
                from ifsapp.inventory_part i
            left join ifsapp.accounting_group a on i.accounting_group =
                                                  a.ACCOUNTING_GROUP
            left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
                                                          f.part_product_family
            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
                                                 i.second_commodity
            /*left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
                                                         i.part_product_code*/
            left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
            left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
            left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
                                                        p.part_product_code
            where  i.contract = 'WDA01'
            and trunc(i.create_date)  = to_date(:PreviousDate, 'YYYY/MM/DD')
                ";

                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                   //,DBConn.AddParameter("PreviousDate_from", dtPreviousDate.AddDays(-10).ToString("yyyy/MM/dd"))
                };

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                //oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool CheckProductCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(Product objProduct)
        {
            try
            {
                bool IsSave = false;
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.PRODUCT_CODE) AS CheckCount 
                    FROM NRTAN.BI_DIM_PRODUCT  t1 
                    where t1.PRODUCT_CODE = :PRODUCT_CODE
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("PRODUCT_CODE",objProduct.PRODUCT_CODE) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertProductTo_BI_NRTANServerIncrementalProcess(objProduct);
                    }

                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - CheckProductCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ex;
            }
        }

        public bool InsertProductTo_BI_NRTANServerIncrementalProcess(Product objProduct)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;

            #region Sql OLD

//            string stringQuerry = @"
//                 
//                INSERT INTO NRTAN.BI_DIM_PRODUCT
//                (
//                PRODUCT_CODE	,
//                PRODUCT_DESC	,
//                ACCOUNTING_GROUP_ID	,	
//                ACCOUNTING_GROUP_DESC	,
//                PRODUCT_FAMILY_ID	,		
//                PRODUCT_FAMILY_DESC	,		
//                COMMODITY_ID	,	
//                COMMODITY_DESC	,
//                OWNER_ID	,
//                OWNER_DESC	,
//                BRAND_ID	,		
//                BRAND_DESC	,	
//                STATUS	 ,
//               date_of_introduction,
//               replacement_product,
//               cbm,
//               product_attribute1,
//               product_attribute2,
//               product_attribute3,
//               product_attribute4,
//               product_attribute5,
//               RETAIL_PRICE,
//               INCREMENTAL_VOLUMN
//                )
//                VALUES
//                (
//                :PRODUCT_CODE	,
//                :PRODUCT_DESC	,
//                :ACCOUNTING_GROUP_ID	,	
//                :ACCOUNTING_GROUP_DESC	,
//                :PRODUCT_FAMILY_ID	,		
//                :PRODUCT_FAMILY_DESC	,		
//                :COMMODITY_ID	,	
//                :COMMODITY_DESC	,
//                :OWNER_ID	,
//                :OWNER_DESC	,
//                :BRAND_ID	,		
//                :BRAND_DESC	,	
//                :PART_STATUS	, 
//                TO_DATE(:date_of_introduction,'yyyy-MM-dd') ,
//               :replacement_product,
//               :cbm,
//               :product_attribute1,
//               :product_attribute2,
//               :product_attribute3,
//               :product_attribute4,
//               :product_attribute5,
//               :Sales_Price,
//               :INCREMENTAL_VOLUMN
//                )
//
//                ";

            #endregion
            
            #region Sql 2020/06/11

            string stringQuerry = @"
                 
                INSERT INTO NRTAN.BI_DIM_PRODUCT
                (
                PRODUCT_CODE	,
                PRODUCT_DESC	,
                ACCOUNTING_GROUP_ID	,	
                ACCOUNTING_GROUP_DESC	,
                PRODUCT_FAMILY_ID	,		
                PRODUCT_FAMILY_DESC	,		
                COMMODITY_ID	,	
                COMMODITY_DESC	,
                OWNER_ID	,
                OWNER_DESC	,
                BRAND_ID	,		
                BRAND_DESC	,	
                STATUS	 ,
               date_of_introduction,
               replacement_product,
               cbm,
               product_attribute1,
               product_attribute2,
               product_attribute3,
               product_attribute4,
               product_attribute5,
               RETAIL_PRICE
                )
                VALUES
                (
                :PRODUCT_CODE	,
                :PRODUCT_DESC	,
                :ACCOUNTING_GROUP_ID	,	
                :ACCOUNTING_GROUP_DESC	,
                :PRODUCT_FAMILY_ID	,		
                :PRODUCT_FAMILY_DESC	,		
                :COMMODITY_ID	,	
                :COMMODITY_DESC	,
                :OWNER_ID	,
                :OWNER_DESC	,
                :BRAND_ID	,		
                :BRAND_DESC	,	
                :PART_STATUS	, 
                TO_DATE(:date_of_introduction,'yyyy-MM-dd') ,
               :replacement_product,
               :cbm,
               :product_attribute1,
               :product_attribute2,
               :product_attribute3,
               :product_attribute4,
               :product_attribute5,
               :Sales_Price
                )

                ";
            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("PRODUCT_CODE",objProduct.PRODUCT_CODE ) 
                        ,DBConn.AddParameter("PRODUCT_DESC",objProduct.PRODUCT_DESC ) 
                        ,DBConn.AddParameter("ACCOUNTING_GROUP_ID",objProduct.ACCOUNTING_GROUP_ID ) 
                        ,DBConn.AddParameter("ACCOUNTING_GROUP_DESC",objProduct.ACCOUNTING_GROUP_DESC ) 
                        ,DBConn.AddParameter("PRODUCT_FAMILY_ID",objProduct.PRODUCT_FAMILY_ID ) 
                        ,DBConn.AddParameter("PRODUCT_FAMILY_DESC",objProduct.PRODUCT_FAMILY_DESC ) 
                        ,DBConn.AddParameter("COMMODITY_ID",objProduct.COMMODITY_ID ) 
                        ,DBConn.AddParameter("COMMODITY_DESC",objProduct.COMMODITY_DESC ) 
                        ,DBConn.AddParameter("OWNER_ID",objProduct.OWNER_ID ) 
                        ,DBConn.AddParameter("OWNER_DESC",objProduct.OWNER_DESC ) 
                        ,DBConn.AddParameter("BRAND_ID",objProduct.BRAND_ID ) 
                        ,DBConn.AddParameter("BRAND_DESC",objProduct.BRAND_DESC ) 
                        ,DBConn.AddParameter("PART_STATUS",objProduct.PART_STATUS ) 
                        ,DBConn.AddParameter("date_of_introduction",objProduct.DATE_OF_INTRODUCTION.ToString("yyyy/MM/dd") )
                        ,DBConn.AddParameter("replacement_product",objProduct.REPLACEMENT_PRODUCT ) 
                        ,DBConn.AddParameter("cbm",objProduct.CBM ) 
                        ,DBConn.AddParameter("product_attribute1",objProduct.PRODUCT_ATTRIBUTE_1 ) 
                        ,DBConn.AddParameter("product_attribute2",objProduct.PRODUCT_ATTRIBUTE_2 ) 
                        ,DBConn.AddParameter("product_attribute3",objProduct.PRODUCT_ATTRIBUTE_3 ) 
                        ,DBConn.AddParameter("product_attribute4",objProduct.PRODUCT_ATTRIBUTE_4 ) 
                        ,DBConn.AddParameter("product_attribute5",objProduct.PRODUCT_ATTRIBUTE_5 ) 
                        ,DBConn.AddParameter("Sales_Price",objProduct.Sales_Price )
                        //,DBConn.AddParameter("INCREMENTAL_VOLUMN",objProduct.INCREMENTAL_VOLUMN )

                        

                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objProduct, ex.StackTrace), "Error - In InsertProductTo_BI_NRTANServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }

        public int InsertProductTo_BI_NRTANServerIncrementalProcess(List<Product> listProduct)
        {
            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql OLD

//                string stringQuerry = @"
//                 
//                INSERT INTO NRTAN.BI_DIM_PRODUCT
//                (
//                PRODUCT_CODE	,
//                PRODUCT_DESC	,
//                ACCOUNTING_GROUP_ID	,	
//                ACCOUNTING_GROUP_DESC	,
//                PRODUCT_FAMILY_ID	,		
//                PRODUCT_FAMILY_DESC	,		
//                COMMODITY_ID	,	
//                COMMODITY_DESC	,
//                OWNER_ID	,
//                OWNER_DESC	,
//                BRAND_ID	,		
//                BRAND_DESC	,	
//                STATUS	 ,
//               date_of_introduction,
//               replacement_product,
//               cbm,
//               product_attribute1,
//               product_attribute2,
//               product_attribute3,
//               product_attribute4,
//               product_attribute5,
//               RETAIL_PRICE,
//               INCREMENTAL_VOLUMN
//                )
//                VALUES
//                (
//                :PRODUCT_CODE	,
//                :PRODUCT_DESC	,
//                :ACCOUNTING_GROUP_ID	,	
//                :ACCOUNTING_GROUP_DESC	,
//                :PRODUCT_FAMILY_ID	,		
//                :PRODUCT_FAMILY_DESC	,		
//                :COMMODITY_ID	,	
//                :COMMODITY_DESC	,
//                :OWNER_ID	,
//                :OWNER_DESC	,
//                :BRAND_ID	,		
//                :BRAND_DESC	,	
//                :PART_STATUS	, 
//                TO_DATE(:date_of_introduction,'yyyy-MM-dd') ,
//               :replacement_product,
//               :cbm,
//               :product_attribute1,
//               :product_attribute2,
//               :product_attribute3,
//               :product_attribute4,
//               :product_attribute5,
//               :Sales_Price,
//               :INCREMENTAL_VOLUMN
//                )
//
//                ";
                #endregion

                #region Sql 2020/06/11

                string stringQuerry = @"
                 
                INSERT INTO NRTAN.BI_DIM_PRODUCT
                (
                PRODUCT_CODE	,
                PRODUCT_DESC	,
                ACCOUNTING_GROUP_ID	,	
                ACCOUNTING_GROUP_DESC	,
                PRODUCT_FAMILY_ID	,		
                PRODUCT_FAMILY_DESC	,		
                COMMODITY_ID	,	
                COMMODITY_DESC	,
                OWNER_ID	,
                OWNER_DESC	,
                BRAND_ID	,		
                BRAND_DESC	,	
                STATUS	 ,
               date_of_introduction,
               replacement_product,
               cbm,
               product_attribute1,
               product_attribute2,
               product_attribute3,
               product_attribute4,
               product_attribute5,
               RETAIL_PRICE
                )
                VALUES
                (
                :PRODUCT_CODE	,
                :PRODUCT_DESC	,
                :ACCOUNTING_GROUP_ID	,	
                :ACCOUNTING_GROUP_DESC	,
                :PRODUCT_FAMILY_ID	,		
                :PRODUCT_FAMILY_DESC	,		
                :COMMODITY_ID	,	
                :COMMODITY_DESC	,
                :OWNER_ID	,
                :OWNER_DESC	,
                :BRAND_ID	,		
                :BRAND_DESC	,	
                :PART_STATUS	, 
                TO_DATE(:date_of_introduction,'yyyy-MM-dd') ,
               :replacement_product,
               :cbm,
               :product_attribute1,
               :product_attribute2,
               :product_attribute3,
               :product_attribute4,
               :product_attribute5,
               :Sales_Price
                )

                ";

                #endregion

                foreach (Product objProduct in listProduct)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("PRODUCT_CODE",objProduct.PRODUCT_CODE ) 
                        ,DBConn.AddParameter("PRODUCT_DESC",objProduct.PRODUCT_DESC ) 
                        ,DBConn.AddParameter("ACCOUNTING_GROUP_ID",objProduct.ACCOUNTING_GROUP_ID ) 
                        ,DBConn.AddParameter("ACCOUNTING_GROUP_DESC",objProduct.ACCOUNTING_GROUP_DESC ) 
                        ,DBConn.AddParameter("PRODUCT_FAMILY_ID",objProduct.PRODUCT_FAMILY_ID ) 
                        ,DBConn.AddParameter("PRODUCT_FAMILY_DESC",objProduct.PRODUCT_FAMILY_DESC ) 
                        ,DBConn.AddParameter("COMMODITY_ID",objProduct.COMMODITY_ID ) 
                        ,DBConn.AddParameter("COMMODITY_DESC",objProduct.COMMODITY_DESC ) 
                        ,DBConn.AddParameter("OWNER_ID",objProduct.OWNER_ID ) 
                        ,DBConn.AddParameter("OWNER_DESC",objProduct.OWNER_DESC ) 
                        ,DBConn.AddParameter("BRAND_ID",objProduct.BRAND_ID ) 
                        ,DBConn.AddParameter("BRAND_DESC",objProduct.BRAND_DESC ) 
                        ,DBConn.AddParameter("PART_STATUS",objProduct.PART_STATUS ) 
                        ,DBConn.AddParameter("date_of_introduction",objProduct.DATE_OF_INTRODUCTION.ToString("yyyy/MM/dd") )
                        ,DBConn.AddParameter("replacement_product",objProduct.REPLACEMENT_PRODUCT ) 
                        ,DBConn.AddParameter("cbm",objProduct.CBM ) 
                        ,DBConn.AddParameter("product_attribute1",objProduct.PRODUCT_ATTRIBUTE_1 ) 
                        ,DBConn.AddParameter("product_attribute2",objProduct.PRODUCT_ATTRIBUTE_2 ) 
                        ,DBConn.AddParameter("product_attribute3",objProduct.PRODUCT_ATTRIBUTE_3 ) 
                        ,DBConn.AddParameter("product_attribute4",objProduct.PRODUCT_ATTRIBUTE_4 ) 
                        ,DBConn.AddParameter("product_attribute5",objProduct.PRODUCT_ATTRIBUTE_5 ) 
                        ,DBConn.AddParameter("Sales_Price",objProduct.Sales_Price )
                        //,DBConn.AddParameter("INCREMENTAL_VOLUMN",objProduct.INCREMENTAL_VOLUMN )
                };

                        DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;


                    }
                    catch (Exception ex)
                    {
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objProduct, ex.StackTrace), "Error - In InsertProductTo_BI_NRTANServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                oOracleConnection.Close();

            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }

            return intRecordCount;
        }


        #region BI_DIM_PRODUCT_IV

        public DataTable GetBI_DIM_PRODUCT_IV(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;
            try
            {

                #region SQL 2020-06-24

//                selectQuery = @"
//                                
//                select DISTINCT i.part_no AS PRODUCT_CODE,
//                CASE
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF035', 'PF058') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01',i.part_no), 0) * 1.25, 10) --Refrigerators
//                    WHEN ifsapp.inventory_part_api.get_second_commodity(i.contract, i.part_no) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Sofa
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF003') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Foot Bicycle
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF124') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 9, 10) --Wood Working Matchine
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF151') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 3.5, 10) --Zoje Matchine
//                    ELSE
//                        TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0), 10)
//                END  AS INCREMENTAL_VOLUMN
//                
//                from ifsapp.inventory_part i
//            left join ifsapp.accounting_group a on i.accounting_group =
//                                                  a.ACCOUNTING_GROUP
//            left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
//            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
//                                                          f.part_product_family
//            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
//                                                 i.second_commodity
//            left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
//                                                         i.part_product_code
//            left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
//            left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
//            left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
//                                                        p.part_product_code
//            where i.contract = 'BAD01'
//            and trunc(i.create_date)  = to_date(:PreviousDate, 'YYYY/MM/DD')
//
//            UNION
//
//                select DISTINCT i.part_no AS PRODUCT_CODE,
//                CASE
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF035', 'PF058') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01',i.part_no), 0) * 1.25, 10) --Refrigerators
//                    WHEN ifsapp.inventory_part_api.get_second_commodity(i.contract, i.part_no) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Sofa
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF003') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Foot Bicycle
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF124') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 9, 10) --Wood Working Matchine
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF151') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 3.5, 10) --Zoje Matchine
//                    ELSE
//                        TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0), 10)
//                END                                                                   AS INCREMENTAL_VOLUMN
//                
//                from ifsapp.inventory_part i
//            left join ifsapp.accounting_group a on i.accounting_group =
//                                                  a.ACCOUNTING_GROUP
//            left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
//            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
//                                                          f.part_product_family
//            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
//                                                 i.second_commodity
//            left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
//                                                         i.part_product_code
//            left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
//            left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
//            left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
//                                                        p.part_product_code
//            where SUBSTR(i.contract,1,1) IN ('R','W','P')
//            and trunc(i.create_date)  = to_date(:PreviousDate, 'YYYY/MM/DD')
//            and SUBSTR(I.second_commodity,1,2) IN ( '2P','2C','GV')
//
//            UNION
//
//            select DISTINCT s.catalog_no as PRODUCT_CODE,
//                CASE
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(s.contract, s.catalog_no) IN ('PF035', 'PF058') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01',s.catalog_no), 0) * 1.25, 10) --Refrigerators
//                    WHEN ifsapp.inventory_part_api.get_second_commodity(s.contract, s.catalog_no) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no), 0) * 1.75, 10) --Sofa
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(s.contract, s.catalog_no) IN ('PF003') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no), 0) * 1.75, 10) --Foot Bicycle
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(s.contract, s.catalog_no) IN ('PF124') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no), 0) * 9, 10) --Wood Working Matchine
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(s.contract, s.catalog_no) IN ('PF151') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no), 0) * 3.5, 10) --Zoje Matchine
//                    ELSE
//                        TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no), 0), 10)
//                END                                                                   AS INCREMENTAL_VOLUMN
//                
//            from IFSAPP.SALES_PART s
//            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on s.part_product_family =
//                                                          f.part_product_family
//            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
//                                                 s.catalog_group
//            where  s.catalog_type_db = 'NON'  ---NON INVENTORY PART
//            and  trunc(s.date_entered)  =  to_date(:PreviousDate, 'YYYY/MM/DD')
//            
//                ";

                #endregion

                #region SQL 2020-07-01

//                selectQuery = @"
//--Module Name:  BI_DIM_PRODUCT_IV
//--Description:  'BI_DIM_PRODUCT_IV' SINGER SELECT QUERY
//--Created By:   AnushkaR
//--Created Date: 2020/07/01
//--Updated By:   AnushkaR
//--Updated Date: 2020/07/01
//                
//    SELECT 
//        DISTINCT I.PART_NO AS PRODUCT_CODE --01
//        
//        ,CASE
//            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF035', 'PF058') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01',I.PART_NO), 0) * 1.25, 10) --Refrigerators
//            WHEN IFSAPP.INVENTORY_PART_API.GET_SECOND_COMMODITY(I.CONTRACT, I.PART_NO) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Sofa
//            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF003') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Foot Bicycle
//            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF124') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 9, 10) --Wood Working Matchine
//            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF151') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 3.5, 10) --Zoje Matchine
//            ELSE
//                TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0), 10)
//        END  AS INCREMENTAL_VOLUMN  --02
//    
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = I.PART_NO 
//            AND C.CHARACTERISTIC_CODE = 'BT1'), '') AS BASICTEC1 --03
//        
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = I.PART_NO 
//            AND C.CHARACTERISTIC_CODE = 'BT2'), '') AS BASICTEC2 --04
//            
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = I.PART_NO 
//            AND C.CHARACTERISTIC_CODE = 'BT3'), '') AS BASICTEC3  --05
//        
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = I.PART_NO 
//            AND C.CHARACTERISTIC_CODE = 'BT4'), '') AS BASICTEC4  --06
//        
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = I.PART_NO 
//            AND C.CHARACTERISTIC_CODE = 'BT5'), '') AS BASICTEC5  --07
//            
//        ,NVL((
//        SELECT  C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = I.PART_NO 
//            AND C.CHARACTERISTIC_CODE = 'COL'), '') AS COLOURCODE --08
//    
//    FROM 
//        IFSAPP.INVENTORY_PART I
//        LEFT JOIN IFSAPP.ACCOUNTING_GROUP A ON I.ACCOUNTING_GROUP = A.ACCOUNTING_GROUP
//        LEFT JOIN IFSAPP.REVERT_RESTRICTION_DAYS RRD ON I.PART_NO = RRD.PART_NO
//        LEFT JOIN IFSAPP.INVENTORY_PRODUCT_FAMILY F ON I.PART_PRODUCT_FAMILY = F.PART_PRODUCT_FAMILY
//        LEFT JOIN IFSAPP.COMMODITY_GROUP G ON G.SECOND_COMMODITY = I.SECOND_COMMODITY
//        LEFT JOIN IFSAPP.SIN_BRAND_OWNERSHIP_TAB B ON B.BRAND_ID = I.PART_PRODUCT_CODE
//        LEFT JOIN IFSAPP.PURCHASE_PART_SUPPLIER E ON I.PART_NO = E.PART_NO
//        LEFT JOIN IFSAPP.SUPPLIER_INFO O ON E.VENDOR_NO = O.SUPPLIER_ID
//        LEFT JOIN IFSAPP.INVENTORY_PRODUCT_CODE P ON I.PART_PRODUCT_CODE = P.PART_PRODUCT_CODE
//    
//    WHERE 
//        I.CONTRACT = 'BAD01'
//        AND TRUNC(I.CREATE_DATE) = TO_DATE(:PreviousDate, 'YYYY/MM/DD')
//
//
//UNION
//
//
//    SELECT 
//        DISTINCT I.PART_NO AS PRODUCT_CODE --01
//        
//        ,CASE
//            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF035', 'PF058') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01',I.PART_NO), 0) * 1.25, 10) --Refrigerators
//            WHEN IFSAPP.INVENTORY_PART_API.GET_SECOND_COMMODITY(I.CONTRACT, I.PART_NO) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Sofa
//            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF003') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Foot Bicycle
//            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF124') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 9, 10) --Wood Working Matchine
//            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF151') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 3.5, 10) --Zoje Matchine
//            ELSE
//                TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0), 10)
//        END    AS INCREMENTAL_VOLUMN --02
//    
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = I.PART_NO 
//            AND C.CHARACTERISTIC_CODE = 'BT1'), '') AS BASICTEC1 --03
//        
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = I.PART_NO 
//            AND C.CHARACTERISTIC_CODE = 'BT2'), '') AS BASICTEC2 --04
//            
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = I.PART_NO 
//            AND C.CHARACTERISTIC_CODE = 'BT3'), '') AS BASICTEC3  --05
//        
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = I.PART_NO 
//            AND C.CHARACTERISTIC_CODE = 'BT4'), '') AS BASICTEC4  --06
//        
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = I.PART_NO 
//            AND C.CHARACTERISTIC_CODE = 'BT5'), '') AS BASICTEC5  --07
//            
//        ,NVL((
//        SELECT  C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = I.PART_NO 
//            AND C.CHARACTERISTIC_CODE = 'COL'), '') AS COLOURCODE --08
//
//    FROM 
//        IFSAPP.INVENTORY_PART I
//        LEFT JOIN IFSAPP.ACCOUNTING_GROUP A ON I.ACCOUNTING_GROUP = A.ACCOUNTING_GROUP
//        LEFT JOIN IFSAPP.REVERT_RESTRICTION_DAYS RRD ON I.PART_NO = RRD.PART_NO
//        LEFT JOIN IFSAPP.INVENTORY_PRODUCT_FAMILY F ON I.PART_PRODUCT_FAMILY = F.PART_PRODUCT_FAMILY
//        LEFT JOIN IFSAPP.COMMODITY_GROUP G ON G.SECOND_COMMODITY = I.SECOND_COMMODITY
//        LEFT JOIN IFSAPP.SIN_BRAND_OWNERSHIP_TAB B ON B.BRAND_ID = I.PART_PRODUCT_CODE
//        LEFT JOIN IFSAPP.PURCHASE_PART_SUPPLIER E ON I.PART_NO = E.PART_NO
//        LEFT JOIN IFSAPP.SUPPLIER_INFO O ON E.VENDOR_NO = O.SUPPLIER_ID
//        LEFT JOIN IFSAPP.INVENTORY_PRODUCT_CODE P ON I.PART_PRODUCT_CODE = P.PART_PRODUCT_CODE
//        
//    WHERE 
//        SUBSTR(I.CONTRACT,1,1) IN ('R','W','P')
//        AND TRUNC(I.CREATE_DATE)  = TO_DATE(:PreviousDate, 'YYYY/MM/DD')
//        AND SUBSTR(I.SECOND_COMMODITY,1,2) IN ( '2P','2C','GV')
//
//
//UNION
//
//
//    SELECT 
//        DISTINCT S.CATALOG_NO AS PRODUCT_CODE --01
//        
//        ,CASE
//            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(S.CONTRACT, S.CATALOG_NO) IN ('PF035', 'PF058') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01',S.CATALOG_NO), 0) * 1.25, 10) --Refrigerators
//            WHEN IFSAPP.INVENTORY_PART_API.GET_SECOND_COMMODITY(S.CONTRACT, S.CATALOG_NO) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', S.CATALOG_NO), 0) * 1.75, 10) --Sofa
//            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(S.CONTRACT, S.CATALOG_NO) IN ('PF003') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', S.CATALOG_NO), 0) * 1.75, 10) --Foot Bicycle
//            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(S.CONTRACT, S.CATALOG_NO) IN ('PF124') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', S.CATALOG_NO), 0) * 9, 10) --Wood Working Matchine
//            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(S.CONTRACT, S.CATALOG_NO) IN ('PF151') 
//                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', S.CATALOG_NO), 0) * 3.5, 10) --Zoje Matchine
//            ELSE
//                TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', S.CATALOG_NO), 0), 10)
//        END AS INCREMENTAL_VOLUMN --02
//    
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = S.CATALOG_NO
//            AND C.CHARACTERISTIC_CODE = 'BT1'), '') AS BASICTEC1 --03
//        
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = S.CATALOG_NO
//            AND C.CHARACTERISTIC_CODE = 'BT2'), '') AS BASICTEC2 --04
//            
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = S.CATALOG_NO
//            AND C.CHARACTERISTIC_CODE = 'BT3'), '') AS BASICTEC3  --05
//        
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = S.CATALOG_NO
//            AND C.CHARACTERISTIC_CODE = 'BT4'), '') AS BASICTEC4  --06
//        
//        ,NVL((
//        SELECT C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = S.CATALOG_NO
//            AND C.CHARACTERISTIC_CODE = 'BT5'), '') AS BASICTEC5  --07
//            
//        ,NVL((
//        SELECT  C.ATTR_VALUE      
//        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
//        WHERE 
//            C.CONTRACT = 'BAD01' 
//            AND  C.PART_NO = S.CATALOG_NO
//            AND C.CHARACTERISTIC_CODE = 'COL'), '') AS COLOURCODE --08
//
//    FROM 
//        IFSAPP.SALES_PART S
//        LEFT JOIN IFSAPP.INVENTORY_PRODUCT_FAMILY F ON S.PART_PRODUCT_FAMILY = F.PART_PRODUCT_FAMILY
//        LEFT JOIN IFSAPP.COMMODITY_GROUP G ON G.SECOND_COMMODITY = S.CATALOG_GROUP
//        
//    WHERE 
//        S.CATALOG_TYPE_DB = 'NON'  ---NON INVENTORY PART
//        AND TRUNC(S.DATE_ENTERED) = TO_DATE(:PreviousDate, 'YYYY/MM/DD')
//
//";

                #endregion

                #region SQL 2020-08-17

                selectQuery = @"
--Module Name:  BI_DIM_PRODUCT_IV
--Description:  'BI_DIM_PRODUCT_IV' SINGER SELECT QUERY
--Created By:   AnushkaR
--Created Date: 2020/07/01
--Updated By:   AnushkaR
--Updated Date: 2020/08/17
                
    SELECT 
        DISTINCT I.PART_NO AS PRODUCT_CODE --01
        
        ,CASE
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF035', 'PF058') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01',I.PART_NO), 0) * 1.25, 10) --Refrigerators
            WHEN IFSAPP.INVENTORY_PART_API.GET_SECOND_COMMODITY(I.CONTRACT, I.PART_NO) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Sofa
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF003') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Foot Bicycle
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF124') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 9, 10) --Wood Working Matchine
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF151') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 3.5, 10) --Zoje Matchine
            ELSE
                TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0), 10)
        END  AS INCREMENTAL_VOLUMN  --02
    
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT1'), '') AS BASICTEC1 --03
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT2'), '') AS BASICTEC2 --04
            
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT3'), '') AS BASICTEC3  --05
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT4'), '') AS BASICTEC4  --06
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT5'), '') AS BASICTEC5  --07
            
        ,NVL((
        SELECT  C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'COL'), '') AS COLOURCODE --08
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT6'), '') AS BASICTEC6  --09
    
    FROM 
        IFSAPP.INVENTORY_PART I
        LEFT JOIN IFSAPP.ACCOUNTING_GROUP A ON I.ACCOUNTING_GROUP = A.ACCOUNTING_GROUP
        LEFT JOIN IFSAPP.REVERT_RESTRICTION_DAYS RRD ON I.PART_NO = RRD.PART_NO
        LEFT JOIN IFSAPP.INVENTORY_PRODUCT_FAMILY F ON I.PART_PRODUCT_FAMILY = F.PART_PRODUCT_FAMILY
        LEFT JOIN IFSAPP.COMMODITY_GROUP G ON G.SECOND_COMMODITY = I.SECOND_COMMODITY
        LEFT JOIN IFSAPP.SIN_BRAND_OWNERSHIP_TAB B ON B.BRAND_ID = I.PART_PRODUCT_CODE
        LEFT JOIN IFSAPP.PURCHASE_PART_SUPPLIER E ON I.PART_NO = E.PART_NO
        LEFT JOIN IFSAPP.SUPPLIER_INFO O ON E.VENDOR_NO = O.SUPPLIER_ID
        LEFT JOIN IFSAPP.INVENTORY_PRODUCT_CODE P ON I.PART_PRODUCT_CODE = P.PART_PRODUCT_CODE
    
    WHERE 
        I.CONTRACT = 'BAD01'
        AND TRUNC(I.CREATE_DATE) = TO_DATE(:PreviousDate, 'YYYY/MM/DD')


UNION


    SELECT 
        DISTINCT I.PART_NO AS PRODUCT_CODE --01
        
        ,CASE
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF035', 'PF058') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01',I.PART_NO), 0) * 1.25, 10) --Refrigerators
            WHEN IFSAPP.INVENTORY_PART_API.GET_SECOND_COMMODITY(I.CONTRACT, I.PART_NO) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Sofa
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF003') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Foot Bicycle
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF124') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 9, 10) --Wood Working Matchine
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF151') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 3.5, 10) --Zoje Matchine
            ELSE
                TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0), 10)
        END    AS INCREMENTAL_VOLUMN --02
    
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT1'), '') AS BASICTEC1 --03
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT2'), '') AS BASICTEC2 --04
            
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT3'), '') AS BASICTEC3  --05
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT4'), '') AS BASICTEC4  --06
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT5'), '') AS BASICTEC5  --07
            
        ,NVL((
        SELECT  C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'COL'), '') AS COLOURCODE --08
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT6'), '') AS BASICTEC6  --09

    FROM 
        IFSAPP.INVENTORY_PART I
        LEFT JOIN IFSAPP.ACCOUNTING_GROUP A ON I.ACCOUNTING_GROUP = A.ACCOUNTING_GROUP
        LEFT JOIN IFSAPP.REVERT_RESTRICTION_DAYS RRD ON I.PART_NO = RRD.PART_NO
        LEFT JOIN IFSAPP.INVENTORY_PRODUCT_FAMILY F ON I.PART_PRODUCT_FAMILY = F.PART_PRODUCT_FAMILY
        LEFT JOIN IFSAPP.COMMODITY_GROUP G ON G.SECOND_COMMODITY = I.SECOND_COMMODITY
        LEFT JOIN IFSAPP.SIN_BRAND_OWNERSHIP_TAB B ON B.BRAND_ID = I.PART_PRODUCT_CODE
        LEFT JOIN IFSAPP.PURCHASE_PART_SUPPLIER E ON I.PART_NO = E.PART_NO
        LEFT JOIN IFSAPP.SUPPLIER_INFO O ON E.VENDOR_NO = O.SUPPLIER_ID
        LEFT JOIN IFSAPP.INVENTORY_PRODUCT_CODE P ON I.PART_PRODUCT_CODE = P.PART_PRODUCT_CODE
        
    WHERE 
        SUBSTR(I.CONTRACT,1,1) IN ('R','W','P')
        AND TRUNC(I.CREATE_DATE)  = TO_DATE(:PreviousDate, 'YYYY/MM/DD')
        AND SUBSTR(I.SECOND_COMMODITY,1,2) IN ( '2P','2C','GV')


UNION


    SELECT 
        DISTINCT S.CATALOG_NO AS PRODUCT_CODE --01
        
        ,CASE
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(S.CONTRACT, S.CATALOG_NO) IN ('PF035', 'PF058') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01',S.CATALOG_NO), 0) * 1.25, 10) --Refrigerators
            WHEN IFSAPP.INVENTORY_PART_API.GET_SECOND_COMMODITY(S.CONTRACT, S.CATALOG_NO) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', S.CATALOG_NO), 0) * 1.75, 10) --Sofa
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(S.CONTRACT, S.CATALOG_NO) IN ('PF003') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', S.CATALOG_NO), 0) * 1.75, 10) --Foot Bicycle
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(S.CONTRACT, S.CATALOG_NO) IN ('PF124') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', S.CATALOG_NO), 0) * 9, 10) --Wood Working Matchine
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(S.CONTRACT, S.CATALOG_NO) IN ('PF151') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', S.CATALOG_NO), 0) * 3.5, 10) --Zoje Matchine
            ELSE
                TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', S.CATALOG_NO), 0), 10)
        END AS INCREMENTAL_VOLUMN --02
    
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = S.CATALOG_NO
            AND C.CHARACTERISTIC_CODE = 'BT1'), '') AS BASICTEC1 --03
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = S.CATALOG_NO
            AND C.CHARACTERISTIC_CODE = 'BT2'), '') AS BASICTEC2 --04
            
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = S.CATALOG_NO
            AND C.CHARACTERISTIC_CODE = 'BT3'), '') AS BASICTEC3  --05
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = S.CATALOG_NO
            AND C.CHARACTERISTIC_CODE = 'BT4'), '') AS BASICTEC4  --06
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = S.CATALOG_NO
            AND C.CHARACTERISTIC_CODE = 'BT5'), '') AS BASICTEC5  --07
            
        ,NVL((
        SELECT  C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = S.CATALOG_NO
            AND C.CHARACTERISTIC_CODE = 'COL'), '') AS COLOURCODE --08
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = S.CATALOG_NO
            AND C.CHARACTERISTIC_CODE = 'BT6'), '') AS BASICTEC6  --09

    FROM 
        IFSAPP.SALES_PART S
        LEFT JOIN IFSAPP.INVENTORY_PRODUCT_FAMILY F ON S.PART_PRODUCT_FAMILY = F.PART_PRODUCT_FAMILY
        LEFT JOIN IFSAPP.COMMODITY_GROUP G ON G.SECOND_COMMODITY = S.CATALOG_GROUP
        
    WHERE 
        S.CATALOG_TYPE_DB = 'NON'  ---NON INVENTORY PART
        AND TRUNC(S.DATE_ENTERED) = TO_DATE(:PreviousDate, 'YYYY/MM/DD')

";

                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                   //,DBConn.AddParameter("PreviousDate_from", dtPreviousDate.AddDays(-10).ToString("yyyy/MM/dd"))
                };

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                //oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable GetBI_DIM_PRODUCT_IV_DMD(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionDMD_MANTHAN();
            string selectQuery = string.Empty;
            try
            {

                #region SQL 2020-06-24

//                selectQuery = @"
//                select DISTINCT i.part_no AS PRODUCT_CODE,
//                CASE
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF035', 'PF058') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01',i.part_no), 0) * 1.25, 10) --Refrigerators
//                    WHEN ifsapp.inventory_part_api.get_second_commodity(i.contract, i.part_no) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Sofa
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF003') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Foot Bicycle
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF124') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 9, 10) --Wood Working Matchine
//                    WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF151') 
//                        THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 3.5, 10) --Zoje Matchine
//                    ELSE
//                        TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0), 10)
//                END                                                                   AS INCREMENTAL_VOLUMN
//           
//                from ifsapp.inventory_part i
//            left join ifsapp.accounting_group a on i.accounting_group =
//                                                  a.ACCOUNTING_GROUP
//            left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
//            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
//                                                          f.part_product_family
//            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
//                                                 i.second_commodity
//            /*left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
//                                                         i.part_product_code*/
//            left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
//            left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
//            left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
//                                                        p.part_product_code
//            where  i.contract = 'WDA01'
//            and trunc(i.create_date)  = to_date(:PreviousDate, 'YYYY/MM/DD')
//                ";

                #endregion

                #region SQL 2020-07-01

//                selectQuery = @"
//--Module Name:  BI_DIM_PRODUCT_IV
//--Description:  'BI_DIM_PRODUCT_IV' DMD SELECT QUERY
//--Created By:   AnushkaR
//--Created Date: 2020/07/01
//--Updated By:   AnushkaR
//--Updated Date: 2020/07/01
//
//SELECT 
//    DISTINCT I.PART_NO AS PRODUCT_CODE --01
//    
//    ,CASE
//        WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF035', 'PF058') 
//            THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01',I.PART_NO), 0) * 1.25, 10) --Refrigerators
//        WHEN IFSAPP.INVENTORY_PART_API.GET_SECOND_COMMODITY(I.CONTRACT, I.PART_NO) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
//            THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Sofa
//        WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF003') 
//            THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Foot Bicycle
//        WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF124') 
//            THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 9, 10) --Wood Working Matchine
//        WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF151') 
//            THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 3.5, 10) --Zoje Matchine
//        ELSE
//            TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0), 10)
//    END AS INCREMENTAL_VOLUMN --02
//  
//    ,'' AS BASICTEC1 --03
//    
//    ,'' AS BASICTEC2 --04
//        
//    ,'' AS BASICTEC3  --05
//    
//    ,'' AS BASICTEC4  --06
//    
//    ,'' AS BASICTEC5  --07
//        
//    ,'' AS COLOURCODE --08
//
//FROM 
//    IFSAPP.INVENTORY_PART I
//    LEFT JOIN IFSAPP.ACCOUNTING_GROUP A ON I.ACCOUNTING_GROUP = A.ACCOUNTING_GROUP
//    LEFT JOIN IFSAPP.REVERT_RESTRICTION_DAYS RRD ON I.PART_NO = RRD.PART_NO
//    LEFT JOIN IFSAPP.INVENTORY_PRODUCT_FAMILY F ON I.PART_PRODUCT_FAMILY = F.PART_PRODUCT_FAMILY
//    LEFT JOIN IFSAPP.COMMODITY_GROUP G ON G.SECOND_COMMODITY = I.SECOND_COMMODITY
//    /*LEFT JOIN IFSAPP.SIN_BRAND_OWNERSHIP_TAB B ON B.BRAND_ID = I.PART_PRODUCT_CODE*/
//    LEFT JOIN IFSAPP.PURCHASE_PART_SUPPLIER E ON I.PART_NO = E.PART_NO
//    LEFT JOIN IFSAPP.SUPPLIER_INFO O ON E.VENDOR_NO = O.SUPPLIER_ID
//    LEFT JOIN IFSAPP.INVENTORY_PRODUCT_CODE P ON I.PART_PRODUCT_CODE = P.PART_PRODUCT_CODE
//    
//WHERE  
//    I.CONTRACT = 'WDA01'
//    AND TRUNC(I.CREATE_DATE)  = TO_DATE(:PreviousDate, 'YYYY/MM/DD')
//
//";

                #endregion

                #region SQL 2020-08-17

                selectQuery = @"
--Module Name:  BI_DIM_PRODUCT_IV
--Description:  'BI_DIM_PRODUCT_IV' DMD SELECT QUERY
--Created By:   AnushkaR
--Created Date: 2020/07/01
--Updated By:   AnushkaR
--Updated Date: 2020/08/17

SELECT 
    DISTINCT I.PART_NO AS PRODUCT_CODE --01
    
    ,CASE
        WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF035', 'PF058') 
            THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01',I.PART_NO), 0) * 1.25, 10) --Refrigerators
        WHEN IFSAPP.INVENTORY_PART_API.GET_SECOND_COMMODITY(I.CONTRACT, I.PART_NO) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
            THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Sofa
        WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF003') 
            THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Foot Bicycle
        WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF124') 
            THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 9, 10) --Wood Working Matchine
        WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF151') 
            THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 3.5, 10) --Zoje Matchine
        ELSE
            TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0), 10)
    END AS INCREMENTAL_VOLUMN --02
  
    ,'' AS BASICTEC1 --03
    
    ,'' AS BASICTEC2 --04
        
    ,'' AS BASICTEC3  --05
    
    ,'' AS BASICTEC4  --06
    
    ,'' AS BASICTEC5  --07
        
    ,'' AS COLOURCODE --08
    
    ,'' AS BASICTEC6  --09

FROM 
    IFSAPP.INVENTORY_PART I
    LEFT JOIN IFSAPP.ACCOUNTING_GROUP A ON I.ACCOUNTING_GROUP = A.ACCOUNTING_GROUP
    LEFT JOIN IFSAPP.REVERT_RESTRICTION_DAYS RRD ON I.PART_NO = RRD.PART_NO
    LEFT JOIN IFSAPP.INVENTORY_PRODUCT_FAMILY F ON I.PART_PRODUCT_FAMILY = F.PART_PRODUCT_FAMILY
    LEFT JOIN IFSAPP.COMMODITY_GROUP G ON G.SECOND_COMMODITY = I.SECOND_COMMODITY
    /*LEFT JOIN IFSAPP.SIN_BRAND_OWNERSHIP_TAB B ON B.BRAND_ID = I.PART_PRODUCT_CODE*/
    LEFT JOIN IFSAPP.PURCHASE_PART_SUPPLIER E ON I.PART_NO = E.PART_NO
    LEFT JOIN IFSAPP.SUPPLIER_INFO O ON E.VENDOR_NO = O.SUPPLIER_ID
    LEFT JOIN IFSAPP.INVENTORY_PRODUCT_CODE P ON I.PART_PRODUCT_CODE = P.PART_PRODUCT_CODE
    
WHERE  
    I.CONTRACT = 'WDA01'
    AND TRUNC(I.CREATE_DATE)  = TO_DATE(:PreviousDate, 'YYYY/MM/DD')

";

                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                   //,DBConn.AddParameter("PreviousDate_from", dtPreviousDate.AddDays(-10).ToString("yyyy/MM/dd"))
                };

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                //oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool CheckBI_DIM_PRODUCT_IVCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(Product objProduct)
        {
            try
            {
                bool IsSave = false;
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql

                string stringQuerry = @"
                    SELECT COUNT(t1.PRODUCT_CODE) AS CheckCount 
                    FROM NRTAN.BI_DIM_PRODUCT_IV  t1 
                    where t1.PRODUCT_CODE = :PRODUCT_CODE
                ";

                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("PRODUCT_CODE",objProduct.PRODUCT_CODE) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertBI_DIM_PRODUCT_IVTo_BI_NRTANServerIncrementalProcess(objProduct);
                    }

                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - CheckBI_DIM_PRODUCT_IVCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ex;
            }
        }

        public bool InsertBI_DIM_PRODUCT_IVTo_BI_NRTANServerIncrementalProcess(Product objProduct)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;

            #region Sql 2020/07/01

            string stringQuerry = @"
                             
                            INSERT INTO NRTAN.BI_DIM_PRODUCT_IV
                            (
                            PRODUCT_CODE,
                            INCREMENTAL_VOLUMN,
                            BASICTEC1,
                            BASICTEC2,
                            BASICTEC3,
                            BASICTEC4,
                            BASICTEC5,
                            COLOURCODE,
                            BASICTEC6
                            )
                            VALUES
                            (
                            :PRODUCT_CODE,
                            :INCREMENTAL_VOLUMN,
                            :BASICTEC1,
                            :BASICTEC2,
                            :BASICTEC3,
                            :BASICTEC4,
                            :BASICTEC5,
                            :COLOURCODE,
                            :BASICTEC6
                            )
            
                            ";

            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                { 
                        DBConn.AddParameter("PRODUCT_CODE",objProduct.PRODUCT_CODE ) 
                        ,DBConn.AddParameter("INCREMENTAL_VOLUMN",objProduct.INCREMENTAL_VOLUMN )
                        ,DBConn.AddParameter("BASICTEC1",objProduct.BASICTEC1 )
                        ,DBConn.AddParameter("BASICTEC2",objProduct.BASICTEC2 )
                        ,DBConn.AddParameter("BASICTEC3",objProduct.BASICTEC3 )
                        ,DBConn.AddParameter("BASICTEC4",objProduct.BASICTEC4 )
                        ,DBConn.AddParameter("BASICTEC5",objProduct.BASICTEC5 )
                        ,DBConn.AddParameter("COLOURCODE",objProduct.COLOURCODE )
                        ,DBConn.AddParameter("BASICTEC6",objProduct.BASICTEC6 )
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objProduct, ex.StackTrace), "Error - In InsertBI_DIM_PRODUCT_IVTo_BI_NRTANServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }

        public int InsertBI_DIM_PRODUCT_IVTo_BI_NRTANServerIncrementalProcess(List<Product> listProduct)
        {
            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql 2020/07/01

                string stringQuerry = @"
                             
                            INSERT INTO NRTAN.BI_DIM_PRODUCT_IV
                            (
                            PRODUCT_CODE,
                            INCREMENTAL_VOLUMN,
                            BASICTEC1,
                            BASICTEC2,
                            BASICTEC3,
                            BASICTEC4,
                            BASICTEC5,
                            COLOURCODE,
                            BASICTEC6
                            )
                            VALUES
                            (
                            :PRODUCT_CODE,
                            :INCREMENTAL_VOLUMN,
                            :BASICTEC1,
                            :BASICTEC2,
                            :BASICTEC3,
                            :BASICTEC4,
                            :BASICTEC5,
                            :COLOURCODE,
                            :BASICTEC6
                            )
            
                            ";
                #endregion

                foreach (Product objProduct in listProduct)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[] 
                    { 
                        DBConn.AddParameter("PRODUCT_CODE",objProduct.PRODUCT_CODE ) 
                        ,DBConn.AddParameter("INCREMENTAL_VOLUMN",objProduct.INCREMENTAL_VOLUMN )
                        ,DBConn.AddParameter("BASICTEC1",objProduct.BASICTEC1 )
                        ,DBConn.AddParameter("BASICTEC2",objProduct.BASICTEC2 )
                        ,DBConn.AddParameter("BASICTEC3",objProduct.BASICTEC3 )
                        ,DBConn.AddParameter("BASICTEC4",objProduct.BASICTEC4 )
                        ,DBConn.AddParameter("BASICTEC5",objProduct.BASICTEC5 )
                        ,DBConn.AddParameter("COLOURCODE",objProduct.COLOURCODE )
                        ,DBConn.AddParameter("BASICTEC6",objProduct.BASICTEC6 )
                    };

                        DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;


                    }
                    catch (Exception ex)
                    {
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objProduct, ex.StackTrace), "Error - In InsertBI_DIM_PRODUCT_IVTo_BI_NRTANServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                oOracleConnection.Close();

            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }

            return intRecordCount;
        }

        #endregion


        public DataTable GetStatusMaster(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            string selectQuery = string.Empty;
            try
            {

                #region new sql
                selectQuery = @"
                select product_status as PRODUCT_STATUS, prod_status_desc as PROD_STATUS_DESC from NRTAN.bi_dim_status
                ";
                #endregion

                oDataTable = new DataTable();

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool CheckStatusMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(Status_Master objStatus_Master)
        {
            try
            {
                bool IsSave = false;
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.PRODUCT_STATUS) AS CheckCount 
                    FROM NRTAN.BI_DIM_STATUS  t1 
                    where t1.PRODUCT_STATUS = :PRODUCT_STATUS
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("PRODUCT_STATUS",objStatus_Master.Product_Status) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertStatusMasterTo_BI_NRTANServerIncrementalProcess(objStatus_Master);
                    }

                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - CheckStatusMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ex;
            }
        }

        public bool InsertStatusMasterTo_BI_NRTANServerIncrementalProcess(Status_Master objStatus_Master)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;
            #region Sql
            string stringQuerry = @"
                 
                 insert into NRTAN.bi_dim_status
                (
                    product_status
                    , prod_status_desc
                )
                values
                (
                    :product_status
                    , :prod_status_desc
                )

                ";
            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("product_status",objStatus_Master.Product_Status ) 
                        ,DBConn.AddParameter("prod_status_desc",objStatus_Master.Prod_Status_Desc ) 
                         
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objStatus_Master, ex.StackTrace), "Error - In InsertStatusMasterTo_BI_NRTANServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }


        public DataTable GetSalesTypeMaster(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;
            try
            {

                #region new sql
                selectQuery = @"
                select  a.sale_type  as SALESTYPE, a.description as SALESTYPEDESC from IFSAPP.TYPE_OF_SALE  a
                ";
                #endregion

                oDataTable = new DataTable();

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool CheckSalesTypeMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(SalesTypeMaster objSalesTypeMaster)
        {
            try
            {
                bool IsSave = false;
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.SALESTYPE) AS CheckCount 
                    FROM NRTAN.BI_DIM_SALES_TYPE  t1 
                    where t1.SALESTYPE = :SALESTYPE
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("SALESTYPE",objSalesTypeMaster.SalesType) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertSalesTypeMasterTo_BI_NRTANServerIncrementalProcess(objSalesTypeMaster);
                    }

                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - CheckStatusMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ex;
            }
        }

        public bool InsertSalesTypeMasterTo_BI_NRTANServerIncrementalProcess(SalesTypeMaster objSalesTypeMaster)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;
            #region Sql
            string stringQuerry = @"
                 
                 insert into NRTAN.bi_dim_sales_type
                (
                salestype
                , salestypedesc
                )
                values
                (
                :salestype
                , :salestypedesc
                )

                ";
            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("salestype",objSalesTypeMaster.SalesType ) 
                        ,DBConn.AddParameter("salestypedesc",objSalesTypeMaster.SalesTypeDesc ) 
                         
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objSalesTypeMaster, ex.StackTrace), "Error - In InsertSalesTypeMasterTo_BI_NRTANServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }


        public DataTable GetSalesPromoter(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;
            try
            {

                #region new sql
                selectQuery = @"
 
                select distinct cp.emp_no as SALES_PROMOTER_ID,
                case
                  when cp.free_field7 is null then
                   'HAA01'
                  else
                   cp.free_field7 
                end as BRANCH_ID,
                SUBSTR((CP.fname), 1, 50) as EMP_FIRST_NAME,
                SUBSTR((CP.name2), 1, 50) as EMP_MIDDLE_NAME,
                SUBSTR((CP.lname), 1, 50) as EMP_LAST_NAME,
                SUBSTR((PA.internal_display_name), 1, 50) as PROMOTER_NAME 

              from ifsapp.company_person_all   CP,
                   IFSAPP.PERS                 PA,
                   IFSAPP.COMPANY_PERS_ASSIGN  CA,
                   IFSAPP.EMP_TRAINING_HISTORY TH,
                   ifsapp.company_position     CPO,
                   ifsapp.emp_employed_time    ET
             where CP.emp_no = PA.person_id
               and cp.emp_no = ca.emp_no
               AND CP.emp_no = TH.emp_no
               AND CP.pos_code = CPO.pos_code
               AND CP.emp_no = ET.emp_no
               and ca.primary = '1'
               AND CP.employee_status <> 'Inactive'
              AND TRUNC(ET.date_of_employment) = to_date(:PreviousDate, 'YYYY/MM/DD') 
             group by cp.emp_no,
                      cp.free_field7,
                      CP.fname,
                      CP.name2,
                      CP.lname,
                      PA.internal_display_name,
                      CPO.position_title,
                      ET.date_of_employment,
                      ca.valid_to,
                      cp.emp_cat_name,
                      cp.sup_emp_no
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                };

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool CheckSalesPromoterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(SalesPromoter objSalesPromoter)
        {
            try
            {
                bool IsSave = false;
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.SALES_PROMOTER_ID) AS CheckCount 
                    FROM NRTAN.BI_SALES_PROMOTER  t1 
                    where t1.SALES_PROMOTER_ID = :SALES_PROMOTER_ID
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("SALES_PROMOTER_ID",objSalesPromoter.SalesPromoterID) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertSalesPromoterTo_BI_NRTANServerIncrementalProcess(objSalesPromoter);
                    }

                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - CheckStatusMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ex;
            }
        }

        public bool InsertSalesPromoterTo_BI_NRTANServerIncrementalProcess(SalesPromoter objSalesPromoter)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;
            #region Sql
            string stringQuerry = @"
                  insert into NRTAN.bi_sales_promoter
                  (
                sales_promoter_id
                , promoter_name
                , branch_id
                )
                values
                  (
                :sales_promoter_id
                , :promoter_name
                , :branch_id
                )
                ";
            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("sales_promoter_id",objSalesPromoter.SalesPromoterID ) 
                        ,DBConn.AddParameter("promoter_name",objSalesPromoter.PromoterName ) 
                        ,DBConn.AddParameter("branch_id",objSalesPromoter.BranchID ) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objSalesPromoter, ex.StackTrace), "Error - In InsertSalesPromoterTo_BI_NRTANServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }


        public DataTable GetSalesStatusMaster(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;
            try
            {

                #region new sql
                selectQuery = @"

                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                };

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool CheckSalesStatusMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(SalesStatus objSalesStatus)
        {
            try
            {
                bool IsSave = false;
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.SALES_STATUS) AS CheckCount 
                    FROM NRTAN.BI_SALES_STATUS  t1 
                    where t1.SALES_STATUS = :SALES_STATUS
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("SALES_STATUS",objSalesStatus.SALES_STATUS) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertSalesStatusMasterTo_BI_NRTANServerIncrementalProcess(objSalesStatus);
                    }

                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - CheckStatusMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ex;
            }
        }

        public bool InsertSalesStatusMasterTo_BI_NRTANServerIncrementalProcess(SalesStatus objSalesStatus)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;
            #region Sql
            string stringQuerry = @"
           insert into bi_sales_status
            (
            sales_status
            , plan_start_date
            , confirm_date
            , transacion_id
            )
        values
            (
            :sales_status
            ,TO_DATE(:plan_start_date,'yyyy-MM-dd') 
            ,TO_DATE(:confirm_date,'yyyy-MM-dd')  
            ,:transacion_id
            )
                ";
            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("SALES_STATUS",objSalesStatus.SALES_STATUS ) 
                        ,DBConn.AddParameter("PLAN_START_DATE",objSalesStatus.PLAN_START_DATE.ToString("yyyy/MM/dd")) 
                        ,DBConn.AddParameter("CONFIRM_DATE",objSalesStatus.CONFIRM_DATE.ToString("yyyy/MM/dd")) 
                        ,DBConn.AddParameter("TRANSACION_ID",objSalesStatus.TRANSACION_ID )  
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objSalesStatus, ex.StackTrace), "Error - In InsertSalesStatusMasterTo_BI_NRTANServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }
        
             public DataTable Get_BI_DIM_PRODUCT_IV_Changes(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region New Querry

                //selectQuery = @"
                // select 
                //        DISTINCT i.part_no,
                //        upper( ifsapp.Inventory_Part_Status_Par_API.Get_Description(i.part_status) ) as part_status,   
                //        nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no),0) as CBM,
                //        CASE
                //            WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF035', 'PF058') 
                //                THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01',i.part_no), 0) * 1.25, 10) --Refrigerators
                //            WHEN ifsapp.inventory_part_api.get_second_commodity(i.contract, i.part_no) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
                //                THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Sofa
                //            WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF003') 
                //                THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Foot Bicycle
                //            WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF124') 
                //                THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 9, 10) --Wood Working Matchine
                //            WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF151') 
                //                THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 3.5, 10) --Zoje Matchine
                //            ELSE
                //                TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0), 10)
                //        END                                                                   AS INCREMENTAL_VOLUMN

                //from ifsapp.inventory_part i
                //left join ifsapp.accounting_group a on i.accounting_group =
                //                                      a.ACCOUNTING_GROUP
                //left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
                //left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
                //                                              f.part_product_family
                //left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
                //                                     i.second_commodity
                //left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
                //                                             i.part_product_code
                //left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
                //left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
                //left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
                //                                            p.part_product_code

                //where I.contract = 'BAD01'
                //and trunc(i.last_activity_date)  >= to_date(:PreviousDate, 'YYYY/MM/DD')
                //and trunc(i.last_activity_date)  <= to_date(:PreviousDate, 'YYYY/MM/DD')
                //and i.description not in ('DEMO SAMET HOOD')
                //and i.part_no not in
                //    ('29M63STIF6', 'DELL-3568-I5NB', 'EMPL-HOME-02', 'EMPL-CHILFUR-08',
                //     'AGC-CLS-W7', 'Y-RXV357', 'AG-GRS-07', '29M63SXIF6',
                //     'SPM-75X75-IF6', 'Y-NSP60', 'HU-P9-G-PROMO', 'MCKM757C')
                //and substr(e.vendor_no, 1, 2) in ('FS', 'LS', 'IT', 'CA','AC','WA','FA','MA','WB')
                //and e.primary_vendor_db = 'Y'
                //and substr(e.CONTRACT, 1, 1) in ('W', 'F', 'P','D','B','M','S')
                //AND  SUBSTR(i.second_commodity,1,2) <> '2P' 
                //";

                #endregion

                #region selectQuery 2020/09/25

                selectQuery = @"
--MODULE NAME:  BI_DIM_PRODUCT_IV
--DESCRIPTION:  'BI_DIM_PRODUCT' SELECT UPDATE QUERY
--CREATED BY:   DINIDUR / ANUSHKAR
--CREATED DATE: 2020/09/25
--UPDATED BY:   DINIDUR
--UPDATED DATE: 2020/09/25            
  SELECT 
        DISTINCT I.PART_NO AS PRODUCT_CODE --01
        
        ,CASE
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF035', 'PF058') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01',I.PART_NO), 0) * 1.25, 10) --Refrigerators
            WHEN IFSAPP.INVENTORY_PART_API.GET_SECOND_COMMODITY(I.CONTRACT, I.PART_NO) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Sofa
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF003') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 1.75, 10) --Foot Bicycle
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF124') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 9, 10) --Wood Working Matchine
            WHEN IFSAPP.INVENTORY_PART_API.GET_PART_PRODUCT_FAMILY(I.CONTRACT, I.PART_NO) IN ('PF151') 
                THEN TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0) * 3.5, 10) --Zoje Matchine
            ELSE
                TRUNC(NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0), 10)
        END  AS INCREMENTAL_VOLUMN  --02
    
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT1'), '') AS BASICTEC1 --03
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT2'), '') AS BASICTEC2 --04
            
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT3'), '') AS BASICTEC3  --05
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT4'), '') AS BASICTEC4  --06
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT5'), '') AS BASICTEC5  --07
            
        ,NVL((
        SELECT  C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'COL'), '') AS COLOURCODE --08
        
        ,NVL((
        SELECT C.ATTR_VALUE      
        FROM IFSAPP.INV_PART_DISCRETE_CHAR C 
        WHERE 
            C.CONTRACT = 'BAD01' 
            AND  C.PART_NO = I.PART_NO 
            AND C.CHARACTERISTIC_CODE = 'BT6'), '') AS BASICTEC6  --09
    
    FROM 
        IFSAPP.INVENTORY_PART I
    
    WHERE 
        I.CONTRACT = 'BAD01'
        AND TRUNC(I.last_activity_date) = TO_DATE(:PreviousDate, 'YYYY/MM/DD')


";

                #endregion


                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[]
                {
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))//noneed to add @ before parameter value
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }
        public DataTable GetIFSSystemChangedProductCodes(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region New Querry

                //selectQuery = @"
                // select 
                //        DISTINCT i.part_no,
                //        upper( ifsapp.Inventory_Part_Status_Par_API.Get_Description(i.part_status) ) as part_status,   
                //        nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no),0) as CBM,
                //        CASE
                //            WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF035', 'PF058') 
                //                THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01',i.part_no), 0) * 1.25, 10) --Refrigerators
                //            WHEN ifsapp.inventory_part_api.get_second_commodity(i.contract, i.part_no) IN ('2GFS1','2GF19','2GF21','2GFS5','2GFS1','2GI19','2GIS5','2GI20','2GIS1','2GL19','2GLS5','2GLS1','2G196','2G716','2G293','2G294','1C049') 
                //                THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Sofa
                //            WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF003') 
                //                THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 1.75, 10) --Foot Bicycle
                //            WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF124') 
                //                THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 9, 10) --Wood Working Matchine
                //            WHEN ifsapp.inventory_part_api.get_part_product_family(i.contract, i.part_no) IN ('PF151') 
                //                THEN TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0) * 3.5, 10) --Zoje Matchine
                //            ELSE
                //                TRUNC(nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no), 0), 10)
                //        END                                                                   AS INCREMENTAL_VOLUMN

                //from ifsapp.inventory_part i
                //left join ifsapp.accounting_group a on i.accounting_group =
                //                                      a.ACCOUNTING_GROUP
                //left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
                //left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
                //                                              f.part_product_family
                //left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
                //                                     i.second_commodity
                //left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
                //                                             i.part_product_code
                //left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
                //left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
                //left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
                //                                            p.part_product_code

                //where I.contract = 'BAD01'
                //and trunc(i.last_activity_date)  >= to_date(:PreviousDate, 'YYYY/MM/DD')
                //and trunc(i.last_activity_date)  <= to_date(:PreviousDate, 'YYYY/MM/DD')
                //and i.description not in ('DEMO SAMET HOOD')
                //and i.part_no not in
                //    ('29M63STIF6', 'DELL-3568-I5NB', 'EMPL-HOME-02', 'EMPL-CHILFUR-08',
                //     'AGC-CLS-W7', 'Y-RXV357', 'AG-GRS-07', '29M63SXIF6',
                //     'SPM-75X75-IF6', 'Y-NSP60', 'HU-P9-G-PROMO', 'MCKM757C')
                //and substr(e.vendor_no, 1, 2) in ('FS', 'LS', 'IT', 'CA','AC','WA','FA','MA','WB')
                //and e.primary_vendor_db = 'Y'
                //and substr(e.CONTRACT, 1, 1) in ('W', 'F', 'P','D','B','M','S')
                //AND  SUBSTR(i.second_commodity,1,2) <> '2P' 
                //";

                #endregion

                #region selectQuery 2020/09/25

                selectQuery = @"
                 
--MODULE NAME:  BI_DIM_PRODUCT
--DESCRIPTION:  'BI_DIM_PRODUCT' SELECT UPDATE QUERY
--CREATED BY:   DINIDUR / CHATHURAS / ANUSHKAR
--CREATED DATE: 2020/09/25
--UPDATED BY:   DINIDUR
--UPDATED DATE: 2020/09/25

SELECT 
    DISTINCT I.PART_NO                                                          AS PRODUCT_CODE,
    I.DESCRIPTION                                                               AS PRODUCT_DESC,
    I.ACCOUNTING_GROUP                                                          AS ACCOUNTING_GROUP_ID,
    IFSAPP.ACCOUNTING_GROUP_API.GET_DESCRIPTION(I.ACCOUNTING_GROUP)             AS ACCOUNTING_GROUP_DESC,
    I.PART_PRODUCT_FAMILY                                                       AS PRODUCT_FAMILY_ID,
    IFSAPP.INVENTORY_PRODUCT_FAMILY_API.GET_DESCRIPTION(I.PART_PRODUCT_FAMILY)  AS PRODUCT_FAMILY_DESC,
    I.SECOND_COMMODITY                                                          AS COMMODITY_ID,
    IFSAPP.COMMODITY_GROUP_API.GET_DESCRIPTION(I.SECOND_COMMODITY)              AS COMMODITY_DESC,
    I.PART_PRODUCT_CODE                                                         AS BRAND_ID,
    IFSAPP.INVENTORY_PRODUCT_CODE_API.GET_DESCRIPTION(I.PART_PRODUCT_CODE)      AS BRAND_DESC,
    UPPER(IFSAPP.INVENTORY_PART_STATUS_PAR_API.GET_DESCRIPTION(I.PART_STATUS))  AS PART_STATUS,
    NVL(IFSAPP.INVENTORY_PART_API.GET_VOLUME('WAA01', I.PART_NO), 0)            AS CBM,
    ''                                                                          AS PRODUCT_ATTRIBUTE_1,
    ''                                                                          AS PRODUCT_ATTRIBUTE_2,
    ''                                                                          AS PRODUCT_ATTRIBUTE_3,
    ''                                                                          AS PRODUCT_ATTRIBUTE_4,
    ''                                                                          AS PRODUCT_ATTRIBUTE_5,
    TRUNC(IFSAPP.SALES_PRICE_LIST_PART_API.GET_SALES_PRICE('1', I.PART_NO),4)   AS SALES_PRICE

FROM 
    IFSAPP.INVENTORY_PART I
    
WHERE  
    I.CONTRACT = 'BAD01'
    AND TRUNC(I.LAST_ACTIVITY_DATE)  = TO_DATE(:PreviousDate, 'YYYY/MM/DD')
    
                ";

                #endregion


                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))//noneed to add @ before parameter value
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }
        
             public bool Check_BI_DIM_PRODUCT_VI_ExsistInBI_NRTANAndContinueProductUpdateProcess(Product objProduct)
        {
            bool IsSave = false;

            try
            {

                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.product_code) AS CheckCount 
                    FROM NRTAN.bi_dim_product_iv  t1 
                    where t1.product_code = :PRODUCT_CODE
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[]
                   {
                   DBConn.AddParameter("PRODUCT_CODE",objProduct.PRODUCT_CODE)
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 1)
                    {
                        IsSave = Update_BI_DIM_PRODUCT_IV_DetailsToBI_NRTANServer(objProduct);

                    }
                }
                oOracleConnection.Close();
                return IsSave;


            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In UpdateProductDetailsToBI_NRTANServer ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return IsSave;

        }


        public bool CheckProductCodeExsistInBI_NRTANAndContinueProductUpdateProcess(Product objProduct)
        {
            bool IsSave = false;

            try
            {

                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.product_code) AS CheckCount 
                    FROM NRTAN.bi_dim_product  t1 
                    where t1.product_code = :PRODUCT_CODE
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("PRODUCT_CODE",objProduct.PRODUCT_CODE) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 1)
                    {
                        IsSave = UpdateProductDetailsToBI_NRTANServer(objProduct);

                    }
                }
                oOracleConnection.Close();
                return IsSave;


            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In UpdateProductDetailsToBI_NRTANServer ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return IsSave;

        }
        

             public bool Update_BI_DIM_PRODUCT_IV_DetailsToBI_NRTANServer(Product objProduct)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;

           

            #region Sql 2020/09/25

            string stringQuerry = @"
 --MODULE NAME:  BI_DIM_PRODUCT_IV
--DESCRIPTION:  'BI_DIM_PRODUCT_IV' UPDATE UPDATE QUERY
--CREATED BY:   DINIDUR 
--CREATED DATE: 2020/09/25
--UPDATED BY:   DINIDUR
--UPDATED DATE: 2020/09/25
 UPDATE 
        NRTAN.BI_DIM_PRODUCT_IV SET 
          INCREMENTAL_VOLUMN= :INCREMENTAL_VOLUMN,
          BASICTEC1 =:BASICTEC1,
          BASICTEC2=:BASICTEC2,
          BASICTEC3=:BASICTEC3,
          BASICTEC4=:BASICTEC4,
          BASICTEC5=:BASICTEC5,
          COLOURCODE=:COLOURCODE,
          BASICTEC6=:BASICTEC6,
          DATE_UPDATED = SYSDATE
WHERE 
      PRODUCT_CODE = :PRODUCT_CODE
            ";

            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[]
                   {

                         DBConn.AddParameter("INCREMENTAL_VOLUMN",objProduct.INCREMENTAL_VOLUMN )
                         ,DBConn.AddParameter("PRODUCT_CODE",objProduct.PRODUCT_CODE )
                        ,DBConn.AddParameter("BASICTEC1",objProduct.BASICTEC1 )
                        ,DBConn.AddParameter("BASICTEC2",objProduct.BASICTEC2 )
                        ,DBConn.AddParameter("BASICTEC3",objProduct.BASICTEC3 )
                        ,DBConn.AddParameter("BASICTEC4",objProduct.BASICTEC4 )
                        ,DBConn.AddParameter("BASICTEC5",objProduct.BASICTEC5 )
                        ,DBConn.AddParameter("COLOURCODE",objProduct.COLOURCODE )
                        ,DBConn.AddParameter("BASICTEC6",objProduct.BASICTEC6 )
                        

                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objProduct, ex.StackTrace), "Error - In UpdateProductDetailsToBI_NRTANServer", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }




        public bool UpdateProductDetailsToBI_NRTANServer(Product objProduct)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;

            #region Sql OLD

//            string stringQuerry = @"
//            UPDATE NRTAN.bi_dim_product SET status = :part_status, CBM = :CBM, INCREMENTAL_VOLUMN = :INCREMENTAL_VOLUMN, date_updated = SYSDATE WHERE PRODUCT_CODE = :PRODUCT_CODE
//            ";

            #endregion

            #region Sql 2020/06/11

            //string stringQuerry = @"
            //UPDATE NRTAN.bi_dim_product SET status = :part_status, CBM = :CBM, date_updated = SYSDATE WHERE PRODUCT_CODE = :PRODUCT_CODE
            //";

            #endregion

            #region Sql 2020/09/25

            string stringQuerry = @"
 --MODULE NAME:  BI_DIM_PRODUCT
--DESCRIPTION:  'BI_DIM_PRODUCT' UPDATE UPDATE QUERY
--CREATED BY:   DINIDUR / CHATHURAS / ANUSHKAR
--CREATED DATE: 2020/09/25
--UPDATED BY:   DINIDUR
--UPDATED DATE: 2020/09/25
 UPDATE 
        NRTAN.BI_DIM_PRODUCT SET 
          PRODUCT_DESC= :PRODUCT_DESC ,
          ACCOUNTING_GROUP_ID =:ACCOUNTING_GROUP_ID,
          ACCOUNTING_GROUP_DESC=:ACCOUNTING_GROUP_DESC,
          PRODUCT_FAMILY_ID=:PRODUCT_FAMILY_ID,
          PRODUCT_FAMILY_DESC=:PRODUCT_FAMILY_DESC,
          COMMODITY_ID=:COMMODITY_ID,
          COMMODITY_DESC=:COMMODITY_DESC,
          BRAND_ID=:BRAND_ID,
          BRAND_DESC=:BRAND_DESC,
          PRODUCT_ATTRIBUTE1=:PRODUCT_ATTRIBUTE1,
          PRODUCT_ATTRIBUTE2=:PRODUCT_ATTRIBUTE2,
          PRODUCT_ATTRIBUTE3=:PRODUCT_ATTRIBUTE3,
          PRODUCT_ATTRIBUTE4=:PRODUCT_ATTRIBUTE4,
          PRODUCT_ATTRIBUTE5=:PRODUCT_ATTRIBUTE5,
          RETAIL_PRICE=:SALES_PRICE,
          STATUS = :PART_STATUS,
          CBM = :CBM,
          DATE_UPDATED = SYSDATE
WHERE 
      PRODUCT_CODE = :PRODUCT_CODE
            ";

            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 

                         DBConn.AddParameter("PRODUCT_CODE",objProduct.PRODUCT_CODE )
                        ,DBConn.AddParameter("PRODUCT_DESC",objProduct.PRODUCT_DESC )
                        ,DBConn.AddParameter("ACCOUNTING_GROUP_ID",objProduct.ACCOUNTING_GROUP_ID )
                        ,DBConn.AddParameter("ACCOUNTING_GROUP_DESC",objProduct.ACCOUNTING_GROUP_DESC )
                        ,DBConn.AddParameter("PRODUCT_FAMILY_ID",objProduct.PRODUCT_FAMILY_ID )
                        ,DBConn.AddParameter("PRODUCT_FAMILY_DESC",objProduct.PRODUCT_FAMILY_DESC )
                        ,DBConn.AddParameter("COMMODITY_ID",objProduct.COMMODITY_ID )
                        ,DBConn.AddParameter("COMMODITY_DESC",objProduct.COMMODITY_DESC )                     
                        ,DBConn.AddParameter("BRAND_ID",objProduct.BRAND_ID )
                        ,DBConn.AddParameter("BRAND_DESC",objProduct.BRAND_DESC )
                        ,DBConn.AddParameter("PART_STATUS",objProduct.PART_STATUS )
                        ,DBConn.AddParameter("CBM",objProduct.CBM )
                        ,DBConn.AddParameter("PRODUCT_ATTRIBUTE1",objProduct.PRODUCT_ATTRIBUTE_1 )
                        ,DBConn.AddParameter("PRODUCT_ATTRIBUTE2",objProduct.PRODUCT_ATTRIBUTE_2 )
                        ,DBConn.AddParameter("PRODUCT_ATTRIBUTE3",objProduct.PRODUCT_ATTRIBUTE_3 )
                        ,DBConn.AddParameter("PRODUCT_ATTRIBUTE4",objProduct.PRODUCT_ATTRIBUTE_4 )
                        ,DBConn.AddParameter("PRODUCT_ATTRIBUTE5",objProduct.PRODUCT_ATTRIBUTE_5 )
                        ,DBConn.AddParameter("SALES_PRICE",objProduct.Sales_Price )
                        
                        
                        
                       
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objProduct, ex.StackTrace), "Error - In UpdateProductDetailsToBI_NRTANServer", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }


        public DataTable GetRentSystemUpdateShopCodes(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionnRENT();
            string selectQuery = string.Empty;

            try
            {
                #region New Querry
                selectQuery = @"
                SELECT  B.SITEID as STORE_CODE,SUM(B.SQUAREAREA) AS TotalSquareFeet
                FROM   ifsapp.SIN_RENT_BUILDINGMAIN B 
                WHERE to_date(B.LASTUPDATEDATE,'YYYY/MM/DD') =  to_date(:PreviousDate,'YYYY/MM/DD')
                AND B.STATUS = 'A'
                GROUP BY B.SITEID
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))//noneed to add @ before parameter value
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool CheckRENTStoreCodeExsistInBI_NRTANAndContinueProductUpdateProcess(Store objStore)
        {
            bool IsSave = false;

            try
            {

                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.SITE_ID) AS CheckCount 
                    FROM NRTAN.bi_dim_store  t1 
                    where SITE_ID = :SITE_ID
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("SITE_ID",objStore.SITE_ID) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) > 0)
                    {
                        IsSave = UpdateBI_NRTANServerShopSquareFeet(objStore);

                    }
                }
                oOracleConnection.Close();
                return IsSave;


            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In UpdateIFSSystemChangedProductDetails ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return IsSave;

        }

        public bool UpdateBI_NRTANServerShopSquareFeet(Store objStore)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;

            try
            {

                OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("SITE_ID",objStore.SITE_ID ) 
                        ,DBConn.AddParameter("capacities",objStore.CAPACITIES ) 
                };


                #region Sql
                string stringQuerry = @"
                UPDATE NRTAN.bi_dim_store SET capacities= :capacities WHERE SITE_ID= :SITE_ID
                ";
                #endregion


                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objStore, ex.StackTrace), "Error - In UpdateBI_NRTANServerShopSquareFeet", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }


        public DataTable GetPromotion(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region New Sql
                selectQuery = @"
                SELECT 
                to_char(R.rule_no) as PROMO_CODE,--1
                R.description as PROMO_DESC,--2
                to_date( R.date_entered) AS date_entered--3
                ,R.valid_from AS PROMO_ST_DATE --4
                ,R.valid_to AS PROMO_END_DATE --5
                ,R.part AS PROMOTION_FREE_ITEM --6
                ,TL.site AS APPLY_SITE --7
                ,TL.part_no AS PROMOTION_EFFECT_MAIN_ITEM --8
                ,T.rule_type_no AS PROMO_TYPE	--9
                ,T.description AS PROMO_TYPE_DESC	--10
                FROM IFSAPP.HPNRET_RULE_TYPE T
                INNER JOIN IFSAPP.HPNRET_RULE R ON R.rule_type_no = T.rule_type_no
                INNER JOIN IFSAPP.HPNRET_RULE_TEMP_DET RD ON RD.rule_no = R.rule_no
                INNER JOIN IFSAPP.HPNRET_RULE_TEMPLATE RH ON RH.template_id = RD.template_id   
                INNER JOIN IFSAPP.HPNRET_RULE_LINK TL ON TL.template_id = RH.template_id AND RD.template_id = TL.template_id 
                WHERE T.rule_type_no NOT IN ( 'DISCOUNT' )
                AND  to_date( R.date_entered)  = to_date(:PreviousDate, 'YYYY/MM/DD')
                AND ifsapp.sales_part_api.get_catalog_group('BAD01',R.part) NOT IN ('2G450') ---DO NOT GET NON INVENTORY ITEMS BY USING CATALOG NO
                ";
                #endregion
                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool CheckPromotionCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(Promotion objNeenOpalPromotion)
        {
            try
            {
                bool IsSave = false;
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.PROMO_CODE) AS CheckCount 
                    FROM NRTAN.BI_DIM_PROMOTION  t1 
                    where t1.PROMO_CODE = :PROMO_CODE
                    AND t1.APPLY_SITE = :APPLY_SITE
                    AND t1.PROMOTION_EFFECT_MAIN_ITEM = :PROMOTION_EFFECT_MAIN_ITEM
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("PROMO_CODE",objNeenOpalPromotion.PROMO_CODE) 
                   ,DBConn.AddParameter("APPLY_SITE",objNeenOpalPromotion.APPLY_SITE)
                   ,DBConn.AddParameter("PROMOTION_EFFECT_MAIN_ITEM",objNeenOpalPromotion.PROMOTION_EFFECT_MAIN_ITEM)
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertPromotionCodeTo_BI_NRTAN_ServerIncrementalProcess(objNeenOpalPromotion);
                    }

                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - CheckPromotionCodeExistsIn_NRTAN_ServerAndProcessTo_NRTAN_Server.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ex;
            }
        }

        public bool InsertPromotionCodeTo_BI_NRTAN_ServerIncrementalProcess(Promotion objNeenOpalPromotion)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;


            #region Sql
            string stringQuerry = @" 
            INSERT INTO NRTAN.BI_DIM_PROMOTION
            (
             PROMO_CODE	 ,		
            PROMO_DESC	, 		
            PROMO_ENTER_DATE	, 		
            PROMO_ST_DATE	 ,		
            PROMO_END_DATE	 ,		
            PROMOTION_FREE_ITEM	 ,		
            APPLY_SITE,	 			
            PROMOTION_EFFECT_MAIN_ITEM,	 		
            PROMO_TYPE	 ,		
            PROMO_TYPE_DESC 
            )
            VALUES
            (
            :PROMO_CODE	 ,		
            :PROMO_DESC	 ,		
            :PROMO_ENTER_DATE	 ,		
            :PROMO_ST_DATE	, 		
            :PROMO_END_DATE	 ,		
            :PROMOTION_FREE_ITEM	 ,		
            :APPLY_SITE	 	,		
            :PROMOTION_EFFECT_MAIN_ITEM	 	,	
            :PROMO_TYPE	 	,	
            :PROMO_TYPE_DESC
            )

                ";
            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("PROMO_CODE",objNeenOpalPromotion.PROMO_CODE ) 
                        ,DBConn.AddParameter("PROMO_DESC",objNeenOpalPromotion.PROMO_DESC) 
                        ,DBConn.AddParameter("PROMO_ENTER_DATE",objNeenOpalPromotion.PROMO_ENTER_DATE) 
                        ,DBConn.AddParameter("PROMO_ST_DATE",objNeenOpalPromotion.PROMO_ST_DATE) 
                        ,DBConn.AddParameter("PROMO_END_DATE",objNeenOpalPromotion.PROMO_END_DATE) 
                        ,DBConn.AddParameter("PROMOTION_FREE_ITEM",objNeenOpalPromotion.PROMOTION_FREE_ITEM) 
                        ,DBConn.AddParameter("APPLY_SITE",objNeenOpalPromotion.APPLY_SITE) 
                        ,DBConn.AddParameter("PROMOTION_EFFECT_MAIN_ITEM",objNeenOpalPromotion.PROMOTION_EFFECT_MAIN_ITEM) 
                        ,DBConn.AddParameter("PROMO_TYPE",objNeenOpalPromotion.PROMO_TYPE) 
                        ,DBConn.AddParameter("PROMO_TYPE_DESC",objNeenOpalPromotion.PROMO_TYPE_DESC) 

                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objNeenOpalPromotion, ex.StackTrace), "Error - In InsertPromotionCodeTo_NRTAN_ServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }


        public DataTable GetSalesLocation(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region New Sql
                selectQuery = @"
                select L.sales_location_no as SALES_LOCATION_CODE, L.description as SALES_LOCATION_DESC 
                from IFSAPP.HPNRET_SALES_LOCATION L
                where     SUBSTR(L.objversion,1,4 ) = :PassYear
                AND SUBSTR(L.objversion,5,2 ) = :PassMonth
                AND SUBSTR(L.objversion,7,2 ) = :PassDate
                ";
                #endregion
                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                    DBConn.AddParameter("PassYear",dtPreviousDate.Year)
                   ,DBConn.AddParameter("PassMonth",dtPreviousDate.Month)
                   ,DBConn.AddParameter("PassDate",dtPreviousDate.Day)
                };

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);

                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool CheckSalesLocationCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(SalesLocation objSalesLocation)
        {
            try
            {
                bool IsSave = false;
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.SALES_LOCATION_CODE) AS CheckCount 
                    FROM NRTAN.BI_DIM_SALES_LOCATION  t1 
                    where t1.SALES_LOCATION_CODE = :SALES_LOCATION_CODE  
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("SALES_LOCATION_CODE",objSalesLocation.SALES_LOCATION_CODE) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertSalesLocationCodeTo_BI_NRTAN_ServerIncrementalProcess(objSalesLocation);
                    }

                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - CheckPromotionCodeExistsIn_NRTAN_ServerAndProcessTo_NRTAN_Server.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ex;
            }
        }

        public bool InsertSalesLocationCodeTo_BI_NRTAN_ServerIncrementalProcess(SalesLocation objSalesLocation)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;


            #region Sql
            string stringQuerry = @" 
            INSERT INTO NRTAN.BI_DIM_SALES_LOCATION
            (
             SALES_LOCATION_CODE	 ,		
            SALES_LOCATION_DESC	 
            )
            VALUES
            (
            :SALES_LOCATION_CODE	 ,		
            :SALES_LOCATION_DESC	  
            )

                ";
            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("SALES_LOCATION_CODE",objSalesLocation.SALES_LOCATION_CODE ) 
                        ,DBConn.AddParameter("SALES_LOCATION_DESC",objSalesLocation.SALES_LOCATION_DESC) 
 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objSalesLocation, ex.StackTrace), "Error - In InsertSalesLocationCodeTo_BI_NRTAN_ServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }


        public DataTable GetInventoryLocation(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region old Sql
                //                selectQuery = @"
                //                SELECT L.location_no as INVENTORY_LOCATION_CODE , l.location_name as INVENTORY_LOCATION_DESC 
                //                , l.location_group as INVENTORY_LOCATION_GROUP , '' as INVENTORY_LOCATION_GROUP_DESC  
                //                FROM IFSAPP.INVENTORY_LOCATION L
                //                WHERE L.contract = 'BAD01'
                //                ORDER BY L.location_group
                //                ";
                #endregion

                #region New Sql
             //   selectQuery = @"
             //   SELECT L.location_no    as INVENTORY_LOCATION_CODE,
             //      l.location_name  as INVENTORY_LOCATION_DESC,
             //      SL.FINAL_DESC    AS INVENTORY_LOCATION_GROUP1,
             //      l.location_group as INVENTORY_LOCATION_GROUP2
             // FROM IFSAPP.INVENTORY_LOCATION L
             // LEFT OUTER JOIN ifsapp.SIN_BI_SALES_LOCATION SL ON SL.LOCATION_GROUP =
             //                                                    l.location_group
             //WHERE L.contract = 'BAD01'
             //ORDER BY L.location_group
             //   ";
                #endregion

                #region New Sql 2020/10/05

//                selectQuery = @"
//--MODULE NAME:  BI_DIM_INVENTORY_LOCATION
//--DESCRIPTION:  'BI_DIM_INVENTORY_LOCATION' SINGER SELECT QUERY
//--CREATED BY:   DINIDUR
//--CREATED DATE: 2020/10/05
//--UPDATED BY:   DINIDUR
//--UPDATED DATE: 2020/10/05

//SELECT  DISTINCT 
//        L.LOCATION_NO    AS INVENTORY_LOCATION_CODE,
//        L.LOCATION_NAME  AS INVENTORY_LOCATION_DESC,
//        SL.FINAL_DESC    AS INVENTORY_LOCATION_GROUP1,
//        L.LOCATION_GROUP AS INVENTORY_LOCATION_GROUP2
//FROM IFSAPP.INVENTORY_LOCATION L
//        LEFT OUTER JOIN 
//        IFSAPP.SIN_BI_SALES_LOCATION SL ON SL.LOCATION_GROUP = L.LOCATION_GROUP
//        ORDER BY 
//        L.LOCATION_GROUP
//                ";

                #endregion

                #region selectQuery 2020/10/08

                selectQuery = @"
--MODULE NAME:  BI_DIM_INVENTORY_LOCATION
--DESCRIPTION:  'BI_DIM_INVENTORY_LOCATION' SINGER SELECT QUERY
--CREATED BY:   DINIDUR
--CREATED DATE: 2020/10/05
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2020/10/08

SELECT  DISTINCT 
        TRIM(L.LOCATION_NO)    AS INVENTORY_LOCATION_CODE,
        L.LOCATION_NAME        AS INVENTORY_LOCATION_DESC,
        SL.FINAL_DESC          AS INVENTORY_LOCATION_GROUP1,
        L.LOCATION_GROUP       AS INVENTORY_LOCATION_GROUP2
FROM IFSAPP.INVENTORY_LOCATION L
        LEFT OUTER JOIN 
        IFSAPP.SIN_BI_SALES_LOCATION SL ON SL.LOCATION_GROUP = L.LOCATION_GROUP

ORDER BY 
        L.LOCATION_GROUP
";

                #endregion

                oDataTable = new DataTable();


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);

                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable GetSalesLocationDMD()
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionDMD();
            string selectQuery = string.Empty;

            try
            {

                #region Querry
                selectQuery = @"
                SELECT L.location_no as INVENTORY_LOCATION_CODE , l.location_name as INVENTORY_LOCATION_DESC 
                , l.location_group as INVENTORY_LOCATION_GROUP , '' as INVENTORY_LOCATION_GROUP_DESC  
                FROM IFSAPP.INVENTORY_LOCATION L
                WHERE L.contract = 'WDA01'
                ORDER BY L.location_group
                ";
                #endregion

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }

        }

        public bool CheckInventoryLocationCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(InventoryLocation objInventoryLocation)
        {
            try
            {
                bool IsSave = false;
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.INVENTORY_LOCATION_CODE) AS CheckCount 
                    FROM NRTAN.BI_DIM_INVENTORY_LOCATION  t1 
                    where t1.INVENTORY_LOCATION_CODE = :INVENTORY_LOCATION_CODE  
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("INVENTORY_LOCATION_CODE",objInventoryLocation.INVENTORY_LOCATION_CODE) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertInventoryLocationCodeTo_BI_NRTAN_ServerIncrementalProcess(objInventoryLocation);
                    }

                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - CheckInventoryLocationCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ex;
            }
        }

        public bool InsertInventoryLocationCodeTo_BI_NRTAN_ServerIncrementalProcess(InventoryLocation objInventoryLocation)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;


            #region Sql
            string stringQuerry = @" 
            INSERT INTO NRTAN.BI_DIM_INVENTORY_LOCATION
            (
             INVENTORY_LOCATION_CODE ,	 		
            INVENTORY_LOCATION_DESC,	 		
            INVENTORY_LOCATION_GROUP1,	 		
            INVENTORY_LOCATION_GROUP2	 
            )
            VALUES
            (
            :INVENTORY_LOCATION_CODE ,	 		
            :INVENTORY_LOCATION_DESC,	 		
            :INVENTORY_LOCATION_GROUP_1,	 		
            :INVENTORY_LOCATION_GROUP_2	 
            )

                ";
            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                    DBConn.AddParameter("INVENTORY_LOCATION_CODE",objInventoryLocation.INVENTORY_LOCATION_CODE ) 
                    ,DBConn.AddParameter("INVENTORY_LOCATION_DESC",objInventoryLocation.INVENTORY_LOCATION_DESC) 
                    ,DBConn.AddParameter("INVENTORY_LOCATION_GROUP_1",objInventoryLocation.INVENTORY_LOCATION_GROUP1 ) 
                    ,DBConn.AddParameter("INVENTORY_LOCATION_GROUP_2",objInventoryLocation.INVENTORY_LOCATION_GROUP2) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objInventoryLocation, ex.StackTrace), "Error - In InsertSalesLocationCodeTo_BI_NRTAN_ServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }

        public DataTable GetReturnCode()
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region Querry
                selectQuery = @"
                 SELECT T.RET_REASON_CODE,T.RET_REASON_DESC FROM 
                (
                select VRsn.reason AS RET_REASON_CODE,VRsn.description  AS RET_REASON_DESC
                from ifsapp.hpnret_variation_reason VRsn  
                UNION 
                select MRsn.return_reason_code as RET_REASON_CODE ,MRsn.return_reason_description as RET_REASON_DESC 
                from  IFSAPP.RETURN_MATERIAL_REASON   MRsn 
                )T 
                ";
                #endregion

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }

        }

        public DataTable GetReturnCodeDMD()
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionDMD();
            string selectQuery = string.Empty;

            try
            {

                #region Querry
                selectQuery = @"
                SELECT T.RET_REASON_CODE,T.RET_REASON_DESC FROM 
                (
                select VRsn.reason AS RET_REASON_CODE,VRsn.description  AS RET_REASON_DESC
                from ifsapp.hpnret_variation_reason VRsn  
                UNION 
                select MRsn.return_reason_code as RET_REASON_CODE ,MRsn.return_reason_description as RET_REASON_DESC 
                from  IFSAPP.RETURN_MATERIAL_REASON   MRsn
                )T
                ";
                #endregion

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }

        }

        public bool CheckReturnCodeExistsIn_BI_NRTANServerAndProcessToNRTANServer(ReturnCode objReturnCode)
        {
            try
            {
                bool IsSave = false;
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.RET_REASON_CODE) AS CheckCount 
                    FROM NRTAN.BI_DIM_RETURN   t1 
                    where t1.RET_REASON_CODE = :RET_REASON_CODE
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                     DBConn.AddParameter("RET_REASON_CODE",objReturnCode.RET_REASON_CODE) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertReturnCodeToBI_NRTANServerIncrementalProcess(objReturnCode);

                    }

                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In CheckReturnCodeExistsIn_BI_NRTANServerAndProcessToNRTANServer ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ex;
            }
        }

        public bool InsertReturnCodeToBI_NRTANServerIncrementalProcess(ReturnCode objReturnCode)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;

            #region Querry
            string insertQuery = @" 
                       INSERT INTO NRTAN.BI_DIM_RETURN   
                      (
                      RET_REASON_CODE,
                       RET_REASON_DESC 
                       )
                    VALUES
                      (
                       :RET_REASON_CODE,
                       :RET_REASON_DESC 
                       )
                    ";
            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   {
                        DBConn.AddParameter("RET_REASON_CODE", objReturnCode.RET_REASON_CODE) 
                        ,DBConn.AddParameter("RET_REASON_DESC",objReturnCode.RET_REASON_DESC) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(insertQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objReturnCode, ex.StackTrace), "Error - In InsertReturnCodeToBI_NRTANServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }


        public DataTable GetCustomer(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region SQL
                selectQuery = @"
                --BOTH	
               select CI.customer_id AS CUSTOMER_CODE,	
               HCG.first_name AS FIRST_NAME,	
               HCG.last_name AS LAST_NAME,	
               CIA.address1 || '_' || CIA.address2 as RECIDENCE,	
               max(case	
                     when CICM.method_id_db = 'MOBILE' then	
                      CICM.value	
                   end) as MOBILE,	
               max(case	
                     when CICM.method_id_db = 'PHONE' then	
                      CICM.value	
                   end) as TELEPHONE,	
               max(case	
                     when CICM.method_id_db = 'E_MAIL' then	
                      CICM.value	
                   end) as EMAIL,	
                   ' ' AS Loyality_STATUS	
               	
               	
               	
          FROM ifsapp.customer_info CI	
          LEFT JOIN ifsapp.hpnret_customer_guarantor HCG ON HCG.id = CI.customer_id	
          LEFT JOIN IFSAPP.Loyalty_Customer_Info LCI ON LCI.customer_id =	
                                                        CI.customer_id	
          LEFT JOIN ifsapp.customer_info_address CIA ON CIA.customer_id =	
                                                        CI.customer_id	
          LEFT JOIN ifsapp.customer_info_comm_method CICM ON CICM.customer_id =	
                                                             CI.customer_id	
          LEFT JOIN ifsapp.identity_invoice_info III ON III.identity =	
                                                        CI.customer_id	
          LEFT JOIN IFSAPP.Customer_Order_Line COL ON COL.customer_no =	
                                                      CI.customer_id	
          LEFT JOIN IFSAPP.direct_sales_dtl_tab y on y.order_no = COL.order_no	
                                                 and y.line_no = COL.line_no	
                                                 and y.line_item_no =	
                                                     COL.line_item_no	
                                                 and y.rel_no = COL.rel_no	
         where TRUNC(y.transaction_date) = TO_DATE(:PreviousDate, 'yyyy/mm/dd')  
          	
         group by CI.customer_id,	
                  HCG.first_name,	
                  HCG.last_name,	
                  CI.creation_date,	
                  HCG.hpnret_gender,	
                  LCI.card_type,	
                  CIA.city,	
                  HCG.nationality,	
                  HCG.date_of_birth,	
                  HCG.cat_date,	
                  CIA.city,	
                  III.group_id,	
                  CIA.address1,	
                  CIA.address2	
        union	
        select CI.customer_id AS CUSTOMER_CODE,	
               HCG.first_name AS FIRST_NAME,	
               HCG.last_name AS LAST_NAME,	
               CIA.address1 || '_' || CIA.address2 as RECIDENCE,	
                max(case	
                     when CICM.method_id_db = 'MOBILE' then	
                      CICM.value	
                   end) as MOBILE,	
               max(case	
                     when CICM.method_id_db = 'PHONE' then	
                      CICM.value	
                   end) as TELEPHONE,	
               max(case	
                     when CICM.method_id_db = 'E_MAIL' then	
                      CICM.value	
                   end) as EMAIL,	
               ' ' AS Loyality_STATUS	
               	
               	
                	
          FROM ifsapp.customer_info CI	
          LEFT JOIN ifsapp.hpnret_customer_guarantor HCG ON HCG.id = CI.customer_id	
          LEFT JOIN IFSAPP.Loyalty_Customer_Info LCI ON LCI.customer_id =	
                                                        CI.customer_id	
          LEFT JOIN ifsapp.customer_info_address CIA ON CIA.customer_id =	
                                                        CI.customer_id	
          LEFT JOIN ifsapp.customer_info_comm_method CICM ON CICM.customer_id =	
                                                             CI.customer_id	
          LEFT JOIN ifsapp.identity_invoice_info III ON III.identity =	
                                                        CI.customer_id	
          LEFT JOIN IFSAPP.Customer_Order_Line COL ON COL.customer_no =	
                                                      CI.customer_id	
          LEFT JOIN IFSAPP.indirect_sales_dtl_tab y on y.order_no = COL.order_no	
                                                   and y.line_no = COL.line_no	
                                                   and y.line_item_no =	
                                                       COL.line_item_no	
                                                   and y.rel_no = COL.rel_no	
          left join IFSAPP.CUST_ORD_CUSTOMER_ENT COCE ON COCE.customer_id =	
                                                         Y.DEALER	
          LEFT JOIN IFSAPP.HPNRET_LEVEL_OVERVIEW_ZONE LOZ ON LOZ.Zone_Id =	
                                                             COCE.market_code	
         where TRUNC(y.transaction_date) = TO_DATE(:PreviousDate, 'yyyy/mm/dd')  
         	
         group by CI.customer_id,	
                  HCG.first_name,	
                  HCG.last_name,	
                  CI.creation_date,	
                  HCG.hpnret_gender,	
                  LCI.card_type,	
                  CIA.city,	
                  HCG.nationality,	
                  HCG.date_of_birth,	
                  HCG.cat_date,	
                  CIA.city,	
                  III.group_id,	
                  CIA.address1,	
                  CIA.address2,	
                  LOZ.Area_Id,	
                  LOZ.Area,	
                  LOZ.Zone_Id,	
                  LOZ.Zone	
                  ,LOZ.Zone_Id	
                  ,LOZ.Zone	
                  ,LOZ.Channel_Id	
                  ,LOZ.Channel 	
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd")) 
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }

        }

        public DataTable GetCustomerDMD(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionDMD();
            string selectQuery = string.Empty;

            try
            {

                #region SQL
                selectQuery = @"
 
                 
               select 
               CI.customer_id AS CUSTOMER_CODE,
               HCG.first_name AS FIRST_NAME,
               HCG.last_name AS LAST_NAME,
               CIA.address1 || '_' || CIA.address2 as RECIDENCE,
               max(case
                     when CICM.method_id_db = 'MOBILE' then
                      CICM.value
                   end) as MOBILE,
               max(case
                     when CICM.method_id_db = 'PHONE' then
                      CICM.value
                   end) as TELEPHONE,
               max(case
                     when CICM.method_id_db = 'E_MAIL' then
                      CICM.value
                   end) as EMAIL,
               ' ' AS Loyality_STATUS
               
               
                
               
               
          FROM ifsapp.customer_info CI
          LEFT JOIN ifsapp.hpnret_customer_guarantor HCG ON HCG.id = CI.customer_id
          LEFT JOIN IFSAPP.Loyalty_Customer_Info LCI ON LCI.customer_id =
                                                        CI.customer_id
          LEFT JOIN ifsapp.customer_info_address CIA ON CIA.customer_id =
                                                        CI.customer_id
          LEFT JOIN ifsapp.customer_info_comm_method CICM ON CICM.customer_id =
                                                             CI.customer_id
          LEFT JOIN ifsapp.identity_invoice_info III ON III.identity =
                                                        CI.customer_id
          LEFT JOIN IFSAPP.Customer_Order_Line COL ON COL.customer_no =
                                                      CI.customer_id
          LEFT JOIN IFSAPP.indirect_sales_dtl_tab y on y.order_no = COL.order_no
                                                   and y.line_no = COL.line_no
                                                   and y.line_item_no =
                                                       COL.line_item_no
                                                   and y.rel_no = COL.rel_no
          left join IFSAPP.CUST_ORD_CUSTOMER_ENT COCE ON COCE.customer_id =
                                                         Y.DEALER
          LEFT JOIN IFSAPP.HPNRET_LEVEL_OVERVIEW_ZONE LOZ ON LOZ.Zone_Id =
                                                             COCE.market_code
         where (y.transaction_date) =  TO_DATE(:PreviousDate, 'yyyy/mm/dd')  
         group by CI.customer_id,
                  HCG.first_name,
                  HCG.last_name,
                  CI.creation_date,
                  HCG.hpnret_gender,
                  LCI.card_type,
                  CIA.city,
                  HCG.nationality,
                  HCG.date_of_birth,
                  HCG.cat_date,
                  CIA.city,
                  III.group_id,
                  CIA.address1,
                  CIA.address2,
                  LOZ.Area_Id,
                  LOZ.Area,
                  LOZ.Zone_Id,
                  LOZ.Zone
                  ,LOZ.Channel_Id
         ,LOZ.Channel
                 
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd")) 
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }

        }

        public bool CheckCustomerCodeExistsInBI_NRTANServerAndProcessToNRTANServer(SingerCustomer objSingerCustomer)
        {
            try
            {
                bool IsSave = false;
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.CUSTOMER_CODE) AS CheckCount 
                    FROM NRTAN.BI_DIM_CUSTOMER  t1 
                    where t1.CUSTOMER_CODE = :CUSTOMER_CODE
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("CUSTOMER_CODE",objSingerCustomer.CUSTOMER_CODE) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertCustomerToBI_NRTANServerIncrementalProcess(objSingerCustomer);
                    }

                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In CheckCustomerCodeExistsInRTANServerAndProcessToIRTANServer ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ex;
            }
        }

        public bool InsertCustomerToBI_NRTANServerIncrementalProcess(SingerCustomer objSingerCustomer)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;

            #region Querry


            string insertQuery = @"
            INSERT INTO NRTAN.BI_DIM_CUSTOMER
            (
            CUSTOMER_CODE ,
            FIRST_NAME	 ,
            LAST_NAME	 ,
            RECIDENCE	,
            MOBILE	,
            TELEPHONE	,
            EMAIL	,
            LOYALITY_STATUS	
            )
            VALUES
            (
            :CUSTOMER_CODE ,
            :FIRST_NAME	 ,
            :LAST_NAME	 ,
            :RECIDENCE	,
            :MOBILE	,
            :TELEPHONE	,
            :EMAIL	,
            :LOYALITY_STATUS	
            )
                    ";
            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                    DBConn.AddParameter("CUSTOMER_CODE",objSingerCustomer.CUSTOMER_CODE ) 
                    ,DBConn.AddParameter("FIRST_NAME",objSingerCustomer.FIRST_NAME ) 
                    ,DBConn.AddParameter("LAST_NAME",objSingerCustomer.LAST_NAME )
                    ,DBConn.AddParameter("RECIDENCE",objSingerCustomer.RECIDENCE ) 
                    ,DBConn.AddParameter("MOBILE",objSingerCustomer.MOBILE )   
                    ,DBConn.AddParameter("TELEPHONE",objSingerCustomer.TELEPHONE )   
                    ,DBConn.AddParameter("EMAIL",objSingerCustomer.EMAIL )   
                    ,DBConn.AddParameter("LOYALITY_STATUS",objSingerCustomer.LOYALITY_STATUS )   


                };

                DataRow dRow = (DataRow)dbConnection.Executes(insertQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;

                oOracleConnection.Close();

                return isSave;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objSingerCustomer, ex.StackTrace), "Error - In InsertProductToIRTANServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }


        public DataTable GetDiscount(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            string selectQuery = string.Empty;

            try
            {
                #region New Sql
                selectQuery = @"
                 
                ";
                #endregion
                oDataTable = new DataTable();
                //OracleParameter[] param = new OracleParameter[] 
                //{ 
                //    DBConn.AddParameter("",dtPreviousDate.Year)
                //};

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);

                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool CheckDiscountTypeCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(Discount objDiscount)
        {
            try
            {
                bool IsSave = false;
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.DISCOUNT_TYPE) AS CheckCount 
                    FROM NRTAN.BI_DIM_DISCOUNT  t1 
                    where t1.DISCOUNT_TYPE = :DISCOUNT_TYPE  
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("DISCOUNT_TYPE",objDiscount.DISCOUNT_TYPE) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertDiscountCodeTo_BI_NRTAN_ServerIncrementalProcess(objDiscount);
                    }

                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - CheckDiscountTypeCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ex;
            }
        }

        public bool InsertDiscountCodeTo_BI_NRTAN_ServerIncrementalProcess(Discount objDiscount)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;


            #region Sql
            string stringQuerry = @" 
            INSERT INTO NRTAN.BI_DIM_DISCOUNT
            (
            DISCOUNT_TYPE	,
            DISCOUNT_TYPE_DESC	,			
            PRODUCT_CODE	
            )
            VALUES
            (
            :DISCOUNT_TYPE	,
            :DISCOUNT_TYPE_DESC	,			
            :PRODUCT_CODE	
            )

                ";
            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("DISCOUNT_TYPE",objDiscount.DISCOUNT_TYPE ) 
                        ,DBConn.AddParameter("DISCOUNT_TYPE_DESC",objDiscount.DISCOUNT_TYPE_DESC) 
                        ,DBConn.AddParameter("PRODUCT_CODE",objDiscount.ProductCode) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objDiscount, ex.StackTrace), "Error - In InsertDiscountCodeTo_BI_NRTAN_ServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }


        public DataTable GetSalesman(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region New Sql
                selectQuery = @"
                SELECT S.salesman_code AS SALESMAN_CODE 
                ,IFSAPP.PERSON_INFO_API.Get_Name(S.salesman_code) AS SALESMAN_NAME
                ,ifsapp.HPNRET_LEVEL_API.Get_Hpnret_Level_Type(h.level_id) AS SALESMAN_TYPE
                ,S.contract   AS BRANCH_ID
                ,CASE WHEN  S.branch_manager = 'TRUE' THEN 'ACTIVE' ELSE 'INACTIVE'  end AS STATUS
                FROM IFSAPP.SALES_PART_SALESMAN S
                INNER JOIN IFSAPP.HPNRET_LEVEL_HIERARCHY h ON H.site_id  = S.contract
                ";
                #endregion
                oDataTable = new DataTable();
                //OracleParameter[] param = new OracleParameter[] 
                //{ 
                //    DBConn.AddParameter("",dtPreviousDate.Year)
                //};

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);

                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool CheckSalesmanCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(Salesmen objSalesmen)
        {
            try
            {
                bool IsSave = false;
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                    SELECT COUNT(t1.SALESMAN_CODE) AS CheckCount 
                    FROM NRTAN.BI_DIM_SALESMAN  t1 
                    where t1.SALESMAN_CODE = :SALESMAN_CODE  
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                   { 
                   DBConn.AddParameter("SALESMAN_CODE",objSalesmen.SALESMAN_CODE) 
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);

                if (dRow != null)
                {
                    if (Convert.ToInt32(dRow["CheckCount"]) == 0)
                    {
                        IsSave = this.InsertSalesmanToBI_NRTANServerIncrementalProcess(objSalesmen);
                    }

                }
                oOracleConnection.Close();
                return IsSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - CheckSalesmanCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                throw ex;
            }
        }

        public bool InsertSalesmanToBI_NRTANServerIncrementalProcess(Salesmen objSalesmen)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            bool isSave = false;

            #region Querry


            string insertQuery = @"
            INSERT INTO NRTAN.BI_DIM_SALESMAN
            (
            SALESMAN_CODE	,		
            SALESMAN_TYPE	,			
            BRANCH_ID	,		
            STATUS		 
            )
            VALUES
            (
            :SALESMAN_CODE	,		
            :SALESMAN_TYPE	,			
            :BRANCH_ID	,		
            :STATUS	 
            )
                    ";
            #endregion

            try
            {
                OracleParameter[] param = new OracleParameter[] 
                   { 
                    DBConn.AddParameter("SALESMAN_CODE",objSalesmen.SALESMAN_CODE) 
                    ,DBConn.AddParameter("SALESMAN_TYPE",objSalesmen.SALESMAN_TYPE ) 
                    ,DBConn.AddParameter("BRANCH_ID",objSalesmen.BRANCH_ID )
                    ,DBConn.AddParameter("STATUS",objSalesmen.STATUS ) 
                     
                };

                DataRow dRow = (DataRow)dbConnection.Executes(insertQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;

                oOracleConnection.Close();

                return isSave;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objSalesmen, ex.StackTrace), "Error - In InsertSalesmanToBI_NRTANServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }

        public DataTable NeenOpal_BI_GetSaleslineNotInPKG(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region New Sql
//                selectQuery = @"     
//           --SALE LINE NOT IN PKG--BI
//        SELECT 
//         
//        tblMain.SITE_CODE  ,--2  --************PK
//        tblMain.SALES_PART, --16
//        tblMain.UNITS,--17
//        tblMain.SALES_PRICE,--9
//        CASE WHEN  tblMain.UNITS = 0 THEN 0 ELSE   ROUND(tblMain.DISCOUNT_AMOUNT/tblMain.UNITS,2) END AS UNIT_DISCOUNT_AMOUNT ,  --14
//        CASE WHEN  tblMain.UNITS = 0 THEN 0 ELSE  ROUND(((tblMain.DISCOUNT_AMOUNT/tblMain.UNITS)/100),2) END AS  UNIT_DISCOUNT_PRENTENTAGE ,
//        tblMain.VALUE,--18
//        (tblMain.DISCOUNT_AMOUNT ) AS TOTAL_DISCOUNT,
//        tblMain.LOCATION_NO,--28
//         tblMain.ORDER_NO,--3  --************PK THIS IS CALLED SALES_TRANS_NO
//         tblMain.TDATE,--1     --************PK
//        SUBSTR(tblMain.EMPLOYEE_CODE,1,12) AS PROMOTER_CODE ,--17
//         tblMain.AUTHORIZED_USER_ID ,--29
//         tblMain.STATE ,--30
//       tblMain.CUSTOMER_NO,--4
//       tblMain.LENGTH_OF_CONTRACT   ,--24
//         ' ' AS ADVANCE_PAYMENT_NO ,  
//         0  AS ADVANCE_PAYMENT_AMOUNT ,  
//       tblMain.SALE_TYPE , --61    --************PK
//       tblMain.SALES_LOCATION_NO ,--27
//        tblMain.SERVICE_CHARGE , --31
//         tblMain.FIRST_PAYMENT ,  --32
//        tblMain.BUDGET_BOOK_HIRE_PRICE ,  --34
//        tblMain.MONTHLY_PAYMENT  ,--33
//         tblMain.BUDGET_BOOK  , --23
//         tblMain.PRICE_LIST, --35
//          tblMain.GROSS_HIRE_PRICE  --36
//          ,tblMain.VAL_SURAKSHA_HP_LINES as SURAKSHA_VALUE--37
//          ,tblMain.val_sanasuma_hp_lines AS SANASUMA_VALUE--38
//          ,tblMain.Senasuma_Plus_Value--39
//          ,tblMain.Senasuma_Plus_Mobile_Value--40
//          ,tblMain.Senasuma_Post_Value--41
//          ,tblMain.TRANSAACTION_TYPE --42
//          ,tblMain.SALES_VARIATION  --43
//          ,tblMain.FREE_ISSUE -- 44
//          ,tblMain.salesman_code -- 45
//          ,tblMain.Vallibel_Introduced  --46
//          ,tblMain.SERIAL_NO --47
//          ,tblMain.order_category --48
//        
//    --FOR PRIMARY KEY
//    
//    ,1 AS TILL_NO --************PK  THIS IS HARD CODE FOR EVERY SQL
//    ,tblMain.SALE_LINE_TYPE  --20 --************PK
//    ,tblMain.LINE_NO AS SALE_LINE_NO --************PK
//
//    ,NVL ( ifsapp.Sin_FN_DayWiseTotal_Singer_Tax(SUBSTR(tblMain.SITE_CODE,1,5),tblMain.SALES_PART,SUBSTR(tblMain.ORDER_NO,5,1),NVL(tblMain.VALUE,0)) , 0 ) AS sales_tax_value--49
//    ,tblMain.INVENTORY_VALUE   --50
//    ,CASE WHEN  SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'NM' THEN 'NORMAL'
//       WHEN SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'SP' THEN 'SPECIAL'
//       WHEN SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'GR' THEN 'GROUP'
//        ELSE ''  END 
//      AS SALES_GROUPING    --51
//      , CASE WHEN  tblMain.TaxCallType = 'D' THEN      
//         
//       NVL(  ( select DISTINCT SUBSTR(au.reason,1,20)   
//                from ifsapp.hpnret_auth_variation au 
//                where au.account_no =  tblMain.ORDER_NO  
//                   AND au.variation IN (  select  DISTINCT A.variation from ifsapp.hpnret_variation_reason a  WHERE A.variation <> 'EarlyClosure')
//                   AND au.state = 'Active'
//                   AND au.utilized = '1'  
//                   and tblMain.UNITS < 0--THIS IS TAKE TO AVOID MULTi ROWS
//                   AND TO_DATE(tblMain.TDATE,'YYYY-MM-DD') =   TRUNC(au.utilized_date)
//                   AND ROWNUM = 1 
//                )  , ' ')
//                
//                
//        ELSE 
//        tblMain.RET_REASON_CODE
//       END  AS RET_REASON_CODE   --52
//       , tblMain.Status_586P   --63
//, tblMain.discount_type --12
//, tblMain.discount_reason --12
//,tblMain.TaxCallType as SalesFileType
//,tblMain.PROMOTION_CODE--15
//,NVL(tblMain.Cash_Conversion_Days,0)  AS Cash_Conversion_Days-- 49
//,tblMain.Cash_Conversion_Cal_Days -- 50
//, tblMain.Authorized_Date --51
//,tblMain.Utilized_Date     --52
//,tblMain.AUTHORIZED_NAME  --53       
//,tblMain.Line_Status  --54
//,tblMain.Revert_Reversed_Bal --55
//,tblMain.Total_Amount_Paid -- 56     
//,tblMain.HPNRET_GROUPING   --57
// ,tblMain.EnteredBy --58
// , tblMain.EnteredName  --59
// , tblMain.SALESMAN_TYPE  --60
//
//  FROM (
//
//        select distinct TO_CHAR(s.transaction_date, 'YYYY-MM-DD') AS TDATE, --1       
//          s.shop_code  as site_code, --2     
//                s.order_no, --3
//                c.customer_no, --4
//                c.date_entered, --5
//                s.variation, --6
//                case
//                  WHEN substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'H' then
//                   (hhh.sales_promoter)
//                  when substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'R' THEN
//                   (hco.sales_promoter)
//                end as EMPLOYEE_CODE, --7
//                S.TRANSACTION_ID as line_no, --8
//                TRUNC(s.cash_price, 2) as sales_price, --9
//                TRUNC(s.cash_price, 2) as sales_price_1, --10
//                
//                TRUNC(SUM(C.COST)/COUNT(*)) as inventory_value, --11
//                
//                t.discount_type, --12
//                case
//                  WHEN t.discount_type = 'G' THEN
//                   'GENERAL'
//                  when t.discount_type = 'S' THEN
//                   'SPECIAL'
//                  else
//                   ''
//                end as discount_reason, --13
//                
//                ((select TRUNC(((sum(s.cash_units + s.hire_units)) /
//                              count(*) *
//                               (nvl(sum(t.discount_amount),
//                                     (sum(t.discount * t.calculation_basis) / 100)))),
//                               2)
//                    from ifsapp.cust_order_line_discount t
//                   where t.order_no = s.order_no
//                     and t.line_no = s.line_no
//                     and t.rel_no = s.rel_no
//                     and t.line_item_no = s.line_item_no
//                     AND s.STATE NOT IN ('RevertReversed', 'Reverted'))) as discount_amount, --14
//                
//                (nvl(nvl((SELECT to_char(d.rule_no) promo_rule_number
//                           FROM ifsapp.hpnret_hp_dtl d
//                          WHERE d.account_no = s.order_no
//                            AND d.conn_line_no = s.line_no
//                            AND d.conn_rel_no = s.rel_no
//                            AND d.conn_line_item_no = s.line_item_no
//                            AND d.catalog_type = 'Inventory part'
//                            AND ROWNUM = 1),
//                         (SELECT to_char(l.free1) promo_rule_number
//                            FROM ifsapp.Hpnret_Cust_Order_Line l,
//                                 ifsapp.hpnret_customer_order  h
//                           WHERE l.free1 IS NOT NULL
//                             AND h.order_no = l.order_no
//                             AND h.cash_conv = 'FALSE'
//                             AND l.order_no = s.order_no
//                             AND l.conn_line_no = s.line_no
//                             AND l.conn_rel_no = s.rel_no
//                             AND l.conn_line_item_no = s.line_item_no
//                             AND ROWNUM = 1)),
//                     (SELECT to_char(c.discount_source_id) promo_rule_number
//                        FROM ifsapp.Cust_Order_Line_Discount c
//                       WHERE c.user_id = 'PROMO'
//                         AND c.discount_source_id IS NOT NULL
//                         AND c.order_no = s.order_no
//                         AND c.line_no = s.line_no
//                         AND c.rel_no = s.rel_no
//                         AND c.line_item_no = s.line_item_no
//                         AND ROWNUM = 1))) AS PROMOTION_CODE, --15
//                
//                s.sales_part, --16
//                case
//                  WHEN substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'H' and s.hire_units > 0 and
//                       (s.state = 'SALE') THEN
//                   (s.hire_units)
//                    WHEN substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'H' and s.hire_units < 0 and
//                       (s.state = 'SALE') THEN
//                   (s.hire_units)
//                  when substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and s.cash_units > 0 and
//                       (s.state = 'SALE') then
//                   (s.cash_units)
//                    when substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and s.cash_units < 0 and
//                       (s.state = 'SALE') then
//                   (s.cash_units)
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and (s.state = 'CashConverted') and
//                       (s.hire_units) < 0 then
//                   (s.hire_units)
//                  when (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and (s.variation = 'CashCon') and
//                       s.cash_units > 0 then
//                   (s.cash_units)
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and (s.variation = 'ExchgOut') and
//                       (s.hire_units) > 0 then
//                   (s.hire_units)
//                  when (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and (s.variation = 'ExchgOut') and
//                       (s.cash_units) > 0 then
//                   (s.cash_units)
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and s.reverts_units > 0 and
//                       s.state = 'Reverted' then
//                   (s.reverts_units * -1)
//                  when (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and s.revert_reverse_units > 0 and
//                       s.state = 'RevertReversed' then
//                   (s.revert_reverse_units)
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and s.hire_units < 0 and
//                       s.state = 'Returned' then
//                   (s.hire_units)
//                  when (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and s.cash_units < 0 and
//                       s.state = 'Returned' then
//                   (s.cash_units)
//                  WHEN substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'H' and s.hire_units < 0 and
//                       (s.state = 'ExchangedIn') THEN
//                   (s.hire_units)
//                  WHEN substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and s.cash_units < 0 and
//                       (s.state = 'ExchangedIn') THEN
//                   (s.cash_units)
//
//                   WHEN substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'H' and s.hire_units < 0 and
//                       (s.state = 'Closed') THEN
//                   (s.hire_units)
//
//                END as units, --17
//                
//                Trunc(CASE
//                        WHEN substr(s.order_no,
//                                    instr(s.order_no, '-', -1, 1) + 1,
//                                    1) = 'H' and s.net_hire_cash_value > 0 and
//                             (s.state = 'SALE') THEN
//                         (s.net_hire_cash_value)
//                                    WHEN substr(s.order_no,
//                                    instr(s.order_no, '-', -1, 1) + 1,
//                                    1) = 'H' and s.net_hire_cash_value < 0 and
//                             (s.state = 'SALE') THEN
//                         (s.net_hire_cash_value)
//                        when substr(s.order_no,
//                                    instr(s.order_no, '-', -1, 1) + 1,
//                                    1) = 'R' and s.cash_value > 0 and
//                             (s.state = 'SALE') then
//                         (s.cash_value)
//                                    when substr(s.order_no,
//                                    instr(s.order_no, '-', -1, 1) + 1,
//                                    1) = 'R' and s.cash_value < 0 and
//                             (s.state = 'SALE') then
//                         (s.cash_value)
//                        WHEN (substr(s.order_no,
//                                     instr(s.order_no, '-', -1, 1) + 1,
//                                     1) = 'H') and (s.state = 'CashConverted') and
//                             (s.net_hire_cash_value) < 0 then
//                         (s.net_hire_cash_value)
//                        when (substr(s.order_no,
//                                     instr(s.order_no, '-', -1, 1) + 1,
//                                     1) = 'R') and (s.variation = 'CashCon') and
//                             s.cash_value > 0 then
//                         s.cash_value
//                        WHEN (substr(s.order_no,
//                                     instr(s.order_no, '-', -1, 1) + 1,
//                                     1) = 'H') and (s.variation = 'ExchgOut') and
//                             (s.net_hire_cash_value) > 0 then
//                         s.net_hire_cash_value
//                        when (substr(s.order_no,
//                                     instr(s.order_no, '-', -1, 1) + 1,
//                                     1) = 'R') and (s.variation = 'ExchgOut') and
//                             (s.cash_value) > 0 then
//                         s.cash_value
//                        WHEN (substr(s.order_no,
//                                     instr(s.order_no, '-', -1, 1) + 1,
//                                     1) = 'H') and s.reverts_value > 0 and
//                             s.state = 'Reverted' then
//                         (s.reverts_value * -1)
//                        when (substr(s.order_no,
//                                     instr(s.order_no, '-', -1, 1) + 1,
//                                     1) = 'H') and s.revert_reverse_value > 0 and
//                             s.state = 'RevertReversed' then
//                         s.revert_reverse_value
//                        WHEN (substr(s.order_no,
//                                     instr(s.order_no, '-', -1, 1) + 1,
//                                     1) = 'H') and s.net_hire_cash_value < 0 and
//                             s.state = 'Returned' then
//                         s.net_hire_cash_value
//                        when (substr(s.order_no,
//                                     instr(s.order_no, '-', -1, 1) + 1,
//                                     1) = 'R') and s.cash_value < 0 and
//                             s.state = 'Returned' then
//                         s.cash_value
//                        WHEN substr(s.order_no,
//                                    instr(s.order_no, '-', -1, 1) + 1,
//                                    1) = 'H' and s.net_hire_cash_value < 0 and
//                             (s.state = 'ExchangedIn') THEN
//                         (s.net_hire_cash_value)
//                        WHEN substr(s.order_no,
//                                    instr(s.order_no, '-', -1, 1) + 1,
//                                    1) = 'R' and s.cash_value < 0 and
//                             (s.state = 'ExchangedIn') THEN
//                         (s.cash_value)
//                            WHEN substr(s.order_no,
//                                    instr(s.order_no, '-', -1, 1) + 1,
//                                    1) = 'H' and s.net_hire_cash_value < 0 and
//                             (s.state = 'Closed') THEN
//                         (s.net_hire_cash_value)
//
//                      END,
//                      2) as value, --18
//                
//                CASE
//                  WHEN substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'H' and s.net_hire_cash_value > 0 and
//                       (s.state = 'SALE') THEN
//                   '201'
//                  when substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and s.cash_value > 0 and
//                       (s.state = 'SALE') THEN
//                   '101'
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'H') and (s.state = 'CashConverted') and
//                       (s.net_hire_cash_value) < 0 THEN
//                   '205'
//                  when (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and (s.variation = 'CashCon') and
//                       s.cash_value > 0 then
//                   '105'
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and (s.variation = 'ExchgOut') and
//                       (s.net_hire_cash_value) > 0 THEN
//                   '203'
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and (s.variation = 'ExchgOut') and
//                       (s.cash_value) > 0 THEN
//                   '103'
//                  when (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and (s.reverts_value > 0) and
//                       s.state = 'Reverted' THEN
//                   '206'
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and (s.revert_reverse_value > 0) and
//                       s.state = 'RevertReversed' THEN
//                   '207'
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and s.net_hire_cash_value < 0 and
//                       s.state = 'Returned' THEN
//                   '204'
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and s.cash_value < 0 and
//                       s.state = 'Returned' THEN
//                   '104'
//                  WHEN substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'H' and s.net_hire_cash_value < 0 and
//                       (s.state = 'ExchangedIn') then
//                   '202'
//                  WHEN substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and s.cash_value < 0 and
//                       (s.state = 'ExchangedIn') then
//                   '102'
//                END AS TRANSACTION_SUBTYPE_CODE, --19
//                
//                case
//                  WHEN substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'H' and s.net_hire_cash_value > 0 and
//                       (s.state = 'SALE') THEN
//                   '1'
//                  when substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and s.cash_value > 0 and
//                       (s.state = 'SALE') THEN
//                   '1'
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and (s.state = 'CashConverted') and
//                       (s.net_hire_cash_value) < 0 THEN
//                   '2'
//                  when (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and (s.variation = 'CashCon') and
//                       s.cash_value > 0 then
//                   '1'
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and (s.variation = 'ExchgOut') and
//                       (s.net_hire_cash_value) > 0 THEN
//                   '1'
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and (s.variation = 'ExchgOut') and
//                       (s.cash_value) > 0 THEN
//                   '1'
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and (s.reverts_value > 0) and
//                       s.state = 'Reverted' THEN
//                   '2'
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and (s.revert_reverse_value > 0) and
//                       s.state = 'RevertReversed' THEN
//                   '1'
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'H') and s.net_hire_cash_value < 0 and
//                       s.state = 'Returned' THEN
//                   '2'
//                  WHEN (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and s.cash_value < 0 and
//                       s.state = 'Returned' THEN
//                   '2'
//                  WHEN substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'H' and s.net_hire_cash_value < 0 and
//                       (s.state = 'ExchangedIn') then
//                   '2'
//                  WHEN substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and s.cash_value < 0 and
//                       (s.state = 'ExchangedIn') then
//                   '2'
//                end as SALE_LINE_TYPE, --20
//                
//                 l.location_group as SALES_LOCATION, --21
//                
//                TRUNC(s.cash_price, 2) as ORIGINAL_PRODUCT_PRICE, --22
//                hhh.budget_book_id AS BUDGET_BOOK, --23
//                hhh.length_of_contract as LENGTH_OF_CONTRACT, --24
//                'D' AS TaxCallType --25
//                ,' ' as RET_REASON_CODE--26
//             ,   case
//         WHEN substr(s.order_no, instr(s.order_no, '-', -1, 1) + 1, 1) = 'H' then
//          IFSAPP.Hpnret_Hp_Head_Api.Get_Sales_Location_No(s.order_no, '1')
//         WHEN substr(s.order_no, instr(s.order_no, '-', -1, 1) + 1, 1) = 'R' THEN
//          IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sales_Location_No(s.order_no)
//       end as SALES_LOCATION_NO    --27
//       ,s.LOCATION_NO AS LOCATION_NO --28
//       ,t.authorized_user AS AUTHORIZED_USER_ID --29
//       ,S.STATE --30
//       , trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Serv_Chg(S.ORDER_NO ,1),2) AS SERVICE_CHARGE--31
//       , trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_First_Payment(S.ORDER_NO ,1),2) as FIRST_PAYMENT --32
//       , trunc(IFSAPP.hpnret_hp_head_api.get_total_monthly_payment(S.ORDER_NO ,1),2) as MONTHLY_PAYMENT --33
//       ,trunc(IFSAPP.hpnret_hp_head_api.get_total_bb_hire_price(S.ORDER_NO ,1),2) as BUDGET_BOOK_HIRE_PRICE --34
//       , IFSAPP.customer_order_api.get_price_list_no(S.ORDER_NO)  AS PRICE_LIST --35
//       ,trunc(IFSAPP.hpnret_hp_head_api.get_gross_hire_value(S.ORDER_NO,1),2) AS GROSS_HIRE_PRICE --36
//       ,s.VAL_SURAKSHA_HP_LINES --37
//       ,s.val_sanasuma_hp_lines --38
//       ,   (     
//       select coc.charge_amount
//  from ifsapp.customer_order_charge coc
// where
// s.order_no = coc.order_no
// and s.line_no = coc.line_no
// and s.rel_no = coc.rel_no
// and s.line_item_no = coc.line_item_no
// and coc.charge_type in ('EXT_PLUS')
//       ) as Senasuma_Plus_Value  --39
//
//       ,   (     
//       select coc.charge_amount
//  from ifsapp.customer_order_charge coc
// where
// s.order_no = coc.order_no
// and s.line_no = coc.line_no
// and s.rel_no = coc.rel_no
// and s.line_item_no = coc.line_item_no
// and coc.charge_type in ('EXT_PLUS-1')
//       ) as Senasuma_Plus_Mobile_Value --40
//       
//       
//        ,  CASE WHEN  (substr(s.order_no,
//                               instr(s.order_no, '-', -1, 1) + 1,
//                               1) = 'R')  THEN    (     
//       select sum(HAP.DOM_AMOUNT) as DOM_AMOUNT
//  from ifsapp.HPNRET_ADV_PAY HAP
// where
// s.order_no = HAP.order_no
// and s.line_no = HAP.line_no
// and s.rel_no = HAP.rel_no
// and s.line_item_no = HAP.line_item_no 
// AND  HAP.ACCOUNT_CODE = 'EXTPO'
//       ) 
//       
//       ELSE (     
//       select sum(HAP.DOM_AMOUNT) as DOM_AMOUNT
//  from ifsapp.HPNRET_ADV_PAY HAP
// where
// s.order_no = HAP.original_account
// and s.line_no = HAP.line_no
// and s.rel_no = HAP.rel_no
// and s.line_item_no = HAP.line_item_no 
// AND  HAP.ACCOUNT_CODE = 'EXTPO'
//       )  END 
//       
//       
//       as Senasuma_Post_Value   --41
//, CASE WHEN   substr(s.order_no,
//                              instr(s.order_no, '-', -1, 1) + 1,
//                              1)  = 'R' THEN 'CASH' ELSE 'HIRE'  END  AS TRANSAACTION_TYPE--42
//  ,s.variation    as SALES_VARIATION  --43                       
//   ,S.FREE_ITEM AS FREE_ISSUE    --44
//   ,s.salesman_code -- 45
//   , (select case when  count(*) > 0 then 'YES' ELSE 'NO' END AS Vallibel_Introduced
// from ifsapp.sin_v_one_product_tab valibe 
//   where valibe.product_code = s.part_no  ) AS Vallibel_Introduced  --46
//  
//          
//   ,(SELECT ST.SERIAL_NO
//          FROM IFSAPP.SERIAL_TRANSACTION ST
//         WHERE   s.order_no = ST.ORDER_NO 
//          and s.line_no = ST.line_no
// and s.rel_no = ST.rel_no
// and s.line_item_no = ST.line_item_no 
//           AND ROWNUM = 1) SERIAL_NO --47
//           
//           ,hco.order_category   --48
//
//,      trunc(hhh.closed_date )  -   TRUNC(hhh.original_sales_date) as Cash_Conversion_Days --49
//
//,  (SELECT   au.remarks
// from ifsapp.hpnret_auth_variation au
// WHERE s.order_no = au.account_no
//  AND ROWNUM = 1
// )   as Cash_Conversion_Cal_Days --50
//
//
//,  
// (SELECT  (au.from_date) AS Authorized_Date
// from ifsapp.hpnret_auth_variation au
// WHERE s.order_no = au.account_no
// AND ROWNUM = 1
// ) Authorized_Date  --51
//                
// ,  
// (SELECT  TRUNC(AU.Utilized_Date) AS Utilized_Date
// from ifsapp.hpnret_auth_variation au
// WHERE s.order_no = au.account_no
//  AND ROWNUM = 1
// ) Utilized_Date  --52
//
//  ,  
// (SELECT  UTAB.DESCRIPTION AS AUTHORIZED_NAME
// from ifsapp.fnd_user_tab UTAB
// WHERE UTAB.IDENTITY = T.authorized_user
// ) AUTHORIZED_NAME  --53   
// 
// ,C.state AS Line_Status --54
//, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Out_Bal(S.ORDER_NO ,1),2) as Revert_Reversed_Bal --55
//, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Amt_Paid(S.ORDER_NO ,1),2) as Total_Amount_Paid --56  
//,  ifsapp.hpnret_pro_fam_group_api.get_hpnret_groupings(0,
//                                                            'EXT WARRANTY',
//                                                            IFSAPP.inventory_part_api.Get_Part_Product_Family(C.contract,
//                                                                                                              C.part_no)) AS HPNRET_GROUPING -- 57 
// 
// ,t.user_id  as EnteredBy --58
//  ,  
// (SELECT  UTAB.DESCRIPTION AS AUTHORIZED_NAME
// from ifsapp.fnd_user_tab UTAB
// WHERE UTAB.IDENTITY = T.user_id
// ) EnteredName  --59
// , (
// SELECT ifsapp.HPNRET_LEVEL_API.Get_Hpnret_Level_Type(HL.level_id) AS SALESMAN_TYPE
//  FROM IFSAPP.HPNRET_LEVEL_HIERARCHY HL WHERE HL.site_id = S.SHOP_CODE
//  )    SALESMAN_TYPE  ----60
// ,    IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sale_Type(S.ORDER_NO)  as     SALE_TYPE   --61
//, (SELECT HR_ADV.account_code FROM ifsapp.hpnret_ADV_PAY HR_ADV where HR_ADV.prod_rel_order_no = c.order_no  AND HR_ADV.objstate = 'Approved')   Status_586P --62 
//               
//  FROM IFSAPP.direct_sales_dtl_tab S
//  left join IFSAPP.INVENTORY_LOCATION l on l.location_no = s.location_no
//                                       AND L.contract = S.SHOP_CODE
//  left join ifsapp.cust_order_line_discount t on s.order_no = t.order_no
//                                             and s.line_no = t.line_no
//                                             and s.line_item_no =
//                                                 t.line_item_no
//                                             and s.rel_no = t.rel_no
//  left join IFSAPP.Customer_Order_Line C on c.order_no = s.order_no
//                                        and c.line_no = s.line_no
//                                        and c.line_item_no = s.line_item_no
//                                        and c.rel_no = s.rel_no
//  left join IFSAPP.Hpnret_Hp_Head hhh on s.order_no = hhh.account_no
//  left join ifsapp.Hpnret_Customer_Order hco on s.order_no = hco.order_no
//where s.catalog_type not in ('PKG')
//   and s.free_item = 'N'
//   AND TRUNC(s.transaction_date) = to_date(:PreviousDate, 'YYYY/MM/DD') --to_date(:PreviousDate, 'YYYY/MM/DD')
//   /*and (SELECT substr(i.second_commodity, 1, 2)
//          FROM ifsapp.inventory_part i
//         WHERE i.contract = s.shop_code
//           AND i.part_no = s.sales_part) NOT IN ( '2P','2C','GV') 
//  AND S.SALES_PART <> 'PARTS'   */ 
//   
//group by l.location_group,
//          (s.transaction_date),
//          s.shop_code,
//          s.order_no,
//          c.customer_no,
//          c.date_entered,
//          s.hire_units,
//          s.reverts_units,
//          s.revert_reverse_units,
//          s.line_no,
//          s.state,
//          s.rel_no,
//          s.sales_part,
//          S.TRANSACTION_ID,
//          s.line_item_no,
//          s.cash_units,
//          s.variation,
//          s.reverts_value,
//          s.revert_reverse_value,
//          s.net_hire_cash_value,
//          s.cash_value,
//          S.cash_price,
//          t.discount_type,
//          s.location_no,
//          hhh.sales_promoter,
//          hco.sales_promoter,
//          hhh.sales_location_no,
//          hco.sales_location_no,
//          hhh.budget_book_id,
//          hhh.length_of_contract,
//          s.special_discount_value,
//          s.promotional_discount_value,
//          S.YEAR,
//          S.PERIOD,
//          S.PART_NO,
//          hhh.contract,
//          hco.contract,t.authorized_user
//         ,s.VAL_SURAKSHA_HP_LINES  
//       ,s.val_sanasuma_hp_lines  
//       ,S.FREE_ITEM 
//       ,s.salesman_code
//       ,hco.order_category
//       ,trunc(hhh.original_sales_date)    
//      , trunc(hhh.closed_date ) 
//       ,C.state,C.contract,C.part_no
//,T.user_id
//,c.order_no
//
//union
//
// select distinct TO_CHAR(p.transaction_date, 'YYYY-MM-DD') AS TDATE, --1
//                /*case
//                  when p.location_no is null then
//                   p.contract || '_' || 'STORE'
//                  when p.location_no is not null then
//                   p.contract || '_' || l.location_group
//                end */  --p.contract as    site_code, --2 --COMMENTED ON 2019/09/27
//        /* CASE WHEN c.customer_no IS NOT NULL THEN c.customer_no ELSE case
//                  WHEN p.order_no is null THEN
//                   to_char(p.transaction_id)
//                  when p.order_no is not null then
//                   p.order_no
//                end END  as    site_code,--2 */  --again commented on 2019-10-21
//            p.dealer as site_code,--2
//
//                case
//                  WHEN p.order_no is null THEN
//                   to_char(p.transaction_id)
//                  when p.order_no is not null then
//                   p.order_no
//                end as order_no, --3
//             --   c.customer_no, --4  --
//            p.dealer as customer_no, --4 --added on 21oct 2019
//                case
//                  When c.date_entered is null then
//                   p.transaction_date
//                  when c.date_entered is not null then
//                   c.date_entered
//                end as date_entered, --5
//                '' as variation, --6
//                p.Sales_Promoter as EMPLOYEE_CODE, --7
//                P.TRANSACTION_ID as line_no, --8
//                TRUNC(P.cash_price, 2) AS sales_price, --9
//                TRUNC(P.cash_price, 2) AS sales_price_1, --10
//                
//               TRUNC(SUM(C.COST)/COUNT(*)) AS inventory_value, --11
//                
//                (select t.discount_type
//                   from ifsapp.cust_order_line_discount t
//                  where t.order_no = p.order_no
//                    and t.line_no = p.line_no
//                    and t.line_item_no = p.line_item_no
//                    and t.rel_no = p.rel_no
//                    AND ROWNUM = 1) as discount_type, --12
//                
//                case
//                  WHEN (select t.discount_type
//                          from ifsapp.cust_order_line_discount t
//                         where t.order_no = p.order_no
//                           and t.line_no = p.line_no
//                           and t.line_item_no = p.line_item_no
//                           and t.rel_no = p.rel_no
//                           AND ROWNUM = 1) = 'G' THEN
//                   'GENERAL'
//                  when (select t.discount_type
//                          from ifsapp.cust_order_line_discount t
//                         where t.order_no = p.order_no
//                           and t.line_no = p.line_no
//                           and t.line_item_no = p.line_item_no
//                           and t.rel_no = p.rel_no
//                           AND ROWNUM = 1) = 'S' then
//                   'SPECIAL'
//                end as discount_reason, --13
//                
//               /* (select nvl(sum(t.discount_amount),
//                            (t.discount * t.calculation_basis) / 100)
//                   from ifsapp.cust_order_line_discount t
//                  where t.order_no = p.order_no
//                    and t.line_no = p.line_no
//                    and t.line_item_no = p.line_item_no
//                    and t.rel_no = p.rel_no
//                  group by t.discount, t.calculation_basis,t.line_no) as discount_amount, --14  */
//            (
//            select  
//               (   
//               sum(nvl(t.discount_amount, 0)) +     sum((nvl(t.calculation_basis, 0) * (nvl(t.discount, 0) / 100)))
//               ) AS DISCOUNT
//            from ifsapp.cust_order_line_discount t
//           where  t.order_no = p.order_no
//                  and t.line_no = p.line_no
//                  and t.line_item_no = p.line_item_no
//                  and t.rel_no = p.rel_no  
//           group by  t.line_no 
//            )as discount_amount,  --14 
//                
//                (nvl(nvl((SELECT to_char(d.rule_no) promo_rule_number
//                           FROM ifsapp.hpnret_hp_dtl d
//                          WHERE d.account_no = p.order_no
//                            AND d.conn_line_no = p.line_no
//                            AND d.conn_rel_no = p.rel_no
//                            AND d.conn_line_item_no = p.line_item_no
//                            AND d.catalog_type = 'Inventory part'
//                            AND ROWNUM = 1),
//                         (SELECT to_char(l.free1) promo_rule_number
//                            FROM ifsapp.Hpnret_Cust_Order_Line l,
//                                 ifsapp.hpnret_customer_order  h
//                           WHERE l.free1 IS NOT NULL
//                             AND h.order_no = l.order_no
//                             AND h.cash_conv = 'FALSE'
//                             AND l.order_no = p.order_no
//                             AND l.conn_line_no = p.line_no
//                             AND l.conn_rel_no = p.rel_no
//                             AND l.conn_line_item_no = p.line_item_no
//                             AND ROWNUM = 1)),
//                     (SELECT to_char(c.discount_source_id) promo_rule_number
//                        FROM ifsapp.Cust_Order_Line_Discount c
//                       WHERE c.user_id = 'PROMO'
//                         AND c.discount_source_id IS NOT NULL
//                         AND c.order_no = p.order_no
//                         AND c.line_no = p.line_no
//                         AND c.rel_no = p.rel_no
//                         AND c.line_item_no = p.line_item_no
//                         AND ROWNUM = 1))) as PROMOTION_CODE, --15
//                
//                p.sales_part, --16
//                case
//                  when substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and p.units > 0.01 then
//                   (p.units)
//                  when substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and p.units < 0.01 then
//                   (p.units)
//                  when p.order_no is null and p.units < 0.01 then
//                   (p.units)
//                  when substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and p.units > 0 and
//                       p.free_issue = 'Y' then
//                   (p.units)
//                  when substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and p.units < 0 and
//                       p.free_issue = 'Y' then
//                   (p.units)
//                when substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1) NOT IN ( 'R' ,'H')  then
//                   (p.units)
//                END as units, --17
//                
//                CASE
//                  when substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and p.value > 0.01 then
//                   (p.value)
//                  when substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and p.value < 0.01 then
//                   (p.value)
//                  when p.order_no is null and p.value < 0.01 then
//                   (p.value)
//                  when substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and p.units > 0 and
//                       p.free_issue = 'Y' then
//                   (p.value)
//                  when substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and p.units < 0 and
//                       p.free_issue = 'Y' then
//                   (p.value)
//                when substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1) NOT IN ( 'R','H')   then
//                   (p.value)
//                END as value, --18
//                
//                CASE
//                  when substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and (p.value >= 0.01) THEN
//                   '101'
//                  WHEN (substr(p.order_no,
//                               instr(p.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and (p.value < 0.01) THEN
//                   '104'
//                  WHEN p.order_no is null and p.value < 0.01 THEN
//                   '104'
//                  when substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and (p.units > 0) and
//                       p.free_issue = 'Y' THEN
//                   '101'
//                  WHEN (substr(p.order_no,
//                               instr(p.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and (p.units < 0) and
//                       p.free_issue = 'Y' THEN
//                   '104'
//                END AS TRANSACTION_SUBTYPE_CODE, --19
//                
//                case
//                  WHEN (substr(p.order_no,
//                               instr(p.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and (p.value >= 0.01) THEN
//                   '1'
//                  WHEN (substr(p.order_no,
//                               instr(p.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and (p.value < 0.00) THEN
//                   '2'
//                  WHEN p.order_no is null and p.value < 0.01 THEN
//                   '2'
//                  when substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1) = 'R' and (p.units > 0) and
//                       p.free_issue = 'Y' THEN
//                   '1'
//                  WHEN (substr(p.order_no,
//                               instr(p.order_no, '-', -1, 1) + 1,
//                               1) = 'R') and (p.units < 0) and
//                       p.free_issue = 'Y' THEN
//                   '2'
//                end as SALE_LINE_TYPE, --20
//                
//                p.Sales_Location as SALES_LOCATION, --21
//                TRUNC(P.cash_price, 2) as ORIGINAL_PRODUCT_PRICE, --22
//                'N/A' as BUDGET_BOOK, --23
//                0 as LENGTH_OF_CONTRACT, --24
//                'I' AS TaxCallType --25
//                ,NVL ( (
//                SELECT  DISTINCT SUBSTR(L.return_reason_code,1,20) 
//                    FROM  ifsapp.Return_Material_Line L 
//                    WHERE p.ORDER_NO = L.order_no 
//                    AND p.line_no = L.line_no
//                    AND p.rel_no = L.rel_no
//                    AND p.line_item_no = L.line_item_no
//                    AND TO_CHAR(p.transaction_date, 'YYYY-MM-DD') = TO_CHAR(L.date_returned,'YYYY-MM-DD')
//                    AND ROWNUM = 1
//
//                ), ' ' ) AS RET_REASON_CODE --26
//               , case
//         WHEN substr(p.order_no, instr(p.order_no, '-', -1, 1) + 1, 1) = 'H' then
//          IFSAPP.Hpnret_Hp_Head_Api.Get_Sales_Location_No(p.order_no, '1')
//         WHEN substr(p.order_no, instr(p.order_no, '-', -1, 1) + 1, 1) = 'R' THEN
//          IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sales_Location_No(p.order_no)
//       end as SALES_LOCATION_NO --27
//       ,p.location_no AS LOCATION_NO--28
//       ,' '  AS AUTHORIZED_USER_ID --29
//       ,' ' AS STATE --30
//        , 0.0 as SERVICE_CHARGE --31 
//        , 0.0  as FIRST_PAYMENT --32
//        ,0.0 AS MONTHLY_PAYMENT --33
//        ,0.0 as BUDGET_BOOK_HIRE_PRICE --34
//        ,IFSAPP.customer_order_api.get_price_list_no(p.order_no) AS PRICE_LIST --35
//        ,0.0 AS GROSS_HIRE_PRICE --36
//        ,0.0 AS VAL_SURAKSHA_HP_LINES   --37
//        ,0.0 AS val_sanasuma_hp_lines -- 38
//        ,0.0 as Senasuma_Plus_Value--39
//        ,0.0 as Senasuma_Plus_Mobile_Value--40
//        ,0.0 as Senasuma_Post_Value--41
//        ,CASE WHEN   substr(p.order_no,
//                              instr(p.order_no, '-', -1, 1) + 1,
//                              1)  = 'R' THEN 'CASH' ELSE 'HIRE'  END  AS TRANSAACTION_TYPE -- 42
//        
//         ,'' as   SALES_VARIATION    --43
//         ,P.free_issue AS FREE_ISSUE   --44
//         , '' as  salesman_code -- 45
//            , (select case when  count(*) > 0 then 'YES' ELSE 'NO' END AS Vallibel_Introduced
// from ifsapp.sin_v_one_product_tab valibe 
//   where valibe.product_code = P.sales_part  ) AS Vallibel_Introduced  --46
//   
//
//              ,(SELECT ST.SERIAL_NO
//          FROM IFSAPP.SERIAL_TRANSACTION ST
//         WHERE   p.order_no = ST.ORDER_NO 
//          and p.line_no = ST.line_no
// and p.rel_no = ST.rel_no
// and p.line_item_no = ST.line_item_no 
//           AND ROWNUM = 1) SERIAL_NO --47
//  , ifsapp.Hpnret_Customer_Order_api.Get_Order_Category(p.order_no) as  order_category     --48    
//   ,0 as   Cash_Conversion_Days    --49
//  , '' as Cash_Conversion_Cal_Days --50  
//, NULL Authorized_Date  --51
//,NULL Utilized_Date       --52       
//  ,  '' AS  AUTHORIZED_NAME  --53       
//,C.state   AS Line_Status     --54
//, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Out_Bal(p.ORDER_NO ,1),2) as Revert_Reversed_Bal --55     
//, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Amt_Paid(p.ORDER_NO ,1),2) as Total_Amount_Paid --56        
//,  ifsapp.hpnret_pro_fam_group_api.get_hpnret_groupings(0,
//                                                            'EXT WARRANTY',
//                                                            IFSAPP.inventory_part_api.Get_Part_Product_Family(C.contract,
//                                                                                                              C.part_no)) AS HPNRET_GROUPING -- 57
//
//  ,''  as EnteredBy --58
//  ,  '' AS EnteredName  --59      
//   
//    , (
// SELECT ifsapp.HPNRET_LEVEL_API.Get_Hpnret_Level_Type(HLHH.level_id) AS SALESMAN_TYPE
//  FROM IFSAPP.HPNRET_LEVEL_HIERARCHY HLHH WHERE HLHH.site_id = P.contract
//  )  AS SALESMAN_TYPE  ----60 
//
//,IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sale_Type(p.ORDER_NO)  as     SALE_TYPE   --61    
//, (SELECT HR_ADV.account_code FROM ifsapp.hpnret_ADV_PAY HR_ADV where HR_ADV.prod_rel_order_no = c.order_no  AND HR_ADV.objstate = 'Approved')   Status_586P --62 
//
//          
//              
//  from IFSAPP.INDIRECT_SALES_DTL p
//  left join IFSAPP.INVENTORY_LOCATION l on l.location_no = p.location_no
//                                       AND L.contract = p.contract
// left join IFSAPP.Customer_Order_Line C on c.order_no = p.order_no
//                                        and c.line_no = p.line_no
//                                        and c.line_item_no = p.line_item_no
//                                        and c.rel_no = p.rel_no
//where p.part_type not in ('PKG')
//and TRUNC(p.transaction_date) = to_date(:PreviousDate, 'YYYY/MM/DD')    --to_date(:PreviousDate, 'YYYY/MM/DD')
//   and p.free_issue = 'N'
//    /*and (SELECT substr(i.second_commodity, 1, 2)
//          FROM ifsapp.inventory_part i
//         WHERE i.contract = P.contract
//           AND i.part_no = P.sales_part) NOT IN ( '2P','2C','GV')
//   AND P.sales_part <> 'PARTS' */
// AND p.DEALER <> 'DM09999'
//group by l.location_group,
//          p.transaction_date,
//          p.location_no,
//          p.order_no,
//          c.customer_no,
//          c.date_entered,
//          p.units,
//          p.value,
//          p.contract,
//          P.cash_price,
//          p.sales_part,
//          p.free_issue,
//          p.transaction_id,
//          p.line_no,
//          p.rel_no,
//          p.line_item_no,
//          P.stat_year,
//          P.stat_period_no,P.free_issue
//          ,C.state,C.contract,C.part_no
//          ,c.order_no
//         ,p.dealer
//        ) tblMain
//           
//              
//         
//                ";
                #endregion

                #region Sql 2020/03/12

                selectQuery = @"     
           --SALE LINE NOT IN PKG--BI
        SELECT 
         
        tblMain.SITE_CODE  ,--2  --************PK
        tblMain.SALES_PART, --16
        tblMain.UNITS,--17
        tblMain.SALES_PRICE,--9
        CASE WHEN  tblMain.UNITS = 0 THEN 0 ELSE   ROUND(tblMain.DISCOUNT_AMOUNT/tblMain.UNITS,2) END AS UNIT_DISCOUNT_AMOUNT ,  --14
        CASE WHEN  tblMain.UNITS = 0 THEN 0 ELSE  ROUND(((tblMain.DISCOUNT_AMOUNT/tblMain.UNITS)/100),2) END AS  UNIT_DISCOUNT_PRENTENTAGE ,
        tblMain.VALUE,--18
        (tblMain.DISCOUNT_AMOUNT ) AS TOTAL_DISCOUNT,
        tblMain.LOCATION_NO,--28
         tblMain.ORDER_NO,--3  --************PK THIS IS CALLED SALES_TRANS_NO
         tblMain.TDATE,--1     --************PK
        SUBSTR(tblMain.EMPLOYEE_CODE,1,12) AS PROMOTER_CODE ,--17
         tblMain.AUTHORIZED_USER_ID ,--29
         tblMain.STATE ,--30
       tblMain.CUSTOMER_NO,--4
       tblMain.LENGTH_OF_CONTRACT   ,--24
         ' ' AS ADVANCE_PAYMENT_NO ,  
         0  AS ADVANCE_PAYMENT_AMOUNT ,  
       tblMain.SALE_TYPE , --61    --************PK
       tblMain.SALES_LOCATION_NO ,--27
        tblMain.SERVICE_CHARGE , --31
         tblMain.FIRST_PAYMENT ,  --32
        tblMain.BUDGET_BOOK_HIRE_PRICE ,  --34
        tblMain.MONTHLY_PAYMENT  ,--33
         tblMain.BUDGET_BOOK  , --23
         tblMain.PRICE_LIST, --35
          tblMain.GROSS_HIRE_PRICE  --36
          ,tblMain.VAL_SURAKSHA_HP_LINES as SURAKSHA_VALUE--37
          ,tblMain.val_sanasuma_hp_lines AS SANASUMA_VALUE--38
          ,tblMain.Senasuma_Plus_Value--39
          ,tblMain.Senasuma_Plus_Mobile_Value--40
          ,tblMain.Senasuma_Post_Value--41
          ,tblMain.TRANSAACTION_TYPE --42
          ,tblMain.SALES_VARIATION  --43
          ,tblMain.FREE_ISSUE -- 44
          ,tblMain.salesman_code -- 45
          ,tblMain.Vallibel_Introduced  --46
          ,tblMain.SERIAL_NO --47
          ,tblMain.order_category --48
        
    --FOR PRIMARY KEY
    
    ,1 AS TILL_NO --************PK  THIS IS HARD CODE FOR EVERY SQL
    ,tblMain.SALE_LINE_TYPE  --20 --************PK
    ,tblMain.LINE_NO AS SALE_LINE_NO --************PK

    ,NVL ( ifsapp.Sin_FN_DayWiseTotal_Singer_Tax(SUBSTR(tblMain.SITE_CODE,1,5),tblMain.SALES_PART,SUBSTR(tblMain.ORDER_NO,5,1),NVL(tblMain.VALUE,0)) , 0 ) AS sales_tax_value--49
    ,tblMain.INVENTORY_VALUE   --50
    ,CASE WHEN  SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'NM' THEN 'NORMAL'
       WHEN SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'SP' THEN 'SPECIAL'
       WHEN SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'GR' THEN 'GROUP'
        ELSE ''  END 
      AS SALES_GROUPING    --51
      , CASE WHEN  tblMain.TaxCallType = 'D' THEN      
         
       NVL(  ( select DISTINCT SUBSTR(au.reason,1,20)   
                from ifsapp.hpnret_auth_variation au 
                where au.account_no =  tblMain.ORDER_NO  
                   AND au.variation IN (  select  DISTINCT A.variation from ifsapp.hpnret_variation_reason a  WHERE A.variation <> 'EarlyClosure')
                   AND au.state = 'Active'
                   AND au.utilized = '1'  
                   and tblMain.UNITS < 0--THIS IS TAKE TO AVOID MULTi ROWS
                   AND TO_DATE(tblMain.TDATE,'YYYY-MM-DD') =   TRUNC(au.utilized_date)
                   AND ROWNUM = 1 
                )  , ' ')
                
                
        ELSE 
        tblMain.RET_REASON_CODE
       END  AS RET_REASON_CODE   --52
       , tblMain.Status_586P   --63
, tblMain.discount_type --12
, tblMain.discount_reason --12
,tblMain.TaxCallType as SalesFileType
,tblMain.PROMOTION_CODE--15
,NVL(tblMain.Cash_Conversion_Days,0)  AS Cash_Conversion_Days-- 49
,tblMain.Cash_Conversion_Cal_Days -- 50
, tblMain.Authorized_Date --51
,tblMain.Utilized_Date     --52
,tblMain.AUTHORIZED_NAME  --53       
,tblMain.Line_Status  --54
,tblMain.Revert_Reversed_Bal --55
,tblMain.Total_Amount_Paid -- 56     
,tblMain.HPNRET_GROUPING   --57
 ,tblMain.EnteredBy --58
 , tblMain.EnteredName  --59
 , tblMain.SALESMAN_TYPE  --60

  FROM (

        select distinct TO_CHAR(s.transaction_date, 'YYYY-MM-DD') AS TDATE, --1       
          s.shop_code  as site_code, --2     
                s.order_no, --3
                c.customer_no, --4
                c.date_entered, --5
                s.variation, --6
                case
                  WHEN substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'H' then
                   (hhh.sales_promoter)
                  when substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'R' THEN
                   (hco.sales_promoter)
                end as EMPLOYEE_CODE, --7
                S.TRANSACTION_ID as line_no, --8
                TRUNC(s.cash_price, 2) as sales_price, --9
                TRUNC(s.cash_price, 2) as sales_price_1, --10
                
                TRUNC(SUM(C.COST)/COUNT(*)) as inventory_value, --11
                
                t.discount_type, --12
                case
                  WHEN t.discount_type = 'G' THEN
                   'GENERAL'
                  when t.discount_type = 'S' THEN
                   'SPECIAL'
                  else
                   ''
                end as discount_reason, --13
                
                ((select TRUNC(((sum(s.cash_units + s.hire_units)) /
                              count(*) *
                               (nvl(sum(t.discount_amount),
                                     (sum(t.discount * t.calculation_basis) / 100)))),
                               2)
                    from ifsapp.cust_order_line_discount t
                   where t.order_no = s.order_no
                     and t.line_no = s.line_no
                     and t.rel_no = s.rel_no
                     and t.line_item_no = s.line_item_no
                     AND s.STATE NOT IN ('RevertReversed', 'Reverted'))) as discount_amount, --14
                
                (nvl(nvl((SELECT to_char(d.rule_no) promo_rule_number
                           FROM ifsapp.hpnret_hp_dtl d
                          WHERE d.account_no = s.order_no
                            AND d.conn_line_no = s.line_no
                            AND d.conn_rel_no = s.rel_no
                            AND d.conn_line_item_no = s.line_item_no
                            AND d.catalog_type = 'Inventory part'
                            AND ROWNUM = 1),
                         (SELECT to_char(l.free1) promo_rule_number
                            FROM ifsapp.Hpnret_Cust_Order_Line l,
                                 ifsapp.hpnret_customer_order  h
                           WHERE l.free1 IS NOT NULL
                             AND h.order_no = l.order_no
                             AND h.cash_conv = 'FALSE'
                             AND l.order_no = s.order_no
                             AND l.conn_line_no = s.line_no
                             AND l.conn_rel_no = s.rel_no
                             AND l.conn_line_item_no = s.line_item_no
                             AND ROWNUM = 1)),
                     (SELECT to_char(c.discount_source_id) promo_rule_number
                        FROM ifsapp.Cust_Order_Line_Discount c
                       WHERE c.user_id = 'PROMO'
                         AND c.discount_source_id IS NOT NULL
                         AND c.order_no = s.order_no
                         AND c.line_no = s.line_no
                         AND c.rel_no = s.rel_no
                         AND c.line_item_no = s.line_item_no
                         AND ROWNUM = 1))) AS PROMOTION_CODE, --15
                
                s.sales_part, --16
                case
                  WHEN substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'H' and s.hire_units > 0 and
                       (s.state = 'SALE') THEN
                   (s.hire_units)
                    WHEN substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'H' and s.hire_units < 0 and
                       (s.state = 'SALE') THEN
                   (s.hire_units)
                  when substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'R' and s.cash_units > 0 and
                       (s.state = 'SALE') then
                   (s.cash_units)
                    when substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'R' and s.cash_units < 0 and
                       (s.state = 'SALE') then
                   (s.cash_units)
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and (s.state = 'CashConverted') and
                       (s.hire_units) < 0 then
                   (s.hire_units)
                  when (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'R') and (s.variation = 'CashCon') and
                       s.cash_units > 0 then
                   (s.cash_units)
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and (s.variation = 'ExchgOut') and
                       (s.hire_units) > 0 then
                   (s.hire_units)
                  when (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'R') and (s.variation = 'ExchgOut') and
                       (s.cash_units) > 0 then
                   (s.cash_units)
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and s.reverts_units > 0 and
                       s.state = 'Reverted' then
                   (s.reverts_units * -1)
                  when (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and s.revert_reverse_units > 0 and
                       s.state = 'RevertReversed' then
                   (s.revert_reverse_units)
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and s.hire_units < 0 and
                       s.state = 'Returned' then
                   (s.hire_units)
                  when (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'R') and s.cash_units < 0 and
                       s.state = 'Returned' then
                   (s.cash_units)
                  WHEN substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'H' and s.hire_units < 0 and
                       (s.state = 'ExchangedIn') THEN
                   (s.hire_units)
                  WHEN substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'R' and s.cash_units < 0 and
                       (s.state = 'ExchangedIn') THEN
                   (s.cash_units)

                   WHEN substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'H' and s.hire_units < 0 and
                       (s.state = 'Closed') THEN
                   (s.hire_units)

                END as units, --17
                
                Trunc(CASE
                        WHEN substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'H' and s.net_hire_cash_value > 0 and
                             (s.state = 'SALE') THEN
                         (s.net_hire_cash_value)
                                    WHEN substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'H' and s.net_hire_cash_value < 0 and
                             (s.state = 'SALE') THEN
                         (s.net_hire_cash_value)
                        when substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'R' and s.cash_value > 0 and
                             (s.state = 'SALE') then
                         (s.cash_value)
                                    when substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'R' and s.cash_value < 0 and
                             (s.state = 'SALE') then
                         (s.cash_value)
                        WHEN (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'H') and (s.state = 'CashConverted') and
                             (s.net_hire_cash_value) < 0 then
                         (s.net_hire_cash_value)
                        when (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'R') and (s.variation = 'CashCon') and
                             s.cash_value > 0 then
                         s.cash_value
                        WHEN (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'H') and (s.variation = 'ExchgOut') and
                             (s.net_hire_cash_value) > 0 then
                         s.net_hire_cash_value
                        when (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'R') and (s.variation = 'ExchgOut') and
                             (s.cash_value) > 0 then
                         s.cash_value
                        WHEN (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'H') and s.reverts_value > 0 and
                             s.state = 'Reverted' then
                         (s.reverts_value * -1)
                        when (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'H') and s.revert_reverse_value > 0 and
                             s.state = 'RevertReversed' then
                         s.revert_reverse_value
                        WHEN (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'H') and s.net_hire_cash_value < 0 and
                             s.state = 'Returned' then
                         s.net_hire_cash_value
                        when (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'R') and s.cash_value < 0 and
                             s.state = 'Returned' then
                         s.cash_value
                        WHEN substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'H' and s.net_hire_cash_value < 0 and
                             (s.state = 'ExchangedIn') THEN
                         (s.net_hire_cash_value)
                        WHEN substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'R' and s.cash_value < 0 and
                             (s.state = 'ExchangedIn') THEN
                         (s.cash_value)
                            WHEN substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'H' and s.net_hire_cash_value < 0 and
                             (s.state = 'Closed') THEN
                         (s.net_hire_cash_value)

                      END,
                      2) as value, --18
                
                CASE
                  WHEN substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'H' and s.net_hire_cash_value > 0 and
                       (s.state = 'SALE') THEN
                   '201'
                  when substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'R' and s.cash_value > 0 and
                       (s.state = 'SALE') THEN
                   '101'
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'H') and (s.state = 'CashConverted') and
                       (s.net_hire_cash_value) < 0 THEN
                   '205'
                  when (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'R') and (s.variation = 'CashCon') and
                       s.cash_value > 0 then
                   '105'
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and (s.variation = 'ExchgOut') and
                       (s.net_hire_cash_value) > 0 THEN
                   '203'
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'R') and (s.variation = 'ExchgOut') and
                       (s.cash_value) > 0 THEN
                   '103'
                  when (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and (s.reverts_value > 0) and
                       s.state = 'Reverted' THEN
                   '206'
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and (s.revert_reverse_value > 0) and
                       s.state = 'RevertReversed' THEN
                   '207'
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and s.net_hire_cash_value < 0 and
                       s.state = 'Returned' THEN
                   '204'
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'R') and s.cash_value < 0 and
                       s.state = 'Returned' THEN
                   '104'
                  WHEN substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'H' and s.net_hire_cash_value < 0 and
                       (s.state = 'ExchangedIn') then
                   '202'
                  WHEN substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'R' and s.cash_value < 0 and
                       (s.state = 'ExchangedIn') then
                   '102'
                END AS TRANSACTION_SUBTYPE_CODE, --19
                
                case
                  WHEN substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'H' and s.net_hire_cash_value > 0 and
                       (s.state = 'SALE') THEN
                   '1'
                  when substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'R' and s.cash_value > 0 and
                       (s.state = 'SALE') THEN
                   '1'
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and (s.state = 'CashConverted') and
                       (s.net_hire_cash_value) < 0 THEN
                   '2'
                  when (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'R') and (s.variation = 'CashCon') and
                       s.cash_value > 0 then
                   '1'
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and (s.variation = 'ExchgOut') and
                       (s.net_hire_cash_value) > 0 THEN
                   '1'
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'R') and (s.variation = 'ExchgOut') and
                       (s.cash_value) > 0 THEN
                   '1'
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and (s.reverts_value > 0) and
                       s.state = 'Reverted' THEN
                   '2'
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and (s.revert_reverse_value > 0) and
                       s.state = 'RevertReversed' THEN
                   '1'
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'H') and s.net_hire_cash_value < 0 and
                       s.state = 'Returned' THEN
                   '2'
                  WHEN (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'R') and s.cash_value < 0 and
                       s.state = 'Returned' THEN
                   '2'
                  WHEN substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'H' and s.net_hire_cash_value < 0 and
                       (s.state = 'ExchangedIn') then
                   '2'
                  WHEN substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'R' and s.cash_value < 0 and
                       (s.state = 'ExchangedIn') then
                   '2'
                end as SALE_LINE_TYPE, --20
                
                 l.location_group as SALES_LOCATION, --21
                
                TRUNC(s.cash_price, 2) as ORIGINAL_PRODUCT_PRICE, --22
                hhh.budget_book_id AS BUDGET_BOOK, --23
                hhh.length_of_contract as LENGTH_OF_CONTRACT, --24
                'D' AS TaxCallType --25
                ,' ' as RET_REASON_CODE--26
             ,   case
         WHEN substr(s.order_no, instr(s.order_no, '-', -1, 1) + 1, 1) = 'H' then
          IFSAPP.Hpnret_Hp_Head_Api.Get_Sales_Location_No(s.order_no, '1')
         WHEN substr(s.order_no, instr(s.order_no, '-', -1, 1) + 1, 1) = 'R' THEN
          IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sales_Location_No(s.order_no)
       end as SALES_LOCATION_NO    --27
       ,s.LOCATION_NO AS LOCATION_NO --28
       ,t.authorized_user AS AUTHORIZED_USER_ID --29
       ,S.STATE --30
       , trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Serv_Chg(S.ORDER_NO ,1),2) AS SERVICE_CHARGE--31
       , trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_First_Payment(S.ORDER_NO ,1),2) as FIRST_PAYMENT --32
       , trunc(IFSAPP.hpnret_hp_head_api.get_total_monthly_payment(S.ORDER_NO ,1),2) as MONTHLY_PAYMENT --33
       ,trunc(IFSAPP.hpnret_hp_head_api.get_total_bb_hire_price(S.ORDER_NO ,1),2) as BUDGET_BOOK_HIRE_PRICE --34
       , IFSAPP.customer_order_api.get_price_list_no(S.ORDER_NO)  AS PRICE_LIST --35
       ,trunc(IFSAPP.hpnret_hp_head_api.get_gross_hire_value(S.ORDER_NO,1),2) AS GROSS_HIRE_PRICE --36
       ,s.VAL_SURAKSHA_HP_LINES --37
       ,s.val_sanasuma_hp_lines --38
       ,   (     
       select coc.charge_amount
  from ifsapp.customer_order_charge coc
 where
 s.order_no = coc.order_no
 and s.line_no = coc.line_no
 and s.rel_no = coc.rel_no
 and s.line_item_no = coc.line_item_no
 and coc.charge_type in ('EXT_PLUS')
       ) as Senasuma_Plus_Value  --39

       ,   (     
       select coc.charge_amount
  from ifsapp.customer_order_charge coc
 where
 s.order_no = coc.order_no
 and s.line_no = coc.line_no
 and s.rel_no = coc.rel_no
 and s.line_item_no = coc.line_item_no
 and coc.charge_type in ('EXT_PLUS-1')
       ) as Senasuma_Plus_Mobile_Value --40
       
       
        ,  CASE WHEN  (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'R')  THEN    (     
       select sum(HAP.DOM_AMOUNT) as DOM_AMOUNT
  from ifsapp.HPNRET_ADV_PAY HAP
 where
 s.order_no = HAP.order_no
 and s.line_no = HAP.line_no
 and s.rel_no = HAP.rel_no
 and s.line_item_no = HAP.line_item_no 
 AND  HAP.ACCOUNT_CODE = 'EXTPO'
       ) 
       
       ELSE (     
       select sum(HAP.DOM_AMOUNT) as DOM_AMOUNT
  from ifsapp.HPNRET_ADV_PAY HAP
 where
 s.order_no = HAP.original_account
 and s.line_no = HAP.line_no
 and s.rel_no = HAP.rel_no
 and s.line_item_no = HAP.line_item_no 
 AND  HAP.ACCOUNT_CODE = 'EXTPO'
       )  END 
       
       
       as Senasuma_Post_Value   --41
, CASE WHEN   substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1)  = 'R' THEN 'CASH' ELSE 'HIRE'  END  AS TRANSAACTION_TYPE--42
  ,s.variation    as SALES_VARIATION  --43                       
   ,S.FREE_ITEM AS FREE_ISSUE    --44
   ,s.salesman_code -- 45
   , (select case when  count(*) > 0 then 'YES' ELSE 'NO' END AS Vallibel_Introduced
 from ifsapp.sin_v_one_product_tab valibe 
   where valibe.product_code = s.part_no  ) AS Vallibel_Introduced  --46
  
          
   ,(SELECT ST.SERIAL_NO
          FROM IFSAPP.SERIAL_TRANSACTION ST
         WHERE   s.order_no = ST.ORDER_NO 
          and s.line_no = ST.line_no
 and s.rel_no = ST.rel_no
 and s.line_item_no = ST.line_item_no 
           AND ROWNUM = 1) SERIAL_NO --47
           
           ,hco.order_category   --48

,      trunc(hhh.closed_date )  -   TRUNC(hhh.original_sales_date) as Cash_Conversion_Days --49

,  (SELECT   au.remarks
 from ifsapp.hpnret_auth_variation au
 WHERE s.order_no = au.account_no
  AND ROWNUM = 1
 )   as Cash_Conversion_Cal_Days --50


,  
 (SELECT  (au.from_date) AS Authorized_Date
 from ifsapp.hpnret_auth_variation au
 WHERE s.order_no = au.account_no
 AND ROWNUM = 1
 ) Authorized_Date  --51
                
 ,  
 (SELECT  TRUNC(AU.Utilized_Date) AS Utilized_Date
 from ifsapp.hpnret_auth_variation au
 WHERE s.order_no = au.account_no
  AND ROWNUM = 1
 ) Utilized_Date  --52

  ,  
 (SELECT  UTAB.DESCRIPTION AS AUTHORIZED_NAME
 from ifsapp.fnd_user_tab UTAB
 WHERE UTAB.IDENTITY = T.authorized_user
 ) AUTHORIZED_NAME  --53   
 
 ,C.state AS Line_Status --54
, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Out_Bal(S.ORDER_NO ,1),2) as Revert_Reversed_Bal --55
, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Amt_Paid(S.ORDER_NO ,1),2) as Total_Amount_Paid --56  
,  ifsapp.hpnret_pro_fam_group_api.get_hpnret_groupings(0,
                                                            'EXT WARRANTY',
                                                            IFSAPP.inventory_part_api.Get_Part_Product_Family(C.contract,
                                                                                                              C.part_no)) AS HPNRET_GROUPING -- 57 
 
 ,(select oldr.user_id
                from ifsapp.cust_order_line_discount oldr
                where oldr.order_no = s.order_no
                and oldr.line_no = s.line_no
                and oldr.rel_no = s.rel_no
                and abs(oldr.line_item_no) = abs(s.line_item_no)
                and rownum = 1)  as EnteredBy --58
  ,  
 (SELECT  UTAB.DESCRIPTION AS AUTHORIZED_NAME
 from ifsapp.fnd_user_tab UTAB
 WHERE UTAB.IDENTITY = T.user_id
 ) EnteredName  --59
 , (
 SELECT ifsapp.HPNRET_LEVEL_API.Get_Hpnret_Level_Type(HL.level_id) AS SALESMAN_TYPE
  FROM IFSAPP.HPNRET_LEVEL_HIERARCHY HL WHERE HL.site_id = S.SHOP_CODE
  )    SALESMAN_TYPE  ----60
 ,    IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sale_Type(S.ORDER_NO)  as     SALE_TYPE   --61
, (SELECT HR_ADV.account_code FROM ifsapp.hpnret_ADV_PAY HR_ADV where HR_ADV.prod_rel_order_no = c.order_no  AND HR_ADV.objstate = 'Approved')   Status_586P --62 
               
  FROM IFSAPP.direct_sales_dtl_tab S
  left join IFSAPP.INVENTORY_LOCATION l on l.location_no = s.location_no
                                       AND L.contract = S.SHOP_CODE
  left join ifsapp.cust_order_line_discount t on s.order_no = t.order_no
                                             and s.line_no = t.line_no
                                             and s.line_item_no =
                                                 t.line_item_no
                                             and s.rel_no = t.rel_no
  left join IFSAPP.Customer_Order_Line C on c.order_no = s.order_no
                                        and c.line_no = s.line_no
                                        and c.line_item_no = s.line_item_no
                                        and c.rel_no = s.rel_no
  left join IFSAPP.Hpnret_Hp_Head hhh on s.order_no = hhh.account_no
  left join ifsapp.Hpnret_Customer_Order hco on s.order_no = hco.order_no
where s.catalog_type not in ('PKG')
   and s.free_item = 'N'
   AND TRUNC(s.transaction_date) = to_date(:PreviousDate, 'YYYY/MM/DD') --to_date(:PreviousDate, 'YYYY/MM/DD')
   /*and (SELECT substr(i.second_commodity, 1, 2)
          FROM ifsapp.inventory_part i
         WHERE i.contract = s.shop_code
           AND i.part_no = s.sales_part) NOT IN ( '2P','2C','GV') 
  AND S.SALES_PART <> 'PARTS'   */ 
   
group by l.location_group,
          (s.transaction_date),
          s.shop_code,
          s.order_no,
          c.customer_no,
          c.date_entered,
          s.hire_units,
          s.reverts_units,
          s.revert_reverse_units,
          s.line_no,
          s.state,
          s.rel_no,
          s.sales_part,
          S.TRANSACTION_ID,
          s.line_item_no,
          s.cash_units,
          s.variation,
          s.reverts_value,
          s.revert_reverse_value,
          s.net_hire_cash_value,
          s.cash_value,
          S.cash_price,
          t.discount_type,
          s.location_no,
          hhh.sales_promoter,
          hco.sales_promoter,
          hhh.sales_location_no,
          hco.sales_location_no,
          hhh.budget_book_id,
          hhh.length_of_contract,
          s.special_discount_value,
          s.promotional_discount_value,
          S.YEAR,
          S.PERIOD,
          S.PART_NO,
          hhh.contract,
          hco.contract,t.authorized_user
         ,s.VAL_SURAKSHA_HP_LINES  
       ,s.val_sanasuma_hp_lines  
       ,S.FREE_ITEM 
       ,s.salesman_code
       ,hco.order_category
       ,trunc(hhh.original_sales_date)    
      , trunc(hhh.closed_date ) 
       ,C.state,C.contract,C.part_no
,T.user_id
,c.order_no

union

 select distinct TO_CHAR(p.transaction_date, 'YYYY-MM-DD') AS TDATE, --1
                /*case
                  when p.location_no is null then
                   p.contract || '_' || 'STORE'
                  when p.location_no is not null then
                   p.contract || '_' || l.location_group
                end */  --p.contract as    site_code, --2 --COMMENTED ON 2019/09/27
        /* CASE WHEN c.customer_no IS NOT NULL THEN c.customer_no ELSE case
                  WHEN p.order_no is null THEN
                   to_char(p.transaction_id)
                  when p.order_no is not null then
                   p.order_no
                end END  as    site_code,--2 */  --again commented on 2019-10-21
            p.dealer as site_code,--2

                case
                  WHEN p.order_no is null THEN
                   to_char(p.transaction_id)
                  when p.order_no is not null then
                   p.order_no
                end as order_no, --3
             --   c.customer_no, --4  --
            p.dealer as customer_no, --4 --added on 21oct 2019
                case
                  When c.date_entered is null then
                   p.transaction_date
                  when c.date_entered is not null then
                   c.date_entered
                end as date_entered, --5
                '' as variation, --6
                p.Sales_Promoter as EMPLOYEE_CODE, --7
                P.TRANSACTION_ID as line_no, --8
                TRUNC(P.cash_price, 2) AS sales_price, --9
                TRUNC(P.cash_price, 2) AS sales_price_1, --10
                
               TRUNC(SUM(C.COST)/COUNT(*)) AS inventory_value, --11
                
                (select t.discount_type
                   from ifsapp.cust_order_line_discount t
                  where t.order_no = p.order_no
                    and t.line_no = p.line_no
                    and t.line_item_no = p.line_item_no
                    and t.rel_no = p.rel_no
                    AND ROWNUM = 1) as discount_type, --12
                
                case
                  WHEN (select t.discount_type
                          from ifsapp.cust_order_line_discount t
                         where t.order_no = p.order_no
                           and t.line_no = p.line_no
                           and t.line_item_no = p.line_item_no
                           and t.rel_no = p.rel_no
                           AND ROWNUM = 1) = 'G' THEN
                   'GENERAL'
                  when (select t.discount_type
                          from ifsapp.cust_order_line_discount t
                         where t.order_no = p.order_no
                           and t.line_no = p.line_no
                           and t.line_item_no = p.line_item_no
                           and t.rel_no = p.rel_no
                           AND ROWNUM = 1) = 'S' then
                   'SPECIAL'
                end as discount_reason, --13
                
               /* (select nvl(sum(t.discount_amount),
                            (t.discount * t.calculation_basis) / 100)
                   from ifsapp.cust_order_line_discount t
                  where t.order_no = p.order_no
                    and t.line_no = p.line_no
                    and t.line_item_no = p.line_item_no
                    and t.rel_no = p.rel_no
                  group by t.discount, t.calculation_basis,t.line_no) as discount_amount, --14  */
            (
            select  
               (   
               sum(nvl(t.discount_amount, 0)) +     sum((nvl(t.calculation_basis, 0) * (nvl(t.discount, 0) / 100)))
               ) AS DISCOUNT
            from ifsapp.cust_order_line_discount t
           where  t.order_no = p.order_no
                  and t.line_no = p.line_no
                  and t.line_item_no = p.line_item_no
                  and t.rel_no = p.rel_no  
           group by  t.line_no 
            )as discount_amount,  --14 
                
                (nvl(nvl((SELECT to_char(d.rule_no) promo_rule_number
                           FROM ifsapp.hpnret_hp_dtl d
                          WHERE d.account_no = p.order_no
                            AND d.conn_line_no = p.line_no
                            AND d.conn_rel_no = p.rel_no
                            AND d.conn_line_item_no = p.line_item_no
                            AND d.catalog_type = 'Inventory part'
                            AND ROWNUM = 1),
                         (SELECT to_char(l.free1) promo_rule_number
                            FROM ifsapp.Hpnret_Cust_Order_Line l,
                                 ifsapp.hpnret_customer_order  h
                           WHERE l.free1 IS NOT NULL
                             AND h.order_no = l.order_no
                             AND h.cash_conv = 'FALSE'
                             AND l.order_no = p.order_no
                             AND l.conn_line_no = p.line_no
                             AND l.conn_rel_no = p.rel_no
                             AND l.conn_line_item_no = p.line_item_no
                             AND ROWNUM = 1)),
                     (SELECT to_char(c.discount_source_id) promo_rule_number
                        FROM ifsapp.Cust_Order_Line_Discount c
                       WHERE c.user_id = 'PROMO'
                         AND c.discount_source_id IS NOT NULL
                         AND c.order_no = p.order_no
                         AND c.line_no = p.line_no
                         AND c.rel_no = p.rel_no
                         AND c.line_item_no = p.line_item_no
                         AND ROWNUM = 1))) as PROMOTION_CODE, --15
                
                p.sales_part, --16
                case
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and p.units > 0.01 then
                   (p.units)
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and p.units < 0.01 then
                   (p.units)
                  when p.order_no is null and p.units < 0.01 then
                   (p.units)
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and p.units > 0 and
                       p.free_issue = 'Y' then
                   (p.units)
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and p.units < 0 and
                       p.free_issue = 'Y' then
                   (p.units)
                when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) NOT IN ( 'R' ,'H')  then
                   (p.units)
                END as units, --17
                
                CASE
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and p.value > 0.01 then
                   (p.value)
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and p.value < 0.01 then
                   (p.value)
                  when p.order_no is null and p.value < 0.01 then
                   (p.value)
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and p.units > 0 and
                       p.free_issue = 'Y' then
                   (p.value)
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and p.units < 0 and
                       p.free_issue = 'Y' then
                   (p.value)
                when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) NOT IN ( 'R','H')   then
                   (p.value)
                END as value, --18
                
                CASE
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and (p.value >= 0.01) THEN
                   '101'
                  WHEN (substr(p.order_no,
                               instr(p.order_no, '-', -1, 1) + 1,
                               1) = 'R') and (p.value < 0.01) THEN
                   '104'
                  WHEN p.order_no is null and p.value < 0.01 THEN
                   '104'
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and (p.units > 0) and
                       p.free_issue = 'Y' THEN
                   '101'
                  WHEN (substr(p.order_no,
                               instr(p.order_no, '-', -1, 1) + 1,
                               1) = 'R') and (p.units < 0) and
                       p.free_issue = 'Y' THEN
                   '104'
                END AS TRANSACTION_SUBTYPE_CODE, --19
                
                case
                  WHEN (substr(p.order_no,
                               instr(p.order_no, '-', -1, 1) + 1,
                               1) = 'R') and (p.value >= 0.01) THEN
                   '1'
                  WHEN (substr(p.order_no,
                               instr(p.order_no, '-', -1, 1) + 1,
                               1) = 'R') and (p.value < 0.00) THEN
                   '2'
                  WHEN p.order_no is null and p.value < 0.01 THEN
                   '2'
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and (p.units > 0) and
                       p.free_issue = 'Y' THEN
                   '1'
                  WHEN (substr(p.order_no,
                               instr(p.order_no, '-', -1, 1) + 1,
                               1) = 'R') and (p.units < 0) and
                       p.free_issue = 'Y' THEN
                   '2'
                end as SALE_LINE_TYPE, --20
                
                p.Sales_Location as SALES_LOCATION, --21
                TRUNC(P.cash_price, 2) as ORIGINAL_PRODUCT_PRICE, --22
                'N/A' as BUDGET_BOOK, --23
                0 as LENGTH_OF_CONTRACT, --24
                'I' AS TaxCallType --25
                ,NVL ( (
                SELECT  DISTINCT SUBSTR(L.return_reason_code,1,20) 
                    FROM  ifsapp.Return_Material_Line L 
                    WHERE p.ORDER_NO = L.order_no 
                    AND p.line_no = L.line_no
                    AND p.rel_no = L.rel_no
                    AND p.line_item_no = L.line_item_no
                    AND TO_CHAR(p.transaction_date, 'YYYY-MM-DD') = TO_CHAR(L.date_returned,'YYYY-MM-DD')
                    AND ROWNUM = 1

                ), ' ' ) AS RET_REASON_CODE --26
               , case
         WHEN substr(p.order_no, instr(p.order_no, '-', -1, 1) + 1, 1) = 'H' then
          IFSAPP.Hpnret_Hp_Head_Api.Get_Sales_Location_No(p.order_no, '1')
         WHEN substr(p.order_no, instr(p.order_no, '-', -1, 1) + 1, 1) = 'R' THEN
          IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sales_Location_No(p.order_no)
       end as SALES_LOCATION_NO --27
       ,p.location_no AS LOCATION_NO--28
       ,' '  AS AUTHORIZED_USER_ID --29
       ,' ' AS STATE --30
        , 0.0 as SERVICE_CHARGE --31 
        , 0.0  as FIRST_PAYMENT --32
        ,0.0 AS MONTHLY_PAYMENT --33
        ,0.0 as BUDGET_BOOK_HIRE_PRICE --34
        ,IFSAPP.customer_order_api.get_price_list_no(p.order_no) AS PRICE_LIST --35
        ,0.0 AS GROSS_HIRE_PRICE --36
        ,0.0 AS VAL_SURAKSHA_HP_LINES   --37
        ,0.0 AS val_sanasuma_hp_lines -- 38
        ,0.0 as Senasuma_Plus_Value--39
        ,0.0 as Senasuma_Plus_Mobile_Value--40
        ,0.0 as Senasuma_Post_Value--41
        ,CASE WHEN   substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1)  = 'R' THEN 'CASH' ELSE 'HIRE'  END  AS TRANSAACTION_TYPE -- 42
        
         ,'' as   SALES_VARIATION    --43
         ,P.free_issue AS FREE_ISSUE   --44
         , '' as  salesman_code -- 45
            , (select case when  count(*) > 0 then 'YES' ELSE 'NO' END AS Vallibel_Introduced
 from ifsapp.sin_v_one_product_tab valibe 
   where valibe.product_code = P.sales_part  ) AS Vallibel_Introduced  --46
   

              ,(SELECT ST.SERIAL_NO
          FROM IFSAPP.SERIAL_TRANSACTION ST
         WHERE   p.order_no = ST.ORDER_NO 
          and p.line_no = ST.line_no
 and p.rel_no = ST.rel_no
 and p.line_item_no = ST.line_item_no 
           AND ROWNUM = 1) SERIAL_NO --47
  , ifsapp.Hpnret_Customer_Order_api.Get_Order_Category(p.order_no) as  order_category     --48    
   ,0 as   Cash_Conversion_Days    --49
  , '' as Cash_Conversion_Cal_Days --50  
, NULL Authorized_Date  --51
,NULL Utilized_Date       --52       
  ,  '' AS  AUTHORIZED_NAME  --53       
,C.state   AS Line_Status     --54
, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Out_Bal(p.ORDER_NO ,1),2) as Revert_Reversed_Bal --55     
, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Amt_Paid(p.ORDER_NO ,1),2) as Total_Amount_Paid --56        
,  ifsapp.hpnret_pro_fam_group_api.get_hpnret_groupings(0,
                                                            'EXT WARRANTY',
                                                            IFSAPP.inventory_part_api.Get_Part_Product_Family(C.contract,
                                                                                                              C.part_no)) AS HPNRET_GROUPING -- 57

  ,''  as EnteredBy --58
  ,  '' AS EnteredName  --59      
   
    , (
 SELECT ifsapp.HPNRET_LEVEL_API.Get_Hpnret_Level_Type(HLHH.level_id) AS SALESMAN_TYPE
  FROM IFSAPP.HPNRET_LEVEL_HIERARCHY HLHH WHERE HLHH.site_id = P.contract
  )  AS SALESMAN_TYPE  ----60 

,IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sale_Type(p.ORDER_NO)  as     SALE_TYPE   --61    
, (SELECT HR_ADV.account_code FROM ifsapp.hpnret_ADV_PAY HR_ADV where HR_ADV.prod_rel_order_no = c.order_no  AND HR_ADV.objstate = 'Approved')   Status_586P --62 

          
              
  from IFSAPP.INDIRECT_SALES_DTL p
  left join IFSAPP.INVENTORY_LOCATION l on l.location_no = p.location_no
                                       AND L.contract = p.contract
 left join IFSAPP.Customer_Order_Line C on c.order_no = p.order_no
                                        and c.line_no = p.line_no
                                        and c.line_item_no = p.line_item_no
                                        and c.rel_no = p.rel_no
where p.part_type not in ('PKG')
and TRUNC(p.transaction_date) = to_date(:PreviousDate, 'YYYY/MM/DD')    --to_date(:PreviousDate, 'YYYY/MM/DD')
   and p.free_issue = 'N'
    /*and (SELECT substr(i.second_commodity, 1, 2)
          FROM ifsapp.inventory_part i
         WHERE i.contract = P.contract
           AND i.part_no = P.sales_part) NOT IN ( '2P','2C','GV')
   AND P.sales_part <> 'PARTS' */
 AND p.DEALER <> 'DM09999'
group by l.location_group,
          p.transaction_date,
          p.location_no,
          p.order_no,
          c.customer_no,
          c.date_entered,
          p.units,
          p.value,
          p.contract,
          P.cash_price,
          p.sales_part,
          p.free_issue,
          p.transaction_id,
          p.line_no,
          p.rel_no,
          p.line_item_no,
          P.stat_year,
          P.stat_period_no,P.free_issue
          ,C.state,C.contract,C.part_no
          ,c.order_no
         ,p.dealer
        ) tblMain
           
              
         
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))//noneed to add @ before parameter value
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;



            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

      

        public DataTable NeenOpal_BI_GetSaleslineNotInPKG_DMD(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionDMD();
            string selectQuery = string.Empty;

            try
            {

                #region Sql 2018/05/14
                selectQuery = @"
 ----BI NOT IN PKG DMD
                    SELECT 
                     
        tblMain.SITE_CODE  ,--2  --************PK
        tblMain.SALES_PART, --16
        tblMain.UNITS,--17
        tblMain.SALES_PRICE,--9
        CASE WHEN  tblMain.UNITS = 0 THEN 0 ELSE   ROUND(tblMain.DISCOUNT_AMOUNT/tblMain.UNITS,2) END AS UNIT_DISCOUNT_AMOUNT ,  --14
        CASE WHEN  tblMain.UNITS = 0 THEN 0 ELSE  ROUND(((tblMain.DISCOUNT_AMOUNT/tblMain.UNITS)/100),2) END AS  UNIT_DISCOUNT_PRENTENTAGE ,
        tblMain.VALUE,--18
        (tblMain.DISCOUNT_AMOUNT ) AS TOTAL_DISCOUNT,
        tblMain.LOCATION_NO,--28
         tblMain.ORDER_NO,--3  --************PK THIS IS CALLED SALES_TRANS_NO
         tblMain.TDATE,--18
          SUBSTR(tblMain.EMPLOYEE_CODE,1,12) AS PROMOTER_CODE ,--17
           tblMain.AUTHORIZED_USER_ID ,--29
           tblMain.STATE ,--30
       tblMain.CUSTOMER_NO,--4
         
         tblMain.LENGTH_OF_CONTRACT   ,--24
         ' ' AS ADVANCE_PAYMENT_NO ,  
         0  AS ADVANCE_PAYMENT_AMOUNT ,  
       tblMain.SALE_TYPE , --61    --************PK
       tblMain.SALES_LOCATION_NO ,--27
        tblMain.SERVICE_CHARGE , --31
         tblMain.FIRST_PAYMENT ,  --32
        tblMain.BUDGET_BOOK_HIRE_PRICE ,  --34
        tblMain.MONTHLY_PAYMENT  ,--33
         tblMain.BUDGET_BOOK  , --23
         tblMain.PRICE_LIST, --35
          tblMain.GROSS_HIRE_PRICE  --36
          ,tblMain.VAL_SURAKSHA_HP_LINES as SURAKSHA_VALUE--37
          ,tblMain.val_sanasuma_hp_lines AS SANASUMA_VALUE--38
          ,tblMain.Senasuma_Plus_Value--39
          ,tblMain.Senasuma_Plus_Mobile_Value--40
          ,tblMain.Senasuma_Post_Value--41
          ,tblMain.TRANSAACTION_TYPE --42
          ,tblMain.SALES_VARIATION  --43
          ,tblMain.FREE_ISSUE -- 44
          ,tblMain.salesman_code -- 45
          ,tblMain.Vallibel_Introduced  --46
          ,tblMain.SERIAL_NO --47
          ,tblMain.order_category --48
        
    --FOR PRIMARY KEY
    
    ,1 AS TILL_NO --************PK  THIS IS HARD CODE FOR EVERY SQL
    ,tblMain.SALE_LINE_TYPE --20 --************PK
    ,tblMain.LINE_NO AS SALE_LINE_NO --************PK
    
   ,NVL ( ifsapp.Sin_FN_DayWiseTotal_DMD_Tax(SUBSTR(tblMain.SITE_CODE,1,5),tblMain.SALES_PART,SUBSTR(tblMain.ORDER_NO,5,1),NVL(tblMain.VALUE,0)) , 0 ) AS sales_tax_value
      ,tblMain.INVENTORY_VALUE
    ,CASE WHEN  SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'NM' THEN 'NORMAL'
       WHEN SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'SP' THEN 'SPECIAL'
       WHEN SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'GR' THEN 'GROUP'
        ELSE ''  END 
      AS SALES_GROUPING
      ,' ' as RET_REASON_CODE
       , tblMain.Status_586P --62
, tblMain.discount_type --12
, tblMain.discount_reason --12
,'DMD' AS  SalesFileType
,tblMain.PROMOTION_CODE--15
,NVL(tblMain.Cash_Conversion_Days,0)  AS Cash_Conversion_Days-- 49
,tblMain.Cash_Conversion_Cal_Days -- 50
, tblMain.Authorized_Date --51
,tblMain.Utilized_Date     --52
,tblMain.AUTHORIZED_NAME  --53       
,tblMain.Line_Status  --54
,tblMain.Revert_Reversed_Bal --55
,tblMain.Total_Amount_Paid -- 56
,tblMain.HPNRET_GROUPING   --57
 ,tblMain.EnteredBy --58
 , tblMain.EnteredName  --59
 , tblMain.SALESMAN_TYPE  --60
    
                    FROM 
                    (
                    select distinct TO_CHAR(p.transaction_date, 'YYYY-MM-DD') AS TDATE,
                    --p.contract as site_code,----COMMENTED ON 2019/09/27.

/*CASE WHEN c.customer_no IS NOT NULL THEN c.customer_no ELSE case
                  WHEN p.order_no is null THEN
                   to_char(p.transaction_id)
                  when p.order_no is not null then
                   p.order_no
                end END  as    site_code,--2 */ --commented on 21oct 2019
   P.dealer as    site_code,--2   added on 21oct 2019
                    case
                      WHEN p.order_no is null THEN
                       to_char(p.transaction_id)
                      when p.order_no is not null then
                       p.order_no
                    end as order_no,
                    --c.customer_no,
                    p.dealer AS customer_no,
                    case
                      When c.date_entered is null then
                       p.transaction_date
                      when c.date_entered is not null then
                       c.date_entered
                    end as date_entered,
                    '' as variation,
                    p.Sales_Promoter as EMPLOYEE_CODE,
                    P.TRANSACTION_ID as line_no,
                    TRUNC(P.cash_price, 2) AS sales_price,
                    TRUNC(P.cash_price, 2) AS sales_price_1,
                
                    TRUNC(SUM(C.COST)/COUNT(*)) AS inventory_value,
                
                    (select t.discount_type
                       from ifsapp.cust_order_line_discount t
                      where t.order_no = p.order_no
                        and t.line_no = p.line_no
                        and t.line_item_no = p.line_item_no
                        and t.rel_no = p.rel_no
                        AND ROWNUM = 1) as discount_type,
                
                    case
                      WHEN (select t.discount_type
                              from ifsapp.cust_order_line_discount t
                             where t.order_no = p.order_no
                               and t.line_no = p.line_no
                               and t.line_item_no = p.line_item_no
                               and t.rel_no = p.rel_no
                               AND ROWNUM = 1) = 'G' THEN
                       'GENERAL'
                      when (select t.discount_type
                              from ifsapp.cust_order_line_discount t
                             where t.order_no = p.order_no
                               and t.line_no = p.line_no
                               and t.line_item_no = p.line_item_no
                               and t.rel_no = p.rel_no
                               AND ROWNUM = 1) = 'S' then
                       'SPECIAL'
                    end as discount_reason,
                
                   /* trunc((select nvl(sum(t.discount_amount),
                                (t.discount * t.calculation_basis) / 100)
                       from ifsapp.cust_order_line_discount t
                      where t.order_no = p.order_no
                        and t.line_no = p.line_no
                        and t.line_item_no = p.line_item_no
                        and t.rel_no = p.rel_no
                      group by t.discount, t.calculation_basis ,t.line_no)  ,2) as discount_amount, ------discount_amount */

(
            select  
               (   
               sum(nvl(t.discount_amount, 0)) +     sum((nvl(t.calculation_basis, 0) * (nvl(t.discount, 0) / 100)))
               ) AS DISCOUNT
            from ifsapp.cust_order_line_discount t
           where  t.order_no = p.order_no
                  and t.line_no = p.line_no
                  and t.line_item_no = p.line_item_no
                  and t.rel_no = p.rel_no  
           group by  t.line_no 
            )as discount_amount,  --14 
                
                    (nvl(nvl((SELECT to_char(d.rule_no) promo_rule_number
                               FROM ifsapp.hpnret_hp_dtl d
                              WHERE d.account_no = p.order_no
                                AND d.conn_line_no = p.line_no
                                AND d.conn_rel_no = p.rel_no
                                AND d.conn_line_item_no = p.line_item_no
                                AND d.catalog_type = 'Inventory part'
                                AND ROWNUM = 1),
                             (SELECT to_char(l.free1) promo_rule_number
                                FROM ifsapp.Hpnret_Cust_Order_Line l,
                                     ifsapp.hpnret_customer_order  h
                               WHERE l.free1 IS NOT NULL
                                 AND h.order_no = l.order_no
                                 AND h.cash_conv = 'FALSE'
                                 AND l.order_no = p.order_no
                                 AND l.conn_line_no = p.line_no
                                 AND l.conn_rel_no = p.rel_no
                                 AND l.conn_line_item_no = p.line_item_no
                                 AND ROWNUM = 1)),
                         (SELECT to_char(c.discount_source_id) promo_rule_number
                            FROM ifsapp.Cust_Order_Line_Discount c
                           WHERE c.user_id = 'PROMO'
                             AND c.discount_source_id IS NOT NULL
                             AND c.order_no = p.order_no
                             AND c.line_no = p.line_no
                             AND c.rel_no = p.rel_no
                             AND c.line_item_no = p.line_item_no
                             AND ROWNUM = 1))) as PROMOTION_CODE, --------------------
                
                    p.sales_part,
                    case
                      when substr(p.order_no,
                                  instr(p.order_no, '-', -1, 1) + 1,
                                  1) = 'R' and p.units > 0.01 then
                       (p.units)
                      when substr(p.order_no,
                                  instr(p.order_no, '-', -1, 1) + 1,
                                  1) = 'R' and p.units < 0.01 then
                       (p.units)
                      when p.order_no is null and p.units < 0.01 then
                       (p.units)
                      when substr(p.order_no,
                                  instr(p.order_no, '-', -1, 1) + 1,
                                  1) = 'R' and p.units > 0 and
                           p.free_issue = 'Y' then
                       (p.units)
                      when substr(p.order_no,
                                  instr(p.order_no, '-', -1, 1) + 1,
                                  1) = 'R' and p.units < 0 and
                           p.free_issue = 'Y' then
                       (p.units)
                    when substr(p.order_no,
                                  instr(p.order_no, '-', -1, 1) + 1,
                                  1) NOT IN ( 'R','H')   then
                       (p.units)
                    END as units,-----------units
                    CASE
                      when substr(p.order_no,
                                  instr(p.order_no, '-', -1, 1) + 1,
                                  1) = 'R' and p.value > 0.01 then
                       (p.value)
                      when substr(p.order_no,
                                  instr(p.order_no, '-', -1, 1) + 1,
                                  1) = 'R' and p.value < 0.01 then
                       (p.value)
                      when p.order_no is null and p.value < 0.01 then
                       (p.value)
                      when substr(p.order_no,
                                  instr(p.order_no, '-', -1, 1) + 1,
                                  1) = 'R' and p.units > 0 and
                           p.free_issue = 'Y' then
                       (p.value)
                      when substr(p.order_no,
                                  instr(p.order_no, '-', -1, 1) + 1,
                                  1) = 'R' and p.units < 0 and
                           p.free_issue = 'Y' then
                       (p.value)
                    when substr(p.order_no,
                                  instr(p.order_no, '-', -1, 1) + 1,
                                  1) NOT IN ( 'R' ,'H')  then
                       (p.value)
                    END as value,
                    CASE
                      when substr(p.order_no,
                                  instr(p.order_no, '-', -1, 1) + 1,
                                  1) = 'R' and (p.value >= 0.01) THEN
                       '101'
                      WHEN (substr(p.order_no,
                                   instr(p.order_no, '-', -1, 1) + 1,
                                   1) = 'R') and (p.value < 0.01) THEN
                       '104'
                      WHEN p.order_no is null and p.value < 0.01 THEN
                       '104'
                      when substr(p.order_no,
                                  instr(p.order_no, '-', -1, 1) + 1,
                                  1) = 'R' and (p.units > 0) and
                           p.free_issue = 'Y' THEN
                       '101'
                      WHEN (substr(p.order_no,
                                   instr(p.order_no, '-', -1, 1) + 1,
                                   1) = 'R') and (p.units < 0) and
                           p.free_issue = 'Y' THEN
                       '104'
                    END AS TRANSACTION_SUBTYPE_CODE,
                    case
                      WHEN (substr(p.order_no,
                                   instr(p.order_no, '-', -1, 1) + 1,
                                   1) = 'R') and (p.value >= 0.01) THEN
                       '1'
                      WHEN (substr(p.order_no,
                                   instr(p.order_no, '-', -1, 1) + 1,
                                   1) = 'R') and (p.value < 0.01) THEN
                       '2'
                      WHEN p.order_no is null and p.value < 0.01 THEN
                       '2'
                      when substr(p.order_no,
                                  instr(p.order_no, '-', -1, 1) + 1,
                                  1) = 'R' and (p.units > 0) and
                           p.free_issue = 'Y' THEN
                       '1'
                      WHEN (substr(p.order_no,
                                   instr(p.order_no, '-', -1, 1) + 1,
                                   1) = 'R') and (p.units < 0) and
                           p.free_issue = 'Y' THEN
                       '2'
                    end as SALE_LINE_TYPE,--20
                    p.Sales_Location as SALES_LOCATION,
                    TRUNC(P.cash_price, 2) as ORIGINAL_PRODUCT_PRICE,
                    'N/A' as BUDGET_BOOK,
                    0 as LENGTH_OF_CONTRACT
                     ----BI
                    , p.Sales_Location  as SALES_LOCATION_NO --27
                    ,p.location_no AS LOCATION_NO--28
                    
                    
                    ,' '  AS AUTHORIZED_USER_ID --29
       ,' ' AS STATE --30
        , 0.0 as SERVICE_CHARGE --31 
        , 0.0  as FIRST_PAYMENT --32
        ,0.0 AS MONTHLY_PAYMENT --33
        ,0.0 as BUDGET_BOOK_HIRE_PRICE --34
        , IFSAPP.customer_order_api.get_price_list_no(p.order_no)  AS PRICE_LIST --35
        ,0.0 AS GROSS_HIRE_PRICE --36
        ,0.0 AS VAL_SURAKSHA_HP_LINES   --37
        ,0.0 AS val_sanasuma_hp_lines -- 38
        ,0.0 as Senasuma_Plus_Value--39
        ,0.0 as Senasuma_Plus_Mobile_Value--40
        ,0.0 as Senasuma_Post_Value--41
        ,CASE WHEN   substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1)  = 'R' THEN 'CASH' ELSE 'HIRE'  END  AS TRANSAACTION_TYPE -- 42
        
         ,'' as   SALES_VARIATION    --43
         ,P.free_issue AS FREE_ISSUE   --44
         , '' as  salesman_code -- 45
      , '' AS Vallibel_Introduced  --46 ------ASK CHATHURA ABOUT IFS FIELD
   
      ,(SELECT ST.SERIAL_NO
          FROM IFSAPP.SERIAL_TRANSACTION ST
         WHERE   p.order_no = ST.ORDER_NO 
          and p.line_no = ST.line_no
 and p.rel_no = ST.rel_no
 and p.line_item_no = ST.line_item_no 
           AND ROWNUM = 1) SERIAL_NO --47
  , ifsapp.Hpnret_Customer_Order_api.Get_Order_Category(p.order_no) as  order_category     --48    
  ,0 as   Cash_Conversion_Days    --49
, '' as Cash_Conversion_Cal_Days --50  
, NULL Authorized_Date  --51
,NULL Utilized_Date       --52    
    ,  '' AS  AUTHORIZED_NAME  --53       
,C.state   AS Line_Status     --54
, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Out_Bal(p.ORDER_NO ,1),2) as Revert_Reversed_Bal --55     
, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Amt_Paid(p.ORDER_NO ,1),2) as Total_Amount_Paid --56          
,'' AS HPNRET_GROUPING --57 
,''  as EnteredBy --58
,  '' AS EnteredName  --59      
,  ''  AS SALESMAN_TYPE  ----60
     
          ,    IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sale_Type(p.ORDER_NO)  as     SALE_TYPE   --61   
   ,  '' as   Status_586P --62             
 ----BI
                    
                    
                    
                    
                    
      from IFSAPP.INDIRECT_SALES_DTL p
      left join IFSAPP.INVENTORY_LOCATION l on l.location_no = p.location_no
                                           AND L.contract = p.contract
      left join IFSAPP.Customer_Order_Line C on c.order_no = p.order_no
                                            and c.line_no = p.line_no
                                            and c.line_item_no = p.line_item_no
                                            and c.rel_no = p.rel_no
     where p.part_type not in ('PKG')
       and p.free_issue = 'N'
       AND TRUNC(p.transaction_date) = to_date(:PreviousDate, 'YYYY/MM/DD')
       --and c.customer_no not in ('DM09001')
        and p.dealer not in ('DM09001')
       /*and (SELECT substr(i.second_commodity, 1, 2)
              FROM ifsapp.inventory_part i
             WHERE i.contract = P.contract
               AND i.part_no = P.sales_part) NOT IN ( '2P','2C','GV')
       AND P.sales_part <> 'PARTS'*/
     group by l.location_group,
              p.transaction_date,
              p.location_no,
              p.order_no,
              c.customer_no,
              c.date_entered,
              p.units,
              p.value,
              p.contract,
              P.cash_price,
              p.sales_part,
              p.free_issue,
              p.transaction_id,
              p.line_no,
              p.rel_no,
              p.line_item_no,
              P.stat_year,
              P.stat_period_no
             ,C.state
, c.order_no
,p.dealer
    )tblMain 
                 
             
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))//noneed to add @ before parameter value
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;



            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable NeenOpal_BI_Get_SalesLineFreeIssues(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region Sql
                selectQuery = @"
----SALE LINE FREE ISSUE

SELECT 
        tblMain.SITE_CODE  ,--2  --************PK
        tblMain.SALES_PART, --16
        tblMain.UNITS,--17
        tblMain.SALES_PRICE,--9
        CASE WHEN  tblMain.UNITS = 0 THEN 0 ELSE   ROUND(tblMain.DISCOUNT_AMOUNT/tblMain.UNITS,2) END AS UNIT_DISCOUNT_AMOUNT ,  --14
        CASE WHEN  tblMain.UNITS = 0 THEN 0 ELSE  ROUND(((tblMain.DISCOUNT_AMOUNT/tblMain.UNITS)/100),2) END AS  UNIT_DISCOUNT_PRENTENTAGE ,
        tblMain.VALUE,--18
        (tblMain.DISCOUNT_AMOUNT ) AS TOTAL_DISCOUNT,
        tblMain.LOCATION_NO,--28
         tblMain.ORDER_NO,--3  --************PK THIS IS CALLED SALES_TRANS_NO
         tblMain.TDATE,--1     --************PK
        SUBSTR(tblMain.EMPLOYEE_CODE,1,12) AS PROMOTER_CODE ,--17
         tblMain.AUTHORIZED_USER_ID ,--29
         tblMain.STATE ,--30
       tblMain.CUSTOMER_NO,--4
       tblMain.LENGTH_OF_CONTRACT   ,--24
         ' ' AS ADVANCE_PAYMENT_NO ,  
         0  AS ADVANCE_PAYMENT_AMOUNT ,  
       tblMain.SALE_TYPE , --61    --************PK
       tblMain.SALES_LOCATION_NO ,--27
        tblMain.SERVICE_CHARGE , --31
         tblMain.FIRST_PAYMENT ,  --32
        tblMain.BUDGET_BOOK_HIRE_PRICE ,  --34
        tblMain.MONTHLY_PAYMENT  ,--33
         tblMain.BUDGET_BOOK  , --23
         tblMain.PRICE_LIST, --35
          tblMain.GROSS_HIRE_PRICE  --36
          ,tblMain.VAL_SURAKSHA_HP_LINES as SURAKSHA_VALUE--37
          ,tblMain.val_sanasuma_hp_lines AS SANASUMA_VALUE--38
          ,tblMain.Senasuma_Plus_Value--39
          ,tblMain.Senasuma_Plus_Mobile_Value--40
          ,tblMain.Senasuma_Post_Value--41
          ,tblMain.TRANSAACTION_TYPE --42
          ,tblMain.SALES_VARIATION  --43
          ,tblMain.FREE_ISSUE -- 44
          ,tblMain.salesman_code -- 45
          ,tblMain.Vallibel_Introduced  --46
          ,tblMain.SERIAL_NO --47
          ,tblMain.order_category --48
        
    --FOR PRIMARY KEY
    
    ,1 AS TILL_NO --************PK  THIS IS HARD CODE FOR EVERY SQL
    ,tblMain.SALE_LINE_TYPE --20 --************PK
    ,tblMain.LINE_NO AS SALE_LINE_NO --************PK

    ,NVL(ifsapp.Sin_FN_DayWiseTotal_Singer_Tax(SUBSTR(tblMain.SITE_CODE,
                                                        1,
                                                        5),
                                                 tblMain.SALES_PART,
                                                 SUBSTR(tblMain.ORDER_NO,
                                                        5,
                                                        1),
                                                 NVL(tblMain.VALUE, 0)),
           0)  AS sales_tax_value--49
    ,tblMain.INVENTORY_VALUE   --50
    ,CASE WHEN  SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'NM' THEN 'NORMAL'
       WHEN SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'SP' THEN 'SPECIAL'
       WHEN SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'GR' THEN 'GROUP'
        ELSE ''  END 
      AS SALES_GROUPING    --51
      ,  ' '  AS RET_REASON_CODE   --52
       , tblMain.Status_586P   --62
, tblMain.discount_type --12
, tblMain.discount_reason --12
,tblMain.TaxCallType as SalesFileType
,tblMain.PROMOTION_CODE--15
,NVL(tblMain.Cash_Conversion_Days,0)  AS Cash_Conversion_Days-- 49
,tblMain.Cash_Conversion_Cal_Days -- 50
, tblMain.Authorized_Date --51
,tblMain.Utilized_Date     --52
,tblMain.AUTHORIZED_NAME  --53       
,tblMain.Line_Status  --54
,tblMain.Revert_Reversed_Bal --55
,tblMain.Total_Amount_Paid -- 56
,tblMain.HPNRET_GROUPING   --57
 ,tblMain.EnteredBy --58
 , tblMain.EnteredName  --59
 , tblMain.SALESMAN_TYPE  --60
       
  from (
  select distinct TO_CHAR(s.transaction_date, 'YYYY-MM-DD') AS TDATE, --1
                        s.shop_code as site_code, --2
                        s.order_no, --3
                        c.customer_no, --4
                        c.date_entered, --5
                        s.variation, --6
                        case
                          WHEN substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'H' then
                           (hhh.sales_promoter)
                          when substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'R' THEN
                           (hco.sales_promoter)
                        end as EMPLOYEE_CODE, --7
                        S.TRANSACTION_ID as line_no, --8
                        TRUNC(s.cash_price, 2) as sales_price, --9
                        TRUNC(s.cash_price, 2) as sales_price_1, --10
                        
                        TRUNC(SUM(C.COST) / COUNT(*)) as inventory_value, --11
                        
                        t.discount_type, --12
                        case
                          WHEN t.discount_type = 'G' THEN
                           'GENERAL'
                          when t.discount_type = 'S' THEN
                           'SPECIAL'
                          else
                           ''
                        end as discount_reason, --13
                        
                        ((select TRUNC(((sum(s.cash_units + s.hire_units)) /
                                       count(*) *
                                       (nvl(sum(t.discount_amount),
                                             (sum(t.discount *
                                                  t.calculation_basis) / 100)))),
                                       2)
                            from ifsapp.cust_order_line_discount t
                           where t.order_no = s.order_no
                             and t.line_no = s.line_no
                             and t.rel_no = s.rel_no
                             and t.line_item_no = s.line_item_no
                             AND s.STATE NOT IN
                                 ('RevertReversed', 'Reverted'))) as discount_amount, --14
                        
                        (nvl(nvl((SELECT to_char(d.rule_no) promo_rule_number
                                   FROM ifsapp.hpnret_hp_dtl d
                                  WHERE d.account_no = s.order_no
                                    AND d.conn_line_no = s.line_no
                                    AND d.conn_rel_no = s.rel_no
                                    AND d.conn_line_item_no = s.line_item_no
                                    AND d.catalog_type = 'Inventory part'
                                    AND ROWNUM = 1),
                                 (SELECT to_char(l.free1) promo_rule_number
                                    FROM ifsapp.Hpnret_Cust_Order_Line l,
                                         ifsapp.hpnret_customer_order  h
                                   WHERE l.free1 IS NOT NULL
                                     AND h.order_no = l.order_no
                                     AND h.cash_conv = 'FALSE'
                                     AND l.order_no = s.order_no
                                     AND l.conn_line_no = s.line_no
                                     AND l.conn_rel_no = s.rel_no
                                     AND l.conn_line_item_no = s.line_item_no
                                     AND ROWNUM = 1)),
                             (SELECT to_char(c.discount_source_id) promo_rule_number
                                FROM ifsapp.Cust_Order_Line_Discount c
                               WHERE c.user_id = 'PROMO'
                                 AND c.discount_source_id IS NOT NULL
                                 AND c.order_no = s.order_no
                                 AND c.line_no = s.line_no
                                 AND c.rel_no = s.rel_no
                                 AND c.line_item_no = s.line_item_no
                                 AND ROWNUM = 1))) AS PROMOTION_CODE, --15
                        
                        s.sales_part, --16
                        case
                          WHEN substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'H' and s.hire_units > 0 and
                               (s.state = 'SALE') THEN
                           (s.hire_units)
                          WHEN substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'H' and s.hire_units < 0 and
                               (s.state = 'SALE') THEN
                           (s.hire_units)
                          when substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'R' and s.cash_units > 0 and
                               (s.state = 'SALE') then
                           (s.cash_units)
                           when substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'R' and s.cash_units < 0 and
                               (s.state = 'SALE') then
                           (s.cash_units)
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and
                               (s.state = 'CashConverted') and
                               (s.hire_units) < 0 then
                           (s.hire_units)
                          when (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'R') and
                               (s.variation = 'CashCon') and s.cash_units > 0 then
                           (s.cash_units)
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and
                               (s.variation = 'ExchgOut') and
                               (s.hire_units) > 0 then
                           (s.hire_units)
                          when (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'R') and
                               (s.variation = 'ExchgOut') and
                               (s.cash_units) > 0 then
                           (s.cash_units)
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and s.reverts_units > 0 and
                               s.state = 'Reverted' then
                           (s.reverts_units * -1)
                          when (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and
                               s.revert_reverse_units > 0 and
                               s.state = 'RevertReversed' then
                           (s.revert_reverse_units)
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and s.hire_units < 0 and
                               s.state = 'Returned' then
                           (s.hire_units)
                          when (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'R') and s.cash_units < 0 and
                               s.state = 'Returned' then
                           (s.cash_units)
                          WHEN substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'H' and s.hire_units < 0 and
                               (s.state = 'ExchangedIn') THEN
                           (s.hire_units)
                          WHEN substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'R' and s.cash_units < 0 and
                               (s.state = 'ExchangedIn') THEN
                           (s.cash_units)
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and (s.state = 'Invoiced') and
                               (s.hire_units) < 0 then
                           (s.hire_units)
                            WHEN substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1) = 'H' and s.hire_units < 0 and
                       (s.state = 'Closed') THEN
                   (s.hire_units)
                        END as units, --17
                        
                            Trunc(CASE
                        WHEN substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'H' and s.net_hire_cash_value > 0 and
                             (s.state = 'SALE') THEN
                         (s.net_hire_cash_value)
                        WHEN substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'H' and s.net_hire_cash_value < 0 and
                             (s.state = 'SALE') THEN
                         (s.net_hire_cash_value)
                        when substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'R' and s.cash_value > 0 and
                             (s.state = 'SALE') then
                         (s.cash_value)
                        when substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'R' and s.cash_value < 0 and
                             (s.state = 'SALE') then
                         (s.cash_value)
                        WHEN (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'H') and (s.state = 'CashConverted') and
                             (s.net_hire_cash_value) < 0 then
                         (s.net_hire_cash_value)
                        when (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'R') and (s.variation = 'CashCon') and
                             s.cash_value > 0 then
                         s.cash_value
                        WHEN (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'H') and (s.variation = 'ExchgOut') and
                             (s.net_hire_cash_value) > 0 then
                         s.net_hire_cash_value
                        when (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'R') and (s.variation = 'ExchgOut') and
                             (s.cash_value) > 0 then
                         s.cash_value
                        WHEN (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'H') and s.reverts_value > 0 and
                             s.state = 'Reverted' then
                         (s.reverts_value * -1)
                        when (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'H') and s.revert_reverse_value > 0 and
                             s.state = 'RevertReversed' then
                         s.revert_reverse_value
                        WHEN (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'H') and s.net_hire_cash_value < 0 and
                             s.state = 'Returned' then
                         s.net_hire_cash_value
                        when (substr(s.order_no,
                                     instr(s.order_no, '-', -1, 1) + 1,
                                     1) = 'R') and s.cash_value < 0 and
                             s.state = 'Returned' then
                         s.cash_value
                        WHEN substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'H' and s.net_hire_cash_value < 0 and
                             (s.state = 'ExchangedIn') THEN
                         (s.net_hire_cash_value)
                        WHEN substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'R' and s.cash_value < 0 and
                             (s.state = 'ExchangedIn') THEN
                         (s.cash_value)
                        WHEN substr(s.order_no,
                                    instr(s.order_no, '-', -1, 1) + 1,
                                    1) = 'H' and s.net_hire_cash_value < 0 and
                             (s.state = 'Closed') THEN
                         (s.net_hire_cash_value)

                      END,
                      2) as value, --18
                        
                        CASE
                          WHEN substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'H' and s.hire_units > 0 and
                               (s.state = 'SALE') THEN
                           '201'
                          when substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'R' and s.cash_units > 0 and
                               (s.state = 'SALE') THEN
                           '101'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and
                               (s.state = 'CashConverted') and
                               (s.hire_units) < 0 THEN
                           '205'
                          when (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'R') and
                               (s.variation = 'CashCon') and s.cash_units > 0 then
                           '105'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and
                               (s.variation = 'ExchgOut') and
                               (s.hire_units) > 0 THEN
                           '203'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'R') and
                               (s.variation = 'ExchgOut') and
                               (s.cash_units) > 0 THEN
                           '103'
                          when (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and (s.reverts_units > 0) and
                               s.state = 'Reverted' THEN
                           '206'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and
                               (s.revert_reverse_units > 0) and
                               s.state = 'RevertReversed' THEN
                           '207'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and s.hire_units < 0 and
                               s.state = 'Returned' THEN
                           '204'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'R') and s.cash_units < 0 and
                               s.state = 'Returned' THEN
                           '104'
                          WHEN substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'H' and s.hire_units < 0 and
                               (s.state = 'ExchangedIn') then
                           '202'
                          WHEN substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'R' and s.cash_units < 0 and
                               (s.state = 'ExchangedIn') then
                           '102'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and (s.state = 'Invoiced') and
                               (s.hire_units) < 0 THEN
                           '205'
                        END AS TRANSACTION_SUBTYPE_CODE, --19
                        
                        case
                          WHEN substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'H' and s.hire_units > 0 and
                               (s.state = 'SALE') THEN
                           '1'
                          when substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'R' and s.cash_units > 0 and
                               (s.state = 'SALE') THEN
                           '1'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and
                               (s.state = 'CashConverted') and
                               (s.hire_units) < 0 THEN
                           '2'
                          when (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'R') and
                               (s.variation = 'CashCon') and s.cash_units > 0 then
                           '1'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and
                               (s.variation = 'ExchgOut') and
                               (s.hire_units) > 0 THEN
                           '1'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'R') and
                               (s.variation = 'ExchgOut') and
                               (s.cash_units) > 0 THEN
                           '1'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and (s.reverts_units > 0) and
                               s.state = 'Reverted' THEN
                           '2'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and
                               (s.revert_reverse_units > 0) and
                               s.state = 'RevertReversed' THEN
                           '1'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and s.hire_units < 0 and
                               s.state = 'Returned' THEN
                           '2'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'R') and s.cash_units < 0 and
                               s.state = 'Returned' THEN
                           '2'
                          WHEN substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'H' and s.hire_units < 0 and
                               (s.state = 'ExchangedIn') then
                           '2'
                          WHEN substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'R' and s.cash_units < 0 and
                               (s.state = 'ExchangedIn') then
                           '2'
                          WHEN (substr(s.order_no,
                                       instr(s.order_no, '-', -1, 1) + 1,
                                       1) = 'H') and (s.state = 'Invoiced') and
                               (s.hire_units) < 0 THEN
                           '2'
                        end as SALE_LINE_TYPE, --20
                        
                        case
                          WHEN substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'H' then
                           (hhh.sales_location_no)
                          when substr(s.order_no,
                                      instr(s.order_no, '-', -1, 1) + 1,
                                      1) = 'R' THEN
                           (hco.sales_location_no)
                        end as SALES_LOCATION, --21
                        
                        TRUNC(s.cash_price, 2) as ORIGINAL_PRODUCT_PRICE, --22
                        hhh.budget_book_id AS BUDGET_BOOK, --23
                        hhh.length_of_contract as LENGTH_OF_CONTRACT, --24
                        'D' AS TaxCallType --25
                        
                        -----BI
                            ,' ' as RET_REASON_CODE--26
             ,   case
         WHEN substr(s.order_no, instr(s.order_no, '-', -1, 1) + 1, 1) = 'H' then
          IFSAPP.Hpnret_Hp_Head_Api.Get_Sales_Location_No(s.order_no, '1')
         WHEN substr(s.order_no, instr(s.order_no, '-', -1, 1) + 1, 1) = 'R' THEN
          IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sales_Location_No(s.order_no)
       end as SALES_LOCATION_NO    --27
       ,s.LOCATION_NO AS LOCATION_NO --28
       ,t.authorized_user AS AUTHORIZED_USER_ID --29
       ,S.STATE --30
       , trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Serv_Chg(S.ORDER_NO ,1),2) AS SERVICE_CHARGE--31
       , trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_First_Payment(S.ORDER_NO ,1),2) as FIRST_PAYMENT --32
       ,trunc(IFSAPP.hpnret_hp_head_api.get_total_monthly_payment(S.ORDER_NO ,1),2) as MONTHLY_PAYMENT --33
       ,trunc(IFSAPP.hpnret_hp_head_api.get_total_bb_hire_price(S.ORDER_NO ,1),2) as BUDGET_BOOK_HIRE_PRICE --34
       , IFSAPP.customer_order_api.get_price_list_no(S.ORDER_NO)  AS PRICE_LIST --35
       ,trunc(IFSAPP.hpnret_hp_head_api.get_gross_hire_value(S.ORDER_NO,1),2) AS GROSS_HIRE_PRICE --36
       ,s.VAL_SURAKSHA_HP_LINES --37
       ,s.val_sanasuma_hp_lines --38
       ,   (     
       select coc.charge_amount
  from ifsapp.customer_order_charge coc
 where
 s.order_no = coc.order_no
 and s.line_no = coc.line_no
 and s.rel_no = coc.rel_no
 and s.line_item_no = coc.line_item_no
 and coc.charge_type in ('EXT_PLUS')
       ) as Senasuma_Plus_Value  --39

       ,   (     
       select coc.charge_amount
  from ifsapp.customer_order_charge coc
 where
 s.order_no = coc.order_no
 and s.line_no = coc.line_no
 and s.rel_no = coc.rel_no
 and s.line_item_no = coc.line_item_no
 and coc.charge_type in ('EXT_PLUS-1')
       ) as Senasuma_Plus_Mobile_Value --40
       
       
        ,  CASE WHEN  (substr(s.order_no,
                               instr(s.order_no, '-', -1, 1) + 1,
                               1) = 'R')  THEN    (     
       select sum(HAP.DOM_AMOUNT) as DOM_AMOUNT
  from ifsapp.HPNRET_ADV_PAY HAP
 where
 s.order_no = HAP.order_no
 and s.line_no = HAP.line_no
 and s.rel_no = HAP.rel_no
 and s.line_item_no = HAP.line_item_no 
 AND  HAP.ACCOUNT_CODE = 'EXTPO'
       ) 
       
       ELSE (     
       select sum(HAP.DOM_AMOUNT) as DOM_AMOUNT
  from ifsapp.HPNRET_ADV_PAY HAP
 where
 s.order_no = HAP.original_account
 and s.line_no = HAP.line_no
 and s.rel_no = HAP.rel_no
 and s.line_item_no = HAP.line_item_no 
 AND  HAP.ACCOUNT_CODE = 'EXTPO'
       )  END 
       
       
       as Senasuma_Post_Value   --41
, CASE WHEN   substr(s.order_no,
                              instr(s.order_no, '-', -1, 1) + 1,
                              1)  = 'R' THEN 'CASH' ELSE 'HIRE'  END  AS TRANSAACTION_TYPE--42
  ,s.variation    as SALES_VARIATION  --43                       
   ,S.FREE_ITEM AS FREE_ISSUE    --44
   ,s.salesman_code -- 45
   , (select case when  count(*) > 0 then 'YES' ELSE 'NO' END AS Vallibel_Introduced
 from ifsapp.sin_v_one_product_tab valibe 
   where valibe.product_code = s.part_no  ) AS Vallibel_Introduced  --46
  
          
   ,(SELECT ST.SERIAL_NO
          FROM IFSAPP.SERIAL_TRANSACTION ST
         WHERE   s.order_no = ST.ORDER_NO 
          and s.line_no = ST.line_no
 and s.rel_no = ST.rel_no
 and s.line_item_no = ST.line_item_no 
           AND ROWNUM = 1) SERIAL_NO --47
           
           ,hco.order_category   --48

, trunc(hhh.closed_date )  -   TRUNC(hhh.original_sales_date) as Cash_Conversion_Days --49

,  (SELECT   au.remarks
 from ifsapp.hpnret_auth_variation au
 WHERE s.order_no = au.account_no
  AND ROWNUM = 1
 )   as Cash_Conversion_Cal_Days --50


,  
 (SELECT  (au.from_date) AS Authorized_Date
 from ifsapp.hpnret_auth_variation au
 WHERE s.order_no = au.account_no
 AND ROWNUM = 1
 ) Authorized_Date  --51
                
 ,  
 (SELECT  TRUNC(AU.Utilized_Date) AS Utilized_Date
 from ifsapp.hpnret_auth_variation au
 WHERE s.order_no = au.account_no
  AND ROWNUM = 1
 ) Utilized_Date  --52  
   ,  
 (SELECT  UTAB.DESCRIPTION AS AUTHORIZED_NAME
 from ifsapp.fnd_user_tab UTAB
 WHERE UTAB.IDENTITY = T.authorized_user
 ) AUTHORIZED_NAME  --53   
 
 ,C.state AS Line_Status --54
, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Out_Bal(S.ORDER_NO ,1),2) as Revert_Reversed_Bal --55
, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Amt_Paid(S.ORDER_NO ,1),2) as Total_Amount_Paid --56 
,  ifsapp.hpnret_pro_fam_group_api.get_hpnret_groupings(0,
                                                            'EXT WARRANTY',
                                                            IFSAPP.inventory_part_api.Get_Part_Product_Family(C.contract,
                                                                                                              C.part_no)) AS HPNRET_GROUPING -- 57
 ,t.user_id  as EnteredBy --58
  ,  
 (SELECT  UTAB.DESCRIPTION AS AUTHORIZED_NAME
 from ifsapp.fnd_user_tab UTAB
 WHERE UTAB.IDENTITY = T.user_id
 ) EnteredName  --59
 , (
 SELECT ifsapp.HPNRET_LEVEL_API.Get_Hpnret_Level_Type(HL.level_id) AS SALESMAN_TYPE
  FROM IFSAPP.HPNRET_LEVEL_HIERARCHY HL WHERE HL.site_id = S.SHOP_CODE
  )    SALESMAN_TYPE  ----60
 ,    IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sale_Type(S.ORDER_NO)  as     SALE_TYPE   --61              
 
, (SELECT HR_ADV.account_code  
          FROM ifsapp.hpnret_ADV_PAY  HR_ADV where HR_ADV.prod_rel_order_no = c.order_no  AND HR_ADV.objstate = 'Approved') as   Status_586P --62 

----BI
          FROM IFSAPP.direct_sales_dtl_tab S
          left join IFSAPP.INVENTORY_LOCATION l on l.location_no =
                                                   s.location_no
                                               AND L.contract = S.SHOP_CODE
          left join ifsapp.cust_order_line_discount t on s.order_no =
                                                         t.order_no
                                                     and s.line_no =
                                                         t.line_no
                                                     and s.line_item_no =
                                                         t.line_item_no
                                                     and s.rel_no = t.rel_no
          left join IFSAPP.Customer_Order_Line C on c.order_no = s.order_no
                                                and c.line_no = s.line_no
                                                and c.line_item_no =
                                                    s.line_item_no
                                                and c.rel_no = s.rel_no
          left join IFSAPP.Hpnret_Hp_Head hhh on s.order_no = hhh.account_no
          left join ifsapp.Hpnret_Customer_Order hco on s.order_no =
                                                        hco.order_no
         where s.catalog_type not in ('PKG')
           AND TRUNC(s.transaction_date) = to_date(:PreviousDate, 'YYYY/MM/DD')  
           /*and (SELECT substr(i.second_commodity, 1, 2)
                  FROM ifsapp.inventory_part i
                 WHERE i.contract = s.shop_code
                   AND i.part_no = s.sales_part) NOT IN ('2P', '2C', 'GV')
           AND S.SALES_PART <> 'PARTS'*/
           and s.free_item = 'Y'
         group by l.location_group,
                  (s.transaction_date),
                  s.shop_code,
                  s.order_no,
                  c.customer_no,
                  c.date_entered,
                  s.hire_units,
                  s.reverts_units,
                  s.revert_reverse_units,
                  s.line_no,
                  s.state,
                  s.rel_no,
                  s.sales_part,
                  S.TRANSACTION_ID,
                  s.line_item_no,
                  s.cash_units,
                  s.variation,
                  s.reverts_value,
                  s.revert_reverse_value,
                  s.net_hire_cash_value,
                  s.cash_value,
                  S.cash_price,
                  t.discount_type,
                  s.location_no,
                  hhh.sales_promoter,
                  hco.sales_promoter,
                  hhh.sales_location_no,
                  hco.sales_location_no,
                  hhh.budget_book_id,
                  hhh.length_of_contract,
                  s.special_discount_value,
                  s.promotional_discount_value,
                  S.YEAR,
                  S.PERIOD,
                  S.PART_NO,
                  hhh.contract,
          hco.contract,t.authorized_user
         ,s.VAL_SURAKSHA_HP_LINES  
       ,s.val_sanasuma_hp_lines  
       ,S.FREE_ITEM 
       ,s.salesman_code
       ,hco.order_category
,    trunc(hhh.original_sales_date)    , trunc(hhh.closed_date ) 
,C.state
,C.contract,C.part_no
,T.user_id
,c.order_no
        
        union
        
        select distinct TO_CHAR(p.transaction_date, 'YYYY-MM-DD') AS TDATE, --1
                        --p.contract  as site_code, --2  --COMMENTED ON 2019/09/27
/*CASE WHEN c.customer_no IS NOT NULL THEN c.customer_no ELSE case
                  WHEN p.order_no is null THEN
                   to_char(p.transaction_id)
                  when p.order_no is not null then
                   p.order_no
                end END  as    site_code,--2  */  --commneted on 21st oct 2019
 p.dealer as site_code,--2   --added on 21st oct 2019
                        case
                          WHEN p.order_no is null THEN
                           to_char(p.transaction_id)
                          when p.order_no is not null then
                           p.order_no
                        end as order_no, --3
                       -- c.customer_no, --4
                p.dealer as customer_no, --4 --added on 21oct 2019
                        case
                          When c.date_entered is null then
                           p.transaction_date
                          when c.date_entered is not null then
                           c.date_entered
                        end as date_entered, --5
                        '' as variation, --6
                        p.Sales_Promoter as EMPLOYEE_CODE, --7
                        P.TRANSACTION_ID as line_no, --8
                        TRUNC(P.cash_price, 2) AS sales_price, --9
                        TRUNC(P.cash_price, 2) AS sales_price_1, --10
                        
                        TRUNC(SUM(C.COST) / COUNT(*)) AS inventory_value, --11
                        
                        (select t.discount_type
                           from ifsapp.cust_order_line_discount t
                          where t.order_no = p.order_no
                            and t.line_no = p.line_no
                            and t.line_item_no = p.line_item_no
                            and t.rel_no = p.rel_no
                            AND ROWNUM = 1) as discount_type, --12
                        
                        case
                          WHEN (select t.discount_type
                                  from ifsapp.cust_order_line_discount t
                                 where t.order_no = p.order_no
                                   and t.line_no = p.line_no
                                   and t.line_item_no = p.line_item_no
                                   and t.rel_no = p.rel_no
                                   AND ROWNUM = 1) = 'G' THEN
                           'GENERAL'
                          when (select t.discount_type
                                  from ifsapp.cust_order_line_discount t
                                 where t.order_no = p.order_no
                                   and t.line_no = p.line_no
                                   and t.line_item_no = p.line_item_no
                                   and t.rel_no = p.rel_no
                                   AND ROWNUM = 1) = 'S' then
                           'SPECIAL'
                        end as discount_reason, --13
                        
                     /*   (select nvl(sum(t.discount_amount),
                                    (t.discount * t.calculation_basis) / 100)
                           from ifsapp.cust_order_line_discount t
                          where t.order_no = p.order_no
                            and t.line_no = p.line_no
                            and t.line_item_no = p.line_item_no
                            and t.rel_no = p.rel_no
                          group by t.discount, t.calculation_basis ,t.line_no) as discount_amount, --14  */

(
            select  
               (   
               sum(nvl(t.discount_amount, 0)) +     sum((nvl(t.calculation_basis, 0) * (nvl(t.discount, 0) / 100)))
               ) AS DISCOUNT
            from ifsapp.cust_order_line_discount t
           where  t.order_no = p.order_no
                  and t.line_no = p.line_no
                  and t.line_item_no = p.line_item_no
                  and t.rel_no = p.rel_no  
           group by  t.line_no 
            )as discount_amount,  --14 
                        
                        (nvl(nvl((SELECT to_char(d.rule_no) promo_rule_number
                                   FROM ifsapp.hpnret_hp_dtl d
                                  WHERE d.account_no = p.order_no
                                    AND d.conn_line_no = p.line_no
                                    AND d.conn_rel_no = p.rel_no
                                    AND d.conn_line_item_no = p.line_item_no
                                    AND d.catalog_type = 'Inventory part'
                                    AND ROWNUM = 1),
                                 (SELECT to_char(l.free1) promo_rule_number
                                    FROM ifsapp.Hpnret_Cust_Order_Line l,
                                         ifsapp.hpnret_customer_order  h
                                   WHERE l.free1 IS NOT NULL
                                     AND h.order_no = l.order_no
                                     AND h.cash_conv = 'FALSE'
                                     AND l.order_no = p.order_no
                                     AND l.conn_line_no = p.line_no
                                     AND l.conn_rel_no = p.rel_no
                                     AND l.conn_line_item_no = p.line_item_no
                                     AND ROWNUM = 1)),
                             (SELECT to_char(c.discount_source_id) promo_rule_number
                                FROM ifsapp.Cust_Order_Line_Discount c
                               WHERE c.user_id = 'PROMO'
                                 AND c.discount_source_id IS NOT NULL
                                 AND c.order_no = p.order_no
                                 AND c.line_no = p.line_no
                                 AND c.rel_no = p.rel_no
                                 AND c.line_item_no = p.line_item_no
                                 AND ROWNUM = 1))) as PROMOTION_CODE, --15
                        
                        p.sales_part, --16
                        case
                          when substr(p.order_no,
                                      instr(p.order_no, '-', -1, 1) + 1,
                                      1) = 'R' and p.units > 0.01 then
                           (p.units)
                          when substr(p.order_no,
                                      instr(p.order_no, '-', -1, 1) + 1,
                                      1) = 'R' and p.units < 0.01 then
                           (p.units)
                          when p.order_no is null and p.units < 0.01 then
                           (p.units)
                          when substr(p.order_no,
                                      instr(p.order_no, '-', -1, 1) + 1,
                                      1) = 'R' and p.units > 0 and
                               p.free_issue = 'Y' then
                           (p.units)
                          when substr(p.order_no,
                                      instr(p.order_no, '-', -1, 1) + 1,
                                      1) = 'R' and p.units < 0 and
                               p.free_issue = 'Y' then
                           (p.units)
when substr(p.order_no,
                                      instr(p.order_no, '-', -1, 1) + 1,
                                      1) NOT IN ( 'R' ,'H')  then
                           (p.units)
                        END as units, --17
                        
                        CASE
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and p.value > 0.01 then
                   (p.value)
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and p.value < 0.01 then
                   (p.value)
                  when p.order_no is null and p.value < 0.01 then
                   (p.value)
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and p.units > 0 and
                       p.free_issue = 'Y' then
                   (p.value)
                  when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) = 'R' and p.units < 0 and
                       p.free_issue = 'Y' then
                   (p.value)
                when substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1) NOT IN ( 'R' ,'H')  then
                   (p.value)
                END as value, --18
                        
                        CASE
                          when substr(p.order_no,
                                      instr(p.order_no, '-', -1, 1) + 1,
                                      1) = 'R' and (p.units > 0) THEN
                           '101'
                          WHEN (substr(p.order_no,
                                       instr(p.order_no, '-', -1, 1) + 1,
                                       1) = 'R') and (p.units < 0) THEN
                           '104'
                          WHEN p.order_no is null and p.units < 0 THEN
                           '104'
                        END AS TRANSACTION_SUBTYPE_CODE, --19
                        
                        case
                          WHEN (substr(p.order_no,
                                       instr(p.order_no, '-', -1, 1) + 1,
                                       1) = 'R') and (p.units > 0) THEN
                           '1'
                          WHEN (substr(p.order_no,
                                       instr(p.order_no, '-', -1, 1) + 1,
                                       1) = 'R') and (p.units < 0) THEN
                           '2'
                          WHEN p.order_no is null and p.units < 0 THEN
                           '2'
                        end as SALE_LINE_TYPE, --20
                        
                        p.Sales_Location as SALES_LOCATION, --21
                        TRUNC(P.cash_price, 2) as ORIGINAL_PRODUCT_PRICE, --22
                        'N/A' as BUDGET_BOOK, --23
                        0 as LENGTH_OF_CONTRACT, --24
                        'I' AS TaxCallType --25
                        
                       -----BI
                        ,NVL ( (
                SELECT  DISTINCT SUBSTR(L.return_reason_code,1,20) 
                    FROM  ifsapp.Return_Material_Line L 
                    WHERE p.ORDER_NO = L.order_no 
                    AND p.line_no = L.line_no
                    AND p.rel_no = L.rel_no
                    AND p.line_item_no = L.line_item_no
                    AND TO_CHAR(p.transaction_date, 'YYYY-MM-DD') = TO_CHAR(L.date_returned,'YYYY-MM-DD')
                    AND ROWNUM = 1

                ), ' ' ) AS RET_REASON_CODE --26
               , case
         WHEN substr(p.order_no, instr(p.order_no, '-', -1, 1) + 1, 1) = 'H' then
          IFSAPP.Hpnret_Hp_Head_Api.Get_Sales_Location_No(p.order_no, '1')
         WHEN substr(p.order_no, instr(p.order_no, '-', -1, 1) + 1, 1) = 'R' THEN
          IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sales_Location_No(p.order_no)
       end as SALES_LOCATION_NO --27
       ,p.location_no AS LOCATION_NO--28
       ,' '  AS AUTHORIZED_USER_ID --29
       ,' ' AS STATE --30
        , 0.0 as SERVICE_CHARGE --31 
        , 0.0  as FIRST_PAYMENT --32
        ,0.0 AS MONTHLY_PAYMENT --33
        ,0.0 as BUDGET_BOOK_HIRE_PRICE --34
        ,IFSAPP.customer_order_api.get_price_list_no(p.order_no)  AS PRICE_LIST --35
        ,0.0 AS GROSS_HIRE_PRICE --36
        ,0.0 AS VAL_SURAKSHA_HP_LINES   --37
        ,0.0 AS val_sanasuma_hp_lines -- 38
        ,0.0 as Senasuma_Plus_Value--39
        ,0.0 as Senasuma_Plus_Mobile_Value--40
        ,0.0 as Senasuma_Post_Value--41
        ,CASE WHEN   substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1)  = 'R' THEN 'CASH' ELSE 'HIRE'  END  AS TRANSAACTION_TYPE -- 42
        
         ,'' as   SALES_VARIATION    --43
         ,P.free_issue AS FREE_ISSUE   --44
         , '' as  salesman_code -- 45
            , (select case when  count(*) > 0 then 'YES' ELSE 'NO' END AS Vallibel_Introduced
 from ifsapp.sin_v_one_product_tab valibe 
   where valibe.product_code = P.sales_part  ) AS Vallibel_Introduced  --46
   

              ,(SELECT ST.SERIAL_NO
          FROM IFSAPP.SERIAL_TRANSACTION ST
         WHERE   p.order_no = ST.ORDER_NO 
          and p.line_no = ST.line_no
 and p.rel_no = ST.rel_no
 and p.line_item_no = ST.line_item_no 
           AND ROWNUM = 1) SERIAL_NO --47
  , ifsapp.Hpnret_Customer_Order_api.Get_Order_Category(p.order_no) as  order_category     --48  
  ,0 as   Cash_Conversion_Days    --49
  , '' as Cash_Conversion_Cal_Days --50  
, NULL Authorized_Date  --51
,NULL Utilized_Date       --52  
 ,  '' AS  AUTHORIZED_NAME  --53       
,C.state   AS Line_Status     --54
, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Out_Bal(p.ORDER_NO ,1),2) as Revert_Reversed_Bal --55     
, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Amt_Paid(p.ORDER_NO ,1),2) as Total_Amount_Paid --56        
,  ifsapp.hpnret_pro_fam_group_api.get_hpnret_groupings(0,
                                                            'EXT WARRANTY',
                                                            IFSAPP.inventory_part_api.Get_Part_Product_Family(C.contract,
                                                                                                              C.part_no)) AS HPNRET_GROUPING -- 57  

  ,''  as EnteredBy --58
  ,  '' AS EnteredName  --59      
   
    , (
 SELECT ifsapp.HPNRET_LEVEL_API.Get_Hpnret_Level_Type(HLHH.level_id) AS SALESMAN_TYPE
  FROM IFSAPP.HPNRET_LEVEL_HIERARCHY HLHH WHERE HLHH.site_id = P.contract
  )  AS SALESMAN_TYPE  ----60   
 ,    IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sale_Type(p.ORDER_NO)  as     SALE_TYPE   --61                        
, (SELECT HR_ADV.account_code  
          FROM ifsapp.hpnret_ADV_PAY  HR_ADV where HR_ADV.prod_rel_order_no = c.order_no  AND HR_ADV.objstate = 'Approved') as   Status_586P --62 

----BI     
                        
          from IFSAPP.INDIRECT_SALES_DTL p
          left join IFSAPP.INVENTORY_LOCATION l on l.location_no =
                                                   p.location_no
                                               AND L.contract = p.contract
          left join IFSAPP.Customer_Order_Line C on c.order_no = p.order_no
                                                and c.line_no = p.line_no
                                                and c.line_item_no =
                                                    p.line_item_no
                                                and c.rel_no = p.rel_no
         where p.part_type not in ('PKG')
           and TRUNC(p.transaction_date) = to_date(:PreviousDate, 'YYYY/MM/DD')  
           /*and (SELECT substr(i.second_commodity, 1, 2)
                  FROM ifsapp.inventory_part i
                 WHERE i.contract = P.contract
                   AND i.part_no = P.sales_part) NOT IN ('2P', '2C', 'GV')
           AND P.sales_part <> 'PARTS'*/
           and p.free_issue = 'Y'
           AND p.DEALER <> 'DM09999'
        --and p.order_no = 'WAA-R927049'
        --and p.order_no in (select sto.order_no from ifsapp.sin_temp_order sto)
         group by l.location_group,
                  p.transaction_date,
                  p.location_no,
                  p.order_no,
                  c.customer_no,
                  c.date_entered,
                  p.units,
                  p.value,
                  p.contract,
                  P.cash_price,
                  p.sales_part,
                  p.free_issue,
                  p.transaction_id,
                  p.line_no,
                  p.rel_no,
                  p.line_item_no,
                  P.stat_year,
                  P.stat_period_no
                  ,P.free_issue
,C.state
,C.contract,C.part_no
,c.order_no
,p.dealer
                  ) tblMain
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))//noneed to add @ before parameter value
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;



            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable NeenOpal_BI_Get_SalesLineFreeIssues_DMD(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionDMD();
            string selectQuery = string.Empty;

            try
            {

                #region Sql 2018/05/14
                selectQuery = @"
 
         ----BI SALE LINEFREE ISSUE DMD
 
 
 SELECT     
        tblMain.SITE_CODE  ,--2  --************PK
        tblMain.SALES_PART, --16
        tblMain.UNITS,--17
        tblMain.SALES_PRICE,--9
        CASE WHEN  tblMain.UNITS = 0 THEN 0 ELSE   ROUND(tblMain.DISCOUNT_AMOUNT/tblMain.UNITS,2) END AS UNIT_DISCOUNT_AMOUNT ,  --14
        CASE WHEN  tblMain.UNITS = 0 THEN 0 ELSE  ROUND(((tblMain.DISCOUNT_AMOUNT/tblMain.UNITS)/100),2) END AS  UNIT_DISCOUNT_PRENTENTAGE ,
        tblMain.VALUE,--18
        (tblMain.DISCOUNT_AMOUNT ) AS TOTAL_DISCOUNT,
        tblMain.LOCATION_NO,--28
         tblMain.ORDER_NO,--3  --************PK THIS IS CALLED SALES_TRANS_NO
         tblMain.TDATE,--18
          SUBSTR(tblMain.EMPLOYEE_CODE,1,12) AS PROMOTER_CODE ,--17
           tblMain.AUTHORIZED_USER_ID ,--29
           tblMain.STATE ,--30
       tblMain.CUSTOMER_NO,--4
         
         tblMain.LENGTH_OF_CONTRACT   ,--24
         ' ' AS ADVANCE_PAYMENT_NO ,  
         0  AS ADVANCE_PAYMENT_AMOUNT ,  
       tblMain.SALE_TYPE , --61    --************PK
       tblMain.SALES_LOCATION_NO ,--27
        tblMain.SERVICE_CHARGE , --31
         tblMain.FIRST_PAYMENT ,  --32
        tblMain.BUDGET_BOOK_HIRE_PRICE ,  --34
        tblMain.MONTHLY_PAYMENT  ,--33
         tblMain.BUDGET_BOOK  , --23
         tblMain.PRICE_LIST, --35
          tblMain.GROSS_HIRE_PRICE  --36
          ,tblMain.VAL_SURAKSHA_HP_LINES as SURAKSHA_VALUE--37
          ,tblMain.val_sanasuma_hp_lines AS SANASUMA_VALUE--38
          ,tblMain.Senasuma_Plus_Value--39
          ,tblMain.Senasuma_Plus_Mobile_Value--40
          ,tblMain.Senasuma_Post_Value--41
          ,tblMain.TRANSAACTION_TYPE --42
          ,tblMain.SALES_VARIATION  --43
          ,tblMain.FREE_ISSUE -- 44
          ,tblMain.salesman_code -- 45
          ,tblMain.Vallibel_Introduced  --46
          ,tblMain.SERIAL_NO --47
          ,tblMain.order_category --48
        
    --FOR PRIMARY KEY
    
    ,1 AS TILL_NO --************PK  THIS IS HARD CODE FOR EVERY SQL
    ,tblMain.SALE_LINE_TYPE --20 --************PK
    ,tblMain.LINE_NO AS SALE_LINE_NO --************PK
    
    ,NVL ( ifsapp.Sin_FN_DayWiseTotal_DMD_Tax(SUBSTR(tblMain.SITE_CODE,1,5),tblMain.SALES_PART,SUBSTR(tblMain.ORDER_NO,5,1),NVL(tblMain.VALUE,0)) , 0 ) AS sales_tax_value
      ,tblMain.INVENTORY_VALUE
    ,CASE WHEN  SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'NM' THEN 'NORMAL'
       WHEN SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'SP' THEN 'SPECIAL'
       WHEN SUBSTR(tblMain.BUDGET_BOOK,1,2) = 'GR' THEN 'GROUP'
        ELSE ''  END 
      AS SALES_GROUPING
      ,' ' as RET_REASON_CODE
       , tblMain.Status_586P --62
       , tblMain.discount_type --12
, tblMain.discount_reason --12
,'DMD' as SalesFileType
 ,tblMain.PROMOTION_CODE--15
,NVL(tblMain.Cash_Conversion_Days,0)  AS Cash_Conversion_Days-- 49
,tblMain.Cash_Conversion_Cal_Days -- 50
, tblMain.Authorized_Date --51
,tblMain.Utilized_Date     --52
,tblMain.AUTHORIZED_NAME  --53       
,tblMain.Line_Status  --54
,tblMain.Revert_Reversed_Bal --55
,tblMain.Total_Amount_Paid -- 56
,tblMain.HPNRET_GROUPING --57 
,tblMain.EnteredBy --58
, tblMain.EnteredName  --59
, tblMain.SALESMAN_TYPE  --60
   FROM (
         
         select distinct TO_CHAR(p.transaction_date, 'YYYY-MM-DD') AS TDATE, --1
                           
                             --p.contract   as site_code, --2--COMMENTED ON 2019/09/27
           /* CASE WHEN c.customer_no IS NOT NULL THEN c.customer_no ELSE case
                  WHEN p.order_no is null THEN
                   to_char(p.transaction_id)
                  when p.order_no is not null then
                   p.order_no
                end END  as    site_code,--2   */

            P.dealer as    site_code,--2
                          case
                            WHEN p.order_no is null THEN
                             to_char(p.transaction_id)
                            when p.order_no is not null then
                             p.order_no
                          end as order_no, --3
                          --c.customer_no, --4
                            p.dealer AS customer_no, --4
                          case
                            When c.date_entered is null then
                             p.transaction_date
                            when c.date_entered is not null then
                             c.date_entered
                          end as date_entered, --5
                          '' as variation, --6
                          p.Sales_Promoter as EMPLOYEE_CODE, --7
                          P.TRANSACTION_ID as line_no, --8
                          TRUNC(P.cash_price, 2) AS sales_price, --9
                          TRUNC(P.cash_price, 2) AS sales_price_1, --10
                          
                          TRUNC(SUM(C.COST)/COUNT(*)) AS inventory_value, --11
                          
                          (select t.discount_type
                             from ifsapp.cust_order_line_discount t
                            where t.order_no = p.order_no
                              and t.line_no = p.line_no
                              and t.line_item_no = p.line_item_no
                              and t.rel_no = p.rel_no
                              AND ROWNUM = 1) as discount_type, --12
                          
                          case
                            WHEN (select t.discount_type
                                    from ifsapp.cust_order_line_discount t
                                   where t.order_no = p.order_no
                                     and t.line_no = p.line_no
                                     and t.line_item_no = p.line_item_no
                                     and t.rel_no = p.rel_no
                                     AND ROWNUM = 1) = 'G' THEN
                             'GENERAL'
                            when (select t.discount_type
                                    from ifsapp.cust_order_line_discount t
                                   where t.order_no = p.order_no
                                     and t.line_no = p.line_no
                                     and t.line_item_no = p.line_item_no
                                     and t.rel_no = p.rel_no
                                     AND ROWNUM = 1) = 'S' then
                             'SPECIAL'
                          end as discount_reason, --13
                          
                       /*   (select nvl(sum(t.discount_amount),
                                      (t.discount * t.calculation_basis) / 100)
                             from ifsapp.cust_order_line_discount t
                            where t.order_no = p.order_no
                              and t.line_no = p.line_no
                              and t.line_item_no = p.line_item_no
                              and t.rel_no = p.rel_no
                            group by t.discount, t.calculation_basis,t.line_no) as discount_amount, --14  */

(
            select  
               (   
               sum(nvl(t.discount_amount, 0)) +     sum((nvl(t.calculation_basis, 0) * (nvl(t.discount, 0) / 100)))
               ) AS DISCOUNT
            from ifsapp.cust_order_line_discount t
           where  t.order_no = p.order_no
                  and t.line_no = p.line_no
                  and t.line_item_no = p.line_item_no
                  and t.rel_no = p.rel_no  
           group by  t.line_no 
            )as discount_amount,  --14 
                          
                          (nvl(nvl((SELECT to_char(d.rule_no) promo_rule_number
                                     FROM ifsapp.hpnret_hp_dtl d
                                    WHERE d.account_no = p.order_no
                                      AND d.conn_line_no = p.line_no
                                      AND d.conn_rel_no = p.rel_no
                                      AND d.conn_line_item_no = p.line_item_no
                                      AND d.catalog_type = 'Inventory part'
                                      AND ROWNUM = 1),
                                   (SELECT to_char(l.free1) promo_rule_number
                                      FROM ifsapp.Hpnret_Cust_Order_Line l,
                                           ifsapp.hpnret_customer_order  h
                                     WHERE l.free1 IS NOT NULL
                                       AND h.order_no = l.order_no
                                       AND h.cash_conv = 'FALSE'
                                       AND l.order_no = p.order_no
                                       AND l.conn_line_no = p.line_no
                                       AND l.conn_rel_no = p.rel_no
                                       AND l.conn_line_item_no = p.line_item_no
                                       AND ROWNUM = 1)),
                               (SELECT to_char(c.discount_source_id) promo_rule_number
                                  FROM ifsapp.Cust_Order_Line_Discount c
                                 WHERE c.user_id = 'PROMO'
                                   AND c.discount_source_id IS NOT NULL
                                   AND c.order_no = p.order_no
                                   AND c.line_no = p.line_no
                                   AND c.rel_no = p.rel_no
                                   AND c.line_item_no = p.line_item_no
                                   AND ROWNUM = 1))) as PROMOTION_CODE, --15
                          
                          p.sales_part, --16
                          case
                            when substr(p.order_no,
                                        instr(p.order_no, '-', -1, 1) + 1,
                                        1) = 'R' and p.units > 0.01 then
                             (p.units)
                            when substr(p.order_no,
                                        instr(p.order_no, '-', -1, 1) + 1,
                                        1) = 'R' and p.units < 0.01 then
                             (p.units)
                            when p.order_no is null and p.units < 0.01 then
                             (p.units)
                            when substr(p.order_no,
                                        instr(p.order_no, '-', -1, 1) + 1,
                                        1) = 'R' and p.units > 0 and
                                 p.free_issue = 'Y' then
                             (p.units)
                            when substr(p.order_no,
                                        instr(p.order_no, '-', -1, 1) + 1,
                                        1) = 'R' and p.units < 0 and
                                 p.free_issue = 'Y' then
                             (p.units)
                            when substr(p.order_no,
                                        instr(p.order_no, '-', -1, 1) + 1,
                                        1) not in ( 'R' ,'H')  then
                             (p.units)
                          END as units, --17
                          
                          CASE
                            when substr(p.order_no,
                                        instr(p.order_no, '-', -1, 1) + 1,
                                        1) = 'R' and p.value > 0.01 then
                             (p.value)
                            when substr(p.order_no,
                                        instr(p.order_no, '-', -1, 1) + 1,
                                        1) = 'R' and p.value < 0.01 then
                             (p.value)
                            when p.order_no is null and p.value < 0.01 then
                             (p.value)
                            when substr(p.order_no,
                                        instr(p.order_no, '-', -1, 1) + 1,
                                        1) = 'R' and p.units > 0 and
                                 p.free_issue = 'Y' then
                             (p.value)
                            when substr(p.order_no,
                                        instr(p.order_no, '-', -1, 1) + 1,
                                        1) = 'R' and p.units < 0 and
                                 p.free_issue = 'Y' then
                             (p.value)
                            when substr(p.order_no,
                                        instr(p.order_no, '-', -1, 1) + 1,
                                        1) NOT IN ( 'R','H')   then
                             (p.value)
                          END as value, --18
                          
                          CASE
                            when substr(p.order_no,
                                        instr(p.order_no, '-', -1, 1) + 1,
                                        1) = 'R' and (p.units > 0) THEN
                             '101'
                            WHEN (substr(p.order_no,
                                         instr(p.order_no, '-', -1, 1) + 1,
                                         1) = 'R') and (p.units < 0) THEN
                             '104'
                            WHEN p.order_no is null and p.units < 0 THEN
                             '104'
                          END AS TRANSACTION_SUBTYPE_CODE, --19
                          
                          case
                            WHEN (substr(p.order_no,
                                         instr(p.order_no, '-', -1, 1) + 1,
                                         1) = 'R') and (p.units > 0) THEN
                             '1'
                            WHEN (substr(p.order_no,
                                         instr(p.order_no, '-', -1, 1) + 1,
                                         1) = 'R') and (p.units < 0) THEN
                             '2'
                            WHEN p.order_no is null and p.units < 0 THEN
                             '2'
                          end as SALE_LINE_TYPE, --20
                          
                          p.Sales_Location as SALES_LOCATION, --21
                          TRUNC(P.cash_price, 2) as ORIGINAL_PRODUCT_PRICE, --22
                          'N/A' as BUDGET_BOOK, --23
                          0 as LENGTH_OF_CONTRACT, --24
                          'I' AS TaxCallType --25
                       ----BI
                    , p.Sales_Location  as SALES_LOCATION_NO --27
                    ,p.location_no AS LOCATION_NO--28
                    
                    
                    ,' '  AS AUTHORIZED_USER_ID --29
       ,' ' AS STATE --30
        , 0.0 as SERVICE_CHARGE --31 
        , 0.0  as FIRST_PAYMENT --32
        ,0 AS MONTHLY_PAYMENT --33
        ,0.0 as BUDGET_BOOK_HIRE_PRICE --34
        ,IFSAPP.customer_order_api.get_price_list_no(p.order_no) AS PRICE_LIST --35
        ,0.0 AS GROSS_HIRE_PRICE --36
        ,0.0 AS VAL_SURAKSHA_HP_LINES   --37
        ,0.0 AS val_sanasuma_hp_lines -- 38
        ,0.0 as Senasuma_Plus_Value--39
        ,0.0 as Senasuma_Plus_Mobile_Value--40
        ,0.0 as Senasuma_Post_Value--41
        ,CASE WHEN   substr(p.order_no,
                              instr(p.order_no, '-', -1, 1) + 1,
                              1)  = 'R' THEN 'CASH' ELSE 'HIRE'  END  AS TRANSAACTION_TYPE -- 42
        
         ,'' as   SALES_VARIATION    --43
         ,P.free_issue AS FREE_ISSUE   --44
         , '' as  salesman_code -- 45
      , '' AS Vallibel_Introduced  --46 ------ASK CHATHURA ABOUT IFS FIELD
   
      ,(SELECT ST.SERIAL_NO
          FROM IFSAPP.SERIAL_TRANSACTION ST
         WHERE   p.order_no = ST.ORDER_NO 
          and p.line_no = ST.line_no
 and p.rel_no = ST.rel_no
 and p.line_item_no = ST.line_item_no 
           AND ROWNUM = 1) SERIAL_NO --47
  , ifsapp.Hpnret_Customer_Order_api.Get_Order_Category(p.order_no) as  order_category     --48   
  ,0 as   Cash_Conversion_Days    --49
  , '' as Cash_Conversion_Cal_Days --50  
, NULL Authorized_Date  --51
,NULL Utilized_Date       --52 
   ,  '' AS  AUTHORIZED_NAME  --53       
,C.state   AS Line_Status     --54
, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Out_Bal(p.ORDER_NO ,1),2) as Revert_Reversed_Bal --55     
, trunc(IFSAPP.Hpnret_Hp_Head_Api.Get_Total_Amt_Paid(p.ORDER_NO ,1),2) as Total_Amount_Paid --56  
,'' AS HPNRET_GROUPING --57
  ,''  as EnteredBy --58
  ,  '' AS EnteredName  --59      
   
    ,'' AS SALESMAN_TYPE  ----60   
 ,    IFSAPP.HPNRET_CUSTOMER_ORDER_API.Get_Sale_Type(p.ORDER_NO)  as     SALE_TYPE   --61
   , '' as   Status_586P --62  
----BI
                    
           from IFSAPP.INDIRECT_SALES_DTL p
           left join IFSAPP.INVENTORY_LOCATION l on l.location_no =
                                                    p.location_no
                                                AND L.contract = p.contract
           left join IFSAPP.Customer_Order_Line C on c.order_no = p.order_no
                                                 and c.line_no = p.line_no
                                                 and c.line_item_no =
                                                     p.line_item_no
                                                 and c.rel_no = p.rel_no
          where p.part_type not in ('PKG')
            AND TRUNC(p.transaction_date) = to_date(:PreviousDate, 'YYYY/MM/DD')
            /*and (SELECT substr(i.second_commodity, 1, 2)
                   FROM ifsapp.inventory_part i
                  WHERE i.contract = P.contract
                    AND i.part_no = P.sales_part) NOT IN ( '2P','2C','GV')
            AND P.sales_part <> 'PARTS'*/
            and p.free_issue = 'Y'
            --and c.customer_no not in ('DM09001')
            and p.dealer  not in ('DM09001')
          group by l.location_group,
                    p.transaction_date,
                    p.location_no,
                    p.order_no,
                    c.customer_no,
                    c.date_entered,
                    p.units,
                    p.value,
                    p.contract,
                    P.cash_price,
                    p.sales_part,
                    p.free_issue,
                    p.transaction_id,
                    p.line_no,
                    p.rel_no,
                    p.line_item_no,
                    P.stat_year,
                    P.stat_period_no
                    ,C.state
                    ,c.order_no
                    ,p.dealer
         
         ) tblMain
             
        
         
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))//noneed to add @ before parameter value
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable NeenOpal_BI_GetPlanSale(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region Sql
                selectQuery = @"
 ----BI PLAN SALE
                             
      SELECT
        tblMain.SITE_CODE  ,--2  --************PK
        tblMain.SALES_PART, --16
        tblMain.UNITS,--17
        tblMain.SALES_PRICE,--9
        CASE WHEN  tblMain.UNITS = 0 THEN 0 ELSE   ROUND(tblMain.DISCOUNT_AMOUNT/tblMain.UNITS,2) END AS UNIT_DISCOUNT_AMOUNT ,  --14
        CASE WHEN  tblMain.UNITS = 0 THEN 0 ELSE  ROUND(((tblMain.DISCOUNT_AMOUNT/tblMain.UNITS)/100),2) END AS  UNIT_DISCOUNT_PRENTENTAGE ,
        tblMain.VALUE,--18
        (tblMain.DISCOUNT_AMOUNT ) AS TOTAL_DISCOUNT,
        tblMain.LOCATION_NO,--28
         tblMain.ORDER_NO,--3  --************PK THIS IS CALLED SALES_TRANS_NO
         tblMain.TDATE,--1     --************PK
        SUBSTR(tblMain.EMPLOYEE_CODE,1,12) AS PROMOTER_CODE ,--17
         tblMain.AUTHORIZED_USER_ID ,--29
         tblMain.STATE ,--30
       tblMain.CUSTOMER_NO,--4
       tblMain.LENGTH_OF_CONTRACT   ,--24
         ' ' AS ADVANCE_PAYMENT_NO ,  
         0  AS ADVANCE_PAYMENT_AMOUNT ,  
       tblMain.SALE_TYPE , --20    --************PK
       tblMain.SALES_LOCATION_NO ,--27
        tblMain.SERVICE_CHARGE , --31
         tblMain.FIRST_PAYMENT ,  --32
        tblMain.BUDGET_BOOK_HIRE_PRICE ,  --34
        tblMain.MONTHLY_PAYMENT  ,--33
         tblMain.BUDGET_BOOK  , --23
         tblMain.PRICE_LIST, --35
          tblMain.GROSS_HIRE_PRICE  --36
          ,tblMain.VAL_SURAKSHA_HP_LINES as SURAKSHA_VALUE--37
          ,tblMain.val_sanasuma_hp_lines AS SANASUMA_VALUE--38
          ,tblMain.Senasuma_Plus_Value--39
          ,tblMain.Senasuma_Plus_Mobile_Value--40
          ,tblMain.Senasuma_Post_Value--41
          ,tblMain.TRANSAACTION_TYPE --42
          ,tblMain.SALES_VARIATION  --43
          ,tblMain.FREE_ISSUE -- 44
          ,tblMain.salesman_code -- 45
          ,tblMain.Vallibel_Introduced  --46
          ,tblMain.SERIAL_NO --47
          ,tblMain.order_category --48
    --FOR PRIMARY KEY
    ,1 AS TILL_NO --************PK  THIS IS HARD CODE FOR EVERY SQL
    ,tblMain.SALE_TYPE AS SALE_LINE_TYPE --************PK
    ,tblMain.LINE_NO AS SALE_LINE_NO --************PK

    ,tblMain.sales_tax_value--49
    ,tblMain.INVENTORY_VALUE   --50
    ,'' AS SALES_GROUPING    --51
    , tblMain.RET_REASON_CODE   --52
       , tblMain.Status_586P   --53
, tblMain.discount_type --12
, tblMain.discount_reason --12
,'P' as SalesFileType
,tblMain.PROMOTION_CODE--15
,NVL(tblMain.Cash_Conversion_Days,0)  AS Cash_Conversion_Days-- 49
,tblMain.Cash_Conversion_Cal_Days -- 50
, tblMain.Authorized_Date --51
,tblMain.Utilized_Date     --52
,tblMain.AUTHORIZED_NAME  --53       
,tblMain.Line_Status  --54
,tblMain.Revert_Reversed_Bal --55
,tblMain.Total_Amount_Paid -- 56
,tblMain.HPNRET_GROUPING  --57
 ,tblMain.EnteredBy --58
 , tblMain.EnteredName  --59
 , tblMain.SALESMAN_TYPE  --60              
                 FROM (  
                SELECT 
                 trunc(a.create_date)  AS TDATE,
                 co.contract  AS SITE_CODE,--2
                       co.order_no AS  ORDER_NO,--3
                       COL.CUSTOMER_NO AS  CUSTOMER_NO,--4
                       trunc(a.create_date) AS DATE_ENTERED,--5
                       '' AS VARIATION,--6
                       '' AS EMPLOYEE_CODE,--7
                        CONCAT(RPAD(COL.line_no, 4, 0), RPAD(COL.line_item_no, 3, 0)) AS LINE_NO, --8
                       NVL(  TRUNC(COL.SALE_UNIT_PRICE -
                           NVL((COL.SALE_UNIT_PRICE * ( TRUNC(COL.DISCOUNT,2) / 100)), 0),2 ),
                           0) as sales_price,--9
        
                       NVL( TRUNC(COL.SALE_UNIT_PRICE -
                           NVL((COL.SALE_UNIT_PRICE * ( TRUNC(COL.DISCOUNT,2) / 100)), 0),2),
                           0) as sales_price_1,--10
         
                        0 AS INVENTORY_VALUE,--11
                       '' AS DISCOUNT_TYPE,--12
                       '' AS DISCOUNT_REASON,--13
       
                        0 AS DISCOUNT_AMOUNT , --14,

                       '' AS PROMOTION_CODE ,--15
                       col.catalog_no  AS SALES_PART, --16
                       COL.BUY_QTY_DUE  AS UNITS,--17
                       NVL( TRUNC(COL.SALE_UNIT_PRICE -
                           NVL((COL.SALE_UNIT_PRICE * ( TRUNC(COL.DISCOUNT,2) / 100)), 0),2),
                           0) AS VALUE,--18
                       '101' AS TRANSACTION_SUBTYPE_CODE,--19
                       '22' AS SALE_TYPE,--20
                       '' AS SALES_LOCATION,--21
                       COL.SALE_UNIT_PRICE  AS ORIGINAL_PRODUCT_PRICE,--22
                       '' AS BUDGET_BOOK  , --23
                       0 AS LENGTH_OF_CONTRACT   ,--24
                       0 AS sales_tax_value--25* 
                     ,' ' as RET_REASON_CODE--26

                     
 ----BI
                           
             ,   ''  as SALES_LOCATION_NO    --27
       ,'' AS LOCATION_NO --28
       ,'' AS AUTHORIZED_USER_ID --29
       
       , 0.0 AS SERVICE_CHARGE--31
       , 0.0 as FIRST_PAYMENT --32
       ,0.0 as MONTHLY_PAYMENT --33
       ,0.0 as BUDGET_BOOK_HIRE_PRICE --34
       ,'' AS PRICE_LIST --35
       ,0.0 AS GROSS_HIRE_PRICE --36
       ,0.0 as VAL_SURAKSHA_HP_LINES --37
       ,0.0 as val_sanasuma_hp_lines --38
       ,0.0  as Senasuma_Plus_Value  --39

       ,0.0  as Senasuma_Plus_Mobile_Value --40
       
       
        ,0.0   as Senasuma_Post_Value   --41
, ''  AS TRANSAACTION_TYPE--42
  ,''    as SALES_VARIATION  --43                       
   ,'' AS FREE_ISSUE    --44
   ,'' AS salesman_code -- 45
   , ''  AS Vallibel_Introduced  --46
   ,'' AS  SERIAL_NO --47
  ,'' AS order_category   --48
            ,a.state             
                         
                    ,a.series_no as ADVANCE_PAYMENT_NO
                    ,a.dom_amount as ADVANCE_PAYMENT_AMOUNT
                    ,a.account_code as Status_586P
  ,0 as   Cash_Conversion_Days    --49
  , '' as Cash_Conversion_Cal_Days --50  
, NULL Authorized_Date  --51
,NULL Utilized_Date       --52
,'' as AUTHORIZED_NAME -- 53
,''   AS Line_Status     --54
,0 as Revert_Reversed_Bal --55  
,0.0 Total_Amount_Paid --56 
,  ifsapp.hpnret_pro_fam_group_api.get_hpnret_groupings(0,
                                                            'EXT WARRANTY',
                                                            IFSAPP.inventory_part_api.Get_Part_Product_Family(COL.contract,
                                                                                                              COL.part_no)) AS HPNRET_GROUPING -- 57
  ,''  as EnteredBy --58
  ,  '' AS EnteredName  --59      
   
    , (
 SELECT ifsapp.HPNRET_LEVEL_API.Get_Hpnret_Level_Type(HLHH.level_id) AS SALESMAN_TYPE
  FROM IFSAPP.HPNRET_LEVEL_HIERARCHY HLHH WHERE HLHH.site_id = CO.contract
  )  AS SALESMAN_TYPE  ----60  
 
---------------------BI
                    
                  FROM IFSAPP.HPNRET_CUSTOMER_ORDER CO,
                       IFSAPP.CUSTOMER_ORDER_LINE   COL,
                       ifsapp.hpnret_ADV_PAY        a

                WHERE trunc(a.create_date) =  TO_DATE(:PreviousDate, 'YYYY/MM/DD') 
 
      
                   and COL.LINE_ITEM_NO <= 0
                   AND CO.ORDER_NO = COL.ORDER_NO
                   AND CO.SALESMAN_CODE IS NOT NULL
                   AND CO.PROD_RELEASE = 'TRUE' 
                   AND CO.STATE = ('Reserved')-- 
                   and col.objstate in ('Reserved', 'Released')--
                   and a.state = 'Approved'
                   AND COL.CATALOG_TYPE_DB not in ('PKG')
                   and co.order_no = a.prod_rel_order_no
                   and a.account_code = '586P'
                   and co.contract = a.contract
                   and COL.SALE_UNIT_PRICE <> 0
   
                   /*and (SELECT substr(i.second_commodity, 1, 2)
                          FROM ifsapp.inventory_part i
                         WHERE i.contract = co.contract
                           AND i.part_no = col.catalog_no) NOT IN ( '2P','2C','GV')
                   AND col.catalog_no <> 'PARTS'*/

                union

                ----Hire 586P

                ----
                SELECT
                trunc(HP.SALES_DATE) AS TDATE,--1
               c.contract  AS SITE_CODE, -- 2
                       c.order_no  AS ORDER_NO,--3
                       C.CUSTOMER_NO AS CUSTOMER_NO,--4
                       trunc(HP.SALES_DATE)  AS DATE_ENTERED,--5
                       '' AS VARIATION,--6
                       '' AS EMPLOYEE_CODE,--7
                       CONCAT(RPAD(C.line_no, 4, 0), RPAD(C.line_item_no, 3, 0)) AS LINE_NO,--8
                       NVL( TRUNC(C.BASE_SALE_UNIT_PRICE -
                           NVL((C.BASE_SALE_UNIT_PRICE * ( TRUNC(C.DISCOUNT,2) / 100)), 0),2),
                           0)    AS sales_price,--9
                       NVL(TRUNC(C.BASE_SALE_UNIT_PRICE -
                           NVL((C.BASE_SALE_UNIT_PRICE * ( TRUNC(C.DISCOUNT,2) / 100)), 0),2),
                           0)   AS sales_price_1 ,--10
                       0 AS INVENTORY_VALUE,--11
                       '' AS DISCOUNT_TYPE,--12
                       '' AS DISCOUNT_REASON,--13
       
                        0 AS DISCOUNT_AMOUNT , --14,

                       '' AS PROMOTION_CODE ,--15
                       c.catalog_no AS SALES_PART, --16
                       C.BUY_QTY_DUE AS UNITS, --17
                       NVL( TRUNC(C.BASE_SALE_UNIT_PRICE -
                           NVL((C.BASE_SALE_UNIT_PRICE * ( TRUNC(C.DISCOUNT,2) / 100)), 0),2),
                           0) AS VALUE,--18
                       '201' AS TRANSACTION_SUBTYPE_CODE,--19
                       '22' AS SALE_TYPE,--20
                       '' AS SALES_LOCATION,--21
                       C.BASE_SALE_UNIT_PRICE AS ORIGINAL_PRODUCT_PRICE,--22
                       '' AS BUDGET_BOOK  , --23
                       0 AS LENGTH_OF_CONTRACT   ,--24
                       0 AS sales_tax_value--25
                       ,' ' as RET_REASON_CODE--26
                       
                      ----BI
                           
             ,   ''  as SALES_LOCATION_NO    --27
       ,'' AS LOCATION_NO --28
       ,'' AS AUTHORIZED_USER_ID --29
       
       , 0.0 AS SERVICE_CHARGE--31
       , 0.0 as FIRST_PAYMENT --32
       ,0.0 as MONTHLY_PAYMENT --33
       ,0.0 as BUDGET_BOOK_HIRE_PRICE --34
       ,'' AS PRICE_LIST --35
       ,0.0 AS GROSS_HIRE_PRICE --36
       ,0.0 as VAL_SURAKSHA_HP_LINES --37
       ,0.0 as val_sanasuma_hp_lines --38
       , 0.0  as Senasuma_Plus_Value  --39

       , 0.0  as Senasuma_Plus_Mobile_Value --40
       
       
        ,   0.0   as Senasuma_Post_Value   --41
, ''  AS TRANSAACTION_TYPE--42
  ,''    as SALES_VARIATION  --43                       
   ,'' AS FREE_ISSUE    --44
   ,'' AS salesman_code -- 45
   , ''  AS Vallibel_Introduced  --46
   ,'' AS  SERIAL_NO --47
  ,'' AS order_category   --48
            ,a.state             
                         
                    ,a.series_no as ADVANCE_PAYMENT_NO
                    ,a.dom_amount as ADVANCE_PAYMENT_AMOUNT
                    ,a.account_code as Status_586P
  ,0 as   Cash_Conversion_Days    --49
  , '' as Cash_Conversion_Cal_Days --50  
, NULL Authorized_Date  --51
,NULL Utilized_Date       --52
,'' as AUTHORIZED_NAME -- 53
 ,C.state AS Line_Status --54
,0 as Revert_Reversed_Bal --55  
,0.0 Total_Amount_Paid --56
,  ifsapp.hpnret_pro_fam_group_api.get_hpnret_groupings(0,
                                                            'EXT WARRANTY',
                                                            IFSAPP.inventory_part_api.Get_Part_Product_Family(C.contract,
                                                                                                              C.part_no)) AS HPNRET_GROUPING -- 57 
  ,''  as EnteredBy --58
  ,  '' AS EnteredName  --59      
   
    , (
 SELECT ifsapp.HPNRET_LEVEL_API.Get_Hpnret_Level_Type(HLHH.level_id) AS SALESMAN_TYPE
  FROM IFSAPP.HPNRET_LEVEL_HIERARCHY HLHH WHERE HLHH.site_id = C.contract
  )  AS SALESMAN_TYPE  ----60  
--------BI


                  FROM IFSAPP.CUSTOMER_ORDER_LINE C,
                       IFSAPP.HPNRET_HP_HEAD_INFO HP,
                       ifsapp.hpnret_ADV_PAY      A
       
                WHERE HP.ACCOUNT_NO = C.ORDER_NO
                   AND SUBSTR(C.ORDER_NO, INSTR(C.ORDER_NO, '-', -1, 1) + 1, 1) = 'H'
                   AND C.CATALOG_TYPE_DB not in ('PKG')
                   AND HP.PROD_RELEASE = 'TRUE'
                  AND HP.STATE = ('Reserved')
                  and c.objstate in ('Reserved', 'Released')
                   and a.state = 'Approved' 
                   AND trunc(HP.SALES_DATE)  =  TO_DATE(:PreviousDate, 'YYYY/MM/DD') 
    
      
                   and C.BASE_SALE_UNIT_PRICE <> 0
                   and c.order_no = a.prod_rel_order_no
                   and a.account_code = '586P'
                   and c.contract = a.contract

                   and c.order_no = a.prod_rel_order_no
                   and a.account_code = '586P'
                   and c.contract = a.contract
                   /*and (SELECT substr(i.second_commodity, 1, 2)
                          FROM ifsapp.inventory_part i
                         WHERE i.contract =  c.contract
                           AND i.part_no = c.catalog_no) NOT IN ( '2P','2C','GV')
                   AND c.catalog_no <> 'PARTS'*/
           )tblMain
                 
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))//noneed to add @ before parameter value
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;



            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                return null;
            }
        }

        public DataTable Get_BI_NRTAN_BackUpSalesLineTableData()
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            string selectQuery = string.Empty;

            try
            {
                #region
                selectQuery = @"
                    SELECT 
                    site_code, 
                    sales_part, 
                    units, 
                    sales_price, 
                    unit_discount_amount, 
                    unit_discount_prententage, 
                    value, 
                    total_discount, 
                    location_no, 
                    order_no, 
                    tdate, 
                    promoter_code, 
                    authorized_user_id, 
                    state, 
                    customer_no, 
                    length_of_contract, 
                    advance_payment_no, 
                    advance_payment_amount, 
                    sale_type, 
                    sales_location_no, 
                    service_charge, 
                    first_payment, 
                    budget_book_hire_price, 
                    monthly_payment, 
                    budget_book, 
                    price_list, 
                    gross_hire_price, 
                    till_no, 
                    sale_line_type, 
                    sale_line_no, 
                    suraksha_value, 
                    sanasuma_value, 
                    senasuma_plus_value, 
                    senasuma_plus_mobile_value, 
                    senasuma_post_value, 
                    transaaction_type, 
                    sales_variation, 
                    free_issue, 
                    salesman_code, 
                    vallibel_introduced, 
                    serial_no, 
                    order_category, 
                    sales_tax_value, 
                    inventory_value, 
                    sales_grouping, 
                    ret_reason_code, 
                    status_586p,
                    DISCOUNT_TYPE_CODE,
                    DISCOUNT_REASON_CODE
                    ,SALESFILETYPE
                    ,PROMOTION_CODE
                    ,CASH_CONVERSION_DAYS	 			
                    ,CASH_CONVERSION_CAL_DAYS	 			
                    ,AUTHORIZED_DATE	 			
                    ,UTILIZED_DATE
                    ,AUTHORIZED_NAME	 			
                    ,LINE_STATUS	 			
                    ,REVERT_REVERSED_BAL	 			
                    ,TOTAL_AMOUNT_PAID	 			
                    ,HPNRET_GROUPING
                    ,SALESMAN_TYPE
                    ,ENTERED_BY
                    ,ENTERED_NAME	 	
                    FROM NRTAN.BackUp_BI_SALE_LINE    
                ";
                #endregion

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                return null;
            }
        }

        public int Insert_BI_NeenOpal_SalesLine(DataTable dtNotInPKGSales)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Querry


                string insertQuery = @"
              insert into nrtan.bi_sale_line
              (
   
               site_code,
               sales_part,
               units,
               sales_price,
               unit_discount_amount,
               unit_discount_prententage,
               value,
               total_discount,
               location_no,
               order_no,
               tdate,
               promoter_code,
               authorized_user_id,
               state,
               customer_no,
               length_of_contract,
               advance_payment_no,
               advance_payment_amount,
               sale_type,
               sales_location_no,
               service_charge,
               first_payment,
               budget_book_hire_price,
               monthly_payment,
               budget_book,
               price_list,
               gross_hire_price,
               till_no,
               sale_line_type,
               sale_line_no,
                suraksha_value,
               sanasuma_value,
               senasuma_plus_value,
               senasuma_plus_mobile_value,
               senasuma_post_value,
               transaaction_type,
               sales_variation,
               free_issue,
               salesman_code,
               vallibel_introduced,
               serial_no,
               order_category,
                sales_tax_value,
                INVENTORY_VALUE,
                SALES_GROUPING,
                RET_REASON_CODE,
                Status_586P,
                DISCOUNT_TYPE_CODE,
                DISCOUNT_REASON_CODE,
                SALESFILETYPE,
                PROMOTION_CODE,
                CASH_CONVERSION_DAYS,
                CASH_CONVERSION_CAL_DAYS,
                AUTHORIZED_DATE,
                UTILIZED_DATE,
                AUTHORIZED_NAME	 ,			
                LINE_STATUS	 ,			
                REVERT_REVERSED_BAL	 ,			
                TOTAL_AMOUNT_PAID	, 			
                HPNRET_GROUPING ,
SALESMAN_TYPE,
ENTERED_BY,
ENTERED_NAME

               )
            values
              (
    
               :site_code,
               :sales_part,
               :units,
               :sales_price,
               :unit_discount_amount,
               :unit_discount_prententage,
               :value,
               :total_discount,
               :location_no,
               :order_no,
               :tdate,
               :promoter_code,
               :authorized_user_id,
               :state,
               :customer_no,
               :length_of_contract,
               :advance_payment_no,
               :advance_payment_amount,
               :sale_type,
               :sales_location_no,
               :service_charge,
               :first_payment,
               :budget_book_hire_price,
               :monthly_payment,
               :budget_book,
               :price_list,
               :gross_hire_price,
               :till_no,
               :sale_line_type,
               :sale_line_no,
               :suraksha_value,
               :sanasuma_value,
               :senasuma_plus_value,
               :senasuma_plus_mobile_value,
               :senasuma_post_value,
               :transaaction_type,
               :sales_variation,
               :free_issue,
               :salesman_code,
               :vallibel_introduced,
               :serial_no,
               :order_category,
               :sales_tax_value,
               :INVENTORY_VALUE,
               :SALES_GROUPING,
               :RET_REASON_CODE,
               :Status_586P,
               :DISCOUNT_TYPE_CODE,
               :DISCOUNT_REASON_CODE,
               :SALESFILETYPE,
                :PROMOTION_CODE,
                :CASH_CONVERSION_DAYS,
                :CASH_CONVERSION_CAL_DAYS,
                :AUTHORIZED_DATE,
                :UTILIZED_DATE ,
                :AUTHORIZED_NAME ,	 			
                :LINE_STATUS,	 			
                :REVERT_REVERSED_BAL,	 			
                :TOTAL_AMOUNT_PAID	, 			
                :HPNRET_GROUPING,
                :SALESMAN_TYPE,
                :ENTERED_BY,
                :ENTERED_NAME
               )
                    ";
                #endregion

                foreach (DataRow dRow in dtNotInPKGSales.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[] 
                        { 
                             
                           DBConn.AddParameter("site_code",dRow["site_code"])    
                           ,DBConn.AddParameter("sales_part",dRow["sales_part"])   
                           ,DBConn.AddParameter("units",dRow["units"])    
                           ,DBConn.AddParameter("sales_price",dRow["sales_price"])    
                           ,DBConn.AddParameter("unit_discount_amount",dRow["unit_discount_amount"])    
                           ,DBConn.AddParameter("unit_discount_prententage",dRow["unit_discount_prententage"])    
                           ,DBConn.AddParameter("value",dRow["value"])    
                           ,DBConn.AddParameter("total_discount",dRow["total_discount"])    
                           ,DBConn.AddParameter("location_no",dRow["location_no"])    
                           ,DBConn.AddParameter("order_no",dRow["order_no"])   
                           ,DBConn.AddParameter("tdate",dRow["tdate"])    
                           ,DBConn.AddParameter("promoter_code",dRow["promoter_code"])    
                           ,DBConn.AddParameter("authorized_user_id",dRow["authorized_user_id"])    
                           ,DBConn.AddParameter("state",dRow["state"])    
                           ,DBConn.AddParameter("customer_no",dRow["customer_no"])    
                           ,DBConn.AddParameter("length_of_contract",dRow["length_of_contract"])   
                           ,DBConn.AddParameter("advance_payment_no",dRow["advance_payment_no"])    
                           ,DBConn.AddParameter("advance_payment_amount",dRow["advance_payment_amount"])    
                           ,DBConn.AddParameter("sale_type",dRow["sale_type"])    
                           ,DBConn.AddParameter("sales_location_no",dRow["sales_location_no"])    
                           ,DBConn.AddParameter("service_charge",dRow["service_charge"])   
                           ,DBConn.AddParameter("first_payment",dRow["first_payment"])    
                           ,DBConn.AddParameter("budget_book_hire_price",dRow["budget_book_hire_price"])    
                           ,DBConn.AddParameter("monthly_payment",dRow["monthly_payment"])    
                           ,DBConn.AddParameter("budget_book",dRow["budget_book"])    
                           ,DBConn.AddParameter("price_list",dRow["price_list"])    
                           ,DBConn.AddParameter("gross_hire_price",dRow["gross_hire_price"])    
                           ,DBConn.AddParameter("till_no",dRow["till_no"])    
                           ,DBConn.AddParameter("sale_line_type",dRow["sale_line_type"])    
                           ,DBConn.AddParameter("sale_line_no",dRow["sale_line_no"])  
                           ,DBConn.AddParameter("suraksha_value",dRow["suraksha_value"])    
                           ,DBConn.AddParameter("sanasuma_value",dRow["sanasuma_value"])    
                           ,DBConn.AddParameter("senasuma_plus_value",dRow["senasuma_plus_value"])    
                           ,DBConn.AddParameter("senasuma_plus_mobile_value",dRow["senasuma_plus_mobile_value"])    
                           ,DBConn.AddParameter("senasuma_post_value",dRow["senasuma_post_value"])    
                           ,DBConn.AddParameter("transaaction_type",dRow["transaaction_type"])    
                           ,DBConn.AddParameter("sales_variation",dRow["sales_variation"])    
                           ,DBConn.AddParameter("free_issue",dRow["free_issue"])  
                           ,DBConn.AddParameter("salesman_code",dRow["salesman_code"]) 
                           ,DBConn.AddParameter("vallibel_introduced",dRow["vallibel_introduced"]) 
                           ,DBConn.AddParameter("serial_no",dRow["serial_no"]) 
                           ,DBConn.AddParameter("order_category",dRow["order_category"]) 

                           ,DBConn.AddParameter("sales_tax_value",dRow["sales_tax_value"]) 
                           ,DBConn.AddParameter("INVENTORY_VALUE",dRow["INVENTORY_VALUE"]) 
                           ,DBConn.AddParameter("SALES_GROUPING",dRow["SALES_GROUPING"]) 
                           ,DBConn.AddParameter("RET_REASON_CODE",dRow["RET_REASON_CODE"]) 
                           ,DBConn.AddParameter("Status_586P",dRow["Status_586P"]) 

                           ,DBConn.AddParameter("DISCOUNT_TYPE_CODE",dRow["DISCOUNT_TYPE_CODE"]) 
                           ,DBConn.AddParameter("DISCOUNT_REASON_CODE",dRow["DISCOUNT_REASON_CODE"]) 
                           ,DBConn.AddParameter("SALESFILETYPE",dRow["SALESFILETYPE"]) 
                           ,DBConn.AddParameter("PROMOTION_CODE",dRow["PROMOTION_CODE"]) 

                           ,DBConn.AddParameter("CASH_CONVERSION_DAYS",dRow["CASH_CONVERSION_DAYS"])
                           ,DBConn.AddParameter("CASH_CONVERSION_CAL_DAYS",dRow["CASH_CONVERSION_CAL_DAYS"]) 
                           ,DBConn.AddParameter("AUTHORIZED_DATE", dRow["AUTHORIZED_DATE"] == null ? (object)DBNull.Value : dRow["AUTHORIZED_DATE"])
                           ,DBConn.AddParameter("Utilized_Date",dRow["Utilized_Date"] == null ? (object)DBNull.Value :dRow["Utilized_Date"] ) 

                           ,DBConn.AddParameter("AUTHORIZED_NAME",dRow["AUTHORIZED_NAME"]) 
                           ,DBConn.AddParameter("LINE_STATUS",dRow["LINE_STATUS"]) 
                           ,DBConn.AddParameter("REVERT_REVERSED_BAL",dRow["REVERT_REVERSED_BAL"]) 
                           ,DBConn.AddParameter("TOTAL_AMOUNT_PAID",dRow["TOTAL_AMOUNT_PAID"])
                           ,DBConn.AddParameter("HPNRET_GROUPING",dRow["HPNRET_GROUPING"]) 

                           ,DBConn.AddParameter("SALESMAN_TYPE",dRow["SALESMAN_TYPE"]) 
                           ,DBConn.AddParameter("ENTERED_BY",dRow["ENTERED_BY"])
                           ,DBConn.AddParameter("ENTERED_NAME",dRow["ENTERED_NAME"]) 

                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                    }
                }

                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        public int Insert_NeenOpal_BI_SalesLineToSingerSideBackUpSalesTable(DataTable dtNotInPKGSales)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnection();

                #region Querry

                string insertQuery = @"
              insert into IFSAPP.SIN_bi_sale_line
              (
   
               site_code,
               sales_part,
               units,
               sales_price,
               unit_discount_amount,
               unit_discount_prententage,
               value,
               total_discount,
               location_no,
               order_no,
               tdate,
               promoter_code,
               authorized_user_id,
               state,
               customer_no,
               length_of_contract,
               advance_payment_no,
               advance_payment_amount,
               sale_type,
               sales_location_no,
               service_charge,
               first_payment,
               budget_book_hire_price,
               monthly_payment,
               budget_book,
               price_list,
               gross_hire_price,
               till_no,
               sale_line_type,
               sale_line_no,
                suraksha_value,
               sanasuma_value,
               senasuma_plus_value,
               senasuma_plus_mobile_value,
               senasuma_post_value,
               transaaction_type,
               sales_variation,
               free_issue,
               salesman_code,
               vallibel_introduced,
               serial_no,
               order_category,
                sales_tax_value,
                INVENTORY_VALUE,
                SALES_GROUPING,
                RET_REASON_CODE,
                Status_586P,
                DISCOUNT_TYPE_CODE,
                DISCOUNT_REASON_CODE,
                SALESFILETYPE,
                PROMOTION_CODE,
                CASH_CONVERSION_DAYS,
                CASH_CONVERSION_CAL_DAYS,
                AUTHORIZED_DATE,
                UTILIZED_DATE,
                AUTHORIZED_NAME	 ,			
                LINE_STATUS	 ,			
                REVERT_REVERSED_BAL	 ,			
                TOTAL_AMOUNT_PAID	, 			
                HPNRET_GROUPING,
SALESMAN_TYPE,
ENTERED_BY,
ENTERED_NAME
               )
            values
              (
               :site_code,
               :sales_part,
               :units,
               :sales_price,
               :unit_discount_amount,
               :unit_discount_prententage,
               :value,
               :total_discount,
               :location_no,
               :order_no,
               :tdate,
               :promoter_code,
               :authorized_user_id,
               :state,
               :customer_no,
               :length_of_contract,
               :advance_payment_no,
               :advance_payment_amount,
               :sale_type,
               :sales_location_no,
               :service_charge,
               :first_payment,
               :budget_book_hire_price,
               :monthly_payment,
               :budget_book,
               :price_list,
               :gross_hire_price,
               :till_no,
               :sale_line_type,
               :sale_line_no,
               :suraksha_value,
               :sanasuma_value,
               :senasuma_plus_value,
               :senasuma_plus_mobile_value,
               :senasuma_post_value,
               :transaaction_type,
               :sales_variation,
               :free_issue,
               :salesman_code,
               :vallibel_introduced,
               :serial_no,
               :order_category,
               :sales_tax_value,
               :INVENTORY_VALUE,
               :SALES_GROUPING,
               :RET_REASON_CODE,
               :Status_586P,
               :DISCOUNT_TYPE_CODE,
               :DISCOUNT_REASON_CODE,
               :SALESFILETYPE,
               :PROMOTION_CODE,
                :CASH_CONVERSION_DAYS,
                :CASH_CONVERSION_CAL_DAYS,
                :AUTHORIZED_DATE,
                :UTILIZED_DATE,
                :AUTHORIZED_NAME ,	 			
                :LINE_STATUS,	 			
                :REVERT_REVERSED_BAL,	 			
                :TOTAL_AMOUNT_PAID	, 			
                :HPNRET_GROUPING,
                :SALESMAN_TYPE,
                :ENTERED_BY,
                :ENTERED_NAME
               )
                    ";
                #endregion

                foreach (DataRow dRow in dtNotInPKGSales.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[] 
                        { 
                            DBConn.AddParameter("site_code",dRow["site_code"])    
                           ,DBConn.AddParameter("sales_part",dRow["sales_part"])   
                           ,DBConn.AddParameter("units",dRow["units"])    
                           ,DBConn.AddParameter("sales_price",dRow["sales_price"])    
                           ,DBConn.AddParameter("unit_discount_amount",dRow["unit_discount_amount"])    
                           ,DBConn.AddParameter("unit_discount_prententage",dRow["unit_discount_prententage"])    
                           ,DBConn.AddParameter("value",dRow["value"])    
                           ,DBConn.AddParameter("total_discount",dRow["total_discount"])    
                           ,DBConn.AddParameter("location_no",dRow["location_no"])    
                           ,DBConn.AddParameter("order_no",dRow["order_no"])   
                           ,DBConn.AddParameter("tdate",dRow["tdate"])    
                           ,DBConn.AddParameter("promoter_code",dRow["promoter_code"])    
                           ,DBConn.AddParameter("authorized_user_id",dRow["authorized_user_id"])    
                           ,DBConn.AddParameter("state",dRow["state"])    
                           ,DBConn.AddParameter("customer_no",dRow["customer_no"])    
                           ,DBConn.AddParameter("length_of_contract",dRow["length_of_contract"])   
                           ,DBConn.AddParameter("advance_payment_no",dRow["advance_payment_no"])    
                           ,DBConn.AddParameter("advance_payment_amount",dRow["advance_payment_amount"])    
                           ,DBConn.AddParameter("sale_type",dRow["sale_type"])    
                           ,DBConn.AddParameter("sales_location_no",dRow["sales_location_no"])    
                           ,DBConn.AddParameter("service_charge",dRow["service_charge"])   
                           ,DBConn.AddParameter("first_payment",dRow["first_payment"])    
                           ,DBConn.AddParameter("budget_book_hire_price",dRow["budget_book_hire_price"])    
                           ,DBConn.AddParameter("monthly_payment",dRow["monthly_payment"])    
                           ,DBConn.AddParameter("budget_book",dRow["budget_book"])    
                           ,DBConn.AddParameter("price_list",dRow["price_list"])    
                           ,DBConn.AddParameter("gross_hire_price",dRow["gross_hire_price"])    
                           ,DBConn.AddParameter("till_no",dRow["till_no"])    
                           ,DBConn.AddParameter("sale_line_type",dRow["sale_line_type"])    
                           ,DBConn.AddParameter("sale_line_no",dRow["sale_line_no"])  
                           ,DBConn.AddParameter("suraksha_value",dRow["suraksha_value"])    
                           ,DBConn.AddParameter("sanasuma_value",dRow["sanasuma_value"])    
                           ,DBConn.AddParameter("senasuma_plus_value",dRow["senasuma_plus_value"])    
                           ,DBConn.AddParameter("senasuma_plus_mobile_value",dRow["senasuma_plus_mobile_value"])    
                           ,DBConn.AddParameter("senasuma_post_value",dRow["senasuma_post_value"])    
                           ,DBConn.AddParameter("transaaction_type",dRow["transaaction_type"])    
                           ,DBConn.AddParameter("sales_variation",dRow["sales_variation"])    
                           ,DBConn.AddParameter("free_issue",dRow["free_issue"])  
                           ,DBConn.AddParameter("salesman_code",dRow["salesman_code"]) 
                           ,DBConn.AddParameter("vallibel_introduced",dRow["vallibel_introduced"]) 
                           ,DBConn.AddParameter("serial_no",dRow["serial_no"]) 
                           ,DBConn.AddParameter("order_category",dRow["order_category"])   
                           ,DBConn.AddParameter("sales_tax_value",dRow["sales_tax_value"]) 
                           ,DBConn.AddParameter("INVENTORY_VALUE",dRow["INVENTORY_VALUE"]) 
                           ,DBConn.AddParameter("SALES_GROUPING",dRow["SALES_GROUPING"]) 
                           ,DBConn.AddParameter("RET_REASON_CODE",dRow["RET_REASON_CODE"]) 
                           ,DBConn.AddParameter("Status_586P",dRow["Status_586P"])
                           ,DBConn.AddParameter("DISCOUNT_TYPE_CODE",dRow["DISCOUNT_TYPE_CODE"]) 
                           ,DBConn.AddParameter("DISCOUNT_REASON_CODE",dRow["DISCOUNT_REASON_CODE"]) 
                           ,DBConn.AddParameter("SALESFILETYPE",dRow["SALESFILETYPE"]) 
                           ,DBConn.AddParameter("PROMOTION_CODE",dRow["PROMOTION_CODE"]) 
                           ,DBConn.AddParameter("CASH_CONVERSION_DAYS",dRow["CASH_CONVERSION_DAYS"])
                           ,DBConn.AddParameter("CASH_CONVERSION_CAL_DAYS",dRow["CASH_CONVERSION_CAL_DAYS"]) 
                           ,DBConn.AddParameter("AUTHORIZED_DATE", dRow["AUTHORIZED_DATE"] == null ? (object)DBNull.Value : dRow["AUTHORIZED_DATE"])
                           ,DBConn.AddParameter("Utilized_Date",dRow["Utilized_Date"] == null ? (object)DBNull.Value :dRow["Utilized_Date"] ) 
                           ,DBConn.AddParameter("AUTHORIZED_NAME",dRow["AUTHORIZED_NAME"]) 
                           ,DBConn.AddParameter("LINE_STATUS",dRow["LINE_STATUS"]) 
                           ,DBConn.AddParameter("REVERT_REVERSED_BAL",dRow["REVERT_REVERSED_BAL"]) 
                           ,DBConn.AddParameter("TOTAL_AMOUNT_PAID",dRow["TOTAL_AMOUNT_PAID"])
                           ,DBConn.AddParameter("HPNRET_GROUPING",dRow["HPNRET_GROUPING"]) 
                           ,DBConn.AddParameter("SALESMAN_TYPE",dRow["SALESMAN_TYPE"]) 
                           ,DBConn.AddParameter("ENTERED_BY",dRow["ENTERED_BY"])
                           ,DBConn.AddParameter("ENTERED_NAME",dRow["ENTERED_NAME"]) 

                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                    }
                }

                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        public void NeenOpal_BI_DeleteRecordsInBackUpSaleLineTable()
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            string selectQuery = string.Empty;

            try
            {
                #region SQL
                selectQuery = @"
                DELETE FROM NRTAN.BackUp_BI_SALE_LINE
                ";
                #endregion


                dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
        }

        public DataRow GetDirectSalesInNotInPackage_ForComparision(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {


                #region Sql
                selectQuery = @"
                 
             select nvl( sum(tblMain.NetUnit),0) as NetUnit , nvl(sum(tblMain.NetValue),0) as NetValue
from
(
SELECT 
       SUM((AA.HIRE_UNITS - AA.REVERTS_UNITS + AA.REVERT_REVERSE_UNITS) +
           (AA.CASH_UNITS)) NetUnit,
       SUM((AA.NET_HIRE_CASH_VALUE - AA.REVERTS_VALUE +
           AA.REVERT_REVERSE_VALUE) + (AA.CASH_VALUE)) NetValue
  FROM IFSAPP.DIRECT_SALES_DTL_TAB AA
WHERE TRUNC(AA.TRANSACTION_DATE)  = TO_DATE(:dtPreviousDate, 'YYYY/MM/DD')  
   AND AA.CATALOG_TYPE NOT IN ('PKG')
   /*AND (SELECT SUBSTR(I.SECOND_COMMODITY, 1, 2)
          FROM IFSAPP.INVENTORY_PART I
         WHERE I.CONTRACT = AA.SHOP_CODE
           AND I.PART_NO = AA.SALES_PART) not in ('2P','2C','GV')
   AND AA.SALES_PART <> 'PARTS'*/
 
   )tblMain

          
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("dtPreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                    
                };

                O_dRow = (DataRow)dbConnection.Executes(selectQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return O_dRow;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataRow GetIndirectSalesInNotInPackage_ForComparision(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {


                #region Sql
                selectQuery = @"
                 
             select nvl( sum(tblMain.NetUnit),0) as NetUnit , nvl(sum(tblMain.NetValue),0) as NetValue
            from
            (
            SELECT  SUM(BB.UNITS) as NetUnit, SUM(BB.VALUE) as NetValue
              FROM IFSAPP.INDIRECT_SALES_DTL_TAB BB
            WHERE TRUNC(BB.TRANSACTION_DATE) = TO_DATE(:dtPreviousDate, 'YYYY/MM/DD') 
               AND BB.PART_TYPE <> 'PKG'
               AND BB.DEALER <> 'DM09999'
               /*AND (SELECT SUBSTR(I.SECOND_COMMODITY, 1, 2)
                      FROM IFSAPP.INVENTORY_PART I
                     WHERE I.CONTRACT = 'BAD01'
                       AND I.PART_NO = BB.SALES_PART) not in ('2P','2C','GV')
               AND BB.SALES_PART <> 'PARTS'*/
               )tblMain

          
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("dtPreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                    
                };

                O_dRow = (DataRow)dbConnection.Executes(selectQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return O_dRow;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataRow GetDMDSalesInNotInPackage_ForComparision(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionDMD();
            string selectQuery = string.Empty;

            try
            {


                #region Sql
                selectQuery = @"
                 
              SELECT  NVL(sum(tblMain.NetUnit),0) as NetUnit, NVL(sum(tblMain.NetValue),0) as NetValue FROM 
(
SELECT  SUM(MM.UNITS) AS NetUnit , SUM(MM.VALUE) as NetValue
  FROM IFSAPP.INDIRECT_SALES_DTL_TAB MM
 WHERE TRUNC(MM.TRANSACTION_DATE) =  TO_DATE(:dtPreviousDate, 'YYYY/MM/DD')  
   AND MM.PART_TYPE NOT IN ('PKG')
   AND MM.DEALER <> 'DM09001'
   AND MM.FREE_ISSUE = 'N'
  /* AND (SELECT SUBSTR(I.SECOND_COMMODITY, 1, 2)
          FROM IFSAPP.INVENTORY_PART I
         WHERE I.CONTRACT = 'WDA01'
           AND I.PART_NO = MM.SALES_PART) not in  ('2P','2C','GV')
   AND MM.SALES_PART <> 'PARTS'*/
  )tblMain

          
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("dtPreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                };

                O_dRow = (DataRow)dbConnection.Executes(selectQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return O_dRow;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataRow GetNeenOpalBackUpSales_ForComparision(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            //OracleConnection oOracleConnection = dbConnection.GetConnection();
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            string selectQuery = string.Empty;

            try
            {

                #region Sql
                selectQuery = @"
                select  
                nvl(sum(sl.units),0)  AS  NetUnit
                ,nvl(sum(sl.value),0)  AS  NetValue
                --from ifsapp.sin_bi_sale_line sl
                from NRTAN.BI_SALE_LINE  sl
                WHERE trunc(sl.tdate) =  TO_DATE(:dtPreviousDate , 'YYYY/MM/DD')  
                AND sl.sale_type not in ('20','21','22')  
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("dtPreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                };

                O_dRow = (DataRow)dbConnection.Executes(selectQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return O_dRow;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool InsertNeenOpalVsIFSFigureSalesToTable(DailyBIVsSingerSalesLog objDailyBIVsSingerSalesLog)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            bool isSave = false;

            try
            {

                OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("TASK_DATE",objDailyBIVsSingerSalesLog.Task_Date.ToString("yyyy/MM/dd") ) 
                        ,DBConn.AddParameter("SingerDirectSaleValue",objDailyBIVsSingerSalesLog.SingerDirectSaleValue ) 
                        ,DBConn.AddParameter("SingerIndirectSaleValue",objDailyBIVsSingerSalesLog.SingerIndirectSaleValue ) 
                        ,DBConn.AddParameter("DMDIndirectSaleValue",objDailyBIVsSingerSalesLog.DMDIndirectSaleValue ) 
                        ,DBConn.AddParameter("NeenOpalSaleValue",objDailyBIVsSingerSalesLog.NeenOpalSaleValue )
                        ,DBConn.AddParameter("SINGERNETSALE",objDailyBIVsSingerSalesLog.SingerNetSale )
                        ,DBConn.AddParameter("DIFFERENCE",objDailyBIVsSingerSalesLog.Difference )
                        ,DBConn.AddParameter("ENTERED_ON",objDailyBIVsSingerSalesLog.EnteredOn.ToString("yyyy/MM/dd hh:mm:ss"))
                        ,DBConn.AddParameter("NEENOPALROWCOUNT",objDailyBIVsSingerSalesLog.NeenOpalAllSaleMethodRowCount )
                        ,DBConn.AddParameter("BACKUPTABLEROWCOUNT",objDailyBIVsSingerSalesLog.BackUpTableRowCount )

                   };


                #region Sql
                string stringQuerry = @"

                INSERT INTO IFSAPP.SIN_BI_DailySalesFigure
                (
                   TASK_DATE
                   ,SingerDirectSaleValue
                   ,SingerIndirectSaleValue
                   ,DMDIndirectSaleValue
                   ,BISALEVALUE
                   ,SINGERNETSALE
                   ,DIFFERENCE
                   ,ENTEREDON
                   ,BIROWCOUNT
                   ,BACKUPTABLEROWCOUNT
                )
                VALUES
                (
                    TO_DATE(:TASK_DATE,'yyyy/MM/dd')
                    ,:SingerDirectSaleValue
                    ,:SingerIndirectSaleValue
                    ,:DMDIndirectSaleValue
                    ,:NeenOpalSaleValue
                    ,:SINGERNETSALE
                    ,:DIFFERENCE
                    ,TO_DATE(:ENTERED_ON,'yyyy/MM/dd HH24:MI:SS') 
                    ,:NEENOPALROWCOUNT
                    ,:BACKUPTABLEROWCOUNT
                )
                ";
                #endregion


                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objDailyBIVsSingerSalesLog, ex.StackTrace), "Error - In InsertNeenOpalVsIFSFigureSalesToTable", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }


        public DataRow GetSingerSideDailySaleBKTableRowCount(DateTime dtPreviousDate)
        {

            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {


                #region Sql
                selectQuery = @"
                select  NVL(count(*),0) as SIN_BKTableRowCount
                from ifsapp.sin_bi_sale_line sl
                WHERE trunc(sl.tdate) =  TO_DATE(:dtPreviousDate , 'YYYY/MM/DD') 
                ";
                #endregion


                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("dtPreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                };

                O_dRow = (DataRow)dbConnection.Executes(selectQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return O_dRow;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return O_dRow;
        }

        public DataRow GetAllSalesMethodRunRowCount(DateTime dtPreviousDate)
        {

            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            string selectQuery = string.Empty;

            try
            {


                #region Sql
                selectQuery = @"
                SELECT TRUNC(L.TASKDATE) AS TaskDate, SUM(L.ROWCOUNT ) AS TotRowCount  
                FROM NRTAN.BI_ETL_SALE_LINE_LOG L
                WHERE L.TASKDATE =  TO_DATE(:dtPreviousDate , 'YYYY/MM/DD')  
                group   BY L.TASKDATE 
                ";
                #endregion

                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("dtPreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                    
                };

                O_dRow = (DataRow)dbConnection.Executes(selectQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return O_dRow;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return O_dRow;
        }

        public DataTable GetInventoryADJ(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region new sql 2018/06/20
                selectQuery = @"
                  select distinct trunc(ITH.date_applied) as ADJ_TRANS_DATE,--ok
                ITH.contract AS STORE_CODE,--ok
                ITH.part_no AS PRODUCT_CODE,----ok
                ITH.location_no AS INVENTORY_LOCATION, --OK
                ITH.transaction_code AS ADJ_TYPE_CODE,----ok
                ITH.transaction_code AS ADJ_TYPE_DESC , ----OK
                  CASE WHEN ITH.transaction_code  = 'INVSCRAP' THEN 
                  
                  --substr(ITH.source,1,20) 
                  (
                    SELECT SUBSTR(S.REJECT_MESSAGE,1,35) --ifs table size is 35.
                    FROM IFSAPP.SCRAPPING_CAUSE S 
                    WHERE S.REJECT_REASON = ITH.reject_code
                  )  
                  
                  ELSE 
                  
                  CASE WHEN ITH.transaction_code IN ('COUNT-IN','NREC')  THEN 'FLUCTUATION IN'  ELSE 
                  CASE WHEN ITH.transaction_code IN ('COUNT-OUT','NISS') THEN 'FLUCTUATION OUT'   END END END AS ADJ_REASON_CODE,----ok


                IPS.qty_onhand AS INV_ACT_QTY,
                decode(ITH.direction,
                       '+',
                       1 * ITH.quantity,
                       '-',
                       -1 * ITH.quantity,
                       ITH.quantity) INV_ADJ_QTY,----ok
                IPS.qty_onhand *
                (nvl((SELECT p.sales_price
                       FROM ifsapp.sales_price_list_part p
                      WHERE p.price_list_no IN ('1')
                        AND p.catalog_no = ITH.part_no
                        AND ROWNUM = 1),
                     (SELECT p.sales_price
                        FROM ifsapp.sales_price_list_part p
                       WHERE p.price_list_no IN ('I', 'J')
                         AND p.min_quantity = 1
                         AND p.catalog_no = ITH.part_no
                         AND ROWNUM = 1))) AS INV_ACT_VAL_AT_PRICE,
                ITH.quantity * (nvl((SELECT p.sales_price
                                      FROM ifsapp.sales_price_list_part p
                                     WHERE p.price_list_no IN ('1')
                                       AND p.catalog_no = ITH.part_no
                                       AND ROWNUM = 1),
                                    (SELECT p.sales_price
                                       FROM ifsapp.sales_price_list_part p
                                      WHERE p.price_list_no IN ('I', 'J')
                                        AND p.min_quantity = 1
                                        AND p.catalog_no = ITH.part_no
                                        AND ROWNUM = 1))) AS INV_ADJ_VAL_AT_PRICE,----ok
                (IPS.qty_onhand * TRUNC(ITH.cost, 2)) AS INV_ACT_VAL_AT_COST,
                TO_CHAR(ITH.COST, 99999999.99) *
                decode(ITH.direction,
                       '+',
                       1 * ITH.quantity,
                       '-',
                       -1 * ITH.quantity,
                       ITH.quantity) INV_ADJ_VAL_AT_COST----ok
  from IFSAPP.INVENTORY_TRANSACTION_HIST2 ITH,
       IFSAPP.inventory_part_in_stock     IPS
 where ITH.transaction_code in
       ('COUNT-IN', 'COUNT-OUT', 'NREC', 'NISS', 'INVSCRAP')
   AND SUBSTR(ITH.contract, 1, 1) NOT IN ('R', 'P')
   AND ITH.contract NOT IN 'WAB01'
   AND trunc(ITH.date_applied) =  TO_DATE(:PreviousDate, 'YYYY/MM/DD') 
   AND ITH.contract = IPS.contract
   AND ITH.part_no = IPS.part_no
   AND ITH.location_no = IPS.location_no
   /* AND (SELECT substr(i.second_commodity, 1, 2)
                              FROM ifsapp.inventory_part i
                             WHERE i.contract = ITH.contract
                               AND i.part_no = ITH.part_no) NOT IN ( '2P','2C','GV') */
               
                   
                ";
                #endregion
                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                return null;
            }

        }

        public int InsertInventoryADJ_NewVersion(DataTable dtInventoryADJ)
        {
            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Querry


                string insertQuery = @"
                   INSERT INTO NRTAN.BI_INV_ADJUSTMENT
                   (
                   ADJ_TRANS_DATE,
                    STORE_CODE,
                    PRODUCT_CODE,
                    INVENTORY_LOCATION,
                    ADJ_TYPE_CODE,
                    ADJ_TYPE_DESC,
                    ADJ_REASON_CODE,
                    INV_ADJ_QTY,
                    INV_ADJ_VAL_AT_PRICE,
                    INV_ADJ_VAL_AT_COST
                    )
 
                 VALUES
                   (
                    :ADJ_TRANS_DATE,
                    :STORE_CODE,
                    :PRODUCT_CODE,
                    :INVENTORY_LOCATION,
                    :ADJ_TYPE_CODE,
                    :ADJ_TYPE_DESC,
                    :ADJ_REASON_CODE,
                    :INV_ADJ_QTY,
                    :INV_ADJ_VAL_AT_PRICE,
                    :INV_ADJ_VAL_AT_COST
                    )
                    ";
                #endregion

                foreach (DataRow dRow in dtInventoryADJ.Rows)
                {
                    try
                    {

                        OracleParameter[] param = new OracleParameter[] 
                        { 
                           DBConn.AddParameter("ADJ_TRANS_DATE",dRow["ADJ_TRANS_DATE"]) 
                           ,DBConn.AddParameter("STORE_CODE",dRow["STORE_CODE"]) 
                           ,DBConn.AddParameter("PRODUCT_CODE",dRow["PRODUCT_CODE"])   
                           ,DBConn.AddParameter("INVENTORY_LOCATION",dRow["INVENTORY_LOCATION"])   
                           ,DBConn.AddParameter("ADJ_TYPE_CODE",dRow["ADJ_TYPE_CODE"]) 
                           ,DBConn.AddParameter("ADJ_TYPE_DESC",dRow["ADJ_TYPE_DESC"])  
                           ,DBConn.AddParameter("ADJ_REASON_CODE",dRow["ADJ_REASON_CODE"])    
                           ,DBConn.AddParameter("INV_ADJ_QTY",dRow["INV_ADJ_QTY"])
                           ,DBConn.AddParameter("INV_ADJ_VAL_AT_PRICE",dRow["INV_ADJ_VAL_AT_PRICE"]) 
                           ,DBConn.AddParameter("INV_ADJ_VAL_AT_COST",dRow["INV_ADJ_VAL_AT_COST"]) 
                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        LoggingHelper.LogErrorToDb(insertQuery, ex.Message);
                    }
                }
                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            finally
            {

            }
            return intRecordCount;

        }

        public DataTable GetStockAgeBand(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionRTAN();
            string selectQuery = string.Empty;

            try
            {
                #region New Querry
                selectQuery = @"
                SELECT 
                STOCKAGE_BAND_DESC	 
                ,STKAGE_PROD_LEVEL1_CODE	 
                ,STOCKAGE_BAND_LOWER	 
                ,STOCKAGE_BAND_UPPER	 
                ,STOCKAGE_BAND_SEQ	 
                FROM RTAN.STG_DIM_STOCKAGE_BAND
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))//noneed to add @ before parameter value
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                return null;
            }

        }

        public int Insert_BI_StockAgeBand_NewVersionCommon(DataTable dtStockAgeBand)
        {
            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Querry
                string insertQuery = @"
                  INSERT INTO NRTAN.BI_STOCKAGE_BAND
                    (
                    STOCKAGE_BAND_DESC	 
                    ,STKAGE_PROD_LEVEL1_CODE	 
                    ,STOCKAGE_BAND_LOWER	 
                    ,STOCKAGE_BAND_UPPER	 
                    ,STOCKAGE_BAND_SEQ	 
                    
                    )
                    VALUES
                    (
                    :STOCKAGE_BAND_DESC	 
                    ,:STKAGE_PROD_LEVEL1_CODE	 
                    ,:STOCKAGE_BAND_LOWER	 
                    ,:STOCKAGE_BAND_UPPER	 
                    ,:STOCKAGE_BAND_SEQ	 
                     
                    )
                ";
                #endregion

                foreach (DataRow dRow in dtStockAgeBand.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[] 
                       { 
                        DBConn.AddParameter("STOCKAGE_BAND_DESC",dRow["STOCKAGE_BAND_DESC"]) 
                        ,DBConn.AddParameter("STKAGE_PROD_LEVEL1_CODE",dRow["STKAGE_PROD_LEVEL1_CODE"]) 
                        ,DBConn.AddParameter("STOCKAGE_BAND_LOWER",dRow["STOCKAGE_BAND_LOWER"]) 
                        ,DBConn.AddParameter("STOCKAGE_BAND_UPPER",dRow["STOCKAGE_BAND_UPPER"]) 
                        ,DBConn.AddParameter("STOCKAGE_BAND_SEQ",dRow["STOCKAGE_BAND_SEQ"]) 
                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }
                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        public DataTable GetMissingStoreCodesFromDailyTransactionTables()
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            string selectQuery = string.Empty;

            try
            {

                #region  sql
                selectQuery = @"
               /* SELECT tblMain.STORE_CODE STORE_CODE, SUBSTR(tblMain.STORE_CODE,1,5)  AS STORE_ID ,SUBSTR(tblMain.STORE_CODE,7,200)  AS STORE_TYPE  FROM
               (
                select DISTINCT aa1.STORE_CODE as STORE_CODE  FROM  IRTAN.STG_SALE_LINE aa1  WHERE aa1.STORE_CODE   NOT IN (SELECT S.STORE_CODE FROM IRTAN.STG_DIM_STORE S )
                )tblMain  */
                ";
                #endregion

                oDataTable = new DataTable();

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("Error From GetMissingStoreCodesFromDailyTransactionTables()", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }

        }

        public DataTable GetMissingProductCodesFromTransaction()
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            string selectQuery = string.Empty;

            try
            {

                #region  sql
                selectQuery = @"
                SELECT  tblProduct.product_code FROM  
                (
                SELECT distinct t1.sales_part  as product_code FROM BI_SALE_LINE t1 where t1.sales_part NOT IN (SELECT P.PRODUCT_CODE FROM BI_DIM_PRODUCT P)
                union
                SELECT distinct t2.product_code FROM BI_INV_ADJUSTMENT t2 where t2.Product_Code NOT IN (SELECT P.PRODUCT_CODE FROM BI_DIM_PRODUCT P)
                union
                SELECT distinct t3.product_code FROM BI_INV_TRANSACTION t3 where t3.Product_Code NOT IN (SELECT P.PRODUCT_CODE FROM BI_DIM_PRODUCT P)
                )tblProduct
                ";
                #endregion

                oDataTable = new DataTable();

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("Error From GetMissingProductCodesFromTransaction()", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }

        }

        public DataTable GetMapping(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region New Sql
                selectQuery = @"                
                SELECT W.BRANCH_ID AS BRANCH_ID, 
                W.SITE_ID AS SITE_ID
                FROM IFSAPP.HPNRET_LEVELS_OVERVIEW W
                ";
                #endregion
                oDataTable = new DataTable();
                //OracleParameter[] param = new OracleParameter[] 
                //{ 
                //    DBConn.AddParameter("",dtPreviousDate.Year)
                //};

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);

                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public int Insert_NeenOpal_BI_Mapping(DataTable dtMapping)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Querry


                string insertQuery = @"

                insert into nrtan.BI_MAP_BR_TO_AD
                (
                 BRANCH_ID,
                 SITE_ID
                )
          
            values
              (
    
               :BRANCH_ID,
               :SITE_ID
               )
                    ";
                #endregion

                foreach (DataRow dRow in dtMapping.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[] 
                        { 
                             
                           DBConn.AddParameter("BRANCH_ID",dRow["BRANCH_ID"])    
                           ,DBConn.AddParameter("SITE_ID",dRow["SITE_ID"])   
                           
                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                    }
                }

                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        public DataTable GetInventoryTransactionReceiveing(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region New Querry
//                selectQuery = @"
//                   ----RECEIVES------------------
//--BR TO BR
//SELECT 
//TRUNC(col.date_entered) AS arrival_date  ,  ----1
//col.catalog_no AS part_no , ----2
//co.contract AS Supplier_Site, ----3
//co.customer_no AS Receiving_Site,  ----4 
//CO.INTERNAL_PO_NO as PO_NO, ----5
//CO.ORDER_NO AS order_no, ----6
//'REQUEST' AS Transaction_Type, ----7
//COL.state AS Transaction_status , ----8
//'BR_TO_BR' AS Transaction_sub_type,  ----9
//COL.line_no AS line_no,--10
//col.rel_no AS release_no,--11
//col.line_item_no AS line_item_no,--12
//nvl(col.buy_qty_due,0)   -  nvl(col.Qty_Shipped,0) AS qty  -- 13
// 
//  FROM IFSAPP.CUSTOMER_ORDER         CO,
//       IFSAPP.CUSTOMER_ORDER_LINE    COL,
//       IFSAPP.HPNRET_LEVELS_OVERVIEW V,
//       IFSAPP.SITE                   SS
//
//WHERE  
//TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
//   AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
//   AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
//   AND CO.SALESMAN_CODE IS NULL
//   AND COL.LINE_ITEM_NO <= 0
//   AND CO.CONTRACT = COL.CONTRACT
//   AND CO.ORDER_NO = COL.ORDER_NO
//   AND V.SITE_ID = CO.CUSTOMER_NO
//   AND CO.CONTRACT = SS.CONTRACT
// --  AND SS.SITE_TYPE IN ('BRANCH')
//   AND SUBSTR (co.contract,1,1)  IN ('B','S','M','F','R','K') ----SUPP
//   AND SUBSTR(co.customer_no,1,1) IN ('B','S','M','F','R','K')  ----REQ
//   
//   UNION
//   
//   ----WH_TO_BR
//
//   
//SELECT 
//TRUNC(col.date_entered) AS arrival_date  ,  ----1
//col.catalog_no AS part_no , ----2
//co.contract AS Supplier_Site, ----3
//co.customer_no AS Receiving_Site,  ----4 
//CO.INTERNAL_PO_NO as PO_NO, ----5
//CO.ORDER_NO AS order_no, ----6
//'REQUEST' AS Transaction_Type, ----7
//COL.state AS Transaction_status , ----8
//'WH_TO_BR' AS Transaction_sub_type,  ----9
//COL.line_no AS line_no,--10
//col.rel_no AS release_no,--11
//col.line_item_no AS line_item_no,--12
//nvl(col.buy_qty_due,0)   -  nvl(col.Qty_Shipped,0) AS qty  -- 13
// 
//  FROM IFSAPP.CUSTOMER_ORDER         CO,
//       IFSAPP.CUSTOMER_ORDER_LINE    COL,
//       IFSAPP.HPNRET_LEVELS_OVERVIEW V,
//       IFSAPP.SITE                   SS
//
//WHERE  
//TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
//   AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
//   AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
//   AND CO.SALESMAN_CODE IS NULL
//   AND COL.LINE_ITEM_NO <= 0
//   AND CO.CONTRACT = COL.CONTRACT
//   AND CO.ORDER_NO = COL.ORDER_NO
//   AND V.SITE_ID = CO.CUSTOMER_NO
//   AND CO.CONTRACT = SS.CONTRACT
//   AND SS.SITE_TYPE IN ('FACTORY', 'WAREHOUSE')
//   AND  co.contract  IN ('WAA01','WBU01','WBN01') 
//   AND SUBSTR(co.customer_no,1,1) <> 'W'
//   AND V.CHANNEL_ID IN (4,11,12)  
// 
// 
//  UNION
//  
//  --Indirect Chanel POs
//              SELECT 
//              TRUNC(col.date_entered) AS arrival_date, ----1
//              col.catalog_no AS part_no , ----2
//               co.contract AS Supplier_Site, ----3
//              co.customer_no AS Receiving_Site, ----4
//              CO.INTERNAL_PO_NO as PO_NO, ----5
//              co.order_no AS order_no, ----6
//              'REQUEST' AS Transaction_Type, ----7
//              col.state AS Transaction_status ,---8
//              'WH_TO_BR' AS Transaction_sub_type,  ----9
//              col.line_no AS line_no,----10
//            col.rel_no AS release_no,----11
//            col.line_item_no AS line_item_no ,----12
//              nvl(col.buy_qty_due,0)   -  nvl(col.Qty_Shipped,0) AS qty  -- 13
//              
//                FROM IFSAPP.Customer_Order             co,
//                    IFSAPP.Customer_Order_Line        col,
//                    IFSAPP.site                       ss,
//                    IFSAPP.CUST_ORD_CUSTOMER_ENT      cc,
//                    ifsapp.hpnret_level_overview_zone zz,
//                    ifsapp.hpnret_customer_order      t
//            WHERE    
//            TO_DATE(TO_CHAR( co.date_entered,'YYYY/MM/DD'),'YYYY/MM/DD') =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
//            AND co.state IN ('Planned',
//                            'Partially Delivered',
//                            'Released',
//                            'Reserved'
//                            )
//            AND col.state IN ('Planned',
//                                'Partially Delivered',
//                                'Released',
//                            'Reserved'
//                        )
//            AND col.line_item_no <= 0
//            AND cc.CUSTOMER_ID = co.customer_no--THIS JOIN NOT
//            AND co.SALESMAN_CODE is not NULL
//            AND co.contract = ss.contract
//            AND t.contract = col.contract
//            AND ss.site_type IN ('FACTORY', 'WAREHOUSE')
//            AND t.order_no = col.order_no
//            AND co.contract = col.contract
//            AND co.order_no = col.order_no
//            AND zz.zone_id = cc.market_code
//            and col.buy_qty_due > col.Qty_Shipped
//            AND  co.contract  IN ('WAA01','WBU01','WBN01')
//            AND SUBSTR(co.customer_no,1,1) <> 'W'
//            and t.credit_appr = 'TRUE'
//   ------WH_TO_BR
//   UNION
//   
//   
//   ----FCT_TO_BR
//  
//SELECT 
//TRUNC(col.date_entered) AS arrival_date  ,  ----1
//col.catalog_no AS part_no , ----2
//co.contract AS Supplier_Site, ----3
//co.customer_no AS Receiving_Site,  ----4 
//CO.INTERNAL_PO_NO as PO_NO, ----5
//CO.ORDER_NO AS order_no, ----6
//'REQUEST' AS Transaction_Type, ----7
//COL.state AS Transaction_status , ----8
//'FCT_TO_BR' AS Transaction_sub_type,  ----9
//COL.line_no AS line_no,--10
//col.rel_no AS release_no,--11
//col.line_item_no AS line_item_no,--12
//nvl(col.buy_qty_due,0)   -  nvl(col.Qty_Shipped,0) AS qty  -- 13
//
//  FROM IFSAPP.CUSTOMER_ORDER         CO,
//       IFSAPP.CUSTOMER_ORDER_LINE    COL,
//       IFSAPP.HPNRET_LEVELS_OVERVIEW V,
//       IFSAPP.SITE                   SS
//
//WHERE  
//TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
//   AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
//   AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
//   AND CO.SALESMAN_CODE IS NULL
//   AND COL.LINE_ITEM_NO <= 0
//   AND CO.CONTRACT = COL.CONTRACT
//   AND CO.ORDER_NO = COL.ORDER_NO
//   AND V.SITE_ID = CO.CUSTOMER_NO
//   AND CO.CONTRACT = SS.CONTRACT
//   AND SS.SITE_TYPE IN ('FACTORY','BRANCH')
//   AND SUBSTR (co.contract,1,1)  IN ('F') ----SUPP
//   AND SUBSTR(co.customer_no,1,1) IN ('B','S','M','F','R','K')  ----REQ
//   AND co.customer_no NOT IN ('FAP01')
//   
//   ----FCT_TO_BR
//   
//   UNION
//   
//   ----WH TO WH
//
//SELECT 
//TRUNC(col.date_entered) AS arrival_date  ,  ----1
//col.catalog_no AS part_no , ----2
//co.contract AS Supplier_Site, ----3
//co.customer_no AS Receiving_Site,  ----4 
//CO.INTERNAL_PO_NO as PO_NO, ----5
//CO.ORDER_NO AS order_no, ----6
//'REQUEST' AS Transaction_Type, ----7
//COL.state AS Transaction_status , ----8
//'WH_TO_WH' AS Transaction_sub_type,  ----9
//COL.line_no AS line_no,--10
//col.rel_no AS release_no,--11
//col.line_item_no AS line_item_no,--12
//nvl(col.buy_qty_due,0)   -  nvl(col.Qty_Shipped,0) AS qty  -- 13
//
// 
//  FROM IFSAPP.CUSTOMER_ORDER         CO,
//       IFSAPP.CUSTOMER_ORDER_LINE    COL,
//       IFSAPP.HPNRET_LEVELS_OVERVIEW V,
//       IFSAPP.SITE                   SS
//
//WHERE  
//TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
//   AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
//   AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
//   AND CO.SALESMAN_CODE IS NULL
//   AND COL.LINE_ITEM_NO <= 0
//   AND CO.CONTRACT = COL.CONTRACT
//   AND CO.ORDER_NO = COL.ORDER_NO
//   AND V.SITE_ID = CO.CUSTOMER_NO
//   AND CO.CONTRACT = SS.CONTRACT
//   AND SS.SITE_TYPE IN ('WAREHOUSE')
//   AND SUBSTR (co.contract,1,1)  IN ('W') ----SUPP
//   AND SUBSTR(co.customer_no,1,1) = 'W'   ----REQ
//
//   ----WH TO WH
//   
//   UNION
//   
//   
//----BR_TO_WH
//
//SELECT 
//TRUNC(col.date_entered) AS arrival_date  ,  ----1
//col.catalog_no AS part_no , ----2
//co.contract AS Supplier_Site, ----3
//co.customer_no AS Receiving_Site,  ----4 
//CO.INTERNAL_PO_NO as PO_NO, ----5
//CO.ORDER_NO AS order_no, ----6
//'REQUEST' AS Transaction_Type, ----7
//COL.state AS Transaction_status , ----8
//'BR_TO_WH' AS Transaction_sub_type,  ----9
//COL.line_no AS line_no,--10
//col.rel_no AS release_no,--11
//col.line_item_no AS line_item_no,--12
//nvl(col.buy_qty_due,0)   -  nvl(col.Qty_Shipped,0) AS qty  -- 13
// 
//  FROM IFSAPP.CUSTOMER_ORDER         CO,
//       IFSAPP.CUSTOMER_ORDER_LINE    COL,
//       IFSAPP.HPNRET_LEVELS_OVERVIEW V,
//       IFSAPP.SITE                   SS
//
//WHERE  
//TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
//   AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
//   AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
//   AND CO.SALESMAN_CODE IS NULL
//   AND COL.LINE_ITEM_NO <= 0
//   AND CO.CONTRACT = COL.CONTRACT
//   AND CO.ORDER_NO = COL.ORDER_NO
//   AND V.SITE_ID = CO.CUSTOMER_NO
//   AND CO.CONTRACT = SS.CONTRACT
//   AND SS.SITE_TYPE IN ('BRANCH','WAREHOUSE')
//   AND SUBSTR (co.contract,1,1)  IN ('B','S','M','F','R','K') ----SUPP
//   AND SUBSTR(co.customer_no,1,1) IN ('W')  ----REQ
//   
//   ----BR_TO_WH
//   
//   UNION
//   
//   ----FCT_TO_WH
//SELECT 
//TRUNC(col.date_entered) AS arrival_date  ,  ----1
//col.catalog_no AS part_no , ----2
//co.contract AS Supplier_Site, ----3
//co.customer_no AS Receiving_Site,  ----4 
//CO.INTERNAL_PO_NO as PO_NO, ----5
//CO.ORDER_NO AS order_no, ----6
//'REQUEST' AS Transaction_Type, ----7
//COL.state AS Transaction_status , ----8
//'FCT_TO_WH' AS Transaction_sub_type,  ----9
//COL.line_no AS line_no,--10
//col.rel_no AS release_no,--11
//col.line_item_no AS line_item_no,--12
//nvl(col.buy_qty_due,0)   -  nvl(col.Qty_Shipped,0) AS qty  -- 13
//
// 
//  FROM IFSAPP.CUSTOMER_ORDER         CO,
//       IFSAPP.CUSTOMER_ORDER_LINE    COL,
//       IFSAPP.HPNRET_LEVELS_OVERVIEW V,
//       IFSAPP.SITE                   SS
//
//WHERE  
//TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate ,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
//   AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
//   AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
//   AND CO.SALESMAN_CODE IS NULL
//   AND COL.LINE_ITEM_NO <= 0
//   AND CO.CONTRACT = COL.CONTRACT
//   AND CO.ORDER_NO = COL.ORDER_NO
//   AND V.SITE_ID = CO.CUSTOMER_NO
//   AND CO.CONTRACT = SS.CONTRACT
//   AND SS.SITE_TYPE IN ('FACTORY','WAREHOUSE')
//   AND SUBSTR (co.contract,1,1)  IN ('F') ----SUPP
//   AND SUBSTR(co.customer_no,1,1) IN ('W')  ----REQ
//   
//   ----FCT_TO_WH
//   
//   UNION
//   
//   ----BR_TO_FCT
//
//SELECT 
//TRUNC(col.date_entered) AS arrival_date  ,  ----1
//col.catalog_no AS part_no , ----2
//co.contract AS Supplier_Site, ----3
//co.customer_no AS Receiving_Site,  ----4 
//CO.INTERNAL_PO_NO as PO_NO, ----5
//CO.ORDER_NO AS order_no, ----6
//'REQUEST' AS Transaction_Type, ----7
//COL.state AS Transaction_status , ----8
//'BR_TO_FCT' AS Transaction_sub_type,  ----9
//COL.line_no AS line_no,--10
//col.rel_no AS release_no,--11
//col.line_item_no AS line_item_no,--12
//nvl(col.buy_qty_due,0)   -  nvl(col.Qty_Shipped,0) AS qty  -- 13
//
// 
//  FROM IFSAPP.CUSTOMER_ORDER         CO,
//       IFSAPP.CUSTOMER_ORDER_LINE    COL,
//       IFSAPP.HPNRET_LEVELS_OVERVIEW V,
//       IFSAPP.SITE                   SS
//
//WHERE  
//TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
//   AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
//   AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
//   AND CO.SALESMAN_CODE IS NULL
//   AND COL.LINE_ITEM_NO <= 0
//   AND CO.CONTRACT = COL.CONTRACT
//   AND CO.ORDER_NO = COL.ORDER_NO
//   AND V.SITE_ID = CO.CUSTOMER_NO
//   AND CO.CONTRACT = SS.CONTRACT
//   AND SS.SITE_TYPE IN ('FACTORY','BRANCH','SERVICES')
//   AND SUBSTR (co.contract,1,1)  IN ('B','S','M','F','R','K') ----SUPP
//   AND co.contract NOT IN ('FAR01','FAQ01','FAS01','FAP01')
//   AND  co.customer_no  IN  ('FAR01','FAQ01','FAS01','FAP01')  ----REQ
//   
//   ----BR_TO_FCT
//   
//   UNION
//   
//   ----WH_TO_FCT
//SELECT 
//TRUNC(col.date_entered) AS arrival_date  ,  ----1
//col.catalog_no AS part_no , ----2
//co.contract AS Supplier_Site, ----3
//co.customer_no AS Receiving_Site,  ----4 
//CO.INTERNAL_PO_NO as PO_NO, ----5
//CO.ORDER_NO AS order_no, ----6
//'REQUEST' AS Transaction_Type, ----7
//COL.state AS Transaction_status , ----8
//'WH_TO_FCT' AS Transaction_sub_type,  ----9
//COL.line_no AS line_no,--10
//col.rel_no AS release_no,--11
//col.line_item_no AS line_item_no,--12
//nvl(col.buy_qty_due,0)   -  nvl(col.Qty_Shipped,0) AS qty  -- 13
//
// 
//  FROM IFSAPP.CUSTOMER_ORDER         CO,
//       IFSAPP.CUSTOMER_ORDER_LINE    COL,
//       IFSAPP.HPNRET_LEVELS_OVERVIEW V,
//       IFSAPP.SITE                   SS
//
//WHERE  
//TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
//   AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
//   AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
//   AND CO.SALESMAN_CODE IS NULL
//   AND COL.LINE_ITEM_NO <= 0
//   AND CO.CONTRACT = COL.CONTRACT
//   AND CO.ORDER_NO = COL.ORDER_NO
//   AND V.SITE_ID = CO.CUSTOMER_NO
//   AND CO.CONTRACT = SS.CONTRACT
//   AND SS.SITE_TYPE IN ('FACTORY','WAREHOUSE')
//   AND SUBSTR (co.contract,1,1)  IN ('W') ----SUPP
//   AND  (co.customer_no) IN ('FAR01','FAQ01','FAS01','FAP01')  ----REQ
//   
//   ----WH_TO_FCT
//   
//   UNION
//   
//   ----FCT TO FCT
//SELECT 
//TRUNC(col.date_entered) AS arrival_date  ,  ----1
//col.catalog_no AS part_no , ----2
//co.contract AS Supplier_Site, ----3
//co.customer_no AS Receiving_Site,  ----4 
//CO.INTERNAL_PO_NO as PO_NO, ----5
//CO.ORDER_NO AS order_no, ----6
//'REQUEST' AS Transaction_Type, ----7
//COL.state AS Transaction_status , ----8
//'FCT_TO_FCT' AS Transaction_sub_type,  ----9
//COL.line_no AS line_no,--10
//col.rel_no AS release_no,--11
//col.line_item_no AS line_item_no,--12
//nvl(col.buy_qty_due,0)   -  nvl(col.Qty_Shipped,0) AS qty  -- 13
//
// 
//  FROM IFSAPP.CUSTOMER_ORDER         CO,
//       IFSAPP.CUSTOMER_ORDER_LINE    COL,
//       IFSAPP.HPNRET_LEVELS_OVERVIEW V,
//       IFSAPP.SITE                   SS
//
//WHERE  
//TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
//   AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
//   AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
//   AND CO.SALESMAN_CODE IS NULL
//   AND COL.LINE_ITEM_NO <= 0
//   AND CO.CONTRACT = COL.CONTRACT
//   AND CO.ORDER_NO = COL.ORDER_NO
//   AND V.SITE_ID = CO.CUSTOMER_NO
//   AND CO.CONTRACT = SS.CONTRACT
//   AND SS.SITE_TYPE IN ('FACTORY')
//   AND (co.contract)  IN  ('FAR01','FAQ01','FAS01','FAP01') ----SUPP
//   AND (co.customer_no) IN ('FAR01','FAQ01','FAS01','FAP01')  ----REQ
//    
//   
//   --FCT TO FCT
//                
//                ";
                #endregion

                #region SQL 2020/01/27
                selectQuery = @"
------------------------RECEIVES-----------------------------------------------------------

    --BR TO BR--
    SELECT 
        TRUNC(col.date_entered) AS arrival_date  ,                  --1
        col.catalog_no AS part_no ,                                 --2
        co.contract AS Supplier_Site,                               --3
        co.customer_no AS Receiving_Site,                           --4 
        CO.INTERNAL_PO_NO as PO_NO,                                 --5
        CO.ORDER_NO AS order_no,                                    --6
        'REQUEST' AS Transaction_Type,                              --7
        COL.state AS Transaction_status ,                           --8
        'BR_TO_BR' AS Transaction_sub_type,                         --9
        COL.line_no AS line_no,                                     --10
        col.rel_no AS release_no,                                   --11
        col.line_item_no AS line_item_no,                           --12
        nvl(col.buy_qty_due,0)   -  nvl(col.Qty_Shipped,0) AS qty   --13
     
    FROM 
        IFSAPP.CUSTOMER_ORDER         CO,
        IFSAPP.CUSTOMER_ORDER_LINE    COL,
        IFSAPP.HPNRET_LEVELS_OVERVIEW V,
        IFSAPP.SITE                   SS
    
    WHERE  
        TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
        AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
        AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
        AND CO.SALESMAN_CODE IS NULL
        AND COL.LINE_ITEM_NO <= 0
        AND CO.CONTRACT = COL.CONTRACT
        AND CO.ORDER_NO = COL.ORDER_NO
        AND V.SITE_ID = CO.CUSTOMER_NO
        AND CO.CONTRACT = SS.CONTRACT
        AND ifsapp.site_api.Get_Site_Type(co.contract) IN ('BRANCH')      --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(co.customer_no) IN ('BRANCH')   --Added on 2020/01/27 By AnushkaR
    ------------
  
   
UNION
   
   
    --WH_TO_BR--
    SELECT 
        TRUNC(col.date_entered) AS arrival_date,                   --1
        col.catalog_no AS part_no,                                 --2
        co.contract AS Supplier_Site,                              --3
        co.customer_no AS Receiving_Site,                          --4 
        CO.INTERNAL_PO_NO as PO_NO,                                --5
        CO.ORDER_NO AS order_no,                                   --6
        'REQUEST' AS Transaction_Type,                             --7
        COL.state AS Transaction_status ,                          --8
        'WH_TO_BR' AS Transaction_sub_type,                        --9
        COL.line_no AS line_no,                                    --10
        col.rel_no AS release_no,                                  --11
        col.line_item_no AS line_item_no,                          --12
        nvl(col.buy_qty_due,0)   -  nvl(col.Qty_Shipped,0) AS qty  --13
     
    FROM 
        IFSAPP.CUSTOMER_ORDER         CO,
        IFSAPP.CUSTOMER_ORDER_LINE    COL,
        IFSAPP.HPNRET_LEVELS_OVERVIEW V,
        IFSAPP.SITE                   SS
    
    WHERE  
        TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
        AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
        AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
        AND CO.SALESMAN_CODE IS NULL
        AND COL.LINE_ITEM_NO <= 0
        AND CO.CONTRACT = COL.CONTRACT
        AND CO.ORDER_NO = COL.ORDER_NO
        AND V.SITE_ID = CO.CUSTOMER_NO
        AND CO.CONTRACT = SS.CONTRACT
        AND ifsapp.site_api.Get_Site_Type(co.contract) IN ('WAREHOUSE')     --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(co.customer_no) IN ('BRANCH')     --Added on 2020/01/27 By AnushkaR
        AND V.CHANNEL_ID IN (4,11,12)  
    ------------
 
 
UNION
  
  
    --Indirect Chanel POs (WH_TO_DL)--
    SELECT 
        TRUNC(col.date_entered) AS arrival_date,                   --1
        col.catalog_no AS part_no,                                 --2
        co.contract AS Supplier_Site,                              --3
        co.customer_no AS Receiving_Site,                          --4
        CO.INTERNAL_PO_NO as PO_NO,                                --5
        co.order_no AS order_no,                                   --6
        'REQUEST' AS Transaction_Type,                             --7
        col.state AS Transaction_status ,                          --8
        'WH_TO_DL' AS Transaction_sub_type,                        --9
        col.line_no AS line_no,                                    --10
        col.rel_no AS release_no,                                  --11
        col.line_item_no AS line_item_no ,                         --12
        nvl(col.buy_qty_due,0) - nvl(col.Qty_Shipped,0) AS qty     --13
              
    FROM 
        IFSAPP.Customer_Order             co,
        IFSAPP.Customer_Order_Line        col,
        IFSAPP.site                       ss,
        IFSAPP.CUST_ORD_CUSTOMER_ENT      cc,
        ifsapp.hpnret_level_overview_zone zz,
        ifsapp.hpnret_customer_order      t
        
    WHERE    
        TO_DATE(TO_CHAR( co.date_entered,'YYYY/MM/DD'),'YYYY/MM/DD') =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
        AND co.state IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
        AND col.state IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
        AND col.line_item_no <= 0
        AND cc.CUSTOMER_ID = co.customer_no--THIS JOIN NOT
        AND co.SALESMAN_CODE is not NULL
        AND co.contract = ss.contract
        AND t.contract = col.contract
        AND t.order_no = col.order_no
        AND co.contract = col.contract
        AND co.order_no = col.order_no
        AND zz.zone_id = cc.market_code
        AND col.buy_qty_due > col.Qty_Shipped
        AND ifsapp.site_api.Get_Site_Type(co.contract) IN ('WAREHOUSE')                                --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(co.customer_no) NOT IN ('WAREHOUSE', 'BRANCH', 'FACTORY')    --Added on 2020/01/27 By AnushkaR
        AND (t.credit_appr = 'TRUE' or t.cred_appr_hauth = 'TRUE')                                     --Added on 2020/01/27 By AnushkaR
    ------------


UNION
   
   
    --FCT_TO_BR--
    SELECT 
        TRUNC(col.date_entered) AS arrival_date,                --1
        col.catalog_no AS part_no,                              --2
        co.contract AS Supplier_Site,                           --3
        co.customer_no AS Receiving_Site,                       --4 
        CO.INTERNAL_PO_NO as PO_NO,                             --5
        CO.ORDER_NO AS order_no,                                --6
        'REQUEST' AS Transaction_Type,                          --7
        COL.state AS Transaction_status ,                       --8
        'FCT_TO_BR' AS Transaction_sub_type,                    --9
        COL.line_no AS line_no,                                 --10
        col.rel_no AS release_no,                               --11
        col.line_item_no AS line_item_no,                       --12
        nvl(col.buy_qty_due,0) - nvl(col.Qty_Shipped,0) AS qty  --13

    FROM 
        IFSAPP.CUSTOMER_ORDER         CO,
        IFSAPP.CUSTOMER_ORDER_LINE    COL,
        IFSAPP.HPNRET_LEVELS_OVERVIEW V,
        IFSAPP.SITE                   SS

    WHERE  
        TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
        AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
        AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
        AND CO.SALESMAN_CODE IS NULL
        AND COL.LINE_ITEM_NO <= 0
        AND CO.CONTRACT = COL.CONTRACT
        AND CO.ORDER_NO = COL.ORDER_NO
        AND V.SITE_ID = CO.CUSTOMER_NO
        AND CO.CONTRACT = SS.CONTRACT
        AND ifsapp.site_api.Get_Site_Type(co.contract) IN ('FACTORY')      --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(co.customer_no) IN ('BRANCH')    --Added on 2020/01/27 By AnushkaR
    ------------
   
   
UNION
   
   
    --WH TO WH--
    SELECT 
        TRUNC(col.date_entered) AS arrival_date,                --1
        col.catalog_no AS part_no,                              --2
        co.contract AS Supplier_Site,                           --3
        co.customer_no AS Receiving_Site,                       --4 
        CO.INTERNAL_PO_NO as PO_NO,                             --5
        CO.ORDER_NO AS order_no,                                --6
        'REQUEST' AS Transaction_Type,                          --7
        COL.state AS Transaction_status ,                       --8
        'WH_TO_WH' AS Transaction_sub_type,                     --9
        COL.line_no AS line_no,                                 --10
        col.rel_no AS release_no,                               --11
        col.line_item_no AS line_item_no,                       --12
        nvl(col.buy_qty_due,0) - nvl(col.Qty_Shipped,0) AS qty  --13

    FROM 
        IFSAPP.CUSTOMER_ORDER         CO,
        IFSAPP.CUSTOMER_ORDER_LINE    COL,
        IFSAPP.HPNRET_LEVELS_OVERVIEW V,
        IFSAPP.SITE                   SS

    WHERE  
        TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
        AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
        AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
        AND CO.SALESMAN_CODE IS NULL
        AND COL.LINE_ITEM_NO <= 0
        AND CO.CONTRACT = COL.CONTRACT
        AND CO.ORDER_NO = COL.ORDER_NO
        AND V.SITE_ID = CO.CUSTOMER_NO
        AND CO.CONTRACT = SS.CONTRACT
        AND ifsapp.site_api.Get_Site_Type(co.contract) IN ('WAREHOUSE')        --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(co.customer_no) IN ('WAREHOUSE')     --Added on 2020/01/27 By AnushkaR
    ------------


UNION
   
   
    --BR_TO_WH--
    SELECT 
        TRUNC(col.date_entered) AS arrival_date,                --1
        col.catalog_no AS part_no,                              --2
        co.contract AS Supplier_Site,                           --3
        co.customer_no AS Receiving_Site,                       --4 
        CO.INTERNAL_PO_NO as PO_NO,                             --5
        CO.ORDER_NO AS order_no,                                --6
        'REQUEST' AS Transaction_Type,                          --7
        COL.state AS Transaction_status ,                       --8
        'BR_TO_WH' AS Transaction_sub_type,                     --9
        COL.line_no AS line_no,                                 --10
        col.rel_no AS release_no,                               --11
        col.line_item_no AS line_item_no,                       --12
        nvl(col.buy_qty_due,0) - nvl(col.Qty_Shipped,0) AS qty  --13
 
    FROM 
        IFSAPP.CUSTOMER_ORDER         CO,
        IFSAPP.CUSTOMER_ORDER_LINE    COL,
        IFSAPP.HPNRET_LEVELS_OVERVIEW V,
        IFSAPP.SITE                   SS

    WHERE  
        TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
        AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
        AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
        AND CO.SALESMAN_CODE IS NULL
        AND COL.LINE_ITEM_NO <= 0
        AND CO.CONTRACT = COL.CONTRACT
        AND CO.ORDER_NO = COL.ORDER_NO
        AND V.SITE_ID = CO.CUSTOMER_NO
        AND CO.CONTRACT = SS.CONTRACT
        AND ifsapp.site_api.Get_Site_Type(co.contract) IN ('BRANCH')         --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(co.customer_no) IN ('WAREHOUSE')   --Added on 2020/01/27 By AnushkaR
    ------------
           
   
UNION
   
   
    --FCT_TO_WH--
    SELECT 
        TRUNC(col.date_entered) AS arrival_date,                --1
        col.catalog_no AS part_no,                              --2
        co.contract AS Supplier_Site,                           --3
        co.customer_no AS Receiving_Site,                       --4 
        CO.INTERNAL_PO_NO as PO_NO,                             --5
        CO.ORDER_NO AS order_no,                                --6
        'REQUEST' AS Transaction_Type,                          --7
        COL.state AS Transaction_status ,                       --8
        'FCT_TO_WH' AS Transaction_sub_type,                    --9
        COL.line_no AS line_no,                                 --10
        col.rel_no AS release_no,                               --11
        col.line_item_no AS line_item_no,                       --12
        nvl(col.buy_qty_due,0) - nvl(col.Qty_Shipped,0) AS qty  --13

    FROM 
        IFSAPP.CUSTOMER_ORDER         CO,
        IFSAPP.CUSTOMER_ORDER_LINE    COL,
        IFSAPP.HPNRET_LEVELS_OVERVIEW V,
        IFSAPP.SITE                   SS

    WHERE  
        TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate ,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
        AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
        AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
        AND CO.SALESMAN_CODE IS NULL
        AND COL.LINE_ITEM_NO <= 0
        AND CO.CONTRACT = COL.CONTRACT
        AND CO.ORDER_NO = COL.ORDER_NO
        AND V.SITE_ID = CO.CUSTOMER_NO
        AND CO.CONTRACT = SS.CONTRACT
        AND ifsapp.site_api.Get_Site_Type(co.contract) IN ('FACTORY')         --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(co.customer_no) IN ('WAREHOUSE')    --Added on 2020/01/27 By AnushkaR
    ------------

   
UNION
   

    --BR_TO_FCT--
    SELECT 
        TRUNC(col.date_entered) AS arrival_date,                --1
        col.catalog_no AS part_no,                              --2
        co.contract AS Supplier_Site,                           --3
        co.customer_no AS Receiving_Site,                       --4 
        CO.INTERNAL_PO_NO as PO_NO,                             --5
        CO.ORDER_NO AS order_no,                                --6
        'REQUEST' AS Transaction_Type,                          --7
        COL.state AS Transaction_status,                        --8
        'BR_TO_FCT' AS Transaction_sub_type,                    --9
        COL.line_no AS line_no,                                 --10
        col.rel_no AS release_no,                               --11
        col.line_item_no AS line_item_no,                       --12
        nvl(col.buy_qty_due,0) - nvl(col.Qty_Shipped,0) AS qty  --13

    FROM 
        IFSAPP.CUSTOMER_ORDER         CO,
        IFSAPP.CUSTOMER_ORDER_LINE    COL,
        IFSAPP.HPNRET_LEVELS_OVERVIEW V,
        IFSAPP.SITE                   SS

    WHERE  
        TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
        AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
        AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
        AND CO.SALESMAN_CODE IS NULL
        AND COL.LINE_ITEM_NO <= 0
        AND CO.CONTRACT = COL.CONTRACT
        AND CO.ORDER_NO = COL.ORDER_NO
        AND V.SITE_ID = CO.CUSTOMER_NO
        AND CO.CONTRACT = SS.CONTRACT
        AND ifsapp.site_api.Get_Site_Type(co.contract) IN ('BRANCH')         --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(co.customer_no) IN ('FACTORY')     --Added on 2020/01/27 By AnushkaR
    ------------
   
   
UNION
   
   
    --WH_TO_FCT--
    SELECT 
        TRUNC(col.date_entered) AS arrival_date,                --1
        col.catalog_no AS part_no,                              --2
        co.contract AS Supplier_Site,                           --3
        co.customer_no AS Receiving_Site,                       --4 
        CO.INTERNAL_PO_NO as PO_NO,                             --5
        CO.ORDER_NO AS order_no,                                --6
        'REQUEST' AS Transaction_Type,                          --7
        COL.state AS Transaction_status ,                       --8
        'WH_TO_FCT' AS Transaction_sub_type,                    --9
        COL.line_no AS line_no,                                 --10
        col.rel_no AS release_no,                               --11
        col.line_item_no AS line_item_no,                       --12
        nvl(col.buy_qty_due,0) - nvl(col.Qty_Shipped,0) AS qty  --13

    FROM 
        IFSAPP.CUSTOMER_ORDER         CO,
        IFSAPP.CUSTOMER_ORDER_LINE    COL,
        IFSAPP.HPNRET_LEVELS_OVERVIEW V,
        IFSAPP.SITE                   SS

    WHERE  
        TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
        AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
        AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
        AND CO.SALESMAN_CODE IS NULL
        AND COL.LINE_ITEM_NO <= 0
        AND CO.CONTRACT = COL.CONTRACT
        AND CO.ORDER_NO = COL.ORDER_NO
        AND V.SITE_ID = CO.CUSTOMER_NO
        AND CO.CONTRACT = SS.CONTRACT
        AND ifsapp.site_api.Get_Site_Type(co.contract) IN ('BRANCH')         --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(co.customer_no) IN ('FACTORY')     --Added on 2020/01/27 By AnushkaR
    ------------
       
   
UNION
   
   
    --FCT TO FCT--
    SELECT 
        TRUNC(col.date_entered) AS arrival_date,                --1
        col.catalog_no AS part_no,                              --2
        co.contract AS Supplier_Site,                           --3
        co.customer_no AS Receiving_Site,                       --4 
        CO.INTERNAL_PO_NO as PO_NO,                             --5
        CO.ORDER_NO AS order_no,                                --6
        'REQUEST' AS Transaction_Type,                          --7
        COL.state AS Transaction_status,                        --8
        'FCT_TO_FCT' AS Transaction_sub_type,                   --9
        COL.line_no AS line_no,                                 --10
        col.rel_no AS release_no,                               --11
        col.line_item_no AS line_item_no,                       --12
        nvl(col.buy_qty_due,0) - nvl(col.Qty_Shipped,0) AS qty  --13

    FROM 
        IFSAPP.CUSTOMER_ORDER         CO,
        IFSAPP.CUSTOMER_ORDER_LINE    COL,
        IFSAPP.HPNRET_LEVELS_OVERVIEW V,
        IFSAPP.SITE                   SS

    WHERE  
        TO_DATE(TO_CHAR( CO.DATE_ENTERED,'YYYY/MM/DD'),'YYYY/MM/DD')  =  TO_DATE(:PreviousDate,'YYYY/MM/DD') --AND TO_DATE( :InBoundToDate,'YYYY/MM/DD HH24:MI:SS')      
        AND CO.STATE IN ('Planned', 'Partially Delivered', 'Released', 'Reserved')
        AND COL.state IN  ('Planned','Partially Delivered','Released','Reserved')
        AND CO.SALESMAN_CODE IS NULL
        AND COL.LINE_ITEM_NO <= 0
        AND CO.CONTRACT = COL.CONTRACT
        AND CO.ORDER_NO = COL.ORDER_NO
        AND V.SITE_ID = CO.CUSTOMER_NO
        AND CO.CONTRACT = SS.CONTRACT
        AND SS.SITE_TYPE IN ('FACTORY')
        AND ifsapp.site_api.Get_Site_Type(co.contract) IN ('FACTORY')        --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(co.customer_no) IN ('FACTORY')     --Added on 2020/01/27 By AnushkaR
    ------------
    
    
------------------------------------------------------------------------------------------------------------------
    
";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))//noneed to add @ before parameter value
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                return null;
            }

        }

        public int Insert_BI_Inventory_Transaction(DataTable dtInventoryTransaction)
        {
            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Querry
                string insertQuery = @"
                  insert into NRTAN.bi_inv_transaction
  (
   inv_date,
   product_code,
   supplier_site,
   receiving_site,
   po_no,
   order_no,
   transaction_type,
   transaction_status,
   transanaction_sub_type,
   qty,
   line_no,
   release_no,
   line_item_no)
values
  (
   :inv_date,
   :product_code,
   :supplier_site,
   :receiving_site,
   :po_no,
   :order_no,
   :transaction_type,
   :transaction_status,
   :transanaction_sub_type,
   :qty,
   :line_no,
   :release_no,
   :line_item_no)
                ";
                #endregion

                foreach (DataRow dRow in dtInventoryTransaction.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[] 
                       { 
                        DBConn.AddParameter("inv_date",dRow["arrival_date"]) 
                        ,DBConn.AddParameter("product_code",dRow["part_no"]) 
                        ,DBConn.AddParameter("supplier_site",dRow["supplier_site"]) 
                        ,DBConn.AddParameter("receiving_site",dRow["receiving_site"]) 
                        ,DBConn.AddParameter("po_no",dRow["po_no"]) 
                        ,DBConn.AddParameter("order_no",dRow["order_no"]) 
                        ,DBConn.AddParameter("transaction_type",dRow["transaction_type"]) 
                        ,DBConn.AddParameter("transaction_status",dRow["transaction_status"]) 
                        ,DBConn.AddParameter("transanaction_sub_type",dRow["transanaction_sub_type"]) 
                        ,DBConn.AddParameter("qty",dRow["qty"]) 
                        ,DBConn.AddParameter("line_no",dRow["line_no"]) 
                        ,DBConn.AddParameter("release_no",dRow["release_no"]) 
                        ,DBConn.AddParameter("line_item_no",dRow["line_item_no"]) 
                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }
                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        public DataTable GetInventoryTransactionSupplies(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region sql
//                selectQuery = @"
//                 
//--SUPPLIES
//
//SELECT 
//
// t.DATE_APPLIED AS arrival_date    ----1
// ,t.part_no AS part_no   ----2
// ,T.contract    AS Supplier_Site  ----3
// ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no,
//                                                      T.release_no,
//                                                      T.sequence_no,
//                                                      T.line_item_no)    AS Receiving_Site   ----4 
// ,c.CUSTOMER_PO_NO  as PO_NO  ----5
// ,T.order_no AS order_no  ----6
// , 'SUPPLIES' AS Transaction_Type  ----7
// , C.state  AS   Transaction_status   ----8
// 
// ,'BR_TO_BR' AS Transaction_sub_type   ----9
// , T.release_no AS line_no --10
// ,T.sequence_no AS release_no --11
// ,T.line_item_no AS line_item_no --12 
// ,T.quantity  AS qty  -- 13
//
//  FROM ifsapp.INVENTORY_TRANSACTION_HIST2 T,
//       IFSAPP.CUSTOMER_ORDER              c,
//       IFSAPP.IDENTITY_INVOICE_INFO       nn,
//       IFSAPP.IDENTITY_INVOICE_INFO       nk
// WHERE TRANSACTION_CODE = 'SHIPTRAN'
//   and t.DATE_APPLIED =  TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
//   AND t.order_no = c.order_no
//   AND IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no,
//                                                      T.release_no,
//                                                      T.sequence_no,
//                                                      T.line_item_no) =
//       nk.identity
//   AND T.contract = nn.identity
//   AND nn.GROUP_ID IN ('BM', 'ERM')
//   AND nn.party_type IN ('Customer')
//   AND nk.GROUP_ID IN ('BM', 'ERM')
//   AND nk.party_type IN ('Customer')
//   AND IFSAPP.inventory_part_api.Get_Acc_Group(t.contract, t.part_no) <>
//       'BOOKS'
//    AND  SUBSTR(T.contract,1,1) IN ('B','S','M','F','R','K') ----SUPPLIER
// AND  SUBSTR(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no,
//                                                      T.release_no,
//                                                      T.sequence_no,
//                                                      T.line_item_no),1,1)   IN ('B','S','M','F','R','K')  ----RECEIVER
//                                                      
// UNION
// 
// 
// SELECT 
// t.DATE_APPLIED AS arrival_date    ----1
// ,t.part_no AS part_no   ----2
// ,T.contract    AS Supplier_Site  ----3
// ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no,
//                                                      T.release_no,
//                                                      T.sequence_no,
//                                                      T.line_item_no)    AS Receiving_Site   ----4 
// ,c.CUSTOMER_PO_NO  as PO_NO  ----5
// ,T.order_no AS order_no  ----6
// , 'SUPPLIES' AS Transaction_Type  ----7
// , C.state  AS   Transaction_status   ----8
// 
// ,'BR_TO_FCT' AS Transaction_sub_type   ----9
// , T.release_no AS line_no --10
// ,T.sequence_no AS release_no --11
// ,T.line_item_no AS line_item_no --12 
// ,T.quantity  AS qty  -- 13
//
//  FROM ifsapp.INVENTORY_TRANSACTION_HIST2 T,
//       IFSAPP.CUSTOMER_ORDER              c,
//       IFSAPP.IDENTITY_INVOICE_INFO       nn,
//       IFSAPP.IDENTITY_INVOICE_INFO       nk
// WHERE TRANSACTION_CODE = 'SHIPTRAN'
//   and t.DATE_APPLIED =  TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
//   AND t.order_no = c.order_no
//   AND IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no,
//                                                      T.release_no,
//                                                      T.sequence_no,
//                                                      T.line_item_no) =
//       nk.identity
//   AND T.contract = nn.identity
//   AND nn.GROUP_ID IN ('BM', 'ERM')
//   AND nn.party_type IN ('Customer')
//   AND nk.GROUP_ID IN ('BM', 'ERM')
//   AND nk.party_type IN ('Customer')
//   AND IFSAPP.inventory_part_api.Get_Acc_Group(t.contract, t.part_no) <>
//       'BOOKS'
//    AND  SUBSTR(T.contract,1,1) IN ('B','S','M','F','R','K') ----SUPPLIER
//    AND T.contract NOT IN ('FAP01')
// AND  IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no,
//                                                      T.release_no,
//                                                      T.sequence_no,
//                                                      T.line_item_no)   IN ('FAR01','FAQ01','FAS01','FAP01')  ----RECEIVER                                                   
//                                                      
//  UNION
//  
//  SELECT 
//
// t.DATE_APPLIED AS arrival_date    ----1
// ,t.part_no AS part_no   ----2
// ,T.contract    AS Supplier_Site  ----3
// ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no,
//                                                      T.release_no,
//                                                      T.sequence_no,
//                                                      T.line_item_no)    AS Receiving_Site   ----4 
// ,c.CUSTOMER_PO_NO  as PO_NO  ----5
// ,T.order_no AS order_no  ----6
// , 'SUPPLIES' AS Transaction_Type  ----7
// , C.state  AS   Transaction_status   ----8
// 
// ,'BR_TO_WH' AS Transaction_sub_type   ----9
// , T.release_no AS line_no --10
// ,T.sequence_no AS release_no --11
// ,T.line_item_no AS line_item_no --12 
// ,T.quantity  AS qty  -- 13
//
//
//  FROM ifsapp.INVENTORY_TRANSACTION_HIST2 T,
//       IFSAPP.CUSTOMER_ORDER              c,
//       IFSAPP.IDENTITY_INVOICE_INFO       nn,
//       IFSAPP.IDENTITY_INVOICE_INFO       nk
// WHERE TRANSACTION_CODE = 'SHIPTRAN'
//   and t.DATE_APPLIED =  TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
//   AND t.order_no = c.order_no
//   AND IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no,
//                                                      T.release_no,
//                                                      T.sequence_no,
//                                                      T.line_item_no) =
//       nk.identity
//   AND T.contract = nn.identity
//   AND nn.GROUP_ID IN ('BM', 'ERM')
//   AND nn.party_type IN ('Customer')
//   AND nk.GROUP_ID IN ('BM', 'ERM')
//   AND nk.party_type IN ('Customer')
//   AND IFSAPP.inventory_part_api.Get_Acc_Group(t.contract, t.part_no) <>
//       'BOOKS'
//    AND  SUBSTR(T.contract,1,1) IN ('B','S','M','F','R','K') ----SUPPLIER
// AND  SUBSTR(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no,
//                                                      T.release_no,
//                                                      T.sequence_no,
//                                                      T.line_item_no),1,1)   IN ('W')  ----RECEIVER                                                  
//                                                      
//  UNION
//  
//  ----FCT TO FCT
//
//SELECT  trunc(tp.act_delivery_date) as arrival_date --1
//       ,  IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Part_No(co.order_no,
//                                                  co.line_no,
//                                                  co.rel_no,
//                                                  co.LINE_ITEM_NO) Part_no --2
//       ,tp.contract as Supplier_Site --3
//       ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO) as Receiving_Site --4  
//           
//     , cu.Customer_po_no as  PO_NO    --5
//     , co.order_no as order_no --6  
//     , 'SUPPLIES' AS Transaction_Type  ----7
//    ,tp.state  AS Transaction_status   ----8
//    , 'FCT_TO_FCT' AS Transaction_sub_type  ----9
//    ,co.line_no   AS line_no --10
//    ,co.rel_no AS release_no --11
//    ,co.line_item_no AS line_item_no --12
//   ,  co.ACTUAL_QTY_RESERVED  AS qty  -- 13
//                                                  
//  FROM ifsapp.TRN_TRIP_PLAN         tp,
//       ifsapp.TRN_TRIP_PLAN_CO_LINE co,
//       IFSAPP.Site                  ss,
//       IFSAPP.Customer_Order        cu
//
//WHERE
//
//tp.contract = ss.contract
//AND tp.trip_no = co.trip_no
//AND tp.RELEASE_NO = co.RELEASE_NO
//AND cu.order_no = co.order_no
//AND tp.state IN ('Closed', 'Delivered')
//AND ss.site_type IN ( 'FACTORY')
//and  (tp.contract)    IN  ('FAR01','FAQ01','FAS01','FAP01') --supplier
//and   IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO)      IN ('FAR01','FAQ01','FAS01','FAP01')   ----receiver
//                                       
//AND trunc(tp.act_delivery_date)  =   TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
// 
//UNION
//
//----FCT TO BR
//
//SELECT  trunc(tp.act_delivery_date) as arrival_date --1
//       ,  IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Part_No(co.order_no,
//                                                  co.line_no,
//                                                  co.rel_no,
//                                                  co.LINE_ITEM_NO) Part_no --2
//       ,tp.contract as Supplier_Site --3
//       ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO) as Receiving_Site --4  
//           
//     , cu.Customer_po_no as  PO_NO    --5
//     , co.order_no as order_no --6  
//     , 'SUPPLIES' AS Transaction_Type  ----7
//    ,tp.state  AS Transaction_status   ----8
//    , 'FCT_TO_BR' AS Transaction_sub_type  ----9
//    ,co.line_no   AS line_no --10
//    ,co.rel_no AS release_no --11
//    ,co.line_item_no AS line_item_no --12
//   ,  co.ACTUAL_QTY_RESERVED  AS qty  -- 13
//                                                  
//  FROM ifsapp.TRN_TRIP_PLAN         tp,
//       ifsapp.TRN_TRIP_PLAN_CO_LINE co,
//       IFSAPP.Site                  ss,
//       IFSAPP.Customer_Order        cu
//
//WHERE
//
//tp.contract = ss.contract
//AND tp.trip_no = co.trip_no
//AND tp.RELEASE_NO = co.RELEASE_NO
//AND cu.order_no = co.order_no
//AND tp.state IN ('Closed', 'Delivered')
//AND ss.site_type IN ( 'FACTORY')
//and (tp.contract)    IN  ('FAR01','FAQ01','FAS01','FAP01')  --supplier
//and  SUBSTR(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO),1,1)      IN ('B','S','M','F','R','K')   ----receiver
//                                                       
// and    IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO)      NOT IN ('FAP01')                                         
//AND trunc(tp.act_delivery_date)  =   TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
//
//UNION
//
//
//----FCT TO WH
//
//SELECT  trunc(tp.act_delivery_date) as arrival_date --1
//       ,  IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Part_No(co.order_no,
//                                                  co.line_no,
//                                                  co.rel_no,
//                                                  co.LINE_ITEM_NO) Part_no --2
//       ,tp.contract as Supplier_Site --3
//       ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO) as Receiving_Site --4  
//           
//     , cu.Customer_po_no as  PO_NO    --5
//     , co.order_no as order_no --6  
//     , 'SUPPLIES' AS Transaction_Type  ----7
//    ,tp.state  AS Transaction_status   ----8
//    , 'FCT_TO_WH' AS Transaction_sub_type  ----9
//    ,co.line_no   AS line_no --10
//    ,co.rel_no AS release_no --11
//    ,co.line_item_no AS line_item_no --12
//   ,  co.ACTUAL_QTY_RESERVED  AS qty  -- 13
//                                                  
//  FROM ifsapp.TRN_TRIP_PLAN         tp,
//       ifsapp.TRN_TRIP_PLAN_CO_LINE co,
//       IFSAPP.Site                  ss,
//       IFSAPP.Customer_Order        cu
//
//WHERE
//
//tp.contract = ss.contract
//AND tp.trip_no = co.trip_no
//AND tp.RELEASE_NO = co.RELEASE_NO
//AND cu.order_no = co.order_no
//AND tp.state IN ('Closed', 'Delivered')
//AND ss.site_type IN ( 'FACTORY')
//and (tp.contract)    IN ('FAR01','FAQ01','FAS01','FAP01')  --supplier
//and  SUBSTR(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO),1,1)      IN ('W')   ----receiver
//                                       
//AND trunc(tp.act_delivery_date)  =   TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
//
//UNION
//
//
//----WH TO BR
//
//SELECT  trunc(tp.act_delivery_date) as arrival_date --1
//       ,  IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Part_No(co.order_no,
//                                                  co.line_no,
//                                                  co.rel_no,
//                                                  co.LINE_ITEM_NO) Part_no --2
//       ,tp.contract as Supplier_Site --3
//       ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO) as Receiving_Site --4  
//           
//     , cu.Customer_po_no as  PO_NO    --5
//     , co.order_no as order_no --6  
//     , 'SUPPLIES' AS Transaction_Type  ----7
//    ,tp.state  AS Transaction_status   ----8
//    , 'WH_TO_BR' AS Transaction_sub_type  ----9
//    ,co.line_no   AS line_no --10
//    ,co.rel_no AS release_no --11
//    ,co.line_item_no AS line_item_no --12
//   ,  co.ACTUAL_QTY_RESERVED  AS qty  -- 13
//                                                  
//  FROM ifsapp.TRN_TRIP_PLAN         tp,
//       ifsapp.TRN_TRIP_PLAN_CO_LINE co,
//       IFSAPP.Site                  ss,
//       IFSAPP.Customer_Order        cu
//
//WHERE
//
//tp.contract = ss.contract
//AND tp.trip_no = co.trip_no
//AND tp.RELEASE_NO = co.RELEASE_NO
//AND cu.order_no = co.order_no
//AND tp.state IN ('Closed', 'Delivered')
//AND ss.site_type IN ( 'WAREHOUSE')
//and SUBSTR(tp.contract,1,1)  =  'W'--supplier
//and  SUBSTR(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO),1,1)    IN ('B','S','M','F','R','K')   ----receiver
//                                                      and IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO)  not in 'FAP01' ----receiver
//                                                        
//AND trunc(tp.act_delivery_date)  =  TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
//
//
//UNION
//
//
//----WH TO FCT
//
//SELECT  trunc(tp.act_delivery_date) as arrival_date --1
//       ,  IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Part_No(co.order_no,
//                                                  co.line_no,
//                                                  co.rel_no,
//                                                  co.LINE_ITEM_NO) Part_no --2
//       ,tp.contract as Supplier_Site --3
//       ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO) as Receiving_Site --4  
//           
//     , cu.Customer_po_no as  PO_NO    --5
//     , co.order_no as order_no --6  
//     , 'SUPPLIES' AS Transaction_Type  ----7
//    ,tp.state  AS Transaction_status   ----8
//    , 'WH_TO_FCT' AS Transaction_sub_type  ----9
//    ,co.line_no   AS line_no --10
//    ,co.rel_no AS release_no --11
//    ,co.line_item_no AS line_item_no --12
//   ,  co.ACTUAL_QTY_RESERVED  AS qty  -- 13
//                                                  
//  FROM ifsapp.TRN_TRIP_PLAN         tp,
//       ifsapp.TRN_TRIP_PLAN_CO_LINE co,
//       IFSAPP.Site                  ss,
//       IFSAPP.Customer_Order        cu
//
//WHERE
//
//tp.contract = ss.contract
//AND tp.trip_no = co.trip_no
//AND tp.RELEASE_NO = co.RELEASE_NO
//AND cu.order_no = co.order_no
//AND tp.state IN ('Closed', 'Delivered')
//AND ss.site_type IN ( 'WAREHOUSE')
//and SUBSTR(tp.contract,1,1)  =  'W'--supplier
//and  IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO)    IN  ('FAR01','FAQ01','FAS01','FAP01')    ----receiver
//                                                       
//                                                        
//AND trunc(tp.act_delivery_date)  =  TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
//
//UNION
//
//----WH TO WH
//
//SELECT  trunc(tp.act_delivery_date) as arrival_date --1
//       ,  IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Part_No(co.order_no,
//                                                  co.line_no,
//                                                  co.rel_no,
//                                                  co.LINE_ITEM_NO) Part_no --2
//       ,tp.contract as Supplier_Site --3
//       ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO) as Receiving_Site --4  
//           
//     , cu.Customer_po_no as  PO_NO    --5
//     , co.order_no as order_no --6  
//     , 'SUPPLIES' AS Transaction_Type  ----7
//    ,tp.state  AS Transaction_status   ----8
//    , 'WH_TO_WH' AS Transaction_sub_type  ----9
//    ,co.line_no   AS line_no --10
//    ,co.rel_no AS release_no --11
//    ,co.line_item_no AS line_item_no --12
//   ,  co.ACTUAL_QTY_RESERVED  AS qty  -- 13
//                                                  
//  FROM ifsapp.TRN_TRIP_PLAN         tp,
//       ifsapp.TRN_TRIP_PLAN_CO_LINE co,
//       IFSAPP.Site                  ss,
//       IFSAPP.Customer_Order        cu
//
//WHERE
//
//tp.contract = ss.contract
//AND tp.trip_no = co.trip_no
//AND tp.RELEASE_NO = co.RELEASE_NO
//AND cu.order_no = co.order_no
//AND tp.state IN ('Closed', 'Delivered')
//AND ss.site_type IN ( 'WAREHOUSE')
//and SUBSTR(tp.contract,1,1)  =  'W'--supplier
//and  SUBSTR(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no,
//                                                      co.line_no,
//                                                      co.rel_no,
//                                                      co.LINE_ITEM_NO),1,1)    IN ('W')   ----receiver
//                                                       
//                                                        
//AND trunc(tp.act_delivery_date)  =  TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
//                ";
                #endregion

                #region SQL 2020-01-27

                selectQuery = @"
                 
------------------------------SUPPLIES--------------------------------------------------------------

    --BR_TO_BR--
    SELECT 
        t.DATE_APPLIED AS arrival_date                                                                                               --1
        ,t.part_no AS part_no                                                                                                        --2
        ,T.contract    AS Supplier_Site                                                                                              --3
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no, T.release_no, T.sequence_no, T.line_item_no) AS Receiving_Site   --4 
        ,c.CUSTOMER_PO_NO  as PO_NO                                                                                                  --5
        ,T.order_no AS order_no                                                                                                      --6
        , 'SUPPLIES' AS Transaction_Type                                                                                             --7
        , C.state  AS   Transaction_status                                                                                           --8
        ,'BR_TO_BR' AS Transaction_sub_type                                                                                          --9
        , T.release_no AS line_no                                                                                                    --10
        ,T.sequence_no AS release_no                                                                                                 --11
        ,T.line_item_no AS line_item_no                                                                                              --12 
        ,T.quantity  AS qty                                                                                                          --13

    FROM 
        ifsapp.INVENTORY_TRANSACTION_HIST2 T,
        IFSAPP.CUSTOMER_ORDER              c,
        IFSAPP.IDENTITY_INVOICE_INFO       nn,
        IFSAPP.IDENTITY_INVOICE_INFO       nk
        
    WHERE 
        TRANSACTION_CODE = 'SHIPTRAN'
        AND t.DATE_APPLIED =  TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
        AND t.order_no = c.order_no
        AND IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no, T.release_no, T.sequence_no, T.line_item_no) = nk.identity
        AND T.contract = nn.identity
        AND nn.GROUP_ID IN ('BM', 'ERM')
        AND nn.party_type IN ('Customer')
        AND nk.GROUP_ID IN ('BM', 'ERM')
        AND nk.party_type IN ('Customer')
        AND IFSAPP.inventory_part_api.Get_Acc_Group(t.contract, t.part_no) <> 'BOOKS'
        AND ifsapp.site_api.Get_Site_Type(T.contract) IN ('BRANCH')                                                                                                 --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no, T.release_no, T.sequence_no, T.line_item_no)) IN ('BRANCH')    --Added on 2020/01/27 By AnushkaR
    
                                                      
UNION
 
    --BR_TO_FCT--
    SELECT 
        t.DATE_APPLIED AS arrival_date                                                                                                --1
        ,t.part_no AS part_no                                                                                                         --2
        ,T.contract    AS Supplier_Site                                                                                               --3
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no, T.release_no, T.sequence_no, T.line_item_no)  AS Receiving_Site   --4 
        ,c.CUSTOMER_PO_NO  as PO_NO                                                                                                   --5
        ,T.order_no AS order_no                                                                                                       --6
        ,'SUPPLIES' AS Transaction_Type                                                                                              --7
        ,C.state  AS   Transaction_status                                                                                            --8
        ,'BR_TO_FCT' AS Transaction_sub_type                                                                                          --9
        ,T.release_no AS line_no                                                                                                     --10
        ,T.sequence_no AS release_no                                                                                                  --11
        ,T.line_item_no AS line_item_no                                                                                               --12 
        ,T.quantity  AS qty                                                                                                           --13
    
    FROM 
        ifsapp.INVENTORY_TRANSACTION_HIST2 T,
        IFSAPP.CUSTOMER_ORDER              c,
        IFSAPP.IDENTITY_INVOICE_INFO       nn,
        IFSAPP.IDENTITY_INVOICE_INFO       nk
    
    WHERE 
        TRANSACTION_CODE = 'SHIPTRAN'
        AND t.DATE_APPLIED =  TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
        AND t.order_no = c.order_no
        AND IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no, T.release_no, T.sequence_no, T.line_item_no) = nk.identity
        AND T.contract = nn.identity
        AND nn.GROUP_ID IN ('BM', 'ERM')
        AND nn.party_type IN ('Customer')
        AND nk.GROUP_ID IN ('BM', 'ERM')
        AND nk.party_type IN ('Customer')
        AND IFSAPP.inventory_part_api.Get_Acc_Group(t.contract, t.part_no) <> 'BOOKS'
        AND ifsapp.site_api.Get_Site_Type(T.contract) IN ('BRANCH')                                                                                                 --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no, T.release_no, T.sequence_no, T.line_item_no)) IN ('FACTORY')   --Added on 2020/01/27 By AnushkaR                                              
         
                                                      
UNION
  
    --BR_TO_WH-
    SELECT 
        t.DATE_APPLIED AS arrival_date                                                                                               --1
        ,t.part_no AS part_no                                                                                                        --2
        ,T.contract    AS Supplier_Site                                                                                              --3
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no, T.release_no, T.sequence_no, T.line_item_no) AS Receiving_Site   --4 
        ,c.CUSTOMER_PO_NO  as PO_NO                                                                                                  --5
        ,T.order_no AS order_no                                                                                                      --6
        , 'SUPPLIES' AS Transaction_Type                                                                                             --7
        , C.state  AS   Transaction_status                                                                                           --8
        ,'BR_TO_WH' AS Transaction_sub_type                                                                                          --9
        , T.release_no AS line_no                                                                                                    --10
        ,T.sequence_no AS release_no                                                                                                 --11
        ,T.line_item_no AS line_item_no                                                                                              --12 
        ,T.quantity  AS qty                                                                                                          --13

    FROM 
        ifsapp.INVENTORY_TRANSACTION_HIST2 T,
        IFSAPP.CUSTOMER_ORDER              c,
        IFSAPP.IDENTITY_INVOICE_INFO       nn,
        IFSAPP.IDENTITY_INVOICE_INFO       nk
    
    WHERE 
        TRANSACTION_CODE = 'SHIPTRAN'
        AND t.DATE_APPLIED =  TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
        AND t.order_no = c.order_no
        AND IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no, T.release_no, T.sequence_no, T.line_item_no) = nk.identity
        AND T.contract = nn.identity
        AND nn.GROUP_ID IN ('BM', 'ERM')
        AND nn.party_type IN ('Customer')
        AND nk.GROUP_ID IN ('BM', 'ERM')
        AND nk.party_type IN ('Customer')
        AND IFSAPP.inventory_part_api.Get_Acc_Group(t.contract, t.part_no) <> 'BOOKS'
        AND ifsapp.site_api.Get_Site_Type(T.contract) IN ('BRANCH')                                                                                                   --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(T.order_no, T.release_no, T.sequence_no, T.line_item_no)) IN ('WAREHOUSE')   --Added on 2020/01/27 By AnushkaR                       


UNION


    --FCT TO FCT--
    SELECT  
        trunc(tp.act_delivery_date) as arrival_date                                                                            --1
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Part_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO) Part_no               --2
        ,tp.contract as Supplier_Site                                                                                          --3
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO) as Receiving_Site --4 
        ,cu.Customer_po_no as  PO_NO                                                                                           --5
        ,co.order_no as order_no                                                                                               --6  
        ,'SUPPLIES' AS Transaction_Type                                                                                        --7
        ,tp.state  AS Transaction_status                                                                                       --8
        ,'FCT_TO_FCT' AS Transaction_sub_type                                                                                  --9
        ,co.line_no   AS line_no                                                                                               --10
        ,co.rel_no AS release_no                                                                                               --11
        ,co.line_item_no AS line_item_no                                                                                       --12
        ,co.ACTUAL_QTY_RESERVED  AS qty                                                                                        --13
    
    FROM 
        ifsapp.TRN_TRIP_PLAN         tp,
        ifsapp.TRN_TRIP_PLAN_CO_LINE co,
        IFSAPP.Site                  ss,
        IFSAPP.Customer_Order        cu
    
    WHERE
        tp.contract = ss.contract
        AND trunc(tp.act_delivery_date)  =   TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
        AND tp.trip_no = co.trip_no
        AND tp.RELEASE_NO = co.RELEASE_NO
        AND cu.order_no = co.order_no
        AND tp.state IN ('Closed', 'Delivered')
        AND ss.site_type IN ( 'FACTORY')
        AND ifsapp.site_api.Get_Site_Type(tp.contract) IN ('FACTORY')                                                                                           --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO)) IN ('FACTORY')   --Added on 2020/01/27 By AnushkaR


UNION


    --FCT TO BR--
    SELECT  
        trunc(tp.act_delivery_date) as arrival_date                                                                            --1
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Part_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO) Part_no               --2
        ,tp.contract as Supplier_Site                                                                                          --3
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO) as Receiving_Site --4   
        ,cu.Customer_po_no as  PO_NO                                                                                           --5
        ,co.order_no as order_no                                                                                               --6  
        ,'SUPPLIES' AS Transaction_Type                                                                                        --7
        ,tp.state  AS Transaction_status                                                                                       --8
        ,'FCT_TO_BR' AS Transaction_sub_type                                                                                   --9
        ,co.line_no   AS line_no                                                                                               --10
        ,co.rel_no AS release_no                                                                                               --11
        ,co.line_item_no AS line_item_no                                                                                       --12
        ,co.ACTUAL_QTY_RESERVED  AS qty                                                                                        --13
    
    FROM 
        ifsapp.TRN_TRIP_PLAN         tp,
        ifsapp.TRN_TRIP_PLAN_CO_LINE co,
        IFSAPP.Site                  ss,
        IFSAPP.Customer_Order        cu
    
    WHERE
        tp.contract = ss.contract                                      
        AND trunc(tp.act_delivery_date)  =   TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
        AND tp.trip_no = co.trip_no
        AND tp.RELEASE_NO = co.RELEASE_NO
        AND cu.order_no = co.order_no
        AND tp.state IN ('Closed', 'Delivered')
        AND ss.site_type IN ( 'FACTORY')
        AND ifsapp.site_api.Get_Site_Type(tp.contract) IN ('FACTORY')                                                                                        --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO)) IN ('BRANCH') --Added on 2020/01/27 By AnushkaR


UNION


    --FCT TO WH--
    SELECT  
        trunc(tp.act_delivery_date) as arrival_date                                                                            --1
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Part_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO) Part_no               --2
        ,tp.contract as Supplier_Site                                                                                          --3
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO) as Receiving_Site --4  
        ,cu.Customer_po_no as  PO_NO                                                                                           --5
        ,co.order_no as order_no                                                                                               --6  
        ,'SUPPLIES' AS Transaction_Type                                                                                        --7
        ,tp.state  AS Transaction_status                                                                                       --8
        ,'FCT_TO_WH' AS Transaction_sub_type                                                                                   --9
        ,co.line_no   AS line_no                                                                                               --10
        ,co.rel_no AS release_no                                                                                               --11
        ,co.line_item_no AS line_item_no                                                                                       --12
        ,co.ACTUAL_QTY_RESERVED  AS qty                                                                                        --13
    
    FROM 
        ifsapp.TRN_TRIP_PLAN         tp,
        ifsapp.TRN_TRIP_PLAN_CO_LINE co,
        IFSAPP.Site                  ss,
        IFSAPP.Customer_Order        cu
    
    WHERE
        tp.contract = ss.contract
        AND trunc(tp.act_delivery_date)  =   TO_DATE(:PreviousDate, 'YYYY/MM/DD') 
        AND tp.trip_no = co.trip_no
        AND tp.RELEASE_NO = co.RELEASE_NO
        AND cu.order_no = co.order_no
        AND tp.state IN ('Closed', 'Delivered')
        AND ss.site_type IN ( 'FACTORY')
        AND ifsapp.site_api.Get_Site_Type(tp.contract) IN ('FACTORY')                                                                                              --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO)) IN ('WAREHOUSE')    --Added on 2020/01/27 By AnushkaR


UNION


    --WH TO BR--
    SELECT  
        trunc(tp.act_delivery_date) as arrival_date                                                                            --1
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Part_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO) Part_no               --2
        ,tp.contract as Supplier_Site                                                                                          --3
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO) as Receiving_Site --4  
        ,cu.Customer_po_no as  PO_NO                                                                                           --5
        ,co.order_no as order_no                                                                                               --6  
        ,'SUPPLIES' AS Transaction_Type                                                                                        --7
        ,tp.state  AS Transaction_status                                                                                       --8
        ,'WH_TO_BR' AS Transaction_sub_type                                                                                    --9
        ,co.line_no   AS line_no                                                                                               --10
        ,co.rel_no AS release_no                                                                                               --11
        ,co.line_item_no AS line_item_no                                                                                       --12
        ,co.ACTUAL_QTY_RESERVED  AS qty                                                                                        --13

    FROM 
        ifsapp.TRN_TRIP_PLAN         tp,
        ifsapp.TRN_TRIP_PLAN_CO_LINE co,
        IFSAPP.Site                  ss,
        IFSAPP.Customer_Order        cu
    
    WHERE
        tp.contract = ss.contract
        AND trunc(tp.act_delivery_date)  =  TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
        AND tp.trip_no = co.trip_no
        AND tp.RELEASE_NO = co.RELEASE_NO
        AND cu.order_no = co.order_no
        AND tp.state IN ('Closed', 'Delivered')
        AND ifsapp.site_api.Get_Site_Type(tp.contract) IN ('WAREHOUSE')                                                                                        --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO)) IN ('BRANCH')   --Added on 2020/01/27 By AnushkaR


UNION


    --WH TO FCT--
    SELECT  trunc(tp.act_delivery_date) as arrival_date                                                                        --1
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Part_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO) Part_no               --2
        ,tp.contract as Supplier_Site                                                                                          --3
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO) as Receiving_Site --4  
        ,cu.Customer_po_no as  PO_NO                                                                                           --5
        ,co.order_no as order_no                                                                                               --6  
        ,'SUPPLIES' AS Transaction_Type                                                                                        --7
        ,tp.state  AS Transaction_status                                                                                       --8
        ,'WH_TO_FCT' AS Transaction_sub_type                                                                                   --9
        ,co.line_no   AS line_no                                                                                               --10
        ,co.rel_no AS release_no                                                                                               --11
        ,co.line_item_no AS line_item_no                                                                                       --12
        ,co.ACTUAL_QTY_RESERVED  AS qty                                                                                        --13
    
    FROM 
        ifsapp.TRN_TRIP_PLAN         tp,
        ifsapp.TRN_TRIP_PLAN_CO_LINE co,
        IFSAPP.Site                  ss,
        IFSAPP.Customer_Order        cu
    
    WHERE
        tp.contract = ss.contract
        AND trunc(tp.act_delivery_date) = TO_DATE(:PreviousDate, 'YYYY/MM/DD')  
        AND tp.trip_no = co.trip_no
        AND tp.RELEASE_NO = co.RELEASE_NO
        AND cu.order_no = co.order_no
        AND tp.state IN ('Closed', 'Delivered')
        AND ifsapp.site_api.Get_Site_Type(tp.contract) IN ('WAREHOUSE')                                                                                        --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO)) IN ('FACTORY')  --Added on 2020/01/27 By AnushkaR


UNION


    --WH TO WH--
    SELECT  
        trunc(tp.act_delivery_date) as arrival_date                                                                            --1
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Part_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO) Part_no               --2
        ,tp.contract as Supplier_Site                                                                                          --3
        ,IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO) as Receiving_Site --4  
        ,cu.Customer_po_no as  PO_NO                                                                                           --5
        ,co.order_no as order_no                                                                                               --6  
        ,'SUPPLIES' AS Transaction_Type                                                                                        --7
        ,tp.state  AS Transaction_status                                                                                       --8
        ,'WH_TO_WH' AS Transaction_sub_type                                                                                    --9
        ,co.line_no   AS line_no                                                                                               --10
        ,co.rel_no AS release_no                                                                                               --11
        ,co.line_item_no AS line_item_no                                                                                       --12
        ,co.ACTUAL_QTY_RESERVED  AS qty                                                                                        --13
    
    FROM 
        ifsapp.TRN_TRIP_PLAN         tp,
        ifsapp.TRN_TRIP_PLAN_CO_LINE co,
        IFSAPP.Site                  ss,
        IFSAPP.Customer_Order        cu
        
    WHERE
        tp.contract = ss.contract
        AND trunc(tp.act_delivery_date)  =  TO_DATE(:PreviousDate, 'YYYY/MM/DD')
        AND tp.trip_no = co.trip_no
        AND tp.RELEASE_NO = co.RELEASE_NO
        AND cu.order_no = co.order_no
        AND tp.state IN ('Closed', 'Delivered')
        AND ss.site_type IN ( 'WAREHOUSE')
        AND ifsapp.site_api.Get_Site_Type(tp.contract) IN ('WAREHOUSE')                                                                                           --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(IFSAPP.CUSTOMER_ORDER_LINE_API.Get_Customer_No(co.order_no, co.line_no, co.rel_no, co.LINE_ITEM_NO)) IN ('WAREHOUSE')   --Added on 2020/01/27 By AnushkaR



------------------------------------------------------------------------------------------------------------------";

                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))//noneed to add @ before parameter value
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                return null;
            }

        }

        public DataTable GetInventoryTransactionRecipts(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region New Querry
//                selectQuery = @"
//
//----RECEIPT
//select TRUNC(m.trns_date) AS arrival_date --1
//      ,
//       m.part_no AS Part_no --2
//      ,
//       m.delivering_contract as Supplier_Site --3
//      ,
//       m.contract as Receiving_Site --4  
//      ,
//       IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO,
//                                                             m.LINE_NO,
//                                                             m.REL_NO,
//                                                             m.LINE_ITEM_NO) as PO_NO --5
//       
//      ,
//       m.order_no as order_no --6                                                                                                                       
//      ,
//       'RECEIPT' AS Transaction_Type ----7
//      ,
//       ' ' AS Transaction_status ----8
//      ,
//       'WH_TO_BR' AS Transaction_sub_type ----9
//      ,
//       m.line_no AS line_no --10
//      ,
//       m.rel_no AS release_no --11
//      ,
//       m.line_item_no AS line_item_no --12
//      ,
//       m.qty AS qty -- 13
//
//  FROM ifsapp.inv_transit_tracking m
//
// WHERE m.qty > 0
//AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <>
//       'BOOKS'
//AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
//and substr(m.delivering_contract, 1, 1) in 'W'
//AND SUBSTR(m.contract, 1, 1) IN ('B', 'S', 'M', 'F', 'R', 'K')
//
//UNION
//
//select TRUNC(m.trns_date) AS arrival_date --1
//      ,
//       m.part_no AS Part_no --2
//      ,
//       m.delivering_contract as Supplier_Site --3
//      ,
//       m.contract as Receiving_Site --4  
//      ,
//       IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO,
//                                                             m.LINE_NO,
//                                                             m.REL_NO,
//                                                             m.LINE_ITEM_NO) as PO_NO --5
//       
//      ,
//       m.order_no as order_no --6                                                                                                                       
//      ,
//       'RECEIPT' AS Transaction_Type ----7
//      ,
//       ' ' AS Transaction_status ----8
//      ,
//       'WH_TO_WH' AS Transaction_sub_type ----9
//      ,
//       m.line_no AS line_no --10
//      ,
//       m.rel_no AS release_no --11
//      ,
//       m.line_item_no AS line_item_no --12
//      ,
//       m.qty AS qty -- 13
//
//  FROM ifsapp.inv_transit_tracking m
//
// WHERE m.qty > 0
//AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <>
//       'BOOKS'
//       AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
//and substr(m.delivering_contract, 1, 1) in 'W'
//AND SUBSTR(m.contract, 1, 1) IN ('W')
//
//UNION
//
//select TRUNC(m.trns_date) AS arrival_date --1
//      ,
//       m.part_no AS Part_no --2
//      ,
//       m.delivering_contract as Supplier_Site --3
//      ,
//       m.contract as Receiving_Site --4  
//      ,
//       IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO,
//                                                             m.LINE_NO,
//                                                             m.REL_NO,
//                                                             m.LINE_ITEM_NO) as PO_NO --5
//       
//      ,
//       m.order_no as order_no --6                                                                                                                       
//      ,
//       'RECEIPT' AS Transaction_Type ----7
//      ,
//       ' ' AS Transaction_status ----8
//      ,
//       'WH_TO_FCT' AS Transaction_sub_type ----9
//      ,
//       m.line_no AS line_no --10
//      ,
//       m.rel_no AS release_no --11
//      ,
//       m.line_item_no AS line_item_no --12
//      ,
//       m.qty AS qty -- 13
//
//  FROM ifsapp.inv_transit_tracking m
//
// WHERE m.qty > 0
//AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <>
//       'BOOKS'
//       AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
//and substr(m.delivering_contract, 1, 1) in 'W'----supper
//AND (m.contract) IN ('FAR01','FAQ01','FAS01','FAP01')   ---receiver
//
//UNION
//
//select TRUNC(m.trns_date) AS arrival_date --1
//      ,
//       m.part_no AS Part_no --2
//      ,
//       m.delivering_contract as Supplier_Site --3
//      ,
//       m.contract as Receiving_Site --4  
//      ,
//       IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO,
//                                                             m.LINE_NO,
//                                                             m.REL_NO,
//                                                             m.LINE_ITEM_NO) as PO_NO --5
//       
//      ,
//       m.order_no as order_no --6                                                                                                                       
//      ,
//       'RECEIPT' AS Transaction_Type ----7
//      ,
//       ' ' AS Transaction_status ----8
//      ,
//       'BR_TO_BR' AS Transaction_sub_type ----9
//      ,
//       m.line_no AS line_no --10
//      ,
//       m.rel_no AS release_no --11
//      ,
//       m.line_item_no AS line_item_no --12
//      ,
//       m.qty AS qty -- 13
//
//  FROM ifsapp.inv_transit_tracking m
//
// WHERE m.qty > 0
//AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <>
//       'BOOKS'
//       AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
//and substr(m.delivering_contract, 1, 1)   IN ('B','S','M','F','R','K') ----supper
//AND SUBSTR(m.contract, 1, 1) IN ('B','S','M','F','R','K') ---receiver
//
//UNION
//
//select TRUNC(m.trns_date) AS arrival_date --1
//      ,
//       m.part_no AS Part_no --2
//      ,
//       m.delivering_contract as Supplier_Site --3
//      ,
//       m.contract as Receiving_Site --4  
//      ,
//       IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO,
//                                                             m.LINE_NO,
//                                                             m.REL_NO,
//                                                             m.LINE_ITEM_NO) as PO_NO --5
//       
//      ,
//       m.order_no as order_no --6                                                                                                                       
//      ,
//       'RECEIPT' AS Transaction_Type ----7
//      ,
//       ' ' AS Transaction_status ----8
//      ,
//       'BR_TO_WH' AS Transaction_sub_type ----9
//      ,
//       m.line_no AS line_no --10
//      ,
//       m.rel_no AS release_no --11
//      ,
//       m.line_item_no AS line_item_no --12
//      ,
//       m.qty AS qty -- 13
//
//  FROM ifsapp.inv_transit_tracking m
//
// WHERE m.qty > 0
//AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <>
//       'BOOKS'
//       AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
//and substr(m.delivering_contract, 1, 1)   IN ('B','S','M','F','R','K') ----supper
//AND SUBSTR(m.contract, 1, 1) IN ('W') ---receiver
//
//UNION
//
//select TRUNC(m.trns_date) AS arrival_date --1
//      ,
//       m.part_no AS Part_no --2
//      ,
//       m.delivering_contract as Supplier_Site --3
//      ,
//       m.contract as Receiving_Site --4  
//      ,
//       IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO,
//                                                             m.LINE_NO,
//                                                             m.REL_NO,
//                                                             m.LINE_ITEM_NO) as PO_NO --5
//       
//      ,
//       m.order_no as order_no --6                                                                                                                       
//      ,
//       'RECEIPT' AS Transaction_Type ----7
//      ,
//       ' ' AS Transaction_status ----8
//      ,
//       'BR_TO_FCT' AS Transaction_sub_type ----9
//      ,
//       m.line_no AS line_no --10
//      ,
//       m.rel_no AS release_no --11
//      ,
//       m.line_item_no AS line_item_no --12
//      ,
//       m.qty AS qty -- 13
//
//  FROM ifsapp.inv_transit_tracking m
//
// WHERE m.qty > 0
//AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <>
//       'BOOKS'
//       AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
//and substr(m.delivering_contract, 1, 1)   IN ('B','S','M','F','R','K') ----supper
//AND SUBSTR(m.contract, 1, 1) IN ('F') ---receiver
//AND m.contract IN ('FAR01','FAQ01','FAS01','FAP01')  
//
//UNION
//
//select TRUNC(m.trns_date) AS arrival_date --1
//      ,
//       m.part_no AS Part_no --2
//      ,
//       m.delivering_contract as Supplier_Site --3
//      ,
//       m.contract as Receiving_Site --4  
//      ,
//       IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO,
//                                                             m.LINE_NO,
//                                                             m.REL_NO,
//                                                             m.LINE_ITEM_NO) as PO_NO --5
//       
//      ,
//       m.order_no as order_no --6                                                                                                                       
//      ,
//       'RECEIPT' AS Transaction_Type ----7
//      ,
//       ' ' AS Transaction_status ----8
//      ,
//       'FCT_TO_BR' AS Transaction_sub_type ----9
//      ,
//       m.line_no AS line_no --10
//      ,
//       m.rel_no AS release_no --11
//      ,
//       m.line_item_no AS line_item_no --12
//      ,
//       m.qty AS qty -- 13
//
//  FROM ifsapp.inv_transit_tracking m
//
// WHERE m.qty > 0
//AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <>
//       'BOOKS'
//       AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
//and  m.delivering_contract   IN  ('FAR01','FAQ01','FAS01','FAP01')   ----supper
//AND SUBSTR(m.contract, 1, 1) IN ('B','S','M','F','R','K') ---receiver
// 
// UNION
// 
// 
// select TRUNC(m.trns_date) AS arrival_date --1
//      ,
//       m.part_no AS Part_no --2
//      ,
//       m.delivering_contract as Supplier_Site --3
//      ,
//       m.contract as Receiving_Site --4  
//      ,
//       IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO,
//                                                             m.LINE_NO,
//                                                             m.REL_NO,
//                                                             m.LINE_ITEM_NO) as PO_NO --5
//       
//      ,
//       m.order_no as order_no --6                                                                                                                       
//      ,
//       'RECEIPT' AS Transaction_Type ----7
//      ,
//       ' ' AS Transaction_status ----8
//      ,
//       'FCT_TO_WH' AS Transaction_sub_type ----9
//      ,
//       m.line_no AS line_no --10
//      ,
//       m.rel_no AS release_no --11
//      ,
//       m.line_item_no AS line_item_no --12
//      ,
//       m.qty AS qty -- 13
//
//  FROM ifsapp.inv_transit_tracking m
//
// WHERE m.qty > 0
//AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <>
//       'BOOKS'
//       AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
//and m.delivering_contract   IN  ('FAR01','FAQ01','FAS01','FAP01')  ----supper
//AND SUBSTR(m.contract, 1, 1) IN ('W') ---receiver
//
//
//UNION
//select TRUNC(m.trns_date) AS arrival_date --1
//      ,
//       m.part_no AS Part_no --2
//      ,
//       m.delivering_contract as Supplier_Site --3
//      ,
//       m.contract as Receiving_Site --4  
//      ,
//       IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO,
//                                                             m.LINE_NO,
//                                                             m.REL_NO,
//                                                             m.LINE_ITEM_NO) as PO_NO --5
//       
//      ,
//       m.order_no as order_no --6                                                                                                                       
//      ,
//       'RECEIPT' AS Transaction_Type ----7
//      ,
//       ' ' AS Transaction_status ----8
//      ,
//       'FCT_TO_FCT' AS Transaction_sub_type ----9
//      ,
//       m.line_no AS line_no --10
//      ,
//       m.rel_no AS release_no --11
//      ,
//       m.line_item_no AS line_item_no --12
//      ,
//       m.qty AS qty -- 13
//
//  FROM ifsapp.inv_transit_tracking m
//
// WHERE m.qty > 0
//AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <>
//       'BOOKS'
//       AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
//and m.delivering_contract   IN ('FAR01','FAQ01','FAS01','FAP01') ----supper
//AND (m.contract) IN ('FAR01','FAQ01','FAS01','FAP01')  ---receiver
// 
// 
//                ";
                #endregion

                #region SQL 2020/03/03

                selectQuery = @"

------------------------------RECEIPT----------------------------------------------------


    --WH_TO_BR--
    SELECT 
        TRUNC(m.trns_date) AS arrival_date                                                                               --1
        ,m.part_no AS Part_no                                                                                            --2
        ,m.delivering_contract as Supplier_Site                                                                          --3
        ,m.contract as Receiving_Site                                                                                    --4  
        ,IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO, m.LINE_NO, m.REL_NO, m.LINE_ITEM_NO) as PO_NO --5
        ,m.order_no as order_no                                                                                          --6                                                                                                                       
        ,'RECEIPT' AS Transaction_Type                                                                                   --7
        ,' ' AS Transaction_status                                                                                       --8
        ,'WH_TO_BR' AS Transaction_sub_type                                                                              --9
        ,m.line_no AS line_no                                                                                            --10
        ,m.rel_no AS release_no                                                                                          --11
        ,m.line_item_no AS line_item_no                                                                                  --12
        ,m.qty AS qty                                                                                                    --13
    
    FROM 
        ifsapp.inv_transit_tracking m
    
    WHERE 
        m.qty > 0
        AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <> 'BOOKS'
        AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
        AND ifsapp.site_api.Get_Site_Type(m.delivering_contract) IN ('WAREHOUSE')    --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(m.contract) IN ('BRANCH')                  --Added on 2020/01/27 By AnushkaR
    ---------------------


UNION


    --WH_TO_WH--
    SELECT 
        TRUNC(m.trns_date) AS arrival_date                                                                               --1
        ,m.part_no AS Part_no                                                                                            --2
        ,m.delivering_contract as Supplier_Site                                                                          --3
        ,m.contract as Receiving_Site                                                                                    --4  
        ,IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO, m.LINE_NO, m.REL_NO, m.LINE_ITEM_NO) as PO_NO --5
        ,m.order_no as order_no                                                                                          --6                                                                                                                       
        ,'RECEIPT' AS Transaction_Type                                                                                   --7
        ,' ' AS Transaction_status                                                                                       --8
        ,'WH_TO_WH' AS Transaction_sub_type                                                                              --9
        ,m.line_no AS line_no                                                                                            --10
        ,m.rel_no AS release_no                                                                                          --11
        ,m.line_item_no AS line_item_no                                                                                  --12
        ,m.qty AS qty                                                                                                    --13
    
    FROM 
        ifsapp.inv_transit_tracking m
    
    WHERE 
        m.qty > 0
        AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <> 'BOOKS'
        AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
        AND ifsapp.site_api.Get_Site_Type(m.delivering_contract) IN ('WAREHOUSE')    --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(m.contract) IN ('WAREHOUSE')               --Added on 2020/01/27 By AnushkaR
    ---------------------


UNION


    --WH_TO_FCT--
    SELECT 
        TRUNC(m.trns_date) AS arrival_date                                                                               --1
        ,m.part_no AS Part_no                                                                                            --2
        ,m.delivering_contract as Supplier_Site                                                                          --3
        ,m.contract as Receiving_Site                                                                                    --4  
        ,IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO, m.LINE_NO, m.REL_NO, m.LINE_ITEM_NO) as PO_NO --5
        ,m.order_no as order_no                                                                                          --6                                                                                                                       
        ,'RECEIPT' AS Transaction_Type                                                                                   --7
        ,' ' AS Transaction_status                                                                                       --8
        ,'WH_TO_FCT' AS Transaction_sub_type                                                                             --9
        ,m.line_no AS line_no                                                                                            --10
        ,m.rel_no AS release_no                                                                                          --11
        ,m.line_item_no AS line_item_no                                                                                  --12
        ,m.qty AS qty                                                                                                    --13
    
    FROM 
         ifsapp.inv_transit_tracking m
    
    WHERE 
        m.qty > 0
        AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <> 'BOOKS'
        AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
        AND ifsapp.site_api.Get_Site_Type(m.delivering_contract) IN ('WAREHOUSE')    --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(m.contract) IN ('FACTORY')                 --Added on 2020/01/27 By AnushkaR
    ---------------------
        

UNION


    --BR_TO_BR--
    SELECT 
        TRUNC(m.trns_date) AS arrival_date                                                                               --1
        ,m.part_no AS Part_no                                                                                            --2
        ,m.delivering_contract as Supplier_Site                                                                          --3
        ,m.contract as Receiving_Site                                                                                    --4  
        ,IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO, m.LINE_NO, m.REL_NO, m.LINE_ITEM_NO) as PO_NO --5
        ,m.order_no as order_no                                                                                          --6                                                                                                                       
        ,'RECEIPT' AS Transaction_Type                                                                                   --7
        ,' ' AS Transaction_status                                                                                       --8
        ,'BR_TO_BR' AS Transaction_sub_type                                                                              --9
        ,m.line_no AS line_no                                                                                            --10
        ,m.rel_no AS release_no                                                                                          --11
        ,m.line_item_no AS line_item_no                                                                                  --12
        ,m.qty AS qty                                                                                                    --13
    
    FROM 
        ifsapp.inv_transit_tracking m
    
    WHERE 
        m.qty > 0
        AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <> 'BOOKS'
        AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
        AND ifsapp.site_api.Get_Site_Type(m.delivering_contract) IN ('BRANCH')       --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(m.contract) IN ('BRANCH')                  --Added on 2020/01/27 By AnushkaR
    ---------------------


UNION


    --BR_TO_WH--
    SELECT 
        TRUNC(m.trns_date) AS arrival_date                                                                               --1
        ,m.part_no AS Part_no                                                                                            --2
        ,m.delivering_contract as Supplier_Site                                                                          --3
        ,m.contract as Receiving_Site                                                                                    --4  
        ,IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO, m.LINE_NO, m.REL_NO, m.LINE_ITEM_NO) as PO_NO --5
        ,m.order_no as order_no                                                                                          --6                                                                                                                       
        ,'RECEIPT' AS Transaction_Type                                                                                   --7
        ,' ' AS Transaction_status                                                                                       --8
        ,'BR_TO_WH' AS Transaction_sub_type                                                                              --9
        ,m.line_no AS line_no                                                                                            --10
        ,m.rel_no AS release_no                                                                                          --11
        ,m.line_item_no AS line_item_no                                                                                  --12
        ,m.qty AS qty                                                                                                    --13
    
    FROM 
        ifsapp.inv_transit_tracking m
    
    WHERE 
        m.qty > 0
        AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <> 'BOOKS'
        AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
        AND ifsapp.site_api.Get_Site_Type(m.delivering_contract) IN ('BRANCH')       --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(m.contract) IN ('WAREHOUSE')               --Added on 2020/01/27 By AnushkaR
    ---------------------


UNION


    --BR_TO_FCT--
    SELECT 
        TRUNC(m.trns_date) AS arrival_date                                                                               --1
        ,m.part_no AS Part_no                                                                                            --2
        ,m.delivering_contract as Supplier_Site                                                                          --3
        ,m.contract as Receiving_Site                                                                                    --4  
        ,IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO, m.LINE_NO, m.REL_NO, m.LINE_ITEM_NO) as PO_NO --5
        ,m.order_no as order_no                                                                                          --6                                                                                                                       
        ,'RECEIPT' AS Transaction_Type                                                                                   --7
        ,' ' AS Transaction_status                                                                                       --8
        ,'BR_TO_FCT' AS Transaction_sub_type                                                                             --9
        ,m.line_no AS line_no                                                                                            --10
        ,m.rel_no AS release_no                                                                                          --11
        ,m.line_item_no AS line_item_no                                                                                  --12
        ,m.qty AS qty                                                                                                    --13
    
    FROM 
        ifsapp.inv_transit_tracking m
    
    WHERE 
        m.qty > 0
        AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <> 'BOOKS'
        AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
        AND ifsapp.site_api.Get_Site_Type(m.delivering_contract) IN ('BRANCH')     --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(m.contract) IN ('FACTORY')               --Added on 2020/01/27 By AnushkaR
    ---------------------


UNION


    --FCT_TO_BR--
    SELECT 
        TRUNC(m.trns_date) AS arrival_date                                                                               --1
        ,m.part_no AS Part_no                                                                                            --2
        ,m.delivering_contract as Supplier_Site                                                                          --3
        ,m.contract as Receiving_Site                                                                                    --4  
        ,IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO, m.LINE_NO, m.REL_NO, m.LINE_ITEM_NO) as PO_NO --5
        ,m.order_no as order_no                                                                                          --6                                                                                                                       
        ,'RECEIPT' AS Transaction_Type                                                                                   --7
        ,' ' AS Transaction_status                                                                                       --8
        ,'FCT_TO_BR' AS Transaction_sub_type                                                                             --9
        ,m.line_no AS line_no                                                                                            --10
        ,m.rel_no AS release_no                                                                                          --11
        ,m.line_item_no AS line_item_no                                                                                  --12
        ,m.qty AS qty                                                                                                    --13
    
    FROM 
        ifsapp.inv_transit_tracking m
    
    WHERE 
        m.qty > 0
        AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <> 'BOOKS'
        AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
        AND ifsapp.site_api.Get_Site_Type(m.delivering_contract) IN ('FACTORY')   --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(m.contract) IN ('BRANCH')               --Added on 2020/01/27 By AnushkaR
    ---------------------


UNION


    --FCT_TO_WH--
    SELECT 
        TRUNC(m.trns_date) AS arrival_date                                                                               --1
        ,m.part_no AS Part_no                                                                                            --2
        ,m.delivering_contract as Supplier_Site                                                                          --3
        ,m.contract as Receiving_Site                                                                                    --4  
        ,IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO, m.LINE_NO, m.REL_NO, m.LINE_ITEM_NO) as PO_NO --5
        ,m.order_no as order_no                                                                                          --6                                                                                                                       
        ,'RECEIPT' AS Transaction_Type                                                                                   --7
        ,' ' AS Transaction_status                                                                                       --8
        ,'FCT_TO_WH' AS Transaction_sub_type                                                                             --9
        ,m.line_no AS line_no                                                                                            --10
        ,m.rel_no AS release_no                                                                                          --11
        ,m.line_item_no AS line_item_no                                                                                  --12
        ,m.qty AS qty                                                                                                    --13
    
    FROM 
        ifsapp.inv_transit_tracking m
    
    WHERE 
        m.qty > 0
        AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <> 'BOOKS'
        AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
        AND ifsapp.site_api.Get_Site_Type(m.delivering_contract) IN ('FACTORY')   --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(m.contract) IN ('WAREHOUSE')            --Added on 2020/01/27 By AnushkaR
    ---------------------


UNION


    --FCT_TO_FCT--
    SELECT 
        TRUNC(m.trns_date) AS arrival_date                                                                               --1
        ,m.part_no AS Part_no                                                                                            --2
        ,m.delivering_contract as Supplier_Site                                                                          --3
        ,m.contract as Receiving_Site                                                                                    --4  
        ,IFSAPP.Inv_Transit_Tracking_API.Get_Demand_Order_Ref1(m.ORDER_NO, m.LINE_NO, m.REL_NO, m.LINE_ITEM_NO) as PO_NO --5
        ,m.order_no as order_no                                                                                          --6                                                                                                                       
        ,'RECEIPT' AS Transaction_Type                                                                                   --7
        ,' ' AS Transaction_status                                                                                       --8
        ,'FCT_TO_FCT' AS Transaction_sub_type                                                                            --9
        ,m.line_no AS line_no                                                                                            --10
        ,m.rel_no AS release_no                                                                                          --11
        ,m.line_item_no AS line_item_no                                                                                  --12
        ,m.qty AS qty                                                                                                    --13
    
    FROM 
        ifsapp.inv_transit_tracking m
    
    WHERE 
        m.qty > 0
        AND ifsapp.inventory_part_api.Get_Acc_Group(m.contract, m.part_no) <> 'BOOKS'
        AND TRUNC(m.trns_date) = TO_DATE(:PreviousDate,'YYYY/mm/dd')
        AND ifsapp.site_api.Get_Site_Type(m.delivering_contract) IN ('FACTORY')   --Added on 2020/01/27 By AnushkaR
        AND ifsapp.site_api.Get_Site_Type(m.contract) IN ('FACTORY')              --Added on 2020/01/27 By AnushkaR
    ---------------------


-------------------------------------------------------------------------------------------------------------------------------------";

                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))//noneed to add @ before parameter value
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                return null;
            }

        }

        public DataTable GetInventoryTransactionPurchase(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region New Querry
//                selectQuery = @"
//                --PURCHASE
//
//                --VEN_TO_WH
//                select trunc(hh.date_applied) AS arrival_date  ,  ----1
//                hh.part_no AS part_no , ----2
//                po.vendor_no AS Supplier_Site, ----3
//                hh.contract AS Receiving_Site,  ----4 
//                PO.customer_po_no  as PO_NO, ----5
//                gr.order_no AS order_no, ----6
//                'PURCHASE' AS Transaction_Type, ----7
//                PO.state AS Transaction_status , ----8
//                'VEN_TO_WH' AS Transaction_sub_type,  ----9
//                gr.line_no  AS line_no,--10
//                gr.Release_no AS release_no,--11
//                gr.receipt_no AS line_item_no,--12
//                hh.quantity AS qty  -- 13
// 
//                  FROM ifsapp.inventory_transaction_hist2 hh,
//                       ifsapp.purchase_order              po,
//                       IFSAPP.PURCHASE_RECEIPT_NEW        gr,
//                       IFSAPP.ORDER_COORDINATOR_GROUP     co,
//                       ifsapp.site                        ss
//                WHERE SUBSTR(po.order_no, 1, 1) = co.authorize_group
//                   AND SUBSTR(po.order_no, 1, 1) in ('L','X')
//                   AND hh.contract = ss.contract
//                   AND hh.order_no = po.order_no
//                   AND hh.transaction_code = 'ARRIVAL'
//                   AND ss.site_type IN ('WAREHOUSE')
//                   AND trunc(hh.date_applied)   =  TO_DATE(:PreviousDate, 'YYYY/MM/DD') 
//                   AND hh.order_no = gr.order_no
//                   AND hh.release_no = gr.line_no
//                   AND hh.Sequence_no = gr.Release_no
//                   AND hh.line_item_no = gr.receipt_no
//                   AND SUBSTR(hh.contract,1,1) IN ('W')
//
//   
//                   UNION
//   
//    
//                --VEN_TO_FCT
//                select trunc(hh.date_applied) AS arrival_date  ,  ----1
//                hh.part_no AS part_no , ----2
//                po.vendor_no AS Supplier_Site, ----3
//                hh.contract AS Receiving_Site,  ----4 
//                PO.customer_po_no  as PO_NO, ----5
//                gr.order_no AS order_no, ----6
//                'PURCHASE' AS Transaction_Type, ----7
//                PO.state AS Transaction_status , ----8
//                'VEN_TO_FCT' AS Transaction_sub_type,  ----9
//                gr.line_no  AS line_no,--10
//                gr.Release_no AS release_no,--11
//                gr.receipt_no AS line_item_no,--12
//                hh.quantity AS qty  -- 13
// 
//                  FROM ifsapp.inventory_transaction_hist2 hh,
//                       ifsapp.purchase_order              po,
//                       IFSAPP.PURCHASE_RECEIPT_NEW        gr,
//                       IFSAPP.ORDER_COORDINATOR_GROUP     co,
//                       ifsapp.site                        ss
//                WHERE SUBSTR(po.order_no, 1, 1) = co.authorize_group
//                   AND SUBSTR(po.order_no, 1, 1) in ('L','X')
//                   AND hh.contract = ss.contract
//                   AND hh.order_no = po.order_no
//                   AND hh.transaction_code = 'ARRIVAL'
//                   AND ss.site_type IN ('FACTORY')
//                   AND trunc(hh.date_applied)   =  TO_DATE(:PreviousDate, 'YYYY/MM/DD') 
//                   AND hh.order_no = gr.order_no
//                   AND hh.release_no = gr.line_no
//                   AND hh.Sequence_no = gr.Release_no
//                   AND hh.line_item_no = gr.receipt_no
//                   AND (hh.contract) IN ('FAR01','FAQ01','FAS01','FAP01')
//
//                ";
                #endregion

                #region SQL 2020-01-27

                selectQuery = @"
---------------------------PURCHASE------------------------------------------------------------------

    --VEN_TO_WH--
    SELECT 
        trunc(hh.date_applied) AS arrival_date,  --1
        hh.part_no AS part_no,                   --2
        po.vendor_no AS Supplier_Site,           --3
        hh.contract AS Receiving_Site,           --4 
        PO.customer_po_no  as PO_NO,             --5
        gr.order_no AS order_no,                 --6
        'PURCHASE' AS Transaction_Type,          --7
        PO.state AS Transaction_status ,         --8
        'VEN_TO_WH' AS Transaction_sub_type,     --9
        gr.line_no  AS line_no,                  --10
        gr.Release_no AS release_no,             --11
        gr.receipt_no AS line_item_no,           --12
        hh.quantity AS qty                       --13
    
    FROM 
        ifsapp.inventory_transaction_hist2 hh,
        ifsapp.purchase_order              po,
        IFSAPP.PURCHASE_RECEIPT_NEW        gr,
        IFSAPP.ORDER_COORDINATOR_GROUP     co,
        ifsapp.site                        ss
    
    WHERE 
        SUBSTR(po.order_no, 1, 1) = co.authorize_group
        AND SUBSTR(po.order_no, 1, 1) in ('L','X')
        AND hh.contract = ss.contract
        AND hh.order_no = po.order_no
        AND hh.transaction_code = 'ARRIVAL'
        AND ss.site_type IN ('WAREHOUSE')
        AND trunc(hh.date_applied)   =  TO_DATE(:PreviousDate, 'YYYY/MM/DD') 
        AND hh.order_no = gr.order_no
        AND hh.release_no = gr.line_no
        AND hh.Sequence_no = gr.Release_no
        AND hh.line_item_no = gr.receipt_no
    -----------------


UNION


    --VEN_TO_FCT--
    SELECT 
        trunc(hh.date_applied) AS arrival_date,  --1
        hh.part_no AS part_no,                   --2
        po.vendor_no AS Supplier_Site,           --3
        hh.contract AS Receiving_Site,           --4 
        PO.customer_po_no  as PO_NO,             --5
        gr.order_no AS order_no,                 --6
        'PURCHASE' AS Transaction_Type,          --7
        PO.state AS Transaction_status ,         --8
        'VEN_TO_FCT' AS Transaction_sub_type,    --9
        gr.line_no  AS line_no,                  --10
        gr.Release_no AS release_no,             --11
        gr.receipt_no AS line_item_no,           --12
        hh.quantity AS qty                       --13
    
    FROM 
        ifsapp.inventory_transaction_hist2 hh,
        ifsapp.purchase_order              po,
        IFSAPP.PURCHASE_RECEIPT_NEW        gr,
        IFSAPP.ORDER_COORDINATOR_GROUP     co,
        ifsapp.site                        ss
    
    WHERE 
        SUBSTR(po.order_no, 1, 1) = co.authorize_group
        AND SUBSTR(po.order_no, 1, 1) in ('L','X')
        AND hh.contract = ss.contract
        AND hh.order_no = po.order_no
        AND hh.transaction_code = 'ARRIVAL'
        AND ss.site_type IN ('FACTORY')
        AND trunc(hh.date_applied)   =  TO_DATE(:PreviousDate, 'YYYY/MM/DD') 
        AND hh.order_no = gr.order_no
        AND hh.release_no = gr.line_no
        AND hh.Sequence_no = gr.Release_no
        AND hh.line_item_no = gr.receipt_no
    -----------------


-------------------------------------------------------------------------------------------------";

                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))//noneed to add @ before parameter value
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                return null;
            }

        }

        public DataTable GetFormattedMissingProductDetailsFromIFS(Product objProduct)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;
            try
            {

                #region New on 2019-08-15
                selectQuery = @"
                                
                select DISTINCT i.part_no AS PRODUCT_CODE,
                i.description as PRODUCT_DESC,
                i.accounting_group AS ACCOUNTING_GROUP_ID,
                a.description as ACCOUNTING_GROUP_DESC,
                i.part_product_family AS PRODUCT_FAMILY_ID,
                f.description as PRODUCT_FAMILY_DESC,
                i.second_commodity COMMODITY_ID,
                g.description as COMMODITY_DESC , --pfdescription,
                b.ownership AS OWNER_ID,
                b.ownership_des AS OWNER_DESC,
                i.part_product_code AS BRAND_ID,
                p.description as BRAND_DESC,
                i.part_status  
                ,trunc(i.create_date) as  DATE_OF_INTRODUCTION
                ,RRD.sup_part_no as REPLACEMENT_PRODUCT
                ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no),0) as CBM
                ,'' as PRODUCT_ATTRIBUTE_1
                ,'' as PRODUCT_ATTRIBUTE_2
                ,'' as PRODUCT_ATTRIBUTE_3
                ,'' as PRODUCT_ATTRIBUTE_4
                ,'' as PRODUCT_ATTRIBUTE_5
                ,'INV' AS CATALOG_TYPE
                from ifsapp.inventory_part i
            left join ifsapp.accounting_group a on i.accounting_group =
                                                  a.ACCOUNTING_GROUP
            left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
                                                          f.part_product_family
            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
                                                 i.second_commodity
            left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
                                                         i.part_product_code
            left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
            left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
            left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
                                                        p.part_product_code
            where i.contract = 'BAD01'
            and i.part_no  = :PRODUCT_CODE

            UNION

                select DISTINCT i.part_no AS PRODUCT_CODE,
                i.description as PRODUCT_DESC,
                i.accounting_group AS ACCOUNTING_GROUP_ID,
                a.description as ACCOUNTING_GROUP_DESC,
                i.part_product_family AS PRODUCT_FAMILY_ID,
                f.description as PRODUCT_FAMILY_DESC,
                i.second_commodity COMMODITY_ID,
                g.description as COMMODITY_DESC , --pfdescription,
                b.ownership AS OWNER_ID,
                b.ownership_des AS OWNER_DESC,
                i.part_product_code AS BRAND_ID,
                p.description as BRAND_DESC,
                i.part_status  
                ,trunc(i.create_date) as  DATE_OF_INTRODUCTION
                ,RRD.sup_part_no as REPLACEMENT_PRODUCT
                ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', i.part_no),0) as CBM
                ,'' as PRODUCT_ATTRIBUTE_1
                ,'' as PRODUCT_ATTRIBUTE_2
                ,'' as PRODUCT_ATTRIBUTE_3
                ,'' as PRODUCT_ATTRIBUTE_4
                ,'' as PRODUCT_ATTRIBUTE_5
                ,'INV' AS CATALOG_TYPE
                from ifsapp.inventory_part i
            left join ifsapp.accounting_group a on i.accounting_group =
                                                  a.ACCOUNTING_GROUP
            left join IFSAPP.REVERT_RESTRICTION_DAYS RRD on i.part_no = RRD.part_no
            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on i.part_product_family =
                                                          f.part_product_family
            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
                                                 i.second_commodity
            left join IFSAPP.SIN_BRAND_OWNERSHIP_TAB b on b.brand_id =
                                                         i.part_product_code
            left join IFSAPP.PURCHASE_PART_SUPPLIER e on i.part_no = e.part_no
            left join IFSAPP.SUPPLIER_INFO o on e.vendor_no = o.supplier_id
            left join IFSAPP.INVENTORY_PRODUCT_CODE p on i.part_product_code =
                                                        p.part_product_code
            where SUBSTR(i.contract,1,1) IN ('R','W')
            and i.part_no   = :PRODUCT_CODE 
            and SUBSTR(I.second_commodity,1,2) IN ( '2P','2C','GV')

            UNION

            select DISTINCT s.catalog_no as PRODUCT_CODE,
                s.catalog_desc as PRODUCT_DESC,
                case
                  when s.part_product_family = 'PF025' then
                   'AG001'
                  else
                   'AG002'
                end AS ACCOUNTING_GROUP_ID,
                case
                  when s.part_product_family = 'PF025' then
                   'MACHINES'
                  else
                   'CONSUMER DURABLES'
                end as ACCOUNTING_GROUP_DESC,
                nvl(s.part_product_family,'N/A') AS PRODUCT_FAMILY_ID,
                NVL(f.description,'N/A') as PRODUCT_FAMILY_DESC,
                s.catalog_group as COMMODITY_ID,
                g.description COMMODITY_DESC , --as pfdescription,
                '' AS OWNER_ID,
                '' AS OWNER_DESC,
                '' AS BRAND_ID,
                '' AS BRAND_DESC,
                '' AS part_status
            ,
                trunc(s.date_entered) as  DATE_OF_INTRODUCTION
                ,''  as REPLACEMENT_PRODUCT
                ,nvl(ifsapp.inventory_part_api.get_volume('WAA01', s.catalog_no),0) as CBM
                ,'' as PRODUCT_ATTRIBUTE_1
                ,'' as PRODUCT_ATTRIBUTE_2
                ,'' as PRODUCT_ATTRIBUTE_3
                ,'' as PRODUCT_ATTRIBUTE_4
                ,'' as PRODUCT_ATTRIBUTE_5
                ,'NON' AS CATALOG_TYPE
    
            from IFSAPP.SALES_PART s
            left join IFSAPP.INVENTORY_PRODUCT_FAMILY f on s.part_product_family =
                                                          f.part_product_family
            left join IFSAPP.COMMODITY_GROUP g on g.second_commodity =
                                                 s.catalog_group
            where  s.catalog_type_db = 'NON'  ---NON INVENTORY PART
            and  s.catalog_no  =  :PRODUCT_CODE
            
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PRODUCT_CODE",objProduct.PRODUCT_CODE)
                };

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable GetInvPosition(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region New Sql.OLD
//                selectQuery = @"                
//                SELECT TblMain.AS_AT_DATE,
//       TblMain.PRODUCT_CODE,
//       TblMain.SITE_ID,
//       TblMain.INVENTORY_LOCATION,
//       TblMain.COST,
//       SUM(TblMain.QTY) AS QTY       
//       
//  FROM (SELECT        
//         trunc(SYSDATE) as AS_AT_DATE, --1          
//          SUBSTR((IU.contract || '_' || IL.location_group), 1, 5) as SITE_ID, --2          
//          IU.part_no as PRODUCT_CODE, --3          
//          sum(iu.qty_onhand) as QTY, --4
//          iu.location_no AS INVENTORY_LOCATION, --5    
//          TRUNC(aiu.inventory_value,2) as COST   --6
//          FROM IFSAPP.INVENTORY_PART_IN_STOCK iu        
//          --left join IFSAPP.SIN_STORE_MASTER_TAB SM on IU.contract =  SM.SITE_ID        
//          LEFT JOIN IFSAPP.INVENTORY_LOCATION IL ON IU.contract = IL.contract                                                   
//                                                AND IU.location_no = IL.location_no        
//          LEFT JOIN ifsapp.inventory_part IPA ON IU.contract = IPA.contract                                                
//                                             AND IU.part_no = IPA.part_no        
//          LEFT JOIN IFSAPP.INVENTORY_PART_UNIT_COST aiu on iu.contract = aiu.contract                                                          
//                                                       and iu.part_no = aiu.part_no     
//          
//                                                              
//         where --substr(IPA.second_commodity, 1, 2) NOT IN ('2P', '2C')              
//           --and ipa.accounting_group <> 'BOOKS'              
//           --AND 
//            (iu.qty_onhand) <> 0              
//           --AND SUBSTR(IU.contract, 1, 1) NOT IN ('W') --here WareHouse shop codes will not taken
//                             
//         group by IU.contract,                  
//                   IL.location_group,                   
//                   IU.part_no,                   
//                  -- SM.STORE_DEFAULT_DC_CODE,                   
//                   IU.qty_reserved,
//                   iu.location_no,
//                   aiu.inventory_value
//        
//        ) TblMain
//
// GROUP BY TblMain.AS_AT_DATE,
//          TblMain.SITE_ID,
//          TblMain.PRODUCT_CODE,
//          TblMain.INVENTORY_LOCATION,
//          TblMain.COST
//                ";
                #endregion

                #region new sql with Transit locations
                selectQuery = @" 
                
                SELECT TblMain.AS_AT_DATE,
                       TblMain.PRODUCT_CODE,
                       TblMain.SITE_ID,
                       TblMain.INVENTORY_LOCATION,
                       TblMain.COST,
                       SUM(TblMain.QTY) AS QTY

                  FROM (SELECT trunc(SYSDATE) as AS_AT_DATE, --1          
                               SUBSTR((IU.contract || '_' || IL.location_group), 1, 5) as SITE_ID, --2          
                               IU.part_no as PRODUCT_CODE, --3          
                               sum(iu.qty_onhand) as QTY, --4
                               iu.location_no AS INVENTORY_LOCATION, --5    
                               TRUNC(aiu.inventory_value, 2) as COST --6
               
                          FROM IFSAPP.INVENTORY_PART_IN_STOCK iu
             
                          LEFT JOIN IFSAPP.INVENTORY_LOCATION IL ON IU.contract =
                                                                    IL.contract
                                                                AND IU.location_no =
                                                                    IL.location_no

                          LEFT JOIN IFSAPP.INVENTORY_PART_UNIT_COST aiu on iu.contract =
                                                                           aiu.contract
                                                                       and iu.part_no =
                                                                           aiu.part_no
        
                         where (iu.qty_onhand) <> 0
        
                         group by IU.contract,
                                  IL.location_group,
                                  IU.part_no,
                  
                                  IU.qty_reserved,
                                  iu.location_no,
                                  aiu.inventory_value
        
                        ) TblMain

                 GROUP BY TblMain.AS_AT_DATE,
                          TblMain.SITE_ID,
                          TblMain.PRODUCT_CODE,
                          TblMain.INVENTORY_LOCATION,
                          TblMain.COST

                UNION  

                SELECT tblTransit.AS_AT_DATE,
                       tblTransit.PRODUCT_CODE,
                       tblTransit.SITE_ID,
                       tblTransit.INVENTORY_LOCATION,
                       tblTransit.COST,
                       SUM(tblTransit.QTY) AS QTY
       
                  FROM (
                  SELECT trunc(SYSDATE) as AS_AT_DATE, --1          
                               SUBSTR(( kloo.contract || '_' || 'TRANSIT'), 1, 5) as SITE_ID, --2          
                               kloo.part_no as PRODUCT_CODE, --3          
                               sum(kloo.qty) as QTY, --4
                               'TRANSIT' AS INVENTORY_LOCATION, --5    
                               TRUNC(aiu.inventory_value, 2) as COST -- 6
        
                          FROM  IFSAPP.INV_TRANSIT_TRACKING kloo  
           
                          LEFT JOIN IFSAPP.INVENTORY_PART_UNIT_COST aiu on kloo.contract =
                                                                           aiu.contract
                                                                       and kloo.part_no =
                                                                           aiu.part_no
        
                         WHERE kloo.qty <> 0
                         GROUP BY kloo.contract, kloo.part_no, TRUNC(aiu.inventory_value, 2)
                         ) tblTransit

                 GROUP BY tblTransit.AS_AT_DATE,
                          tblTransit.SITE_ID,
                          tblTransit.PRODUCT_CODE,
                          tblTransit.INVENTORY_LOCATION,
                          tblTransit.COST
                ";
                #endregion

                oDataTable = new DataTable();

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable GetInvPosition_DMD(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionDMD();
            string selectQuery = string.Empty;

            try
            {
                #region New Sql
                selectQuery = @"                
                SELECT TblMain.AS_AT_DATE,
       TblMain.PRODUCT_CODE,
       TblMain.SITE_ID,
       TblMain.INVENTORY_LOCATION,
       TblMain.COST,
       SUM(TblMain.QTY) AS QTY       
       
  FROM (SELECT        
         trunc(SYSDATE) as AS_AT_DATE, --1          
          SUBSTR((IU.contract || '_' || IL.location_group), 1, 5) as SITE_ID, --2          
          IU.part_no as PRODUCT_CODE, --3          
          sum(iu.qty_onhand) as QTY, --4
          iu.location_no AS INVENTORY_LOCATION, --5    
          TRUNC(aiu.inventory_value,2) as COST   --6

          FROM IFSAPP.INVENTORY_PART_IN_STOCK iu  
      
               
          LEFT JOIN IFSAPP.INVENTORY_LOCATION IL ON IU.contract = IL.contract                                                   
                                                AND IU.location_no = IL.location_no        
                   
          LEFT JOIN IFSAPP.INVENTORY_PART_UNIT_COST aiu on iu.contract = aiu.contract                                                          
                                                       and iu.part_no = aiu.part_no     
          
                                                              
         where (iu.qty_onhand) <> 0              
                
         group by IU.contract,                  
                   IL.location_group,                   
                   IU.part_no,                                 
                   IU.qty_reserved,
                   iu.location_no,
                   aiu.inventory_value
        
        ) TblMain

 GROUP BY TblMain.AS_AT_DATE,
          TblMain.SITE_ID,
          TblMain.PRODUCT_CODE,
          TblMain.INVENTORY_LOCATION,
          TblMain.COST

UNION  

SELECT tblTransit.AS_AT_DATE,
       tblTransit.PRODUCT_CODE,
       tblTransit.SITE_ID,
       tblTransit.INVENTORY_LOCATION,
       tblTransit.COST,
       SUM(tblTransit.QTY) AS QTY
       
  FROM (
  SELECT trunc(SYSDATE) as AS_AT_DATE, --1          
               SUBSTR(( kloo.contract || '_' || 'TRANSIT'), 1, 5) as SITE_ID, --2          
               kloo.part_no as PRODUCT_CODE, --3          
               sum(kloo.qty) as QTY, --4
               'TRANSIT' AS INVENTORY_LOCATION, --5    
               TRUNC(aiu.inventory_value, 2) as COST -- 6
        
          FROM  IFSAPP.INV_TRANSIT_TRACKING kloo  
           
          LEFT JOIN IFSAPP.INVENTORY_PART_UNIT_COST aiu on kloo.contract =
                                                           aiu.contract
                                                       and kloo.part_no =
                                                           aiu.part_no
        
         WHERE kloo.qty <> 0
         GROUP BY kloo.contract, kloo.part_no, TRUNC(aiu.inventory_value, 2)
         ) tblTransit

 GROUP BY tblTransit.AS_AT_DATE,
          tblTransit.SITE_ID,
          tblTransit.PRODUCT_CODE,
          tblTransit.INVENTORY_LOCATION,
          tblTransit.COST

                ";
                #endregion
                oDataTable = new DataTable();

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public int Insert_NeenOpal_BI_Inv_Position(DataTable dtInvPosition)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Querry

                string insertQuery = @"

                INSERT INTO NRTAN.BI_INVENTORY_POSITION
                (
                 AS_AT_DATE,
                 PRODUCT_CODE,
                 SITE_ID,
                 INVENTORY_LOCATION,
                 COST,
                 QTY   
                )
          
            values
              (
               :AS_AT_DATE,
               :PRODUCT_CODE,
               :SITE_ID,
               :INVENTORY_LOCATION,
               :COST,
               :QTY
               )
                    ";
                #endregion

                foreach (DataRow dRow in dtInvPosition.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[] 
                        { 
                             
                            DBConn.AddParameter("AS_AT_DATE",dRow["AS_AT_DATE"])    
                           ,DBConn.AddParameter("PRODUCT_CODE",dRow["PRODUCT_CODE"])
                           ,DBConn.AddParameter("SITE_ID",dRow["SITE_ID"])
                           ,DBConn.AddParameter("INVENTORY_LOCATION",dRow["INVENTORY_LOCATION"])
                           ,DBConn.AddParameter("COST",dRow["COST"])
                           ,DBConn.AddParameter("QTY",dRow["QTY"])
                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                    }
                }

                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }


        public DataTable GetCashManagement(DateTime dtPreviousDate, string strLiveYearMonth)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region New Sql
                selectQuery = @"                
                 SELECT

 t.contract AS SITE_ID, --1
 TRUNC(SYSDATE) AS TRAN_DATE, --2
 nvl(t.retention_amount, 0) -
 NVL((select sum(NVL(nn.reserve_open_amount, 0))
       FROM ifsapp.ssl_open_amt_res_dtl nn
      where nn.site_code = t.contract
        and nn.site_code = lv.site_id
        and trunc(t.statement_date) >= to_date(nn.valid_from, 'YYYY/MM/DD')
        AND trunc(t.statement_date) <= to_date(nn.valid_to, 'YYYY/MM/DD')
     
      group by nn.site_code),
     0) AS OPEN_AMOUNT, --3
 
 nvl((select COUNT(nvl(ln.bank_statement_no, 0)) AS BANKED_TRANS
       from ifsapp.bcb_statement_bank_detail ln, ifsapp.bcb_statement hd
      where hd.bcb_statement_id = ln.bcb_statement_id
        and hd.contract = t.contract
        AND hd.contract = LV.SITE_ID
        AND trunc(ln.banked_date) = TO_DATE(:PreviousDate, 'YYYY/MM/DD')
      group by t.contract
     
     ),
     0) AS BANKED_TRANS, --4
 
 nvl((select SUM(ln.amount_banked) AS BANKING_AMOUNT
       from ifsapp.bcb_statement_bank_detail ln, ifsapp.bcb_statement hd
      where hd.bcb_statement_id = ln.bcb_statement_id
        and hd.contract = t.contract
        and hd.contract = lv.site_id
        AND hd.contract = LV.SITE_ID
        AND trunc(ln.banked_date) = TO_DATE(:PreviousDate, 'YYYY/MM/DD') ----HERE IS THE Parameter
      group by t.contract
     
     ),
     0) AS BANKING_AMOUNT, --5     
 
 (
 
  (select count(*) as AccountCount1
     FROM (SELECT COUNT(hp.account_no), hp.contract
             FROM ifsapp.hpnret_hp_head_info hp,
                  ifsapp.hpnret_hp_dtl_info  dt,
                  
                  ifsapp.hpnret_levels_overview v
            WHERE hp.account_no = dt.account_no
              AND v.site_id = hp.contract
              AND v.site_id = dt.contract
              AND dt.supply_code NOT IN ('Pkg')
              AND dt.state = 'Active'
              AND dt.account_rev = dt.account_rev
              AND hp.contract = dt.contract
              AND hp.contract not in ('YEM01')
              and hp.total_outstanding_bal > 0
              AND ((dt.install_amt *
                  (months_between(to_date(:Year_Month, 'YYYY/MM'),
                                    to_date(to_char(hp.original_sales_date,
                                                    'YYYY/MM'),
                                            'YYYY/MM')))) >
                  (dt.hire_price - dt.outstanding_balance))
              AND hp.length_of_contract >=
                  (months_between(to_date(:Year_Month, 'YYYY/MM'),
                                  to_date(to_char(hp.original_sales_date,
                                                  'YYYY/MM'),
                                          'YYYY/MM')))
           
            Group By hp.contract, hp.account_no) tblA
    where tblA.contract = t.contract
      and tblA.contract = lv.site_id
   
   )) +
 
 (
  
  select COUNT(*) as AccountCount2
    from (SELECT hp.contract, count(hp.account_no) AS NoOfAccount2
           
             FROM ifsapp.hpnret_hp_head_info hp,
                  ifsapp.hpnret_hp_dtl_info  dt,
                  
                  ifsapp.hpnret_levels_overview v
           
            WHERE hp.account_no = dt.account_no
              AND v.site_id = hp.contract
              AND v.site_id = dt.contract
              AND dt.supply_code NOT IN ('Pkg')
              AND dt.state = 'Active'
              AND dt.account_rev = dt.account_rev
              AND hp.contract = dt.contract
                 
                 -- AND hp.contract LIKE '&Site' ----join
              AND hp.contract not in ('YEM01')
              and hp.total_outstanding_bal > 0
              AND (dt.install_amt * hp.length_of_contract) >
                  (dt.hire_price - dt.outstanding_balance)
              AND hp.length_of_contract <
                  (months_between(to_date(:Year_Month, 'YYYY/MM'),
                                  to_date(to_char(hp.original_sales_date,
                                                  'YYYY/MM'),
                                          'YYYY/MM')))
           
            Group By hp.contract, hp.account_no
           
           ) tblB
   where tblB.contract = t.contract
     and tblB.contract = lv.site_id
  
  ) as ARREARS_TRANS, --6  ----ask manomi
 
 (select (SELECT SUM((dt.install_amt *
                     (months_between(to_date(:Year_Month, 'YYYY/MM'),
                                      to_date(to_char(hp.original_sales_date,
                                                      'YYYY/MM'),
                                              'YYYY/MM')))) -
                     (dt.hire_price - dt.outstanding_balance))
            FROM ifsapp.hpnret_hp_head_info hp, ifsapp.hpnret_hp_dtl_info dt
           WHERE hp.account_no = dt.account_no
             AND v.site_id = hp.contract
             AND v.site_id = dt.contract
             AND dt.supply_code NOT IN ('Pkg')
             AND dt.state = 'Active'
             AND dt.account_rev = dt.account_rev
             AND hp.contract = dt.contract
             AND hp.total_outstanding_bal > 0
             AND ((dt.install_amt *
                 (months_between(to_date(:Year_Month, 'YYYY/MM'),
                                   to_date(to_char(hp.original_sales_date,
                                                   'YYYY/MM'),
                                           'YYYY/MM')))) >
                 (dt.hire_price - dt.outstanding_balance))
             AND hp.length_of_contract >=
                 (months_between(to_date(:Year_Month, 'YYYY/MM'),
                                 to_date(to_char(hp.original_sales_date,
                                                 'YYYY/MM'),
                                         'YYYY/MM')))
           Group By hp.contract) +
         
         (SELECT SUM((dt.install_amt * hp.length_of_contract) -
                     (dt.hire_price - dt.outstanding_balance))
            FROM ifsapp.hpnret_hp_head_info hp, ifsapp.hpnret_hp_dtl_info dt
           WHERE hp.account_no = dt.account_no
             AND v.site_id = hp.contract
             AND v.site_id = dt.contract
             AND dt.supply_code NOT IN ('Pkg')
             AND dt.state = 'Active'
             AND dt.account_rev = dt.account_rev
             AND hp.contract = dt.contract
             AND hp.total_outstanding_bal > 0
             AND (dt.install_amt * hp.length_of_contract) >
                 (dt.hire_price - dt.outstanding_balance)
             AND hp.length_of_contract <
                 (months_between(to_date(:Year_Month, 'YYYY/MM'),
                                 to_date(to_char(hp.original_sales_date,
                                                 'YYYY/MM'),
                                         'YYYY/MM')))
           Group BY hp.contract)
  
    FROM ifsapp.hpnret_levels_overview v
   WHERE v.SITE_ID not in ('YEM01')
        --AND v.SITE_ID LIKE 'BADO1' ----HERE IS THE Parameter
     and v.site_id = t.contract
     and v.site_id = lv.site_id
  -- and v.Channel_Id IN (4, 7, 10, 11, 12, 2142, 1640)
   GROUP BY v.channel,
            v.area,
            v.district,
            v.SITE_ID,
            IFSAPP.Site_Api.Get_Description(v.SITE_ID))
 
 AS ARREARS_VALUE, --7  --ASK MANOMI
 
 (SELECT count(*) as CASH_ADVANCES_TRANS
    FROM ifsapp.hpnret_adv_pay ah
   WHERE (ah.state = 'Approved' OR ah.state = 'Transferred')
     AND ah.create_date = to_date(:PreviousDate, 'YYYY/MM/DD') ----HERE IS THE Parameter
     AND ah.contract = t.contract
     AND ah.contract = lv.site_id
     and ah.account_code not in ('CASHA')
   GROUP BY t.contract, lv.site_id
  
  ) as CASH_ADVANCES_TRANS, --8 
 
 (SELECT SUM(ah.dom_amount) as CASH_ADVANCES_VALUE
    FROM ifsapp.hpnret_adv_pay ah
   WHERE (ah.state = 'Approved' OR ah.state = 'Transferred')
     AND ah.create_date = to_date(:PreviousDate, 'YYYY/MM/DD') ----HERE IS THE Parameter
     AND ah.contract = t.contract
     AND ah.contract = lv.site_id
        
     and ah.account_code not in ('CASHA')
   GROUP BY t.contract, lv.site_id
  
  ) as CASH_ADVANCES_VALUE, --9 
 
 nvl((SELECT DISTINCT sum(IFSAPP.HPNRET_CUSTOMER_ORDER_API.GET_TOT_UNPAID(CO.ORDER_NO)) OPEN_AMOUNT
     
       FROM IFSAPP.HPNRET_CUSTOMER_ORDER   CO,
            IFSAPP.CUSTOMER_ORDER_INV_HEAD COIH,
            IFSAPP.HPNRET_LEVELS_OVERVIEW  ar
      WHERE CO.ORDER_NO = COIH.CREATORS_REFERENCE
        AND COIH.OBJSTATE IN
            ('PostedAuth', 'PartlyPaidPosted', 'PartlyPaidPrelPosted',
             'PrelPostedAuth', 'Posted')
        AND COIH.INVOICE_DATE <= TO_DATE(:PreviousDate, 'YYYY/MM/DD') ----HERE IS THE Parameter
        AND CO.SALES_TYPE_DB = 'CREDIT'
        AND CO.CONTRACT =
            IFSAPP.USER_ALLOWED_SITE_API.AUTHORIZED(CO.CONTRACT)
        AND CO.CONTRACT = T.contract
        AND CO.CONTRACT = lv.site_id
        and co.contract = ar.site_id
        and ar.site_id = T.contract
        and ar.site_id = lv.site_id
        AND IFSAPP.SITE_API.GET_SITE_TYPE(CO.CONTRACT) = 'BRANCH'),
     0) as OVERDUE_CREDIT_SALES, --10
 
 (
  
  select sum(d.amount) HP_COLLECTION
  
    FROM ifsapp.hpnret_pay_receipt     r,
          ifsapp.hpnret_pay_rec_head    d,
          ifsapp.hpnret_levels_overview ad
  
   WHERE r.contract = ad.site_id
     AND r.receipt_no = d.receipt_no
     AND r.contract = d.contract
     AND d.state IN ('Printed', 'Approved')
     AND r.contract = T.contract
     AND r.contract = lv.site_id
     and d.contract = T.contract
     and d.contract = lv.site_id
        --AND r.admin = 'TRUE'---- both customer payment and ADMIN (branch manager )COLLECTION
     and d.contract not in ('HAA01')
     AND trunc(r.voucher_date) = TO_DATE(:PreviousDate, 'YYYY/MM/DD') ----HERE IS THE Parameter
   group by T.contract, lv.site_id
  
  ) as HP_COLLECTION, --11   
 
 (select distinct SUM(s.cash_value) AS CashValue
    FROM IFSAPP.direct_sales_dtl_tab S
   where TRUNC(s.transaction_date) = to_date(:PreviousDate, 'YYYY/MM/DD')
     AND s.catalog_type not in ('PKG')
     AND s.free_item = 'N'
     and s.shop_code = t.contract
     and s.shop_code = lv.site_id
   GROUP BY t.contract, lv.site_id) +
 (
  
  SELECT
  
   SUM(ah.dom_amount) as CASH_ADVANCES_VALUE
    FROM ifsapp.hpnret_adv_pay ah
   WHERE (ah.state = 'Approved' OR ah.state = 'Transferred')
     AND ah.create_date = to_date(:PreviousDate, 'YYYY/MM/DD') ----HERE IS THE Parameter
     AND ah.contract = t.contract
     AND ah.contract = lv.site_id
  --and ah.account_code not in ('CASHA')
   GROUP BY t.contract, lv.site_id
  
  ) as OTHER_COLLECTIONS, --12 
 
 NVL((
     
     select sum(d.amount) HP_COLLECTION
     
       FROM ifsapp.hpnret_pay_receipt     r,
             ifsapp.hpnret_pay_rec_head    d,
             ifsapp.hpnret_levels_overview ad
     
      WHERE r.contract = ad.site_id
        AND r.receipt_no = d.receipt_no
        AND r.contract = d.contract
        AND d.state IN ('Printed', 'Approved')
        AND r.contract = T.contract
        AND r.contract = lv.site_id
        and d.contract = T.contract
        and d.contract = lv.site_id
        AND r.admin = 'TRUE' ---- ADMIN COLLECTION
        and d.contract not in ('HAA01')
        AND trunc(r.voucher_date) = TO_DATE(:PreviousDate, 'YYYY/MM/DD') ----HERE IS THE Parameter
      group by T.contract, lv.site_id
     
     ),
     0) as ISSUE_VALUE, --13   --this is admin collection  
     
     
       0  AS     BALANCE_ISSUE_AMOUNT ,  --14  --later WITH the issue management system
0 as UNREALISED_DEPOSIT  , --15 --later.neens to get from BRS system Dinesh.

NVL(   (

select  
       count(dt.account_no)

from  ifsapp.hpnret_hp_dtl_info     dt

where dt.supply_code NOT IN ('Pkg')
and dt.cash_price > 0
and dt.outstanding_balance > 0
and dt.contract = t.contract
and dt.contract = lv.site_id
AND to_date(dt.sales_date,'yyyy/mm/dd') <= trunc(SYSDATE) 
group by t.company,lv.site_id

) , 0) as NO_OF_HP_ACCOUNTS,  --16

(


(

select  
       SUM(NVL(pr.amount, 0)) + SUM(NVL(op.amount, 0)) +
       SUM(NVL(pr.discount, 0)) Amount
  from IFSAPP.hpnret_pay_receipt_head ph,
       IFSAPP.hpnret_pay_receipt      pr,
       IFSAPP.HPNRET_HP_OTHER_PAY     op
 where ph.objstate in ('Created')
   and ph.company = pr.company(+)
   and ph.receipt_no = pr.receipt_no(+)
   and pr.company = op.company(+)
   and pr.receipt_no = op.receipt_no(+)
   and pr.line_no = op.line_no(+)
   and ph.contract  = T.contract
   and ph.contract = LV.SITE_ID
   and ph.Contract IN
       (SELECT contract
          FROM IFSAPP.site_public
         WHERE contract =
               IFSAPP.User_Allowed_Site_API.Authorized(ph.Contract))
      --and (ph.receipt_date) between to_date('&from_date_','YYYY/MM/DD') and to_date('&to_date_','YYYY/MM/DD')+1
   and (ph.receipt_date) <= trunc(SYSDATE)
 group by ph.contract
)

+

(


SELECT DISTINCT SUM(ifsapp.hpnret_customer_order_api.get_tot_unpaid(co.order_no)) open_amount
  FROM ifsapp.hpnret_customer_order co, ifsapp.customer_order_inv_head coih
 WHERE co.order_no = coih.creators_reference
   AND co.sales_type_db = 'CASH'
   and coih.series_id in ('CD')
   AND coih.objstate IN
       ('PostedAuth', 'PartlyPaidPosted', 'PartlyPaidPrelPosted',
        'PrelPostedAuth', 'Posted')
   AND co.contract = T.contract
   AND co.contract = lv.site_id
   AND coih.invoice_date <= trunc(SYSDATE)

)

) AS SHORT_REMITTANCE_VALUE,  --17

NVL(   (

select  
       SUM(dt.outstanding_balance)

from  ifsapp.hpnret_hp_dtl_info     dt

where dt.supply_code NOT IN ('Pkg')
and dt.cash_price > 0
and dt.outstanding_balance > 0
and dt.contract = t.contract
and dt.contract = lv.site_id
AND to_date(dt.sales_date,'yyyy/mm/dd') <= trunc(SYSDATE) 
group by t.company,lv.site_id

) , 0) as OUTSTANDING_BALANCE   --18


  FROM IFSAPP.BCB_STATEMENT             t,
       IFSAPP.SITE_CASH_ACCOUNTS_DETAIL t1,
       ifsapp.hpnret_levels_overview    lv,
       IFSAPP.HPNRET_LEVEL_HIERARCHY    h
 WHERE 
--t.contract = UPPER('BAD01') ----HERE IS THE Parameter AND 
   t.contract = lv.site_id
   AND t.state LIKE ('%Open%')
   AND t.short_name = t1.short_name
   AND t.company = t1.company
   AND t.contract = t1.contract
   AND t.cash_account_type = t1.cash_account_type
   AND t1.default_cash_acc = 'TRUE'
   and t.contract = h.site_id
                ";
                #endregion
                oDataTable = new DataTable();

                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                   ,DBConn.AddParameter("Year_Month",strLiveYearMonth)
                };

                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable,param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public int InsertCashManagementTo_BI_NRTANServerIncrementalProcess(List<CashManagement> lstCashManagement)
        {
            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Sql
                string stringQuerry = @"
                INSERT INTO NRTAN.BI_CASH_MANAGEMENT
                (
                SITE_ID	 
                ,TRAN_DATE	 	
                ,OPEN_AMOUNT	 		
                ,BANKED_TRANS	 		
                ,BANKING_AMOUNT	 	
                ,ARREARS_TRANS	 
                ,ARREARS_VALUE	 	
                ,CASH_ADVANCES_TRANS	 
                ,CASH_ADVANCES_VALUE	 
                ,OVERDUE_CREDIT_SALES	 
                ,HP_COLLECTION	 
                ,OTHER_COLLECTIONS	 
                ,ISSUE_VALUE	 
                ,BALANCE_ISSUE_AMOUNT	 
                ,UNREALISED_DEPOSIT	 		
                ,NO_OF_HP_ACCOUNTS	 
                ,SHORT_REMITTANCE_VALUE	 
                ,LEASE_ASSET_OUTSTD_BALANCE	 
                )
                VALUES
                (
                :SITE_ID	 
                ,TO_DATE(:TRAN_DATE,'yyyy-MM-dd')  	
                ,:OPEN_AMOUNT	 		
                ,:BANKED_TRANS	 		
                ,:BANKING_AMOUNT	 	
                ,:ARREARS_TRANS	 
                ,:ARREARS_VALUE	 	
                ,:CASH_ADVANCES_TRANS	 
                ,:CASH_ADVANCES_VALUE	 
                ,:OVERDUE_CREDIT_SALES	 
                ,:HP_COLLECTION	 
                ,:OTHER_COLLECTIONS	 
                ,:ISSUE_VALUE	 
                ,:BALANCE_ISSUE_AMOUNT	 
                ,:UNREALISED_DEPOSIT	 		
                ,:NO_OF_HP_ACCOUNTS	 
                ,:SHORT_REMITTANCE_VALUE	 
                ,:LEASE_ASSET_OUTSTD_BALANCE	 
                )
                ";
                #endregion

                foreach (CashManagement objCashManagement in lstCashManagement)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("SITE_ID",objCashManagement.SITE_ID) 
                        ,DBConn.AddParameter("TRAN_DATE",objCashManagement.TRAN_DATE.ToString("yyyy/MM/dd") )
                        ,DBConn.AddParameter("OPEN_AMOUNT",objCashManagement.OPEN_AMOUNT ) 
                        ,DBConn.AddParameter("BANKED_TRANS",objCashManagement.BANKED_TRANS ) 
                        ,DBConn.AddParameter("BANKING_AMOUNT",objCashManagement.BANKING_AMOUNT ) 
                        ,DBConn.AddParameter("ARREARS_TRANS",objCashManagement.ARREARS_TRANS ) 
                        ,DBConn.AddParameter("ARREARS_VALUE",objCashManagement.ARREARS_VALUE ) 
                        ,DBConn.AddParameter("CASH_ADVANCES_TRANS",objCashManagement.CASH_ADVANCES_TRANS ) 
                        ,DBConn.AddParameter("CASH_ADVANCES_VALUE",objCashManagement.CASH_ADVANCES_VALUE ) 
                        ,DBConn.AddParameter("OVERDUE_CREDIT_SALES",objCashManagement.OVERDUE_CREDIT_SALES ) 
                        ,DBConn.AddParameter("HP_COLLECTION",objCashManagement.HP_COLLECTION ) 
                        ,DBConn.AddParameter("OTHER_COLLECTIONS",objCashManagement.OTHER_COLLECTIONS ) 
                        ,DBConn.AddParameter("ISSUE_VALUE",objCashManagement.ISSUE_VALUE ) 
                        ,DBConn.AddParameter("BALANCE_ISSUE_AMOUNT",objCashManagement.BALANCE_ISSUE_AMOUNT ) 
                        ,DBConn.AddParameter("UNREALISED_DEPOSIT",objCashManagement.UNREALISED_DEPOSIT ) 
                        ,DBConn.AddParameter("NO_OF_HP_ACCOUNTS",objCashManagement.NO_OF_HP_ACCOUNTS ) 
                        ,DBConn.AddParameter("SHORT_REMITTANCE_VALUE",objCashManagement.SHORT_REMITTANCE_VALUE ) 
                        ,DBConn.AddParameter("LEASE_ASSET_OUTSTD_BALANCE",objCashManagement.LEASE_ASSET_OUTSTD_BALANCE ) 
                };

                        DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;


                    }
                    catch (Exception ex)
                    {
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objCashManagement, ex.StackTrace), "Error - In InsertProductTo_BI_NRTANServerIncrementalProcess", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                oOracleConnection.Close();

            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }

            return intRecordCount;
        }

        public bool InsertEndofTheBIWholeProcessRecordForEmail(DateTime dtTaskDate)
        {
            try
            {
                bool isSave = false;

                OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
                string stringQuerry = @"
                

                     INSERT INTO NRTAN.BI_EMAIL_CONTROL 
                    (
                    TASKDATE
                    ,MAILREDYSTATUS
                    ,SENDSTATUS
                    )
                    VALUES
                    (
                    TO_DATE(:TASK_DATE,'yyyy/MM/dd')
                    ,'R'
                    ,'N'
                    )

                ";

                OracleParameter[] param = new OracleParameter[] 
                   { 
                     DBConn.AddParameter("TASK_DATE",dtTaskDate.ToString("yyyy/MM/dd"))
                };

                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In InsertEndofTheNeenOpalWholeProcessRecordForEmail ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                throw ex;
            }
        }

        public DataTable GetIFSSalesFilesTransferDetails(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region New
                selectQuery = @"
                SELECT 
                to_date(:PreviousDate, 'YYYY/MM/DD') as TANSFER_DATE
                ,TblMain.Status
                ,TblMain.File_Type
                FROM
                (
                SELECT   'D' AS File_Type  , CASE WHEN aa.state = 'Ready' THEN 'SUCCESS (SINGER)' ELSE 'UNSUCCESS (SINGER)' END Status
                FROM IFSAPP.transaction_sys_local_tab aa
                WHERE TRUNC(aa.Created) = TO_DATE(:CurrentDATE,'YYYY/MM/DD')  
                AND aa.Username IN ( 'BIMALD' ,'MOHANC')
                AND aa.procedure_name  IN('Hpnret_Direct_Sales_API.Shedule_Transfer' )  
                UNION
                SELECT      'I' AS File_Type , CASE WHEN aa.state = 'Ready' THEN 'SUCCESS (SINGER)' ELSE 'UNSUCCESS (SINGER)' END Status
                FROM IFSAPP.transaction_sys_local_tab aa
                WHERE TRUNC(aa.Created) = TO_DATE(:CurrentDATE,'YYYY/MM/DD')  
                AND aa.Username IN ( 'BIMALD' ,'MOHANC')
                AND aa.procedure_name  IN( 'Hpnret_Indirect_Sales_API.Process') 
                ) TblMain
                ";
                #endregion


                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                   ,DBConn.AddParameter("CurrentDATE",dtPreviousDate.AddDays(1).ToString("yyyy/MM/dd"))
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }

        }

        public DataTable GetIFSS_DMD_SalesFilesTransferDetails(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionDMD();
            string selectQuery = string.Empty;

            try
            {


                #region New
                selectQuery = @"
                SELECT 
                to_date(:PreviousDate, 'YYYY/MM/DD') as TANSFER_DATE
                ,TblMain.Status
                ,TblMain.File_Type
                FROM
                (
                SELECT   'D' AS File_Type  , CASE WHEN aa.state = 'Ready' THEN 'SUCCESS (DMD)' ELSE 'UNSUCCESS (DMD)' END Status
                FROM IFSAPP.transaction_sys_local_tab aa
                WHERE TRUNC(aa.Created) = TO_DATE(:CurrentDATE,'YYYY/MM/DD')  
                AND aa.procedure_name  IN('Hpnret_Direct_Sales_API.Shedule_Transfer' )  
                UNION
                SELECT      'I' AS File_Type , CASE WHEN aa.state = 'Ready' THEN 'SUCCESS (DMD)' ELSE 'UNSUCCESS (DMD)' END Status
                FROM IFSAPP.transaction_sys_local_tab aa
                WHERE TRUNC(aa.Created) = TO_DATE(:CurrentDATE,'YYYY/MM/DD')  
                AND aa.procedure_name  IN( 'Hpnret_Indirect_Sales_API.Run_Process') 
                ) TblMain
                ";
                #endregion


                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))
                   ,DBConn.AddParameter("CurrentDATE",dtPreviousDate.AddDays(1).ToString("yyyy/MM/dd"))
                };


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return oDataTable;

            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }

        }

        public int InsertSingerIFSSalesFileTransactionDetailsTo_NRTAN_Server(DataTable dtSingerIFSFileTransferFile)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Querry


                string insertQuery = @"
                     INSERT INTO NRTAN.SIN_BI_SALES_FILE_TRAN_LOG
                    (
                    TANSFER_DATE
                    ,FILE_TYPE
                    ,STATUS
                    )
                    VALUES(
                    :TANSFER_DATE
                    ,:FILE_TYPE
                    ,:STATUS
                    )
                    ";
                #endregion

                foreach (DataRow dRow in dtSingerIFSFileTransferFile.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[] 
                        { 
                           DBConn.AddParameter("TANSFER_DATE",dRow["TANSFER_DATE"]) 
                           ,DBConn.AddParameter("FILE_TYPE",dRow["File_Type"]) 
                           ,DBConn.AddParameter("STATUS",dRow["Status"])   
                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        LoggingHelper.LogErrorToDb(insertQuery, ex.Message);
                    }
                }

                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        public DataTable GetMktBudget(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {
                #region New Sql
                selectQuery = @"                
                SELECT B.year AS TRAN_YEAR,
                B.contract AS BUDGET_SITE,
                IFSAPP.SITE_API.Get_Description(B.contract) AS BUDGET_DES,
                B.budget AS BUDGET,
                B.budget_utilized AS BUDGET_UTILIZED,
                B.balance AS BALANCE       
                FROM IFSAPP.PURCHASING_BUDGET B
                ";
                #endregion
                oDataTable = new DataTable();


                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, CommandType.Text, oOracleConnection);

                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public int Insert_NeenOpal_Marketing_Budget(DataTable dtMktBudget)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region Querry


                string insertQuery = @"

                insert into NRTAN.BI_MARKETING_BUDGET
                (
                 TRAN_YEAR,
                 BUDGET_SITE,
                 BUDGET_DES,
                 BUDGET,
                 BUDGET_UTILIZED,                 
                 BALANCE
                )
          
            values
              (
    
               :TRAN_YEAR,
               :BUDGET_SITE,
               :BUDGET_DES,
               :BUDGET,
               :BUDGET_UTILIZED,
               :BALANCE               

               )
                    ";
                #endregion

                foreach (DataRow dRow in dtMktBudget.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[] 
                        { 
                             
                           DBConn.AddParameter("TRAN_YEAR",dRow["TRAN_YEAR"])    
                           ,DBConn.AddParameter("BUDGET_SITE",dRow["BUDGET_SITE"])  
                           ,DBConn.AddParameter("BUDGET_DES",dRow["BUDGET_DES"])    
                           ,DBConn.AddParameter("BUDGET",dRow["BUDGET"])
                           ,DBConn.AddParameter("BUDGET_UTILIZED",dRow["BUDGET_UTILIZED"])  
                           ,DBConn.AddParameter("BALANCE",dRow["BALANCE"])    
                                                      
                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                    }
                }

                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        public DataRow GetMonthlyDirectSalesInNotInPackage_ForComparision(int intYear,int intMonth)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {


                #region Sql
                selectQuery = @"
                 
             select nvl( sum(tblMain.NetUnit),0) as NetUnit , nvl(sum(tblMain.NetValue),0) as NetValue
from
(
SELECT 
       SUM((AA.HIRE_UNITS - AA.REVERTS_UNITS + AA.REVERT_REVERSE_UNITS) +
           (AA.CASH_UNITS)) NetUnit,
       SUM((AA.NET_HIRE_CASH_VALUE - AA.REVERTS_VALUE +
           AA.REVERT_REVERSE_VALUE) + (AA.CASH_VALUE)) NetValue
  FROM IFSAPP.DIRECT_SALES_DTL_TAB AA
WHERE ----TRUNC(AA.TRANSACTION_DATE)  = TO_DATE(:dtPreviousDate, 'YYYY/MM/DD')  
    AA.YEAR  =  :intYear
    AND LPAD((AA.PERIOD),2,0) = LPAD((:intMonth),2,0)
    AND AA.CATALOG_TYPE NOT IN ('PKG')
   /*AND (SELECT SUBSTR(I.SECOND_COMMODITY, 1, 2)
          FROM IFSAPP.INVENTORY_PART I
         WHERE I.CONTRACT = AA.SHOP_CODE
           AND I.PART_NO = AA.SALES_PART) not in ('2P','2C','GV')
   AND AA.SALES_PART <> 'PARTS'*/
 
   )tblMain

          
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("intYear",intYear)
                   ,DBConn.AddParameter("intMonth",intMonth)
                    
                };

                O_dRow = (DataRow)dbConnection.Executes(selectQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return O_dRow;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataRow GetMonthlyIndirectSalesInNotInPackage_ForComparision(int intYear, int intMonth)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {


                #region Sql
                selectQuery = @"
                 
             select nvl( sum(tblMain.NetUnit),0) as NetUnit , nvl(sum(tblMain.NetValue),0) as NetValue
            from
            (
            SELECT  SUM(BB.UNITS) as NetUnit, SUM(BB.VALUE) as NetValue
              FROM IFSAPP.INDIRECT_SALES_DTL_TAB BB
            WHERE ----TRUNC(BB.TRANSACTION_DATE) = TO_DATE(:dtPreviousDate, 'YYYY/MM/DD') 
                BB.STAT_YEAR  =  :intYear
                AND LPAD((BB.STAT_PERIOD_NO),2,0) = LPAD((:intMonth),2,0)
               AND BB.PART_TYPE <> 'PKG'
               AND BB.DEALER <> 'DM09999'
               /*AND (SELECT SUBSTR(I.SECOND_COMMODITY, 1, 2)
                      FROM IFSAPP.INVENTORY_PART I
                     WHERE I.CONTRACT = 'BAD01'
                       AND I.PART_NO = BB.SALES_PART) not in ('2P','2C','GV')
               AND BB.SALES_PART <> 'PARTS'*/
               )tblMain

          
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                    DBConn.AddParameter("intYear",intYear)
                   ,DBConn.AddParameter("intMonth",intMonth)
                };

                O_dRow = (DataRow)dbConnection.Executes(selectQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return O_dRow;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataRow GetMonthlyDMDSalesInNotInPackage_ForComparision(int intYear, int intMonth)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnectionDMD();
            string selectQuery = string.Empty;

            try
            {


                #region Sql
                selectQuery = @"
                 
              SELECT  NVL(sum(tblMain.NetUnit),0) as NetUnit, NVL(sum(tblMain.NetValue),0) as NetValue FROM 
(
SELECT  SUM(MM.UNITS) AS NetUnit , SUM(MM.VALUE) as NetValue
  FROM IFSAPP.INDIRECT_SALES_DTL_TAB MM
 WHERE ----TRUNC(MM.TRANSACTION_DATE) =  TO_DATE(:dtPreviousDate, 'YYYY/MM/DD')  
        MM.Stat_Year  =  :intYear
        AND LPAD((MM.stat_period_no),2,0) = LPAD((:intMonth),2,0)
   AND MM.PART_TYPE NOT IN ('PKG')
   AND MM.DEALER <> 'DM09001'
   AND MM.FREE_ISSUE = 'N'
  /* AND (SELECT SUBSTR(I.SECOND_COMMODITY, 1, 2)
          FROM IFSAPP.INVENTORY_PART I
         WHERE I.CONTRACT = 'WDA01'
           AND I.PART_NO = MM.SALES_PART) not in  ('2P','2C','GV')
   AND MM.SALES_PART <> 'PARTS'*/
  )tblMain

          
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("intYear",intYear)
                   ,DBConn.AddParameter("intMonth",intMonth)
                };

                O_dRow = (DataRow)dbConnection.Executes(selectQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return O_dRow;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataRow GetMonthlyBI_DirectSales_BackUpSales_ForComparision(int intYear, int intMonth)
        {
            oDataTable = new DataTable();
            //OracleConnection oOracleConnection = dbConnection.GetConnection();
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();
            string selectQuery = string.Empty;

            try
            {

                #region Sql
                selectQuery = @"
                select  
                nvl(sum(sl.units),0)  AS  NetUnit
                ,nvl(sum(sl.value),0)  AS  NetValue
                --from ifsapp.sin_bi_sale_line sl
                FROM NRTAN.BI_SALE_LINE sl
                WHERE ----trunc(sl.tdate) =  TO_DATE(:dtPreviousDate , 'YYYY/MM/DD')  
                EXTRACT(YEAR FROM sl.TDATE) = :intYear
                AND LPAD(EXTRACT(MONTH FROM SL.TDATE),2,0)  = LPAD((:intMonth),2,0)
                AND sl.salesfiletype = 'D'
                AND sl.sale_type not in ('20','21','22')  
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("intYear",intYear)
                   ,DBConn.AddParameter("intMonth",intMonth)
                };

                O_dRow = (DataRow)dbConnection.Executes(selectQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return O_dRow;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataRow GetMonthlyBI_InDirectSales_BackUpSales_ForComparision(int intYear, int intMonth)
        {
            oDataTable = new DataTable();
            //OracleConnection oOracleConnection = dbConnection.GetConnection();
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

            string selectQuery = string.Empty;

            try
            {

                #region Sql
                selectQuery = @"
                select  
                nvl(sum(sl.units),0)  AS  NetUnit
                ,nvl(sum(sl.value),0)  AS  NetValue
                --from ifsapp.sin_bi_sale_line sl
                FROM NRTAN.BI_SALE_LINE sl
                WHERE ----trunc(sl.tdate) =  TO_DATE(:dtPreviousDate , 'YYYY/MM/DD')  
                EXTRACT(YEAR FROM sl.TDATE) = :intYear
                AND LPAD(EXTRACT(MONTH FROM SL.TDATE),2,0)  = LPAD((:intMonth),2,0)
                AND sl.salesfiletype = 'I'
                AND sl.sale_type not in ('20','21','22')  
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("intYear",intYear)
                   ,DBConn.AddParameter("intMonth",intMonth)
                };

                O_dRow = (DataRow)dbConnection.Executes(selectQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return O_dRow;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataRow GetMonthlyBI_DMDSales_BackUpSales_ForComparision(int intYear, int intMonth)
        {
            oDataTable = new DataTable();
            //OracleConnection oOracleConnection = dbConnection.GetConnection();
            OracleConnection oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

            string selectQuery = string.Empty;

            try
            {

                #region Sql
                selectQuery = @"
                select  
                nvl(sum(sl.units),0)  AS  NetUnit
                ,nvl(sum(sl.value),0)  AS  NetValue
                --from ifsapp.sin_bi_sale_line sl
                FROM NRTAN.BI_SALE_LINE sl
                WHERE ----trunc(sl.tdate) =  TO_DATE(:dtPreviousDate , 'YYYY/MM/DD')  
                EXTRACT(YEAR FROM sl.TDATE) = :intYear
                AND LPAD(EXTRACT(MONTH FROM SL.TDATE),2,0)  = LPAD((:intMonth),2,0)
                AND sl.salesfiletype = 'DMD'
                AND sl.sale_type not in ('20','21','22')  
                ";
                #endregion

                oDataTable = new DataTable();
                OracleParameter[] param = new OracleParameter[] 
                { 
                   DBConn.AddParameter("intYear",intYear)
                   ,DBConn.AddParameter("intMonth",intMonth)
                };

                O_dRow = (DataRow)dbConnection.Executes(selectQuery, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                oOracleConnection.Close();
                return O_dRow;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public bool InsertMonthlyNeenOpalVsIFSFigureSalesToTable(MonthlyBIVsSingerSalesLog objMonthlyBIVsSingerSalesLog)
        {
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            bool isSave = false;

            try
            {

                OracleParameter[] param = new OracleParameter[] 
                   { 
                        DBConn.AddParameter("TASK_DATE",objMonthlyBIVsSingerSalesLog.Task_Date.ToString("yyyy/MM/dd") ) 
                        ,DBConn.AddParameter("LOG_YEAR",objMonthlyBIVsSingerSalesLog.Log_Year) 
                        ,DBConn.AddParameter("LOG_MONTH",objMonthlyBIVsSingerSalesLog.Log_Month) 

                        ,DBConn.AddParameter("DIRECT_VALUE",objMonthlyBIVsSingerSalesLog.MonthlySingerDirectSaleValue ) 
                        ,DBConn.AddParameter("INDIRECT_VALUE",objMonthlyBIVsSingerSalesLog.MonthlySingerIndirectSaleValue ) 
                        ,DBConn.AddParameter("DMD_VALUE",objMonthlyBIVsSingerSalesLog.MonthlyDMDIndirectSaleValue ) 

                        ,DBConn.AddParameter("BI_DIRECT_VALUE",objMonthlyBIVsSingerSalesLog.MonthlyBI_Direct_SaleValue )
                        ,DBConn.AddParameter("BI_INDIRECT_VALUE",objMonthlyBIVsSingerSalesLog.MonthlyBI_Indirect_SingerNetSale )
                        ,DBConn.AddParameter("BI_DMD_VALUE",objMonthlyBIVsSingerSalesLog.MonthlyBI_DMD_SingerNetSale )

                        ,DBConn.AddParameter("SINGER_NETSALE",objMonthlyBIVsSingerSalesLog.MonthlySinger_NetSale )
                        ,DBConn.AddParameter("BI_NETSALE",objMonthlyBIVsSingerSalesLog.MonthlyBI_NetSale )

                        ,DBConn.AddParameter("DIFFERENCE",objMonthlyBIVsSingerSalesLog.Difference )
                        ,DBConn.AddParameter("ENTERED_ON",objMonthlyBIVsSingerSalesLog.EnteredOn.ToString("yyyy/MM/dd hh:mm:ss"))
                   };


                #region Sql
                string stringQuerry = @"

                INSERT INTO IFSAPP.SIN_BI_MONTHLY_SALES_FIGURE
                (
                   TASK_DATE
                    ,LOG_YEAR
                    ,LOG_MONTH

                   ,DIRECT_VALUE
                   ,INDIRECT_VALUE
                   ,DMD_VALUE

                  ,BI_DIRECT_VALUE              
                  ,BI_INDIRECT_VALUE            
                  ,BI_DMD_VALUE              


                   ,SINGER_NETSALE
                   ,BI_NETSALE

                   ,DIFFERENCE
                   ,ENTEREDON
                )
                VALUES
                (
                    TO_DATE(:TASK_DATE,'yyyy/MM/dd')
                    ,:LOG_YEAR
                    ,:LOG_MONTH

                    ,:DIRECT_VALUE
                    ,:INDIRECT_VALUE
                    ,:DMD_VALUE

                  ,:BI_DIRECT_VALUE              
                  ,:BI_INDIRECT_VALUE            
                  ,:BI_DMD_VALUE    

                    ,:SINGER_NETSALE
                    ,:BI_NETSALE

                    ,:DIFFERENCE
                    ,TO_DATE(:ENTERED_ON,'yyyy/MM/dd HH24:MI:SS') 
                )
                ";
                #endregion


                DataRow dRow = (DataRow)dbConnection.Executes(stringQuerry, ReturnType.DataRow, param, CommandType.Text, oOracleConnection);
                isSave = true;
                oOracleConnection.Close();
                return isSave;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, objMonthlyBIVsSingerSalesLog, ex.StackTrace), "Error - In InsertMonthlyNeenOpalVsIFSFigureSalesToTable", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isSave;
        }


        #region HP WALK

        #region BI_HP_WALK_MEGA

        public DataTable Get_BI_HP_WALK_MEGA(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region selectQuery 2022/03/19

                selectQuery = @"
--MODULE NAME:  BI_HP_WALK_MEGA
--DESCRIPTION:  'BI_HP_WALK_MEGA' SELECT QUERY
--CREATED BY:   ANUSHKAR
--CREATED DATE: 2022/03/19
--UPDATED BY:   
--UPDATED DATE: 

SELECT ORDER_NO,
       CONTRACT,
       CUSTOMER_NO,
       SOURCE_OF_INCOME,
       MONTHLY_INCOME_RANGE,
       PLACE_OF_EMPLOYEMENT,
       AFFORDABLE_FIRST_PAYMENT,
       MONTHLY_PAYMENT_RANGE,
       STATUS,
       REASON,
       APPROVER_ID,
       REMARKS,
       READ,
       NO_GUR_REASON,
       ACTION_TIME,
       ROWVERSION
  FROM ifsapp.nc_hp_cust_income_info_tab";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public int Insert_NeenOpal_BI_HP_WALK_MEGA(DataTable dt_BI_HP_WALK_MEGA)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region insertQuery 2022/03/19

                string insertQuery = @"
--MODULE NAME:  BI_HP_WALK_MEGA
--DESCRIPTION:  'BI_HP_WALK_MEGA' INSERT QUERY
--CREATED BY:   ANUSHKAR
--CREATED DATE: 2022/03/19
--UPDATED BY:   
--UPDATED DATE: 

INSERT INTO NRTAN.BI_HP_WALK_MEGA 
(
    ORDER_NO,
    CONTRACT,
    CUSTOMER_NO,
    SOURCE_OF_INCOME,
    MONTHLY_INCOME_RANGE,
    PLACE_OF_EMPLOYEMENT,
    AFFORDABLE_FIRST_PAYMENT,
    MONTHLY_PAYMENT_RANGE,
    STATUS,
    REASON,
    APPROVER_ID,
    REMARKS,
    READ,
    NO_GUR_REASON,
    ACTION_TIME,
    ROWVERSION
)
VALUES
(
    :ORDER_NO,
    :CONTRACT,
    :CUSTOMER_NO,
    :SOURCE_OF_INCOME,
    :MONTHLY_INCOME_RANGE,
    :PLACE_OF_EMPLOYEMENT,
    :AFFORDABLE_FIRST_PAYMENT,
    :MONTHLY_PAYMENT_RANGE,
    :STATUS,
    :REASON,
    :APPROVER_ID,
    :REMARKS,
    :READ,
    :NO_GUR_REASON,
    :ACTION_TIME,
    :ROWVERSION
)
";

                #endregion

                foreach (DataRow dRow in dt_BI_HP_WALK_MEGA.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[]
                        {

                           DBConn.AddParameter("ORDER_NO",dRow["ORDER_NO"])
                           ,DBConn.AddParameter("CONTRACT",dRow["CONTRACT"])
                           ,DBConn.AddParameter("CUSTOMER_NO",dRow["CUSTOMER_NO"])
                           ,DBConn.AddParameter("SOURCE_OF_INCOME",dRow["SOURCE_OF_INCOME"])
                           ,DBConn.AddParameter("MONTHLY_INCOME_RANGE",dRow["MONTHLY_INCOME_RANGE"])
                           ,DBConn.AddParameter("PLACE_OF_EMPLOYEMENT",dRow["PLACE_OF_EMPLOYEMENT"])
                           ,DBConn.AddParameter("AFFORDABLE_FIRST_PAYMENT",dRow["AFFORDABLE_FIRST_PAYMENT"])
                           ,DBConn.AddParameter("MONTHLY_PAYMENT_RANGE",dRow["MONTHLY_PAYMENT_RANGE"])
                           ,DBConn.AddParameter("STATUS",dRow["STATUS"])
                           ,DBConn.AddParameter("REASON",dRow["REASON"])
                           ,DBConn.AddParameter("APPROVER_ID",dRow["APPROVER_ID"])
                           ,DBConn.AddParameter("REMARKS",dRow["REMARKS"])
                           ,DBConn.AddParameter("READ",dRow["READ"])
                           ,DBConn.AddParameter("NO_GUR_REASON",dRow["NO_GUR_REASON"])
                           ,DBConn.AddParameter("ACTION_TIME",dRow["ACTION_TIME"])
                           ,DBConn.AddParameter("ROWVERSION",dRow["ROWVERSION"])

                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                    }
                }

                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        #endregion

        #region BI_WRITE_OFF_AMT

        public DataTable Get_BI_WRITE_OFF_AMT(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region selectQuery 2022/03/23

                selectQuery = @"
--MODULE NAME:  BI_WRITE_OFF_AMT
--DESCRIPTION:  'BI_WRITE_OFF_AMT' SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/03/22
--UPDATED BY:   
--UPDATED DATE: 

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type AS SALES_GROUPING,
       ifsapp.HPNRET_PAY_DTL_API.Get_Acc_Outstnding_Bal(dt.company, dt.account_no, dt.account_rev) AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT

  FROM ifsapp.hpnret_hp_head_tab dt

 WHERE 
       dt.contract = 'HAD01'
       AND to_date(dt.sales_date) <= to_date(:PreviousDate, 'YYYY/MM/DD')";

                #endregion

                oDataTable = new DataTable();

                OracleParameter[] param = new OracleParameter[]
                {
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                 };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public int Insert_NeenOpal_BI_WRITE_OFF_AMT(DataTable dt_BI_WRITE_OFF_AMT)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region insertQuery 2022/03/23

                string insertQuery = @"
--MODULE NAME:  BI_WRITE_OFF_AMT
--DESCRIPTION:  'BI_WRITE_OFF_AMT ' INSERT QUERY
--CREATED BY:   ANUSHKAR
--CREATED DATE: 2022/02/23
--UPDATED BY:   
--UPDATED DATE: 

INSERT INTO NRTAN.BI_WRITE_OFF_AMT 
(
    SITE_ID,
    SALES_DATE,
    ACCOUNT_NO,
    CUSTOMER_NO,
    SALES_GROUPING,
    TOTAL_RECIEVABLE,
    SANASUMA_VALUE,
    SURAKSHA_VALUE,
    BUDGET_BOOK,
    DOWNPAYMENT,
    FIRST_PAYMENT,
    SALESMAN_ID,
    MONTHLY_PAY,
    AGREED_TO_PAY_DATE,
    PRODUCT_CODE,
    CASH_PRICE,
    GROSS_HIRE_VALUE,
    LENGTH_OF_CONTRACT
)
VALUES
(
    :SITE_ID,
    :SALES_DATE,
    :ACCOUNT_NO,
    :CUSTOMER_NO,
    :SALES_GROUPING,
    :TOTAL_RECIEVABLE,
    :SANASUMA_VALUE,
    :SURAKSHA_VALUE,
    :BUDGET_BOOK,
    :DOWNPAYMENT,
    :FIRST_PAYMENT,
    :SALESMAN_ID,
    :MONTHLY_PAY,
    :AGREED_TO_PAY_DATE,
    :PRODUCT_CODE,
    :CASH_PRICE,
    :GROSS_HIRE_VALUE,
    :LENGTH_OF_CONTRACT
)
";

                #endregion

                foreach (DataRow dRow in dt_BI_WRITE_OFF_AMT.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[]
                        {

                           DBConn.AddParameter("SITE_ID",dRow["SITE_ID"])
                           ,DBConn.AddParameter("SALES_DATE",dRow["SALES_DATE"])
                           ,DBConn.AddParameter("ACCOUNT_NO",dRow["ACCOUNT_NO"])
                           ,DBConn.AddParameter("CUSTOMER_NO",dRow["CUSTOMER_NO"])
                           ,DBConn.AddParameter("SALES_GROUPING",dRow["SALES_GROUPING"])
                           ,DBConn.AddParameter("TOTAL_RECIEVABLE",dRow["TOTAL_RECIEVABLE"])
                           ,DBConn.AddParameter("SANASUMA_VALUE",dRow["SANASUMA_VALUE"])
                           ,DBConn.AddParameter("SURAKSHA_VALUE",dRow["SURAKSHA_VALUE"])
                           ,DBConn.AddParameter("BUDGET_BOOK",dRow["BUDGET_BOOK"])
                           ,DBConn.AddParameter("DOWNPAYMENT",dRow["DOWNPAYMENT"])
                           ,DBConn.AddParameter("FIRST_PAYMENT",dRow["FIRST_PAYMENT"])
                           ,DBConn.AddParameter("SALESMAN_ID",dRow["SALESMAN_ID"])
                           ,DBConn.AddParameter("MONTHLY_PAY",dRow["MONTHLY_PAY"])
                           ,DBConn.AddParameter("AGREED_TO_PAY_DATE",dRow["AGREED_TO_PAY_DATE"])
                           ,DBConn.AddParameter("PRODUCT_CODE",dRow["PRODUCT_CODE"])
                           ,DBConn.AddParameter("CASH_PRICE",dRow["CASH_PRICE"])
                           ,DBConn.AddParameter("GROSS_HIRE_VALUE",dRow["GROSS_HIRE_VALUE"])
                           ,DBConn.AddParameter("LENGTH_OF_CONTRACT",dRow["LENGTH_OF_CONTRACT"])

                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                    }
                }

                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        #endregion

        #region BI_ONLINE_HP_ACC

        public DataTable Get_BI_ONLINE_HP_ACC_Area_13(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region SelectSQL 2022/04/06

                selectQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC' Area 13 SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/02/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/04/06

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type_db AS SALES_GROUPING,
       dt.total_outstanding_bal AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       
       NVL((SELECT ar.additional_pay_
             FROM ifsapp.sin_tot_arr_amt_pr_date ar
            WHERE ar.Account_No = dt.account_no
                  AND ar.account_rev = dt.account_rev
                  AND rownum = 1),
           0) AS ADDITIONAL_PAYMENT,
       
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT,
       
       CASE
           WHEN NVL((SELECT ar.tot_arr_amt
                      FROM ifsapp.sin_tot_arr_amt_pr_date ar
                     WHERE ar.Account_No = dt.account_no
                           AND ar.account_rev = dt.account_rev
                           AND rownum = 1),
                    0) < 0 THEN
            0
           ELSE
            NVL((SELECT ar.tot_arr_amt
                  FROM ifsapp.sin_tot_arr_amt_pr_date ar
                 WHERE ar.Account_No = dt.account_no
                       AND ar.account_rev = dt.account_rev
                       AND rownum = 1),
                0)
        END AS CUSTOMER_ARREARS

  FROM ifsapp.hpnret_hp_head_info dt

 WHERE dt.total_outstanding_bal > 0
       AND to_date(dt.sales_date) <= trunc(SYSDATE - 1)
       and (select l.area_id from ifsapp.hpnret_levels_overview l where l.site_id = dt.contract and rownum = 1) = '13'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable Get_BI_ONLINE_HP_ACC_Area_18(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region SelectSQL 2022/04/06

                selectQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC' Area 18 SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/02/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/04/06

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type_db AS SALES_GROUPING,
       dt.total_outstanding_bal AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       
       NVL((SELECT ar.additional_pay_
             FROM ifsapp.sin_tot_arr_amt_pr_date ar
            WHERE ar.Account_No = dt.account_no
                  AND ar.account_rev = dt.account_rev
                  AND rownum = 1),
           0) AS ADDITIONAL_PAYMENT,
       
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT,
       
       CASE
           WHEN NVL((SELECT ar.tot_arr_amt
                      FROM ifsapp.sin_tot_arr_amt_pr_date ar
                     WHERE ar.Account_No = dt.account_no
                           AND ar.account_rev = dt.account_rev
                           AND rownum = 1),
                    0) < 0 THEN
            0
           ELSE
            NVL((SELECT ar.tot_arr_amt
                  FROM ifsapp.sin_tot_arr_amt_pr_date ar
                 WHERE ar.Account_No = dt.account_no
                       AND ar.account_rev = dt.account_rev
                       AND rownum = 1),
                0)
        END AS CUSTOMER_ARREARS

  FROM ifsapp.hpnret_hp_head_info dt

 WHERE dt.total_outstanding_bal > 0
       AND to_date(dt.sales_date) <= trunc(SYSDATE - 1)
       and (select l.area_id from ifsapp.hpnret_levels_overview l where l.site_id = dt.contract and rownum = 1) = '18'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable Get_BI_ONLINE_HP_ACC_Area_1374(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region SelectSQL 2022/04/06

                selectQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC' Area 1374 SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/02/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/04/06

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type_db AS SALES_GROUPING,
       dt.total_outstanding_bal AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       
       NVL((SELECT ar.additional_pay_
             FROM ifsapp.sin_tot_arr_amt_pr_date ar
            WHERE ar.Account_No = dt.account_no
                  AND ar.account_rev = dt.account_rev
                  AND rownum = 1),
           0) AS ADDITIONAL_PAYMENT,
       
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT,
       
       CASE
           WHEN NVL((SELECT ar.tot_arr_amt
                      FROM ifsapp.sin_tot_arr_amt_pr_date ar
                     WHERE ar.Account_No = dt.account_no
                           AND ar.account_rev = dt.account_rev
                           AND rownum = 1),
                    0) < 0 THEN
            0
           ELSE
            NVL((SELECT ar.tot_arr_amt
                  FROM ifsapp.sin_tot_arr_amt_pr_date ar
                 WHERE ar.Account_No = dt.account_no
                       AND ar.account_rev = dt.account_rev
                       AND rownum = 1),
                0)
        END AS CUSTOMER_ARREARS

  FROM ifsapp.hpnret_hp_head_info dt

 WHERE dt.total_outstanding_bal > 0
       AND to_date(dt.sales_date) <= trunc(SYSDATE - 1)
       and (select l.area_id from ifsapp.hpnret_levels_overview l where l.site_id = dt.contract and rownum = 1) = '1374'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable Get_BI_ONLINE_HP_ACC_Area_13018(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region SelectSQL 2022/04/06

                selectQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC' Area 13018 SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/03/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/04/06

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type_db AS SALES_GROUPING,
       dt.total_outstanding_bal AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       
       NVL((SELECT ar.additional_pay_
             FROM ifsapp.sin_tot_arr_amt_pr_date ar
            WHERE ar.Account_No = dt.account_no
                  AND ar.account_rev = dt.account_rev
                  AND rownum = 1),
           0) AS ADDITIONAL_PAYMENT,
       
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT,
       
       CASE
           WHEN NVL((SELECT ar.tot_arr_amt
                      FROM ifsapp.sin_tot_arr_amt_pr_date ar
                     WHERE ar.Account_No = dt.account_no
                           AND ar.account_rev = dt.account_rev
                           AND rownum = 1),
                    0) < 0 THEN
            0
           ELSE
            NVL((SELECT ar.tot_arr_amt
                  FROM ifsapp.sin_tot_arr_amt_pr_date ar
                 WHERE ar.Account_No = dt.account_no
                       AND ar.account_rev = dt.account_rev
                       AND rownum = 1),
                0)
        END AS CUSTOMER_ARREARS

  FROM ifsapp.hpnret_hp_head_info dt

 WHERE dt.total_outstanding_bal > 0
       AND to_date(dt.sales_date) <= trunc(SYSDATE - 1)
       and (select l.area_id from ifsapp.hpnret_levels_overview l where l.site_id = dt.contract and rownum = 1) = '13018'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable Get_BI_ONLINE_HP_ACC_Area_17(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region SelectSQL 2022/04/06

                selectQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC' Area 17 SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/03/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/04/06

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type_db AS SALES_GROUPING,
       dt.total_outstanding_bal AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       
       NVL((SELECT ar.additional_pay_
             FROM ifsapp.sin_tot_arr_amt_pr_date ar
            WHERE ar.Account_No = dt.account_no
                  AND ar.account_rev = dt.account_rev
                  AND rownum = 1),
           0) AS ADDITIONAL_PAYMENT,
       
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT,
       
       CASE
           WHEN NVL((SELECT ar.tot_arr_amt
                      FROM ifsapp.sin_tot_arr_amt_pr_date ar
                     WHERE ar.Account_No = dt.account_no
                           AND ar.account_rev = dt.account_rev
                           AND rownum = 1),
                    0) < 0 THEN
            0
           ELSE
            NVL((SELECT ar.tot_arr_amt
                  FROM ifsapp.sin_tot_arr_amt_pr_date ar
                 WHERE ar.Account_No = dt.account_no
                       AND ar.account_rev = dt.account_rev
                       AND rownum = 1),
                0)
        END AS CUSTOMER_ARREARS

  FROM ifsapp.hpnret_hp_head_info dt

 WHERE dt.total_outstanding_bal > 0
       AND to_date(dt.sales_date) <= trunc(SYSDATE - 1)
       and (select l.area_id from ifsapp.hpnret_levels_overview l where l.site_id = dt.contract and rownum = 1) = '17'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable Get_BI_ONLINE_HP_ACC_Area_390(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region SelectSQL 2022/04/06

                selectQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC' Area 390 SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/02/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/04/06

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type_db AS SALES_GROUPING,
       dt.total_outstanding_bal AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       
       NVL((SELECT ar.additional_pay_
             FROM ifsapp.sin_tot_arr_amt_pr_date ar
            WHERE ar.Account_No = dt.account_no
                  AND ar.account_rev = dt.account_rev
                  AND rownum = 1),
           0) AS ADDITIONAL_PAYMENT,
       
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT,
       
       CASE
           WHEN NVL((SELECT ar.tot_arr_amt
                      FROM ifsapp.sin_tot_arr_amt_pr_date ar
                     WHERE ar.Account_No = dt.account_no
                           AND ar.account_rev = dt.account_rev
                           AND rownum = 1),
                    0) < 0 THEN
            0
           ELSE
            NVL((SELECT ar.tot_arr_amt
                  FROM ifsapp.sin_tot_arr_amt_pr_date ar
                 WHERE ar.Account_No = dt.account_no
                       AND ar.account_rev = dt.account_rev
                       AND rownum = 1),
                0)
        END AS CUSTOMER_ARREARS

  FROM ifsapp.hpnret_hp_head_info dt

 WHERE dt.total_outstanding_bal > 0
       AND to_date(dt.sales_date) <= trunc(SYSDATE - 1)
       and (select l.area_id from ifsapp.hpnret_levels_overview l where l.site_id = dt.contract and rownum = 1) = '390'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable Get_BI_ONLINE_HP_ACC_Area_5955(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region SelectSQL 2022/04/06

                selectQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC' Area 5955 SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/02/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/04/06

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type_db AS SALES_GROUPING,
       dt.total_outstanding_bal AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       
       NVL((SELECT ar.additional_pay_
             FROM ifsapp.sin_tot_arr_amt_pr_date ar
            WHERE ar.Account_No = dt.account_no
                  AND ar.account_rev = dt.account_rev
                  AND rownum = 1),
           0) AS ADDITIONAL_PAYMENT,
       
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT,
       
       CASE
           WHEN NVL((SELECT ar.tot_arr_amt
                      FROM ifsapp.sin_tot_arr_amt_pr_date ar
                     WHERE ar.Account_No = dt.account_no
                           AND ar.account_rev = dt.account_rev
                           AND rownum = 1),
                    0) < 0 THEN
            0
           ELSE
            NVL((SELECT ar.tot_arr_amt
                  FROM ifsapp.sin_tot_arr_amt_pr_date ar
                 WHERE ar.Account_No = dt.account_no
                       AND ar.account_rev = dt.account_rev
                       AND rownum = 1),
                0)
        END AS CUSTOMER_ARREARS

  FROM ifsapp.hpnret_hp_head_info dt

 WHERE dt.total_outstanding_bal > 0
       AND to_date(dt.sales_date) <= trunc(SYSDATE - 1)
       and (select l.area_id from ifsapp.hpnret_levels_overview l where l.site_id = dt.contract and rownum = 1) = '5955'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable Get_BI_ONLINE_HP_ACC_Area_1657(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region SelectSQL 2022/04/06

                selectQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC' Area 1657 SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/02/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/04/06

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type_db AS SALES_GROUPING,
       dt.total_outstanding_bal AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       
       NVL((SELECT ar.additional_pay_
             FROM ifsapp.sin_tot_arr_amt_pr_date ar
            WHERE ar.Account_No = dt.account_no
                  AND ar.account_rev = dt.account_rev
                  AND rownum = 1),
           0) AS ADDITIONAL_PAYMENT,
       
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT,
       
       CASE
           WHEN NVL((SELECT ar.tot_arr_amt
                      FROM ifsapp.sin_tot_arr_amt_pr_date ar
                     WHERE ar.Account_No = dt.account_no
                           AND ar.account_rev = dt.account_rev
                           AND rownum = 1),
                    0) < 0 THEN
            0
           ELSE
            NVL((SELECT ar.tot_arr_amt
                  FROM ifsapp.sin_tot_arr_amt_pr_date ar
                 WHERE ar.Account_No = dt.account_no
                       AND ar.account_rev = dt.account_rev
                       AND rownum = 1),
                0)
        END AS CUSTOMER_ARREARS

  FROM ifsapp.hpnret_hp_head_info dt

 WHERE dt.total_outstanding_bal > 0
       AND to_date(dt.sales_date) <= trunc(SYSDATE - 1)
       and (select l.area_id from ifsapp.hpnret_levels_overview l where l.site_id = dt.contract and rownum = 1) = '1657'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable Get_BI_ONLINE_HP_ACC_Area_14(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region SelectSQL 2022/04/06

                selectQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC' Area 14 SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/02/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/04/06

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type_db AS SALES_GROUPING,
       dt.total_outstanding_bal AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       
       NVL((SELECT ar.additional_pay_
             FROM ifsapp.sin_tot_arr_amt_pr_date ar
            WHERE ar.Account_No = dt.account_no
                  AND ar.account_rev = dt.account_rev
                  AND rownum = 1),
           0) AS ADDITIONAL_PAYMENT,
       
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT,
       
       CASE
           WHEN NVL((SELECT ar.tot_arr_amt
                      FROM ifsapp.sin_tot_arr_amt_pr_date ar
                     WHERE ar.Account_No = dt.account_no
                           AND ar.account_rev = dt.account_rev
                           AND rownum = 1),
                    0) < 0 THEN
            0
           ELSE
            NVL((SELECT ar.tot_arr_amt
                  FROM ifsapp.sin_tot_arr_amt_pr_date ar
                 WHERE ar.Account_No = dt.account_no
                       AND ar.account_rev = dt.account_rev
                       AND rownum = 1),
                0)
        END AS CUSTOMER_ARREARS

  FROM ifsapp.hpnret_hp_head_info dt

 WHERE dt.total_outstanding_bal > 0
       AND to_date(dt.sales_date) <= trunc(SYSDATE - 1)
       and (select l.area_id from ifsapp.hpnret_levels_overview l where l.site_id = dt.contract and rownum = 1) = '14'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable Get_BI_ONLINE_HP_ACC_Area_13017(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region SelectSQL 2022/04/06

                selectQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC' Area 13017 SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/02/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/04/06

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type_db AS SALES_GROUPING,
       dt.total_outstanding_bal AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       
       NVL((SELECT ar.additional_pay_
             FROM ifsapp.sin_tot_arr_amt_pr_date ar
            WHERE ar.Account_No = dt.account_no
                  AND ar.account_rev = dt.account_rev
                  AND rownum = 1),
           0) AS ADDITIONAL_PAYMENT,
       
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT,
       
       CASE
           WHEN NVL((SELECT ar.tot_arr_amt
                      FROM ifsapp.sin_tot_arr_amt_pr_date ar
                     WHERE ar.Account_No = dt.account_no
                           AND ar.account_rev = dt.account_rev
                           AND rownum = 1),
                    0) < 0 THEN
            0
           ELSE
            NVL((SELECT ar.tot_arr_amt
                  FROM ifsapp.sin_tot_arr_amt_pr_date ar
                 WHERE ar.Account_No = dt.account_no
                       AND ar.account_rev = dt.account_rev
                       AND rownum = 1),
                0)
        END AS CUSTOMER_ARREARS

  FROM ifsapp.hpnret_hp_head_info dt

 WHERE dt.total_outstanding_bal > 0
       AND to_date(dt.sales_date) <= trunc(SYSDATE - 1)
       and (select l.area_id from ifsapp.hpnret_levels_overview l where l.site_id = dt.contract and rownum = 1) = '13017'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable Get_BI_ONLINE_HP_ACC_Area_15(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region SelectSQL 2022/04/06

                selectQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC' Area 15 SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/02/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/04/06

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type_db AS SALES_GROUPING,
       dt.total_outstanding_bal AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       
       NVL((SELECT ar.additional_pay_
             FROM ifsapp.sin_tot_arr_amt_pr_date ar
            WHERE ar.Account_No = dt.account_no
                  AND ar.account_rev = dt.account_rev
                  AND rownum = 1),
           0) AS ADDITIONAL_PAYMENT,
       
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT,
       
       CASE
           WHEN NVL((SELECT ar.tot_arr_amt
                      FROM ifsapp.sin_tot_arr_amt_pr_date ar
                     WHERE ar.Account_No = dt.account_no
                           AND ar.account_rev = dt.account_rev
                           AND rownum = 1),
                    0) < 0 THEN
            0
           ELSE
            NVL((SELECT ar.tot_arr_amt
                  FROM ifsapp.sin_tot_arr_amt_pr_date ar
                 WHERE ar.Account_No = dt.account_no
                       AND ar.account_rev = dt.account_rev
                       AND rownum = 1),
                0)
        END AS CUSTOMER_ARREARS

  FROM ifsapp.hpnret_hp_head_info dt

 WHERE dt.total_outstanding_bal > 0
       AND to_date(dt.sales_date) <= trunc(SYSDATE - 1)
       and (select l.area_id from ifsapp.hpnret_levels_overview l where l.site_id = dt.contract and rownum = 1) = '15'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable Get_BI_ONLINE_HP_ACC_Area_16(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region SelectSQL 2022/04/06

                selectQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC' Area 16 SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/02/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/04/06

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type_db AS SALES_GROUPING,
       dt.total_outstanding_bal AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       
       NVL((SELECT ar.additional_pay_
             FROM ifsapp.sin_tot_arr_amt_pr_date ar
            WHERE ar.Account_No = dt.account_no
                  AND ar.account_rev = dt.account_rev
                  AND rownum = 1),
           0) AS ADDITIONAL_PAYMENT,
       
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT,
       
       CASE
           WHEN NVL((SELECT ar.tot_arr_amt
                      FROM ifsapp.sin_tot_arr_amt_pr_date ar
                     WHERE ar.Account_No = dt.account_no
                           AND ar.account_rev = dt.account_rev
                           AND rownum = 1),
                    0) < 0 THEN
            0
           ELSE
            NVL((SELECT ar.tot_arr_amt
                  FROM ifsapp.sin_tot_arr_amt_pr_date ar
                 WHERE ar.Account_No = dt.account_no
                       AND ar.account_rev = dt.account_rev
                       AND rownum = 1),
                0)
        END AS CUSTOMER_ARREARS

  FROM ifsapp.hpnret_hp_head_info dt

 WHERE dt.total_outstanding_bal > 0
       AND to_date(dt.sales_date) <= trunc(SYSDATE - 1)
       and (select l.area_id from ifsapp.hpnret_levels_overview l where l.site_id = dt.contract and rownum = 1) = '16'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public DataTable Get_BI_ONLINE_HP_ACC_Area_13016(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region SelectSQL 2022/04/06

                selectQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC' Area 13016 SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/02/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/04/06

SELECT dt.contract AS SITE_ID,
       dt.sales_date AS SALES_DATE,
       dt.account_no AS ACCOUNT_NO,
       dt.id AS CUSTOMER_NO,
       dt.customer_type_db AS SALES_GROUPING,
       dt.total_outstanding_bal AS TOTAL_RECIEVABLE,
       ifsapp.hpnret_hp_head_api.Get_Total_Sanasuma(dt.account_no, dt.account_rev) AS SANASUMA_VALUE,
       ifsapp.hpnret_hp_head_api.Get_Total_Suraksha(dt.account_no, dt.account_rev) AS SURAKSHA_VALUE,
       dt.budget_book_id AS BUDGET_BOOK,
       ifsapp.hpnret_hp_dtl_api.Get_Down_Payment(dt.account_no, dt.account_rev, 1) AS DOWNPAYMENT,
       
       NVL((SELECT ar.additional_pay_
             FROM ifsapp.sin_tot_arr_amt_pr_date ar
            WHERE ar.Account_No = dt.account_no
                  AND ar.account_rev = dt.account_rev
                  AND rownum = 1),
           0) AS ADDITIONAL_PAYMENT,
       
       ifsapp.hpnret_hp_dtl_api.Get_First_Payment(dt.account_no, dt.account_rev, 1) AS FIRST_PAYMENT,
       dt.salesman_code AS SALESMAN_ID,
       ifsapp.hpnret_hp_dtl_api.Get_Monthly_Payment(dt.account_no, dt.account_rev) AS MONTHLY_PAY,
       dt.agreed_date AS AGREED_TO_PAY_DATE,
       ifsapp.hpnret_hp_dtl_api.Get_Catalog_No(dt.account_no, dt.account_rev, 1) AS PRODUCT_CODE,
       ifsapp.hpnret_hp_dtl_api.Get_Cash_Price(dt.account_no, dt.account_rev, 1) AS CASH_PRICE,
       ifsapp.hpnret_hp_head_api.Get_Gross_Hire_Value(dt.account_no, dt.account_rev) AS GROSS_HIRE_VALUE,
       dt.Length_Of_Contract AS LENGTH_OF_CONTRACT,
       
       CASE
           WHEN NVL((SELECT ar.tot_arr_amt
                      FROM ifsapp.sin_tot_arr_amt_pr_date ar
                     WHERE ar.Account_No = dt.account_no
                           AND ar.account_rev = dt.account_rev
                           AND rownum = 1),
                    0) < 0 THEN
            0
           ELSE
            NVL((SELECT ar.tot_arr_amt
                  FROM ifsapp.sin_tot_arr_amt_pr_date ar
                 WHERE ar.Account_No = dt.account_no
                       AND ar.account_rev = dt.account_rev
                       AND rownum = 1),
                0)
        END AS CUSTOMER_ARREARS

  FROM ifsapp.hpnret_hp_head_info dt

 WHERE dt.total_outstanding_bal > 0
       AND to_date(dt.sales_date) <= trunc(SYSDATE - 1)
       and (select l.area_id from ifsapp.hpnret_levels_overview l where l.site_id = dt.contract and rownum = 1) = '13016'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public int Insert_NeenOpal_BI_ONLINE_HP_ACC(DataTable dt_BI_ONLINE_HP_ACC)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region insertQuery 2022/02/22

                string insertQuery = @"
--MODULE NAME:  BI_ONLINE_HP_ACC
--DESCRIPTION:  'BI_ONLINE_HP_ACC ' INSERT QUERY
--CREATED BY:   ANUSHKAR
--CREATED DATE: 2022/02/22
--UPDATED BY:   ANUSHKAR
--UPDATED DATE: 2022/02/22

INSERT INTO NRTAN.BI_ONLINE_HP_ACC 
(
    SITE_ID,
    SALES_DATE,
    ACCOUNT_NO,
    CUSTOMER_NO,
    SALES_GROUPING,
    TOTAL_RECIEVABLE,
    SANASUMA_VALUE,
    SURAKSHA_VALUE,
    BUDGET_BOOK,
    DOWNPAYMENT,
    ADDITIONAL_PAYMENT,
    FIRST_PAYMENT,
    SALESMAN_ID,
    MONTHLY_PAY,
    AGREED_TO_PAY_DATE,
    PRODUCT_CODE,
    CASH_PRICE,
    GROSS_HIRE_VALUE,
    LENGTH_OF_CONTRACT,
    CUSTOMER_ARREARS
)
VALUES
(
    :SITE_ID,
    :SALES_DATE,
    :ACCOUNT_NO,
    :CUSTOMER_NO,
    :SALES_GROUPING,
    :TOTAL_RECIEVABLE,
    :SANASUMA_VALUE,
    :SURAKSHA_VALUE,
    :BUDGET_BOOK,
    :DOWNPAYMENT,
    :ADDITIONAL_PAYMENT,
    :FIRST_PAYMENT,
    :SALESMAN_ID,
    :MONTHLY_PAY,
    :AGREED_TO_PAY_DATE,
    :PRODUCT_CODE,
    :CASH_PRICE,
    :GROSS_HIRE_VALUE,
    :LENGTH_OF_CONTRACT,
    :CUSTOMER_ARREARS
)
";

                #endregion

                foreach (DataRow dRow in dt_BI_ONLINE_HP_ACC.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[]
                        {

                           DBConn.AddParameter("SITE_ID",dRow["SITE_ID"])
                           ,DBConn.AddParameter("SALES_DATE",dRow["SALES_DATE"])
                           ,DBConn.AddParameter("ACCOUNT_NO",dRow["ACCOUNT_NO"])
                           ,DBConn.AddParameter("CUSTOMER_NO",dRow["CUSTOMER_NO"])
                           ,DBConn.AddParameter("SALES_GROUPING",dRow["SALES_GROUPING"])
                           ,DBConn.AddParameter("TOTAL_RECIEVABLE",dRow["TOTAL_RECIEVABLE"])
                           ,DBConn.AddParameter("SANASUMA_VALUE",dRow["SANASUMA_VALUE"])
                           ,DBConn.AddParameter("SURAKSHA_VALUE",dRow["SURAKSHA_VALUE"])
                           ,DBConn.AddParameter("BUDGET_BOOK",dRow["BUDGET_BOOK"])
                           ,DBConn.AddParameter("DOWNPAYMENT",dRow["DOWNPAYMENT"])
                           ,DBConn.AddParameter("ADDITIONAL_PAYMENT",dRow["ADDITIONAL_PAYMENT"])
                           ,DBConn.AddParameter("FIRST_PAYMENT",dRow["FIRST_PAYMENT"])
                           ,DBConn.AddParameter("SALESMAN_ID",dRow["SALESMAN_ID"])
                           ,DBConn.AddParameter("MONTHLY_PAY",dRow["MONTHLY_PAY"])
                           ,DBConn.AddParameter("AGREED_TO_PAY_DATE",dRow["AGREED_TO_PAY_DATE"])
                           ,DBConn.AddParameter("PRODUCT_CODE",dRow["PRODUCT_CODE"])
                           ,DBConn.AddParameter("CASH_PRICE",dRow["CASH_PRICE"])
                           ,DBConn.AddParameter("GROSS_HIRE_VALUE",dRow["GROSS_HIRE_VALUE"])
                           ,DBConn.AddParameter("LENGTH_OF_CONTRACT",dRow["LENGTH_OF_CONTRACT"])
                           ,DBConn.AddParameter("CUSTOMER_ARREARS",dRow["CUSTOMER_ARREARS"])

                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                    }
                }

                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        #endregion

        #region BI_BLACKLIST_CUST

        public DataTable Get_BI_BLACKLIST_CUST(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region selectQuery 2022/03/25

                selectQuery = @"
--MODULE NAME:  BI_BLACKLIST_CUST
--DESCRIPTION:  'BI_BLACKLIST_CUST' SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/03/25
--UPDATED BY:   
--UPDATED DATE: 

SELECT id AS CUSTOMER_ID
  FROM ifsapp.hpnret_customer_bd
 WHERE black_listed_cus = 'TRUE'";

                #endregion

                oDataTable = new DataTable();

                //OracleParameter[] param = new OracleParameter[]
                //{
                //   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                // };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, /*param,*/ CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public int Insert_NeenOpal_BI_BLACKLIST_CUST(DataTable dt_BI_BLACKLIST_CUST)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region insertQuery 2022/03/25

                string insertQuery = @"
--MODULE NAME:  BI_BLACKLIST_CUST
--DESCRIPTION:  'BI_BLACKLIST_CUST' INSERT QUERY
--CREATED BY:   ANUSHKAR
--CREATED DATE: 2022/03/25
--UPDATED BY:   
--UPDATED DATE: 

INSERT INTO NRTAN.BI_BLACKLIST_CUST 
(
    CUSTOMER_ID
)
VALUES
(
    :CUSTOMER_ID
)
";

                #endregion

                foreach (DataRow dRow in dt_BI_BLACKLIST_CUST.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[]
                        {
                           DBConn.AddParameter("CUSTOMER_ID",dRow["CUSTOMER_ID"])
                        };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                    }
                }

                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        #endregion

        #region BI_BR_MGR_INCEN

        public DataTable Get_BI_BR_MGR_INCEN(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region selectQuery 2022/03/29

                selectQuery = @"
--MODULE NAME:  BI_BR_MGR_INCEN
--DESCRIPTION:  'BI_BR_MGR_INCEN' SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/03/29
--UPDATED BY:   
--UPDATED DATE: 

SELECT hd.company AS COMPANY,
       hd.exp_statement_id AS EXP_STATEMENT_ID,
       ex.exp_lump_sum_trans_id AS EXP_LUMP_SUM_TRANS_ID,
       hd.exp_statement_no AS EXP_STATEMENT_NO,
       ex.voucher_date AS VOUCHER_DATE,
       ex.transaction_code AS EXPENSE_CODE,
       ifsapp.site_transaction_types_api.get_transaction_type(ex.company, ex.transaction_code) AS EXPENSE_NAME,
       hd.contract AS SHOP_ID,
       ex.voucher_no AS VOUCHER_NO,
       ex.identity AS SALESMAN,
       ex.ledger_item_id AS INVOICE_NO,
       ex.curr_amount AS AMOUNT
  FROM ifsapp.site_expenses hd, ifsapp.site_expenses_detail ex
 WHERE ex.exp_statement_id = hd.exp_statement_id
       AND ex.company = hd.company
       AND ex.party_type_db = 'SUPPLIER'
       AND trunc(ex.voucher_date) = to_date(:PreviousDate, 'YYYY/MM/DD')
       AND ex.transaction_code IN ('BE001', 'BE002', 'BE003', 'BE004', 'BE005', 'BE006', 'BE007', 'BE008', 'BE009', 'BE039', 'BE041', 'BE057')";

                #endregion

                oDataTable = new DataTable();

                OracleParameter[] param = new OracleParameter[]
                {
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                 };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public int Insert_NeenOpal_BI_BR_MGR_INCEN(DataTable dt_BI_BR_MGR_INCEN)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region insertQuery 2022/03/29

                string insertQuery = @"
--MODULE NAME:  BI_BR_MGR_INCEN
--DESCRIPTION:  'BI_BR_MGR_INCEN' INSERT QUERY
--CREATED BY:   ANUSHKAR
--CREATED DATE: 2022/03/29
--UPDATED BY:   
--UPDATED DATE: 

INSERT INTO NRTAN.BI_BR_MGR_INCEN 
(
    COMPANY,
    EXP_STATEMENT_ID,
    EXP_LUMP_SUM_TRANS_ID,
    EXP_STATEMENT_NO,
    VOUCHER_DATE,
    EXPENSE_CODE,
    EXPENSE_NAME,
    SHOP_ID,
    VOUCHER_NO,
    SALESMAN,
    INVOICE_NO,
    AMOUNT
)
VALUES
(
    :COMPANY,
    :EXP_STATEMENT_ID,
    :EXP_LUMP_SUM_TRANS_ID,
    :EXP_STATEMENT_NO,
    :VOUCHER_DATE,
    :EXPENSE_CODE,
    :EXPENSE_NAME,
    :SHOP_ID,
    :VOUCHER_NO,
    :SALESMAN,
    :INVOICE_NO,
    :AMOUNT
)
";

                #endregion

                foreach (DataRow dRow in dt_BI_BR_MGR_INCEN.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[]
                        {

                           DBConn.AddParameter("COMPANY",dRow["COMPANY"])
                           ,DBConn.AddParameter("EXP_STATEMENT_ID",dRow["EXP_STATEMENT_ID"])
                           ,DBConn.AddParameter("EXP_LUMP_SUM_TRANS_ID",dRow["EXP_LUMP_SUM_TRANS_ID"])
                           ,DBConn.AddParameter("EXP_STATEMENT_NO",dRow["EXP_STATEMENT_NO"])
                           ,DBConn.AddParameter("VOUCHER_DATE",dRow["VOUCHER_DATE"])
                           ,DBConn.AddParameter("EXPENSE_CODE",dRow["EXPENSE_CODE"])
                           ,DBConn.AddParameter("EXPENSE_NAME",dRow["EXPENSE_NAME"])
                           ,DBConn.AddParameter("SHOP_ID",dRow["SHOP_ID"])
                           ,DBConn.AddParameter("VOUCHER_NO",dRow["VOUCHER_NO"])
                           ,DBConn.AddParameter("SALESMAN",dRow["SALESMAN"])
                           ,DBConn.AddParameter("INVOICE_NO",dRow["INVOICE_NO"])
                           ,DBConn.AddParameter("AMOUNT",dRow["AMOUNT"])

                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                    }
                }

                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        #endregion

        #region BI_RESALE_VAL_ITEM

        public DataTable Get_BI_RESALE_VAL_ITEM(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region selectQuery 2022/03/29

                selectQuery = @"
--MODULE NAME:  BI_RESALE_VAL_ITEM
--DESCRIPTION:  'BI_RESALE_VAL_ITEM' SELECT QUERY
--CREATED BY:   ANUSHKAR / CHATHURAS
--CREATED DATE: 2022/03/29
--UPDATED BY:   
--UPDATED DATE: 

SELECT au.line_index AS LINE_INDEX,
       au.from_date AS RESALE_DATE,
       au.authorized_user AS AUTHORIZED_USER,
       ifsapp.fnd_user_api.get_description(au.authorized_user) AS AUTHORIZED_BY,
       substr(au.account_no, 0, 3) || '01' AS SHOP_CODE,
       au.account_no AS ACCOUNT_NO,
       au.cash_price AS RESALE_PRICE,
       au.part_no AS PART_NO
  FROM ifsapp.hpnret_auth_variation au
 WHERE au.variation = 'Revert ReSale'
       AND au.state IN ('Active', 'Closed')
       AND au.utilized = '1'
       AND trunc(au.from_date) = TO_DATE(:PreviousDate, 'YYYY/MM/DD')";

                #endregion

                oDataTable = new DataTable();

                OracleParameter[] param = new OracleParameter[]
                {
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                 };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public int Insert_NeenOpal_BI_RESALE_VAL_ITEM(DataTable dt_BI_RESALE_VAL_ITEM)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region insertQuery 2022/03/29

                string insertQuery = @"
--MODULE NAME:  BI_RESALE_VAL_ITEM
--DESCRIPTION:  'BI_RESALE_VAL_ITEM' INSERT QUERY
--CREATED BY:   ANUSHKAR
--CREATED DATE: 2022/03/29
--UPDATED BY:   
--UPDATED DATE: 

INSERT INTO NRTAN.BI_RESALE_VAL_ITEM 
(
    LINE_INDEX,
    RESALE_DATE,
    AUTHORIZED_USER,
    AUTHORIZED_BY,
    SHOP_CODE,
    ACCOUNT_NO,
    RESALE_PRICE,
    PART_NO
)
VALUES
(
    :LINE_INDEX,
    :RESALE_DATE,
    :AUTHORIZED_USER,
    :AUTHORIZED_BY,
    :SHOP_CODE,
    :ACCOUNT_NO,
    :RESALE_PRICE,
    :PART_NO
)
";

                #endregion

                foreach (DataRow dRow in dt_BI_RESALE_VAL_ITEM.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[]
                        {

                           DBConn.AddParameter("LINE_INDEX",dRow["LINE_INDEX"])
                           ,DBConn.AddParameter("RESALE_DATE",dRow["RESALE_DATE"])
                           ,DBConn.AddParameter("AUTHORIZED_USER",dRow["AUTHORIZED_USER"])
                           ,DBConn.AddParameter("AUTHORIZED_BY",dRow["AUTHORIZED_BY"])
                           ,DBConn.AddParameter("SHOP_CODE",dRow["SHOP_CODE"])
                           ,DBConn.AddParameter("ACCOUNT_NO",dRow["ACCOUNT_NO"])
                           ,DBConn.AddParameter("RESALE_PRICE",dRow["RESALE_PRICE"])
                           ,DBConn.AddParameter("PART_NO",dRow["PART_NO"])

                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                    }
                }

                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        #endregion

        #region BI_GUARANTER_INFO

        public DataTable Get_BI_GUARANTER_INFO(DateTime dtPreviousDate)
        {
            oDataTable = new DataTable();
            OracleConnection oOracleConnection = dbConnection.GetConnection();
            string selectQuery = string.Empty;

            try
            {

                #region selectQuery 2022/04/29

                selectQuery = @"
--MODULE NAME:  BI_GUARANTER_INFO
--DESCRIPTION:  'BI_GUARANTER_INFO' SELECT QUERY
--CREATED BY:   ANUSHKA
--CREATED DATE: 2022/04/29
--UPDATED BY:   

SELECT h.account_no AS account_no,
       h.id AS customer_no,
       trunc(h.sales_date) AS sales_date,
       h.guarantor1 AS guarantor1,
       h.guarantor2 AS guarantor2
  FROM ifsapp.hpnret_hp_head h
 WHERE h.state IN ('Active', 'Partially Active')
       AND trunc(h.sales_date) = TO_DATE(:PreviousDate, 'YYYY/MM/DD')";

                #endregion

                oDataTable = new DataTable();

                OracleParameter[] param = new OracleParameter[]
                {
                   DBConn.AddParameter("PreviousDate",dtPreviousDate.ToString("yyyy/MM/dd"))

                 };

                oDataTable = new DataTable();
                oDataTable = (DataTable)dbConnection.Executes(selectQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);


                oOracleConnection.Close();
                return oDataTable;
            }
            catch (Exception ee)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), selectQuery, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                return null;
            }
        }

        public int Insert_NeenOpal_BI_GUARANTER_INFO(DataTable dt_BI_GUARANTER_INFO)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            bool isSave = false;
            int intRecordCount = 0;
            try
            {
                OracleConnection oOracleConnection = new OracleConnection();
                oOracleConnection = dbConnection.GetConnectionnewForNRTAN();

                #region insertQuery 2022/04/29

                string insertQuery = @"
--MODULE NAME:  BI_GUARANTER_INFO
--DESCRIPTION:  'BI_GUARANTER_INFO' INSERT QUERY
--CREATED BY:   ANUSHKAR
--CREATED DATE: 2022/04/29
--UPDATED BY:   
--UPDATED DATE: 

INSERT INTO NRTAN.BI_GUARANTER_INFO 
(
    ACCOUNT_NO,
    CUSTOMER_NO,
    SALES_DATE,
    GUARANTOR1,
    GUARANTOR2
)
VALUES
(
    :ACCOUNT_NO,
    :CUSTOMER_NO,
    :SALES_DATE,
    :GUARANTOR1,
    :GUARANTOR2
)
";

                #endregion

                foreach (DataRow dRow in dt_BI_GUARANTER_INFO.Rows)
                {
                    try
                    {
                        OracleParameter[] param = new OracleParameter[]
                        {

                           DBConn.AddParameter("ACCOUNT_NO",dRow["ACCOUNT_NO"])
                           ,DBConn.AddParameter("CUSTOMER_NO",dRow["CUSTOMER_NO"])
                           ,DBConn.AddParameter("SALES_DATE",dRow["SALES_DATE"])
                           ,DBConn.AddParameter("GUARANTOR1",dRow["GUARANTOR1"])
                           ,DBConn.AddParameter("GUARANTOR2",dRow["GUARANTOR2"])

                       };

                        DataTable dtInsert = (DataTable)dbConnection.Executes(insertQuery, ReturnType.DataTable, param, CommandType.Text, oOracleConnection);
                        isSave = true;
                        intRecordCount = intRecordCount + 1;
                    }
                    catch (Exception ex)
                    {

                        string row = "";
                        for (int j = 0; j < dRow.ItemArray.Length; j++)
                        {
                            row += dRow.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("{0} - {1} - {2}", ex.Message, row, ex.StackTrace), "Error - In inserting Values in  " + method.Name, System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                    }
                }

                oOracleConnection.Close();
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - In inserting ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            finally
            {

            }
            return intRecordCount;
        }

        #endregion

        #endregion

        #endregion
    }
}
