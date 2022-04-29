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
    public class NeenOpal_BI
    {
        #region Private Variables

        private DateTime dtCurrentDate;
        private DateTime dtPreviousDate_BI;

        private int intStoreInsertedRecCount_BI = 0;
        private int int_BI_DIM_STORE_RENT_InsertedRecCount_BI = 0;
        private int intProductInsertedRecCount_BI = 0;
        private int intStatusMasterInsertedRecCount_BI = 0;
        private int intSalesTypeMasterInsertedRecCount_BI = 0;
        private int intSalesPromoterInsertedRecCount_BI = 0;
        private int intSalesStatusInsertedRecCount_BI = 0;
        private int intPromotionInsertRecCount_BI = 0;
        private int intSalesLocationInsertRecCount_BI = 0;
        private int intInventoryLocationInsertedRecCount_BI = 0;
        private int intReturnCodeMethodEffectRecordCount = 0;
        private int intReturnCode_DMDMethodEffectRecordCount = 0;
        private int intCustomerMethodEffectedRecordCount = 0;
        private int intCustomerDMDMethodEffectedRecordCount = 0;
        private int intDiscountMethodEffectedRecordCount = 0;
        private int intSalesMethodEffectedRecordCount_NP = 0;
        private int intBackUpSalesLineMethodEffectRecordCount_BI = 0;
        private int intInventoryADJInsertRecCount = 0;
        private int intStockAgeBandEffectRecordCount = 0;
        private int intMappingBMtoADRecordCount_BI = 0;
        private int intInventoryTransationEffectRecordCount = 0;
        private int intInventoryPositionRecordCount_BI = 0;
        private int intCashManageentRecordCount_BI = 0;
        private int intMarketingBudgetRecordCount_BI = 0;
        private int intPlanned586PSalesRecordCount_BI = 0;
        private int intSalesExcludedRelatedSKUsRecordCount_BI = 0;
        private int intBI_DIM_PRODUCT_IVInsertedRecCount_BI = 0;
        private int int_BI_USER_RLS_RecordCount_BI = 0;
        private int int_BI_PRICE_LIST_RecordCount_BI = 0;
        private int int_BI_AUDIT_WEIGHT_RecordCount_BI = 0;
        private int int_BI_HPC_CCD_RecordCount_BI = 0;
        private int int_SIN_HPC_CCD_RecordCount_BI = 0;
        private int int_BI_ISSUE_SETTLEMENT_CCD_RecordCount_BI = 0;
        private int int_BI_ONLINE_HP_ACC_RecordCount_BI = 0; 
        private int int_BI_HP_WALK_MEGA_RecordCount_BI = 0;
        private int int_BI_WRITE_OFF_AMT_RecordCount_BI = 0;
        private int int_BI_BLACKLIST_CUST_RecordCount_BI = 0;
        private int int_BI_BR_MGR_INCEN_RecordCount_BI = 0;
        private int int_BI_RESALE_VAL_ITEM_RecordCount_BI = 0;
        private int int_BI_GUARANTER_INFO_RecordCount_BI = 0;

        int effectedRecCount_BI = 0;

        private string strUserName = string.Empty;
        private string strMachineName = string.Empty;

        private int PassTableID = 0;

        #endregion

        #region Private Methods

        #region Previous Date

        private DataRow GetSINGERServerCurrentDate()
        {
            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataRow dRow = odbConnection.GetSINGERServerCurrentTime();
            return dRow;
        }

        #endregion

        #region Extraction Log

        private DataRow LoadMethodRelatedTableID(BIAdminControlETITableTypes intPassTableID)
        {
            try
            {
                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                DataRow dRow = odbConnection.GetMethodRelatedTableIDDetails(intPassTableID);
                return dRow;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private bool UpdateBIAdminETLExtractionLogFile(int intPassTableID, DateTime dtTaskDate, DateTime dtUpdatedOn, int intAffectedRowCount)
        {
            try
            {

                bool isUpdated = false;
                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                isUpdated = odbConnection.UpdateBIAdminETLExtractionLogFile(intPassTableID, dtTaskDate, dtUpdatedOn, intAffectedRowCount);
                return isUpdated;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private bool EndFlagInBIAdminExtractionLog(int PassTableID, DateTime dtTaskDate, DateTime dtUpdatedOn, int intStatusFlag)
        {
            try
            {

                bool isSave = false;
                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                isSave = odbConnection.CloseStatusInBIAdminETLExtractionLogFile(PassTableID, dtTaskDate, dtUpdatedOn, intStatusFlag);
                return isSave;
            }
            catch (Exception ex)
            {

                throw ex;
            }

        }

        private bool IsTableRecordInExtractionLogFileAlreadyNotExsists(int PassTableID, DateTime dtTaskDate)
        {
            bool isDayWiseRecIsNotExsists = true;
            try
            {
                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                DataRow drLogTbl = odbConnection.CheckTableRecordInBIExtractionLogFileAlreadyExsists(PassTableID, dtPreviousDate_BI);
                if (drLogTbl != null)
                {
                    int tableLogCount = 0;
                    tableLogCount = Convert.ToInt32(drLogTbl["RowCount"]);
                    if (tableLogCount > 0)
                    {
                        isDayWiseRecIsNotExsists = false;
                    }
                }
                return isDayWiseRecIsNotExsists;
            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - IsTableRecordInExtractionLogFileAlreadyNotExsists()", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return isDayWiseRecIsNotExsists;
        }

        #endregion

        #region Incremental Sale Line Log
        public bool InsertRecordToETLSalesLineLogFile(int intPassTableID, DateTime dtTaskDate, DateTime dtUpdateOn, int intRowCount, string MethodName)
        {
            try
            {

                bool isSave = false;
                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                isSave = odbConnection.InsertToETLSalesLineLogFile(intPassTableID, dtTaskDate, dtUpdateOn, intRowCount, MethodName);
                return isSave;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region BI NRTAN

        private bool Insert_BI_NTAN_ProcessToExtractionLog(int intLogTableID, int intRowCount, DateTime dtTaskDate, int intStatus, DateTime dtStartOn, string strUserName, string strMachineName)
        {
            try
            {

                bool isSave = false;
                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                isSave = odbConnection.InsertStartProcessToBIAdminETLExtractionLogFile(intLogTableID, intRowCount, dtTaskDate, intStatus, dtStartOn, strUserName, strMachineName);
                return isSave;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private void DeleteRecordsIn_BI_NRTAN_ServerTables(DateTime dtTaskDate, int intRelatedTblID)
        {
            try
            {
                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                odbConnection.DeleteRecordsIn_BI_NRTAN_ServerTables(dtTaskDate, intRelatedTblID);
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        private void DeleteRecordsIn_BI_SSLPDB_ServerTables(DateTime dtTaskDate, int intRelatedTblID)
        {
            try
            {
                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                odbConnection.DeleteRecordsIn_BI_SSLPDB_ServerTables(dtTaskDate, intRelatedTblID);
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        private int GetMaster_BI_NRTAN_InsertedRecordCount(int intLogTableID)
        {
            try
            {

                int TotRowCount = 0;
                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                DataRow drRowCount = odbConnection.Get_BI_NRTAN_MasterTableInsertedRecordCount(intLogTableID);
                if (drRowCount != null)
                {
                    TotRowCount = Convert.ToInt32(drRowCount["RecCount"]);
                }

                return TotRowCount;

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region Master Methods

        #region Store

        private void Store()
        {

            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_Store);

            if (drPassTableID != null)
            {

                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);
                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_Store));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        intStoreInsertedRecCount_BI = this.StoreExtraction();
                        intStoreInsertedRecCount_BI = this.StoreExtraction_DMD();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));

                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Store()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int StoreExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();


            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtStore = odbConnection.GetStore(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtStore.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.StoreProcessToBI_NRTAN_Server(dtStore);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int StoreExtraction_DMD()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();


            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtStore = odbConnection.GetStore_DMD(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtStore.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.StoreProcessToBI_NRTAN_Server(dtStore);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int StoreProcessToBI_NRTAN_Server(DataTable dtSINStore)
        {
            List<Store> lstStore = new List<Store>();

            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                int newRecordTotCount = 0;



                DataTable dtStore = new DataTable();
                dtStore = dtSINStore;

                if (dtStore == null)
                {
                    return 0;
                }


                foreach (DataRow dr in dtStore.Rows)
                {

                    try
                    {
                        #region Added to DataRow

                        Store objStore = new Store();

                        objStore.SITE_ID = dr["SITE_ID"].ToString();
                        objStore.SITE_NAME = dr["SITE_NAME"].ToString();
                        objStore.CHANNEL_ID = dr["CHANNEL_ID"].ToString();
                        objStore.CHANNEL_NAME = dr["CHANNEL_NAME"].ToString();
                        objStore.AREA_ID = dr["AREA_ID"].ToString();
                        objStore.AREA_NAME = dr["AREA_NAME"].ToString();
                        objStore.DISTRICT_ID = dr["DISTRICT_ID"].ToString();
                        objStore.DISTRICT_NAME = dr["DISTRICT_NAME"].ToString();
                        objStore.AGENCY_ID = dr["AGENCY_ID"].ToString();
                        objStore.COMPANY_ID = dr["COMPANY_ID"].ToString();
                        objStore.STORE_LATITUDE = dr["STORE_LATITUDE"].ToString();
                        objStore.STORE_LONGITUDE = dr["STORE_LONGITUDE"].ToString();
                        objStore.TYPE_ID = dr["TYPE_ID"].ToString();
                        objStore.TYPE_NAME = dr["TYPE_NAME"].ToString();
                        objStore.PROVINCE_ID = dr["PROVINCE_ID"].ToString();
                        objStore.PROVINCE_NAME = dr["PROVINCE_NAME"].ToString();
                        objStore.STORE_PROVINCE = dr["STORE_PROVINCE"].ToString();
                        objStore.MOBILE_NO = dr["MOBILE_NO"].ToString();
                        objStore.QOS = dr["QOS"].ToString();
                        objStore.CAPACITIES = dr["CAPACITIES"].ToString();
                        objStore.AUDIT_GRADING = dr["AUDIT_GRADING"].ToString();
                        objStore.STORE_ATTRIBUTE_1 = dr["STORE_ATTRIBUTE_1"].ToString();
                        objStore.STORE_ATTRIBUTE_2 = dr["STORE_ATTRIBUTE_2"].ToString();
                        objStore.STORE_ATTRIBUTE_3 = dr["STORE_ATTRIBUTE_3"].ToString();
                        objStore.STORE_ATTRIBUTE_4 = dr["STORE_ATTRIBUTE_4"].ToString();
                        objStore.STORE_ATTRIBUTE_5 = dr["STORE_ATTRIBUTE_5"].ToString();
                        objStore.PRIMARY_WAREHOUSE = dr["PRIMARY_WAREHOUSE"].ToString();

                        #region FLOAT_AMT

                        Double floatVal;
                        try
                        {
                            floatVal = Convert.ToDouble(dr["FLOAT_AMT"].ToString());
                        }
                        catch (Exception ex)
                        {
                            floatVal = 0;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, floatVal, "FLOAT_AMT", dr["FLOAT_AMT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        objStore.FLOAT_AMT = floatVal;

                        #endregion

                        objStore.SL_DISTRICT_ID = dr["SL_DISTRICT_ID"].ToString();
                        objStore.SL_DISTRICT_NAME = dr["SL_DISTRICT_NAME"].ToString();
                        objStore.STORE_TYPE = dr["STORE_TYPE"].ToString();

                        #endregion

                        lstStore.Add(objStore);

                        //bool isNewRecordAdded = this.CheckStoreCodeExsisIN_BI_NRTAN_ServerAndInsertTo_NRTAN_Server(objStore);
                        //if (isNewRecordAdded == true)
                        //{
                        //newRecordTotCount = newRecordTotCount + 1;
                        //}

                    }
                    catch (Exception ee)
                    {

                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }


                }
                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                newRecordTotCount = odbConnection.InsertStoreTo_BI_NRTANServerIncrementalProcess(lstStore);
                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool CheckStoreCodeExsisIN_BI_NRTAN_ServerAndInsertTo_NRTAN_Server(Store objStore)
        {
            NeenOpal_BI_DBAccess odbConnection_New = new NeenOpal_BI_DBAccess();
            return odbConnection_New.CheckStoreCodeExsisIN_BI_NRTAN_ServerAndInsertTo_NRTAN_Server(objStore);
        }

        #endregion

        #region BI_DIM_STORE_RENT

        private void BI_DIM_STORE_RENT()
        {

            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_DIM_STORE_RENT);

            if (drPassTableID != null)
            {

                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);
                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_STORE_RENT));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        int_BI_DIM_STORE_RENT_InsertedRecCount_BI = this.BI_DIM_STORE_RENT_Extraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));

                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Store()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int BI_DIM_STORE_RENT_Extraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();


            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_DIM_STORE_RENT = odbConnection.Get_BI_DIM_STORE_RENT(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_DIM_STORE_RENT.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.BI_DIM_STORE_RENT_ProcessToBI_NRTAN_Server(dt_BI_DIM_STORE_RENT);
            //effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int BI_DIM_STORE_RENT_ProcessToBI_NRTAN_Server(DataTable dtSINStore)
        {
            List<Store> lstStore = new List<Store>();

            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                int newRecordTotCount = 0;



                DataTable dtStore = new DataTable();
                dtStore = dtSINStore;

                if (dtStore == null)
                {
                    return 0;
                }


                foreach (DataRow dr in dtStore.Rows)
                {

                    try
                    {
                        #region Added to DataRow

                        Store objStore = new Store();

                        objStore.SITE_ID = dr["SITE_ID"].ToString();

                        #region RENT_AMOUNT

                        try
                        {
                            objStore.RENT_AMOUNT = Convert.ToDouble(dr["RENT_AMOUNT"].ToString());
                        }
                        catch (Exception ex)
                        {
                            objStore.RENT_AMOUNT = 0.0;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objStore.RENT_AMOUNT, "RENT_AMOUNT", dr["RENT_AMOUNT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region STORE_ATTRIBUTE_6

                        try
                        {
                            objStore.STORE_ATTRIBUTE_6 = Convert.ToDouble(dr["STORE_ATTRIBUTE_6"].ToString());
                        }
                        catch (Exception ex)
                        {
                            objStore.STORE_ATTRIBUTE_6 = 0.0;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objStore.STORE_ATTRIBUTE_6, "STORE_ATTRIBUTE_6", dr["STORE_ATTRIBUTE_6"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region STORE_ATTRIBUTE_7

                        try
                        {
                            objStore.STORE_ATTRIBUTE_7 = Convert.ToDouble(dr["STORE_ATTRIBUTE_7"].ToString());
                        }
                        catch (Exception ex)
                        {
                            objStore.STORE_ATTRIBUTE_7 = 0.0;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objStore.STORE_ATTRIBUTE_7, "STORE_ATTRIBUTE_7", dr["STORE_ATTRIBUTE_7"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region STORE_ATTRIBUTE_8

                        try
                        {
                            objStore.STORE_ATTRIBUTE_8 = Convert.ToDouble(dr["STORE_ATTRIBUTE_8"].ToString());
                        }
                        catch (Exception ex)
                        {
                            objStore.STORE_ATTRIBUTE_8 = 0.0;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objStore.STORE_ATTRIBUTE_8, "STORE_ATTRIBUTE_8", dr["STORE_ATTRIBUTE_8"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region STORE_ATTRIBUTE_9

                        try
                        {
                            objStore.STORE_ATTRIBUTE_9 = Convert.ToDouble(dr["STORE_ATTRIBUTE_9"].ToString());
                        }
                        catch (Exception ex)
                        {
                            objStore.STORE_ATTRIBUTE_9 = 0.0;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objStore.STORE_ATTRIBUTE_9, "STORE_ATTRIBUTE_9", dr["STORE_ATTRIBUTE_9"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #endregion

                        lstStore.Add(objStore);

                        //bool isNewRecordAdded = this.Check_BI_DIM_STORE_RENT_ExsisIN_BI_NRTAN_ServerAndInsertTo_NRTAN_Server(objStore);
                        //if (isNewRecordAdded == true)
                        //{
                        //    newRecordTotCount = newRecordTotCount + 1;
                        //}

                    }
                    catch (Exception ee)
                    {

                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }


                }
                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                newRecordTotCount = odbConnection.Insert_BI_DIM_STORE_RENT_To_BI_NRTANServerIncrementalProcess(lstStore);
                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool Check_BI_DIM_STORE_RENT_ExsisIN_BI_NRTAN_ServerAndInsertTo_NRTAN_Server(Store objStore)
        {
            NeenOpal_BI_DBAccess odbConnection_New = new NeenOpal_BI_DBAccess();
            return odbConnection_New.Check_BI_DIM_STORE_RENT_ExsisIN_BI_NRTAN_ServerAndInsertTo_NRTAN_Server(objStore);
        }

        #endregion

        #region Product

        private void Product()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_Product);

            if (drPassTableID != null)
            {

                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    //this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_Product));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        intProductInsertedRecCount_BI = this.ProductExtraction();
                        intProductInsertedRecCount_BI = this.ProductExtraction_DMD();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Product()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int ProductExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtProduct = odbConnection.GetProduct(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtProduct.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.ProductProcessToBI_NTANServer(dtProduct);
            //effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);

            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int ProductExtraction_DMD()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtProduct = odbConnection.GetProduct_DMD(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtProduct.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.ProductProcessToBI_NTANServer(dtProduct);
            //effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);

            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int ProductProcessToBI_NTANServer(DataTable dtSINProduct)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();
                List<Product> lstProduct = new List<Product>();
                int newRecordTotCount = 0;

                DataTable dtProduct = new DataTable();
                dtProduct = dtSINProduct;

                if (dtProduct == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtProduct.Rows)
                {
                    try
                    {
                        #region Added to DataRow
                        Product objProduct = new Product();

                        objProduct.PRODUCT_CODE = dr["PRODUCT_CODE"].ToString();
                        objProduct.PRODUCT_DESC = dr["PRODUCT_DESC"].ToString();
                        objProduct.ACCOUNTING_GROUP_ID = dr["ACCOUNTING_GROUP_ID"].ToString();
                        objProduct.ACCOUNTING_GROUP_DESC = dr["ACCOUNTING_GROUP_DESC"].ToString();
                        objProduct.PRODUCT_FAMILY_ID = dr["PRODUCT_FAMILY_ID"].ToString();
                        objProduct.PRODUCT_FAMILY_DESC = dr["PRODUCT_FAMILY_DESC"].ToString();
                        objProduct.COMMODITY_ID = dr["COMMODITY_ID"].ToString();
                        objProduct.COMMODITY_DESC = dr["COMMODITY_DESC"].ToString();

                        #region OWNER_ID
                        if (dr["OWNER_ID"].ToString() == "")
                        {
                            objProduct.OWNER_ID = "";
                        }
                        else
                        {
                            objProduct.OWNER_ID = dr["OWNER_ID"].ToString();
                        }
                        #endregion

                        #region OWNER_DESC
                        if (dr["OWNER_DESC"].ToString() == "")
                        {
                            objProduct.OWNER_DESC = "";
                        }
                        else
                        {
                            objProduct.OWNER_DESC = dr["OWNER_DESC"].ToString();
                        }
                        #endregion

                        #region BRAND_ID
                        if (dr["BRAND_ID"].ToString() == "")
                        {
                            objProduct.BRAND_ID = "";
                        }
                        else
                        {
                            objProduct.BRAND_ID = dr["BRAND_ID"].ToString();
                        }
                        #endregion

                        #region BRAND_DESC
                        if (dr["BRAND_DESC"].ToString() == "")
                        {
                            objProduct.BRAND_DESC = "";
                        }
                        else
                        {
                            objProduct.BRAND_DESC = dr["BRAND_DESC"].ToString();
                        }
                        #endregion

                        objProduct.PART_STATUS = dr["part_status"].ToString();

                        objProduct.DATE_OF_INTRODUCTION = Convert.ToDateTime(HelperClass.DateManipulation(Convert.ToDateTime(dr["DATE_OF_INTRODUCTION"].ToString())));

                        #region REPLACEMENT_PRODUCT
                        if (dr["REPLACEMENT_PRODUCT"].ToString() == "")
                        {
                            objProduct.REPLACEMENT_PRODUCT = "";
                        }
                        else
                        {
                            objProduct.REPLACEMENT_PRODUCT = dr["REPLACEMENT_PRODUCT"].ToString();
                        }
                        #endregion

                        #region CBM
                        Double CBM = 0;
                        try
                        {
                            objProduct.CBM = Convert.ToDouble(dr["CBM"]);
                        }
                        catch (Exception ex)
                        {

                            objProduct.CBM = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, CBM, "CBM", dr["CBM"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region PRODUCT_ATTRIBUTE_1
                        if (dr["PRODUCT_ATTRIBUTE_1"].ToString() == "")
                        {
                            objProduct.PRODUCT_ATTRIBUTE_1 = "";
                        }
                        else
                        {
                            objProduct.PRODUCT_ATTRIBUTE_1 = dr["PRODUCT_ATTRIBUTE_1"].ToString();
                        }
                        #endregion

                        #region PROD_ATTRIBUTE2
                        if (dr["PRODUCT_ATTRIBUTE_2"].ToString() == "")
                        {
                            objProduct.PRODUCT_ATTRIBUTE_2 = "";
                        }
                        else
                        {
                            objProduct.PRODUCT_ATTRIBUTE_2 = dr["PRODUCT_ATTRIBUTE_2"].ToString();
                        }
                        #endregion

                        #region PRODUCT_ATTRIBUTE_3
                        if (dr["PRODUCT_ATTRIBUTE_3"].ToString() == "")
                        {
                            objProduct.PRODUCT_ATTRIBUTE_3 = "";
                        }
                        else
                        {
                            objProduct.PRODUCT_ATTRIBUTE_3 = dr["PRODUCT_ATTRIBUTE_3"].ToString();
                        }
                        #endregion

                        #region PRODUCT_ATTRIBUTE_4
                        if (dr["PRODUCT_ATTRIBUTE_4"].ToString() == "")
                        {
                            objProduct.PRODUCT_ATTRIBUTE_4 = "";
                        }
                        else
                        {
                            objProduct.PRODUCT_ATTRIBUTE_4 = dr["PRODUCT_ATTRIBUTE_4"].ToString();
                        }
                        #endregion

                        #region PRODUCT_ATTRIBUTE_5
                        if (dr["PRODUCT_ATTRIBUTE_5"].ToString() == "")
                        {
                            objProduct.PRODUCT_ATTRIBUTE_5 = "";
                        }
                        else
                        {
                            objProduct.PRODUCT_ATTRIBUTE_5 = dr["PRODUCT_ATTRIBUTE_5"].ToString();
                        }
                        #endregion

                        #region Catalog Type
                        objProduct.CATALOG_TYPE = dr["CATALOG_TYPE"].ToString();
                        #endregion

                        #region Sales_Price
                        Double Sales_Price = 0;
                        try
                        {
                            objProduct.Sales_Price = Convert.ToDouble(dr["Sales_Price"]);
                        }
                        catch (Exception ex)
                        {

                            objProduct.Sales_Price = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, Sales_Price, "Sales_Price", dr["Sales_Price"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region INCREMENTAL_VOLUMN

                        try
                        {
                            objProduct.INCREMENTAL_VOLUMN = Convert.ToDouble(dr["INCREMENTAL_VOLUMN"]);
                        }
                        catch (Exception ex)
                        {

                            objProduct.INCREMENTAL_VOLUMN = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objProduct.INCREMENTAL_VOLUMN, "INCREMENTAL_VOLUMN", dr["INCREMENTAL_VOLUMN"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        lstProduct.Add(objProduct);

                        #endregion

                        bool isNewRecordAdded = this.CheckProductCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(objProduct);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }
                    }
                    catch (Exception ee)
                    {

                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }

                }

                //this is for bulk insert
                //NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
                //newRecordTotCount = odbConnectionNew.InsertProductTo_BI_NRTANServerIncrementalProcess(lstBranchExposure);
                //this is for bulk insert

                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool CheckProductCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(Product objProduct)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckProductCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(objProduct);
        }

        #endregion

        #region BI_DIM_PRODUCT_IV

        private void BI_DIM_PRODUCT_IV()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_DIM_PRODUCT_IV);

            if (drPassTableID != null)
            {

                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    //this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_PRODUCT_IV));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        intProductInsertedRecCount_BI = this.BI_DIM_PRODUCT_IVExtraction();
                        intProductInsertedRecCount_BI = this.BI_DIM_PRODUCT_IVExtraction_DMD();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  BI_DIM_PRODUCT_IV()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int BI_DIM_PRODUCT_IVExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_DIM_PRODUCT_IV = odbConnection.GetBI_DIM_PRODUCT_IV(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_DIM_PRODUCT_IV.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.BI_DIM_PRODUCT_IVProcessToBI_NTANServer(dt_BI_DIM_PRODUCT_IV);
            //effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);

            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int BI_DIM_PRODUCT_IVExtraction_DMD()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_DIM_PRODUCT_IV = odbConnection.GetBI_DIM_PRODUCT_IV_DMD(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_DIM_PRODUCT_IV.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.BI_DIM_PRODUCT_IVProcessToBI_NTANServer(dt_BI_DIM_PRODUCT_IV);
            //effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);

            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int BI_DIM_PRODUCT_IVProcessToBI_NTANServer(DataTable dtSIN_BI_DIM_PRODUCT_IV)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();
                List<Product> lstProduct = new List<Product>();
                int newRecordTotCount = 0;

                DataTable dtProduct = new DataTable();
                dtProduct = dtSIN_BI_DIM_PRODUCT_IV;

                if (dtProduct == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtProduct.Rows)
                {
                    try
                    {
                        #region Added to DataRow

                        Product objProduct = new Product();

                        objProduct.PRODUCT_CODE = dr["PRODUCT_CODE"].ToString();

                        #region INCREMENTAL_VOLUMN

                        try
                        {
                            objProduct.INCREMENTAL_VOLUMN = Convert.ToDouble(dr["INCREMENTAL_VOLUMN"]);
                        }
                        catch (Exception ex)
                        {

                            objProduct.INCREMENTAL_VOLUMN = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objProduct.INCREMENTAL_VOLUMN, "INCREMENTAL_VOLUMN", dr["INCREMENTAL_VOLUMN"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region BASICTEC1

                        try
                        {
                            objProduct.BASICTEC1 = dr["BASICTEC1"].ToString();
                        }
                        catch (Exception ex)
                        {
                            objProduct.BASICTEC1 = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objProduct.BASICTEC1, "BASICTEC1", dr["BASICTEC1"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region BASICTEC2

                        try
                        {
                            objProduct.BASICTEC2 = dr["BASICTEC2"].ToString();
                        }
                        catch (Exception ex)
                        {
                            objProduct.BASICTEC2 = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objProduct.BASICTEC2, "BASICTEC2", dr["BASICTEC2"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region BASICTEC3

                        try
                        {
                            objProduct.BASICTEC3 = dr["BASICTEC3"].ToString();
                        }
                        catch (Exception ex)
                        {
                            objProduct.BASICTEC3 = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objProduct.BASICTEC3, "BASICTEC3", dr["BASICTEC3"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region BASICTEC4

                        try
                        {
                            objProduct.BASICTEC4 = dr["BASICTEC4"].ToString();
                        }
                        catch (Exception ex)
                        {
                            objProduct.BASICTEC4 = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objProduct.BASICTEC4, "BASICTEC4", dr["BASICTEC4"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region BASICTEC5

                        try
                        {
                            objProduct.BASICTEC5 = dr["BASICTEC5"].ToString();
                        }
                        catch (Exception ex)
                        {
                            objProduct.BASICTEC5 = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objProduct.BASICTEC5, "BASICTEC5", dr["BASICTEC5"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region COLOURCODE

                        try
                        {
                            objProduct.COLOURCODE = dr["COLOURCODE"].ToString();
                        }
                        catch (Exception ex)
                        {
                            objProduct.COLOURCODE = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objProduct.COLOURCODE, "COLOURCODE", dr["COLOURCODE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region BASICTEC6

                        try
                        {
                            objProduct.BASICTEC6 = dr["BASICTEC6"].ToString();
                        }
                        catch (Exception ex)
                        {
                            objProduct.BASICTEC6 = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objProduct.BASICTEC6, "BASICTEC6", dr["BASICTEC6"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        lstProduct.Add(objProduct);

                        #endregion

                        bool isNewRecordAdded = this.CheckBI_DIM_PRODUCT_IVCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(objProduct);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }
                    }
                    catch (Exception ee)
                    {

                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }

                }

                //this is for bulk insert
                //NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
                //newRecordTotCount = odbConnectionNew.InsertBI_DIM_PRODUCT_IVTo_BI_NRTANServerIncrementalProcess(lstBranchExposure);
                //this is for bulk insert

                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool CheckBI_DIM_PRODUCT_IVCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(Product objProduct)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckBI_DIM_PRODUCT_IVCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(objProduct);
        }

        #endregion

        #region StatusMaster

        private void StatusMaster()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_DIM_STATUS);

            if (drPassTableID != null)
            {

                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);
                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        intStatusMasterInsertedRecCount_BI = this.StatusMasterExtraction();//this is one time file in NRTAN.
                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Product()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int StatusMasterExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtStatusMaster = odbConnection.GetStatusMaster(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtStatusMaster.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.StatusMasterProcessToBI_NTANServer(dtStatusMaster);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int StatusMasterProcessToBI_NTANServer(DataTable dtSINStatusMaster)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                int newRecordTotCount = 0;

                DataTable dtStatusMaster = new DataTable();
                dtStatusMaster = dtSINStatusMaster;

                if (dtStatusMaster == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtStatusMaster.Rows)
                {
                    try
                    {
                        #region Added to DataRow

                        Status_Master objStatus_Master = new Status_Master();
                        objStatus_Master.Product_Status = dr["PRODUCT_STATUS"].ToString();
                        objStatus_Master.Prod_Status_Desc = dr["PROD_STATUS_DESC"].ToString();

                        #endregion

                        bool isNewRecordAdded = this.CheckStatusMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(objStatus_Master);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }
                    }
                    catch (Exception ee)
                    {

                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }

                }
                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool CheckStatusMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(Status_Master objStatus_Master)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckStatusMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(objStatus_Master);
        }

        #endregion

        #region SalesTypeMaster

        private void SalesTypeMaster()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_DIM_SALES_TYPE);

            if (drPassTableID != null)
            {

                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);
                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        intSalesTypeMasterInsertedRecCount_BI = this.SalesTypeMasterExtraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Product()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int SalesTypeMasterExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();


            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtSalesTypeMaster = odbConnection.GetSalesTypeMaster(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtSalesTypeMaster.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.SalesTypeMasterProcessToBI_NTANServer(dtSalesTypeMaster);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int SalesTypeMasterProcessToBI_NTANServer(DataTable dtSINSelseTypeMaster)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                int newRecordTotCount = 0;

                DataTable dtSalesTypeMaster = new DataTable();
                dtSalesTypeMaster = dtSINSelseTypeMaster;

                if (dtSalesTypeMaster == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtSalesTypeMaster.Rows)
                {
                    try
                    {
                        #region Added to DataRow

                        SalesTypeMaster objSalesTypeMaster = new SalesTypeMaster();
                        objSalesTypeMaster.SalesType = dr["SALESTYPE"].ToString();
                        objSalesTypeMaster.SalesTypeDesc = dr["SALESTYPEDESC"].ToString();

                        #endregion

                        bool isNewRecordAdded = this.CheckSalesTypeMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(objSalesTypeMaster);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }
                    }
                    catch (Exception ee)
                    {

                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }

                }
                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool CheckSalesTypeMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(SalesTypeMaster objSalesTypeMaster)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckSalesTypeMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(objSalesTypeMaster);
        }

        #endregion

        #region SalesPromoter

        private void SalesPromoter()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_SALES_PROMOTER);

            if (drPassTableID != null)
            {

                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);
                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        intSalesPromoterInsertedRecCount_BI = this.SalesPromoterExtraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  SalesPromoter()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int SalesPromoterExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtSalesPromoter = odbConnection.GetSalesPromoter(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtSalesPromoter.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.SalesPromoterProcessToBI_NTANServer(dtSalesPromoter);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int SalesPromoterProcessToBI_NTANServer(DataTable dtSINSalesPromoter)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                int newRecordTotCount = 0;

                DataTable dtSalesPromoter = new DataTable();
                dtSalesPromoter = dtSINSalesPromoter;

                if (dtSalesPromoter == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtSalesPromoter.Rows)
                {
                    try
                    {
                        #region Added to DataRow

                        SalesPromoter objSalesPromoter = new SalesPromoter();
                        objSalesPromoter.SalesPromoterID = dr["SALES_PROMOTER_ID"].ToString();
                        objSalesPromoter.PromoterName = dr["PROMOTER_NAME"].ToString();
                        objSalesPromoter.BranchID = dr["BRANCH_ID"].ToString();
                        objSalesPromoter.EMP_FIRST_NAME = dr["EMP_FIRST_NAME"].ToString();
                        objSalesPromoter.EMP_MIDDLE_NAME = dr["EMP_MIDDLE_NAME"].ToString();
                        objSalesPromoter.EMP_LAST_NAME = dr["EMP_LAST_NAME"].ToString();

                        #endregion

                        bool isNewRecordAdded = this.CheckSalesPromoterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(objSalesPromoter);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }
                    }
                    catch (Exception ee)
                    {

                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }

                }
                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool CheckSalesPromoterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(SalesPromoter objSalesPromoter)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckSalesPromoterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(objSalesPromoter);
        }

        #endregion

        #region SalesStatusMaster

        private void SalesStatusMaster()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_SALES_STATUS);

            if (drPassTableID != null)
            {

                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);
                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        intSalesStatusInsertedRecCount_BI = this.SalesStatusMasterExtraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Product()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int SalesStatusMasterExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtSalesStatus = odbConnection.GetSalesStatusMaster(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtSalesStatus.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.SalesStatusProcessToBI_NTANServer(dtSalesStatus);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int SalesStatusProcessToBI_NTANServer(DataTable dtSINSalesStatus)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                int newRecordTotCount = 0;

                DataTable dtSalesStatus = new DataTable();
                dtSalesStatus = dtSINSalesStatus;

                if (dtSalesStatus == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtSalesStatus.Rows)
                {
                    try
                    {
                        #region Added to DataRow
                        SalesStatus objSalesStatus = new SalesStatus();

                        objSalesStatus.SALES_STATUS = dr["SALES_PROMOTER_ID"].ToString();

                        #region PLAN_START_DATE
                        DateTime PLAN_START_DATE;
                        try
                        {
                            DateTime a = Convert.ToDateTime(dr["PLAN_START_DATE"].ToString());
                            string formata = HelperClass.DateManipulationWithTime(a);
                            objSalesStatus.PLAN_START_DATE = Convert.ToDateTime(formata.ToString());
                        }
                        catch (Exception ex)
                        {
                            string dtformated = HelperClass.DateManipulationWithTime(dtPreviousDate_BI);//here pass process run date
                            objSalesStatus.PLAN_START_DATE = Convert.ToDateTime(dtformated.ToString());
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dtformated, "PLAN_START_DATE", dr["PLAN_START_DATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region CONFIRM_DATE
                        DateTime CONFIRM_DATE;
                        try
                        {
                            DateTime a = Convert.ToDateTime(dr["CONFIRM_DATE"].ToString());
                            string formata = HelperClass.DateManipulationWithTime(a);
                            objSalesStatus.CONFIRM_DATE = Convert.ToDateTime(formata.ToString());
                        }
                        catch (Exception ex)
                        {
                            string dtformated = HelperClass.DateManipulationWithTime(dtPreviousDate_BI);//here pass process run date
                            objSalesStatus.CONFIRM_DATE = Convert.ToDateTime(dtformated.ToString());
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dtformated, "PLAN_START_DATE", dr["PLAN_START_DATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        objSalesStatus.TRANSACION_ID = dr["TRANSACION_ID"].ToString();
                        #endregion

                        bool isNewRecordAdded = this.CheckSalesStatusCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(objSalesStatus);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }
                    }
                    catch (Exception ee)
                    {

                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }

                }
                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool CheckSalesStatusCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(SalesStatus objSalesStatus)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckSalesStatusMasterCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(objSalesStatus);
        }

        #endregion

        #region InvventoryLocation
        private void InventoryLocation()
        {

            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_DIM_INVENTORY_LOCATION);

            if (drPassTableID != null)
            {

                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);
                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_INVENTORY_LOCATION));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        intInventoryLocationInsertedRecCount_BI = this.InvventoryLocationExtraction();
                        //this.InventoryLocationDMDExtration();
                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));

                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  InventoryLocation()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int InvventoryLocationExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();


            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtInvventoryLocation = odbConnection.GetInventoryLocation(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtInvventoryLocation.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.InventoryLocationProcessToBI_NRTAN_Server(dtInvventoryLocation);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int InventoryLocationDMDExtration()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();


            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtInventoryLocationDMD = odbConnection.GetSalesLocationDMD();
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtInventoryLocationDMD.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.InventoryLocationProcessToBI_NRTAN_Server(dtInventoryLocationDMD);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int InventoryLocationProcessToBI_NRTAN_Server(DataTable dtSINInventoryLocation)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                int newRecordTotCount = 0;


                DataTable dtInventoryLocation = new DataTable();
                dtInventoryLocation = dtSINInventoryLocation;

                if (dtInventoryLocation == null)
                {
                    return 0;
                }


                foreach (DataRow dr in dtInventoryLocation.Rows)
                {

                    try
                    {
                        #region Added to DataRow
                        InventoryLocation objInventoryLocation = new InventoryLocation();

                        objInventoryLocation.INVENTORY_LOCATION_CODE = dr["INVENTORY_LOCATION_CODE"].ToString();
                        objInventoryLocation.INVENTORY_LOCATION_DESC = dr["INVENTORY_LOCATION_DESC"].ToString();
                        objInventoryLocation.INVENTORY_LOCATION_GROUP1 = dr["INVENTORY_LOCATION_GROUP1"].ToString();
                        objInventoryLocation.INVENTORY_LOCATION_GROUP2 = dr["INVENTORY_LOCATION_GROUP2"].ToString();


                        #endregion

                        bool isNewRecordAdded = this.CheckInventoryLocationCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(objInventoryLocation);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }
                    }
                    catch (Exception ee)
                    {

                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }


                }
                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool CheckInventoryLocationCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(InventoryLocation objInventoryLocation)
        {
            NeenOpal_BI_DBAccess odbConnection_New = new NeenOpal_BI_DBAccess();
            return odbConnection_New.CheckInventoryLocationCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(objInventoryLocation);
        }

        #endregion

        #region SalesLocation
        private void SalesLocation()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_DIM_SALES_LOCATION);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        intSalesLocationInsertRecCount_BI = this.SalesLocationExtraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Promotion()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int SalesLocationExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtSalesLocation = odbConnection.GetSalesLocation(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtSalesLocation.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_SalesLocationProcessTo_BI_NTAN_Server(dtSalesLocation);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int NeenOpal_SalesLocationProcessTo_BI_NTAN_Server(DataTable dtSINSalesLocation)
        {
            try
            {
                int newRecordTotCount = 0;
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                DataTable dtSalesLocation = new System.Data.DataTable();
                dtSalesLocation = dtSINSalesLocation;

                if (dtSalesLocation == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtSalesLocation.Rows)
                {
                    try
                    {
                        SalesLocation objSalesLocation = new SalesLocation();
                        objSalesLocation.SALES_LOCATION_CODE = dr["SALES_LOCATION_CODE"].ToString();
                        objSalesLocation.SALES_LOCATION_DESC = dr["SALES_LOCATION_DESC"].ToString();

                        bool isNewRecordAdded = this.CheckSalesLocationCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(objSalesLocation);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }

                    }
                    catch (Exception ee)
                    {

                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool CheckSalesLocationCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(SalesLocation objSalesLocation)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckSalesLocationCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(objSalesLocation);
        }

        #endregion

        #region Relavent Master
        /// <summary>
        /// this is one time file
        /// </summary>
        private void RelaventType()
        {

            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_DIM_RELAVENT_TYPE);

            if (drPassTableID != null)
            {

                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);
                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  RelaventType()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }
        #endregion

        #region PromotionMaster

        private void Promotion()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_DIM_PROMOTION);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        intPromotionInsertRecCount_BI = this.NeenOpal_PromotionExtraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Promotion()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int NeenOpal_PromotionExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtPromotion = odbConnection.GetPromotion(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtPromotion.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_PromotionProcessTo_BI_NTAN_Server(dtPromotion);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int NeenOpal_PromotionProcessTo_BI_NTAN_Server(DataTable dtSINPromotion)
        {
            try
            {
                int newRecordTotCount = 0;
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                DataTable dtPromotion = new System.Data.DataTable();
                dtPromotion = dtSINPromotion;

                if (dtPromotion == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtPromotion.Rows)
                {
                    try
                    {
                        Promotion objPromotion = new Promotion();
                        objPromotion.PROMO_CODE = dr["PROMO_CODE"].ToString();
                        objPromotion.PROMO_DESC = dr["PROMO_DESC"].ToString();
                        objPromotion.PROMO_ENTER_DATE = Convert.ToDateTime(HelperClass.DateManipulationWithTime(Convert.ToDateTime(dr["date_entered"].ToString())));

                        objPromotion.PROMO_ST_DATE = Convert.ToDateTime(HelperClass.DateManipulationWithTime(Convert.ToDateTime(dr["PROMO_ST_DATE"].ToString())));
                        objPromotion.PROMO_END_DATE = Convert.ToDateTime(HelperClass.DateManipulationWithTime(Convert.ToDateTime(dr["PROMO_END_DATE"].ToString())));
                        objPromotion.PROMOTION_FREE_ITEM = dr["PROMOTION_FREE_ITEM"].ToString();
                        objPromotion.APPLY_SITE = dr["APPLY_SITE"].ToString();
                        objPromotion.PROMOTION_EFFECT_MAIN_ITEM = dr["PROMOTION_EFFECT_MAIN_ITEM"].ToString();

                        objPromotion.PROMO_TYPE = dr["PROMO_TYPE"].ToString();
                        objPromotion.PROMO_TYPE_DESC = dr["PROMO_TYPE_DESC"].ToString();

                        bool isNewRecordAdded = this.CheckPromotionCodeExsisIN_BI_NRTAN_ServerAndInsertTo_NRTAN_Server(objPromotion);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }

                    }
                    catch (Exception ee)
                    {

                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool CheckPromotionCodeExsisIN_BI_NRTAN_ServerAndInsertTo_NRTAN_Server(Promotion objPromotion)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckPromotionCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(objPromotion);
        }

        #endregion

        #region Return Master
        private void Return()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_DIM_RETURN);

            if (drPassTableID != null)
            {

                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);
                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_RETURN));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        intReturnCodeMethodEffectRecordCount = this.ReturnCodeExtration();
                        intReturnCode_DMDMethodEffectRecordCount = this.ReturnCodeDMDExtration();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Return()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int ReturnCodeExtration()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtReturnCodes = odbConnection.GetReturnCode();
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtReturnCodes.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.ReturnCodeProcessToBI_NRTANServer(dtReturnCodes);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int ReturnCodeProcessToBI_NRTANServer(DataTable dtSINReturnCodes)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                int newRecordTotCount = 0;

                DataTable dtReturnCodes = new System.Data.DataTable();
                dtReturnCodes = dtSINReturnCodes;

                if (dtReturnCodes == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtReturnCodes.Rows)
                {
                    try
                    {
                        #region Added to DataRow
                        ReturnCode objReturnCode = new ReturnCode();
                        objReturnCode.RET_REASON_CODE = dr["RET_REASON_CODE"].ToString();
                        objReturnCode.RET_REASON_DESC = dr["RET_REASON_DESC"].ToString();
                        #endregion

                        bool isNewRecordAdded = this.ReturnCodeExsisIN_BI_RTAN_ServerAndInsertTo_NRTAN_Server(objReturnCode);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }
                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }

            return effectedRecCount_BI;
        }

        private bool ReturnCodeExsisIN_BI_RTAN_ServerAndInsertTo_NRTAN_Server(ReturnCode objReturnCode)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckReturnCodeExistsIn_BI_NRTANServerAndProcessToNRTANServer(objReturnCode);
        }

        private int ReturnCodeDMDExtration()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtReturnCodeDMD = odbConnection.GetReturnCodeDMD();
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtReturnCodeDMD.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.ReturnCodeProcessToBI_NRTANServer(dtReturnCodeDMD);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }
        #endregion

        #region Customer

        private void Customer()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_DIM_CUSTOMER);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);
                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);

                    if (IsAdminLogInsert == true)
                    {
                        intCustomerMethodEffectedRecordCount = this.CustomerExtration();
                        intCustomerDMDMethodEffectedRecordCount = this.CustomerDMDExtration();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));

                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Customer()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int CustomerExtration()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtCustomer = odbConnection.GetCustomer(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtCustomer.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.CustomerProcessTo_BI_NRTANServer(dtCustomer);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int CustomerProcessTo_BI_NRTANServer(DataTable dtSINCustomer)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Declare variable
                string CUSTOMER_CODE, CUSTOMER_SALUTATION, FULL_NAME, FIRST_NAME, MIDDLE_NAME, LAST_NAME, GENDER;
                string MARITAL_STATUS, MEMBERSHIP_TYPE, RELIGION, ETHNICITY, EDUCATION, RESIDENCE_TYPE, RESIDENCE_OWNERSHIP, CUSTOMER_LOCATION, RESIDENT_STATUS;
                string LOYALTY_STATUS, INDUSTRY, LIFE_STYLE, CUSTOMER_TIER, NATIONALITY, CUSTOMER_DOOR_NO, CUSTOMER_STREET, CUSTOMER_CITY_CODE;
                string CUSTOMER_CITY, CUSTOMER_DISTRICT_CODE, CUSTOMER_DISTRICT, CUSTOMER_PROVINCE_CODE, CUSTOMER_PROVINCE, CUSTOMER_ZIP_CODE, CUSTOMER_COUNTRY_CODE;
                string CUSTOMER_COUNTRY, RESIDENCE_ADDRESS, EMAIL_ADDRESS, PHONE_NUMBER, MOBILE_NUMBER, FAX_NUMBER, SOCIAL_MEDIA_ADDRESS, COMMS_BY_POST_FLG, COMMS_BY_SMS_FLG;
                string COMMS_BY_EMAIL_FLG, COMMS_BY_SOCIAL_MEDIA_FLG, COMMS_BY_MOBILE_APP_FLG, COMM_LANGUAGE, OFFICE_ADDRESS, COMPANY_NAME, PROOF_OF_IDENTITY_TYPE, PROOF_OF_IDENTITY;
                string HOME_STORE_CODE, HOME_STORE, HOUSEHOLD, LOYALTY_MEMBERSHIP_FLG, CUST_ATTRIBUTE1, CUST_ATTRIBUTE2, CUST_ATTRIBUTE3, CUST_ATTRIBUTE4, CUST_ATTRIBUTE5;
                string CUST_ATTRIBUTE6, CUST_ATTRIBUTE7, CUST_ATTRIBUTE8, CUST_ATTRIBUTE9, CUST_ATTRIBUTE10, CUSTOMER_GROUP_CODE, CUSTOMER_GROUP;
                int INCOME, ACTIVE;
                DateTime dtNew_JOINING_DATE;
                DateTime JOINING_DATE;
                DateTime BIRTH_DATE;
                DateTime VALID_FROM;
                #endregion

                #region Create data table
                DataTable dtBulkCustomer = new DataTable();
                DataRow drBulkCustomer;

                DataColumn colCUSTOMER_CODE = new DataColumn("CUSTOMER_CODE", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_CODE);

                DataColumn colCUSTOMER_SALUTATION = new DataColumn("CUSTOMER_SALUTATION", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_SALUTATION);

                DataColumn colFULL_NAME = new DataColumn("FULL_NAME", typeof(string));
                dtBulkCustomer.Columns.Add(colFULL_NAME);

                DataColumn colFIRST_NAME = new DataColumn("FIRST_NAME", typeof(string));
                dtBulkCustomer.Columns.Add(colFIRST_NAME);

                DataColumn colMIDDLE_NAME = new DataColumn("MIDDLE_NAME", typeof(string));
                dtBulkCustomer.Columns.Add(colMIDDLE_NAME);

                DataColumn colLAST_NAME = new DataColumn("LAST_NAME", typeof(string));
                dtBulkCustomer.Columns.Add(colLAST_NAME);

                DataColumn colINCOME = new DataColumn("INCOME", typeof(string));
                dtBulkCustomer.Columns.Add(colINCOME);

                DataColumn colJOINING_DATE = new DataColumn("JOINING_DATE", typeof(DateTime));
                dtBulkCustomer.Columns.Add(colJOINING_DATE);

                DataColumn colGENDER = new DataColumn("GENDER", typeof(string));
                dtBulkCustomer.Columns.Add(colGENDER);

                DataColumn colMARITAL_STATUS = new DataColumn("MARITAL_STATUS", typeof(string));
                dtBulkCustomer.Columns.Add(colMARITAL_STATUS);

                DataColumn colMEMBERSHIP_TYPE = new DataColumn("MEMBERSHIP_TYPE", typeof(string));
                dtBulkCustomer.Columns.Add(colMEMBERSHIP_TYPE);

                DataColumn colRELIGION = new DataColumn("RELIGION", typeof(string));
                dtBulkCustomer.Columns.Add(colRELIGION);

                DataColumn colETHNICITY = new DataColumn("ETHNICITY", typeof(string));
                dtBulkCustomer.Columns.Add(colETHNICITY);

                DataColumn colEDUCATION = new DataColumn("EDUCATION", typeof(string));
                dtBulkCustomer.Columns.Add(colEDUCATION);

                DataColumn colRESIDENCE_TYPE = new DataColumn("RESIDENCE_TYPE", typeof(string));
                dtBulkCustomer.Columns.Add(colRESIDENCE_TYPE);

                DataColumn colRESIDENCE_OWNERSHIP = new DataColumn("RESIDENCE_OWNERSHIP", typeof(string));
                dtBulkCustomer.Columns.Add(colRESIDENCE_OWNERSHIP);

                DataColumn colCUSTOMER_LOCATION = new DataColumn("CUSTOMER_LOCATION", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_LOCATION);

                DataColumn colRESIDENT_STATUS = new DataColumn("RESIDENT_STATUS", typeof(string));
                dtBulkCustomer.Columns.Add(colRESIDENT_STATUS);

                DataColumn colLOYALTY_STATUS = new DataColumn("LOYALTY_STATUS", typeof(string));
                dtBulkCustomer.Columns.Add(colLOYALTY_STATUS);

                DataColumn colINDUSTRY = new DataColumn("INDUSTRY", typeof(string));
                dtBulkCustomer.Columns.Add(colINDUSTRY);

                DataColumn colLIFE_STYLE = new DataColumn("LIFE_STYLE", typeof(string));
                dtBulkCustomer.Columns.Add(colLIFE_STYLE);

                DataColumn colCUSTOMER_TIER = new DataColumn("CUSTOMER_TIER", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_TIER);

                DataColumn colNATIONALITY = new DataColumn("NATIONALITY", typeof(string));
                dtBulkCustomer.Columns.Add(colNATIONALITY);

                DataColumn colBIRTH_DATE = new DataColumn("BIRTH_DATE", typeof(string));
                dtBulkCustomer.Columns.Add(colBIRTH_DATE);

                DataColumn colCUSTOMER_DOOR_NO = new DataColumn("CUSTOMER_DOOR_NO", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_DOOR_NO);

                DataColumn colCUSTOMER_STREET = new DataColumn("CUSTOMER_STREET", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_STREET);

                DataColumn colCUSTOMER_CITY_CODE = new DataColumn("CUSTOMER_CITY_CODE", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_CITY_CODE);

                DataColumn colCUSTOMER_CITY = new DataColumn("CUSTOMER_CITY", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_CITY);

                DataColumn colCUSTOMER_DISTRICT_CODE = new DataColumn("CUSTOMER_DISTRICT_CODE", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_DISTRICT_CODE);

                DataColumn colCUSTOMER_DISTRICT = new DataColumn("CUSTOMER_DISTRICT", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_DISTRICT);

                DataColumn colCUSTOMER_PROVINCE_CODE = new DataColumn("CUSTOMER_PROVINCE_CODE", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_PROVINCE_CODE);

                DataColumn colCUSTOMER_PROVINCE = new DataColumn("CUSTOMER_PROVINCE", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_PROVINCE);

                DataColumn colCUSTOMER_ZIP_CODE = new DataColumn("CUSTOMER_ZIP_CODE", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_ZIP_CODE);

                DataColumn colCUSTOMER_COUNTRY_CODE = new DataColumn("CUSTOMER_COUNTRY_CODE", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_COUNTRY_CODE);

                DataColumn colCUSTOMER_COUNTRY = new DataColumn("CUSTOMER_COUNTRY", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_COUNTRY);


                DataColumn colRESIDENCE_ADDRESS = new DataColumn("RESIDENCE_ADDRESS", typeof(string));
                dtBulkCustomer.Columns.Add(colRESIDENCE_ADDRESS);

                DataColumn colEMAIL_ADDRESS = new DataColumn("EMAIL_ADDRESS", typeof(string));
                dtBulkCustomer.Columns.Add(colEMAIL_ADDRESS);

                DataColumn colPHONE_NUMBER = new DataColumn("PHONE_NUMBER", typeof(string));
                dtBulkCustomer.Columns.Add(colPHONE_NUMBER);

                DataColumn colMOBILE_NUMBER = new DataColumn("MOBILE_NUMBER", typeof(string));
                dtBulkCustomer.Columns.Add(colMOBILE_NUMBER);

                DataColumn colFAX_NUMBER = new DataColumn("FAX_NUMBER", typeof(string));
                dtBulkCustomer.Columns.Add(colFAX_NUMBER);

                DataColumn colSOCIAL_MEDIA_ADDRESS = new DataColumn("SOCIAL_MEDIA_ADDRESS", typeof(string));
                dtBulkCustomer.Columns.Add(colSOCIAL_MEDIA_ADDRESS);

                DataColumn colCOMMS_BY_POST_FLG = new DataColumn("COMMS_BY_POST_FLG", typeof(string));
                dtBulkCustomer.Columns.Add(colCOMMS_BY_POST_FLG);

                DataColumn colCOMMS_BY_SMS_FLG = new DataColumn("COMMS_BY_SMS_FLG", typeof(string));
                dtBulkCustomer.Columns.Add(colCOMMS_BY_SMS_FLG);

                DataColumn colCOMMS_BY_EMAIL_FLG = new DataColumn("COMMS_BY_EMAIL_FLG", typeof(string));
                dtBulkCustomer.Columns.Add(colCOMMS_BY_EMAIL_FLG);

                DataColumn colCOMMS_BY_SOCIAL_MEDIA_FLG = new DataColumn("COMMS_BY_SOCIAL_MEDIA_FLG", typeof(string));
                dtBulkCustomer.Columns.Add(colCOMMS_BY_SOCIAL_MEDIA_FLG);

                DataColumn colCOMMS_BY_MOBILE_APP_FLG = new DataColumn("COMMS_BY_MOBILE_APP_FLG", typeof(string));
                dtBulkCustomer.Columns.Add(colCOMMS_BY_MOBILE_APP_FLG);

                DataColumn colCOMM_LANGUAGE = new DataColumn("COMM_LANGUAGE", typeof(string));
                dtBulkCustomer.Columns.Add(colCOMM_LANGUAGE);

                DataColumn colOFFICE_ADDRESS = new DataColumn("OFFICE_ADDRESS", typeof(string));
                dtBulkCustomer.Columns.Add(colOFFICE_ADDRESS);

                DataColumn colCOMPANY_NAME = new DataColumn("COMPANY_NAME", typeof(string));
                dtBulkCustomer.Columns.Add(colCOMPANY_NAME);

                DataColumn colPROOF_OF_IDENTITY_TYPE = new DataColumn("PROOF_OF_IDENTITY_TYPE", typeof(string));
                dtBulkCustomer.Columns.Add(colPROOF_OF_IDENTITY_TYPE);

                DataColumn colPROOF_OF_IDENTITY = new DataColumn("PROOF_OF_IDENTITY", typeof(string));
                dtBulkCustomer.Columns.Add(colPROOF_OF_IDENTITY);

                DataColumn colHOME_STORE_CODE = new DataColumn("HOME_STORE_CODE", typeof(string));
                dtBulkCustomer.Columns.Add(colHOME_STORE_CODE);

                DataColumn colHOME_STORE = new DataColumn("HOME_STORE", typeof(string));
                dtBulkCustomer.Columns.Add(colHOME_STORE);

                DataColumn colHOUSEHOLD = new DataColumn("HOUSEHOLD", typeof(string));
                dtBulkCustomer.Columns.Add(colHOUSEHOLD);

                DataColumn colLOYALTY_MEMBERSHIP_FLG = new DataColumn("LOYALTY_MEMBERSHIP_FLG", typeof(string));
                dtBulkCustomer.Columns.Add(colLOYALTY_MEMBERSHIP_FLG);

                DataColumn colCUST_ATTRIBUTE1 = new DataColumn("CUST_ATTRIBUTE1", typeof(string));
                dtBulkCustomer.Columns.Add(colCUST_ATTRIBUTE1);

                DataColumn colCUST_ATTRIBUTE2 = new DataColumn("CUST_ATTRIBUTE2", typeof(string));
                dtBulkCustomer.Columns.Add(colCUST_ATTRIBUTE2);

                DataColumn colCUST_ATTRIBUTE3 = new DataColumn("CUST_ATTRIBUTE3", typeof(string));
                dtBulkCustomer.Columns.Add(colCUST_ATTRIBUTE3);

                DataColumn colCUST_ATTRIBUTE4 = new DataColumn("CUST_ATTRIBUTE4", typeof(string));
                dtBulkCustomer.Columns.Add(colCUST_ATTRIBUTE4);

                DataColumn colCUST_ATTRIBUTE5 = new DataColumn("CUST_ATTRIBUTE5", typeof(string));
                dtBulkCustomer.Columns.Add(colCUST_ATTRIBUTE5);

                DataColumn colCUST_ATTRIBUTE6 = new DataColumn("CUST_ATTRIBUTE6", typeof(string));
                dtBulkCustomer.Columns.Add(colCUST_ATTRIBUTE6);

                DataColumn colCUST_ATTRIBUTE7 = new DataColumn("CUST_ATTRIBUTE7", typeof(string));
                dtBulkCustomer.Columns.Add(colCUST_ATTRIBUTE7);

                DataColumn colCUST_ATTRIBUTE8 = new DataColumn("CUST_ATTRIBUTE8", typeof(string));
                dtBulkCustomer.Columns.Add(colCUST_ATTRIBUTE8);

                DataColumn colCUST_ATTRIBUTE9 = new DataColumn("CUST_ATTRIBUTE9", typeof(string));
                dtBulkCustomer.Columns.Add(colCUST_ATTRIBUTE9);

                DataColumn colCUST_ATTRIBUTE10 = new DataColumn("CUST_ATTRIBUTE10", typeof(string));
                dtBulkCustomer.Columns.Add(colCUST_ATTRIBUTE10);

                DataColumn colACTIVE = new DataColumn("ACTIVE", typeof(string));
                dtBulkCustomer.Columns.Add(colACTIVE);

                DataColumn colVALID_FROM = new DataColumn("VALID_FROM", typeof(string));
                dtBulkCustomer.Columns.Add(colVALID_FROM);

                DataColumn colCUSTOMER_GROUP_CODE = new DataColumn("CUSTOMER_GROUP_CODE", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_GROUP_CODE);

                DataColumn colCUSTOMER_GROUPE = new DataColumn("CUSTOMER_GROUP", typeof(string));
                dtBulkCustomer.Columns.Add(colCUSTOMER_GROUPE);


                #endregion

                int newRecordTotCount = 0;

                DataTable dtCustomer = new System.Data.DataTable();
                dtCustomer = dtSINCustomer;

                if (dtCustomer == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtCustomer.Rows)
                {
                    try
                    {

                        #region Format variable values
                        CUSTOMER_CODE = dr["CUSTOMER_CODE"].ToString();
                        FIRST_NAME = dr["FIRST_NAME"].ToString();
                        if (FIRST_NAME == "")
                        {
                            FIRST_NAME = "";
                        }
                        FIRST_NAME = FIRST_NAME.Replace("'", "");

                        LAST_NAME = dr["LAST_NAME"].ToString();
                        LAST_NAME = LAST_NAME.Replace("'", "");

                        RESIDENCE_ADDRESS = dr["RECIDENCE"].ToString();
                        RESIDENCE_ADDRESS = RESIDENCE_ADDRESS.Replace("'", "");

                        MOBILE_NUMBER = dr["MOBILE"].ToString();
                        PHONE_NUMBER = dr["TELEPHONE"].ToString();
                        EMAIL_ADDRESS = dr["EMAIL"].ToString();
                        LOYALTY_STATUS = dr["Loyality_STATUS"].ToString();


                        #endregion

                        #region Added to DataRow

                        SingerCustomer objCustomer = new SingerCustomer();

                        objCustomer.CUSTOMER_CODE = CUSTOMER_CODE;
                        objCustomer.FIRST_NAME = FIRST_NAME;
                        objCustomer.LAST_NAME = LAST_NAME;
                        objCustomer.RECIDENCE = RESIDENCE_ADDRESS;
                        objCustomer.MOBILE = MOBILE_NUMBER;
                        objCustomer.TELEPHONE = PHONE_NUMBER;
                        objCustomer.EMAIL = EMAIL_ADDRESS;
                        objCustomer.LOYALITY_STATUS = LOYALTY_STATUS;

                        #endregion

                        bool isNewRecordAdded = this.CheckCustomerCodeExsisIN_BI_NRTAN_ServerAndInsertTo_NIRTAN_Server(objCustomer);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }

                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool CheckCustomerCodeExsisIN_BI_NRTAN_ServerAndInsertTo_NIRTAN_Server(SingerCustomer objSingerCustomer)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckCustomerCodeExistsInBI_NRTANServerAndProcessToNRTANServer(objSingerCustomer);
        }

        private int CustomerDMDExtration()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtDIMCustomer = odbConnection.GetCustomerDMD(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtDIMCustomer.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.CustomerDMDProcessToRTANServer(dtDIMCustomer);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int CustomerDMDProcessToRTANServer(DataTable dtDIMCustomer)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Declare variable
                string CUSTOMER_CODE, CUSTOMER_SALUTATION, FULL_NAME, FIRST_NAME, MIDDLE_NAME, LAST_NAME, GENDER;
                string MARITAL_STATUS, MEMBERSHIP_TYPE, RELIGION, ETHNICITY, EDUCATION, RESIDENCE_TYPE, RESIDENCE_OWNERSHIP, CUSTOMER_LOCATION, RESIDENT_STATUS;
                string LOYALTY_STATUS, INDUSTRY, LIFE_STYLE, CUSTOMER_TIER, NATIONALITY, CUSTOMER_DOOR_NO, CUSTOMER_STREET, CUSTOMER_CITY_CODE;
                string CUSTOMER_CITY, CUSTOMER_DISTRICT_CODE, CUSTOMER_DISTRICT, CUSTOMER_PROVINCE_CODE, CUSTOMER_PROVINCE, CUSTOMER_ZIP_CODE, CUSTOMER_COUNTRY_CODE;
                string CUSTOMER_COUNTRY, RESIDENCE_ADDRESS, EMAIL_ADDRESS, PHONE_NUMBER, MOBILE_NUMBER, FAX_NUMBER, SOCIAL_MEDIA_ADDRESS, COMMS_BY_POST_FLG, COMMS_BY_SMS_FLG;
                string COMMS_BY_EMAIL_FLG, COMMS_BY_SOCIAL_MEDIA_FLG, COMMS_BY_MOBILE_APP_FLG, COMM_LANGUAGE, OFFICE_ADDRESS, COMPANY_NAME, PROOF_OF_IDENTITY_TYPE, PROOF_OF_IDENTITY;
                string HOME_STORE_CODE, HOME_STORE, HOUSEHOLD, LOYALTY_MEMBERSHIP_FLG, CUST_ATTRIBUTE1, CUST_ATTRIBUTE2, CUST_ATTRIBUTE3, CUST_ATTRIBUTE4, CUST_ATTRIBUTE5;
                string CUST_ATTRIBUTE6, CUST_ATTRIBUTE7, CUST_ATTRIBUTE8, CUST_ATTRIBUTE9, CUST_ATTRIBUTE10, CUSTOMER_GROUP_CODE, CUSTOMER_GROUP;
                int INCOME, ACTIVE;
                DateTime dtNew_JOINING_DATE;
                DateTime JOINING_DATE;
                DateTime VALID_FROM;
                DateTime BIRTH_DATE;
                #endregion

                int newRecordTotCount = 0;

                DataTable dtCustomer = new System.Data.DataTable();
                dtCustomer = dtDIMCustomer;

                if (dtCustomer == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtCustomer.Rows)
                {
                    try
                    {
                        #region Format variable values
                        CUSTOMER_CODE = dr["CUSTOMER_CODE"].ToString();
                        FIRST_NAME = dr["FIRST_NAME"].ToString();
                        if (FIRST_NAME == "")
                        {
                            FIRST_NAME = "";
                        }
                        FIRST_NAME = FIRST_NAME.Replace("'", "");

                        LAST_NAME = dr["LAST_NAME"].ToString();
                        LAST_NAME = LAST_NAME.Replace("'", "");

                        RESIDENCE_ADDRESS = dr["RECIDENCE"].ToString();
                        RESIDENCE_ADDRESS = RESIDENCE_ADDRESS.Replace("'", "");

                        MOBILE_NUMBER = dr["MOBILE"].ToString();
                        PHONE_NUMBER = dr["TELEPHONE"].ToString();
                        EMAIL_ADDRESS = dr["EMAIL"].ToString();
                        LOYALTY_STATUS = dr["Loyality_STATUS"].ToString();


                        #endregion

                        #region Added to DataRow

                        SingerCustomer objCustomer = new SingerCustomer();

                        objCustomer.CUSTOMER_CODE = CUSTOMER_CODE;
                        objCustomer.FIRST_NAME = FIRST_NAME;
                        objCustomer.LAST_NAME = LAST_NAME;
                        objCustomer.RECIDENCE = RESIDENCE_ADDRESS;
                        objCustomer.MOBILE = MOBILE_NUMBER;
                        objCustomer.TELEPHONE = PHONE_NUMBER;
                        objCustomer.EMAIL = EMAIL_ADDRESS;
                        objCustomer.LOYALITY_STATUS = LOYALTY_STATUS;

                        #endregion

                        bool isNewRecordAdded = this.CheckCustomerCodeExsisIN_BI_NRTAN_ServerAndInsertTo_NIRTAN_Server(objCustomer);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }
                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        #endregion

        #region Discount
        private void Discount()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_DIM_DISCOUNT);

            if (drPassTableID != null)
            {

                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);
                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        //intDiscountMethodEffectedRecordCount = this.DiscountExtration();//one time file
                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Discount()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int DiscountExtration()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtDiscount = odbConnection.GetDiscount(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtDiscount.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.DiscountProcessTo_BI_NRTANServer(dtDiscount);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int DiscountProcessTo_BI_NRTANServer(DataTable dtSINDiscount)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                int newRecordTotCount = 0;

                DataTable dtDiscount = new System.Data.DataTable();
                dtDiscount = dtSINDiscount;

                if (dtDiscount == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtDiscount.Rows)
                {
                    try
                    {

                        #region Added to DataRow

                        Discount objDiscount = new Discount();

                        objDiscount.DISCOUNT_TYPE = dr["DISCOUNT_TYPE"].ToString();
                        objDiscount.DISCOUNT_TYPE_DESC = dr["DISCOUNT_TYPE_DESC"].ToString();
                        //objDiscount.ProductCode = dr["PRODUCT_CODE"].ToString();


                        #endregion

                        bool isNewRecordAdded = this.CheckDiscountCodeExsisIN_BI_NRTAN_ServerAndInsertTo_NIRTAN_Server(objDiscount);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }

                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool CheckDiscountCodeExsisIN_BI_NRTAN_ServerAndInsertTo_NIRTAN_Server(Discount objDiscount)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckDiscountTypeCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(objDiscount);
        }
        #endregion



        #endregion

        #region Transaction Methods

        #region Sales

        private void AllSales()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_DIM_SALES_LINE);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    //this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_SALES_LINE));//Tempory commented.

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        this.NeenOpal_BI_SaleslineNotInPKG();
                        this.NeenOpal_BI_SaleslineNotInPKG_DMD();
                        this.NeenOpal_BI_SalesLineFreeIssues();
                        this.NeenOpal_BI_SalesLineFreeIssues_DMD();
                        //this.NeenOpal_BI_PlanSales();//no need  this
                        this.NeenOpal_BI_SaleLineBackUp();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));

                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  NeenOpal_BI_AllSales()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }

        }

        private int NeenOpal_BI_SaleslineNotInPKG()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtSales = odbConnection.NeenOpal_BI_GetSaleslineNotInPKG(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + dtSales.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = NeenOpal_BI_SalesLine(dtSales);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            bool isSaveRec = InsertRecordToETLSalesLineLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI, method.ToString());
            return effectedRecCount_BI;
        }

        private int NeenOpal_BI_SaleslineNotInPKG_DMD()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtSales = odbConnection.NeenOpal_BI_GetSaleslineNotInPKG_DMD(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + dtSales.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = NeenOpal_BI_SalesLine(dtSales);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            bool isSaveRec = InsertRecordToETLSalesLineLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI, method.ToString());
            return effectedRecCount_BI;
        }

        private int NeenOpal_BI_SalesLineFreeIssues()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtSales = odbConnection.NeenOpal_BI_Get_SalesLineFreeIssues(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + dtSales.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = NeenOpal_BI_SalesLine(dtSales);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            bool isSaveRec = InsertRecordToETLSalesLineLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI, method.ToString());
            return effectedRecCount_BI;
        }

        private int NeenOpal_BI_SalesLineFreeIssues_DMD()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtSales = odbConnection.NeenOpal_BI_Get_SalesLineFreeIssues_DMD(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + dtSales.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = NeenOpal_BI_SalesLine(dtSales);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            bool isSaveRec = InsertRecordToETLSalesLineLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI, method.ToString());
            return effectedRecCount_BI;
        }

        private int NeenOpal_BI_PlanSales()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtSales = odbConnection.NeenOpal_BI_GetPlanSale(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + dtSales.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = NeenOpal_BI_SalesLine(dtSales);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            bool isSaveRec = InsertRecordToETLSalesLineLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI, method.ToString());
            return effectedRecCount_BI;
        }

        private int NeenOpal_BI_SaleLineBackUp()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtBackUpSalesData = odbConnection.Get_BI_NRTAN_BackUpSalesLineTableData();
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + dtBackUpSalesData.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = NeenOpal_SalesLine_BackUpDataForcedByExternal(dtBackUpSalesData);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            bool isSaveRec = InsertRecordToETLSalesLineLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI, method.ToString());

            //delete from BackUp_STG_Sale_Line
            NeenOpal_BI_DeleteRecordsInBackUp_STG_Sale_Line();//this method must run last.If you add new method,plz add before this method.
            //delete from BackUp_STG_Sale_Line

            return effectedRecCount_BI;
        }

        private void InsertToSingerSideBackUpSalesTable(DataTable dtSales)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            LoggingHelper.WriteErrorToFile("----Insert BackUp Start Note :- " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + dtSales.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            int intInsertCount = odbConnection.Insert_NeenOpal_BI_SalesLineToSingerSideBackUpSalesTable(dtSales);
            LoggingHelper.WriteErrorToFile("----Insert BackUp Start Note :- " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + intInsertCount.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
        }

        private int NeenOpal_BI_SalesLine(DataTable dtSales)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Variable

                DateTime SALE_TRANS_DATE;
                DateTime dtNew_SALE_TRANS_DATE;

                string COMPANY_ID;
                string COMPANY_NAME;
                string AGENCY_ID;
                string AGENCY_NAME;
                string SITE_CODE;
                string SALES_PART;
                Double UNITS;
                Double SALES_PRICE;
                Double UNIT_DISCOUNT_AMOUNT;
                Double UNIT_DISCOUNT_PRENTENTAGE;
                Double VALUE;
                Double TOTAL_DISCOUNT;
                string LOCATION_NO;
                string ORDER_NO;

                string PROMOTER_CODE;
                string AUTHORIZED_USER_ID;
                string STATE;
                string CUSTOMER_NO;
                int LENGTH_OF_CONTRACT;
                string ADVANCE_PAYMENT_NO;
                Double ADVANCE_PAYMENT_AMOUNT;
                string SALE_TYPE;
                string SALES_LOCATION_NO;
                Double SERVICE_CHARGE;
                Double FIRST_PAYMENT;
                Double BUDGET_BOOK_HIRE_PRICE;
                Double MONTHLY_PAYMENT;
                string BUDGET_BOOK;
                string PRICE_LIST;
                Double GROSS_HIRE_PRICE;
                string TILL_NO;
                string SALE_LINE_TYPE;
                int SALE_LINE_NO;

                Double SURAKSHA_VALUE;
                Double SANASUMA_VALUE;
                Double Senasuma_Plus_Value;
                Double Senasuma_Plus_Mobile_Value;
                Double Senasuma_Post_Value;
                string TRANSAACTION_TYPE;
                string SALES_VARIATION;
                string FREE_ISSUE;
                string salesman_code;
                string Vallibel_Introduced;
                string SERIAL_NO;
                string order_category;

                Double sales_tax_value;
                Double INVENTORY_VALUE;
                string SALES_GROUPING;
                string RET_REASON_CODE;
                string Status_586P;
                string DISCOUNT_TYPE_CODE;
                string DISCOUNT_REASON_CODE;
                string SALESFILETYPE;
                string PROMOTION_CODE;

                int Cash_Conversion_Days;
                string Cash_Conversion_Cal_Days;
                DateTime? Authorized_Date = null;
                DateTime? Utilized_Date = null;

                string AUTHORIZED_NAME;
                string LINE_STATUS;
                Double REVERT_REVERSED_BAL;
                Double TOTAL_AMOUNT_PAID;
                string HPNRET_GROUPING;

                string SALESMAN_TYPE;
                string EnteredBy;
                string EnteredName;

                #endregion

                #region Create data table
                DataTable dtBulkSales = new DataTable();
                DataRow drBulkSales;

                DataColumn colSALE_TRANS_DATE = new DataColumn("TDATE", typeof(DateTime));
                dtBulkSales.Columns.Add(colSALE_TRANS_DATE);

                DataColumn colSITE_CODE = new DataColumn("SITE_CODE", typeof(string));
                dtBulkSales.Columns.Add(colSITE_CODE);

                DataColumn colSALES_PART = new DataColumn("SALES_PART", typeof(string));
                dtBulkSales.Columns.Add(colSALES_PART);

                DataColumn colUNITS = new DataColumn("UNITS", typeof(Double));
                dtBulkSales.Columns.Add(colUNITS);

                DataColumn colSALES_PRICE = new DataColumn("SALES_PRICE", typeof(Double));
                dtBulkSales.Columns.Add(colSALES_PRICE);

                DataColumn colUNIT_DISCOUNT_AMOUNT = new DataColumn("UNIT_DISCOUNT_AMOUNT", typeof(Double));
                dtBulkSales.Columns.Add(colUNIT_DISCOUNT_AMOUNT);

                DataColumn colUNIT_DISCOUNT_PRENTENTAGE = new DataColumn("UNIT_DISCOUNT_PRENTENTAGE", typeof(Double));
                dtBulkSales.Columns.Add(colUNIT_DISCOUNT_PRENTENTAGE);

                DataColumn colVALUE = new DataColumn("VALUE", typeof(Double));
                dtBulkSales.Columns.Add(colVALUE);

                DataColumn colTOTAL_DISCOUNT = new DataColumn("TOTAL_DISCOUNT", typeof(Double));
                dtBulkSales.Columns.Add(colTOTAL_DISCOUNT);

                DataColumn colLOCATION_NO = new DataColumn("LOCATION_NO", typeof(string));
                dtBulkSales.Columns.Add(colLOCATION_NO);

                DataColumn colORDER_NO = new DataColumn("ORDER_NO", typeof(string));
                dtBulkSales.Columns.Add(colORDER_NO);

                DataColumn colPROMOTER_CODE = new DataColumn("PROMOTER_CODE", typeof(string));
                dtBulkSales.Columns.Add(colPROMOTER_CODE);

                DataColumn colAUTHORIZED_USER_ID = new DataColumn("AUTHORIZED_USER_ID", typeof(string));
                dtBulkSales.Columns.Add(colAUTHORIZED_USER_ID);

                DataColumn colSTATE = new DataColumn("STATE", typeof(string));
                dtBulkSales.Columns.Add(colSTATE);

                DataColumn colCUSTOMER_NO = new DataColumn("CUSTOMER_NO", typeof(string));
                dtBulkSales.Columns.Add(colCUSTOMER_NO);

                DataColumn colLENGTH_OF_CONTRACT = new DataColumn("LENGTH_OF_CONTRACT", typeof(int));
                dtBulkSales.Columns.Add(colLENGTH_OF_CONTRACT);

                DataColumn colADVANCE_PAYMENT_NO = new DataColumn("ADVANCE_PAYMENT_NO", typeof(string));
                dtBulkSales.Columns.Add(colADVANCE_PAYMENT_NO);

                DataColumn colADVANCE_PAYMENT_AMOUNT = new DataColumn("ADVANCE_PAYMENT_AMOUNT", typeof(Double));
                dtBulkSales.Columns.Add(colADVANCE_PAYMENT_AMOUNT);

                DataColumn colSALE_TYPE = new DataColumn("SALE_TYPE", typeof(string));
                dtBulkSales.Columns.Add(colSALE_TYPE);

                DataColumn colSALES_LOCATION_NO = new DataColumn("SALES_LOCATION_NO", typeof(string));
                dtBulkSales.Columns.Add(colSALES_LOCATION_NO);

                DataColumn colSERVICE_CHARGE = new DataColumn("SERVICE_CHARGE", typeof(Double));
                dtBulkSales.Columns.Add(colSERVICE_CHARGE);

                DataColumn colFIRST_PAYMENT = new DataColumn("FIRST_PAYMENT", typeof(Double));
                dtBulkSales.Columns.Add(colFIRST_PAYMENT);

                DataColumn colBUDGET_BOOK_HIRE_PRICE = new DataColumn("BUDGET_BOOK_HIRE_PRICE", typeof(Double));
                dtBulkSales.Columns.Add(colBUDGET_BOOK_HIRE_PRICE);

                DataColumn colMONTHLY_PAYMENT = new DataColumn("MONTHLY_PAYMENT", typeof(Double));
                dtBulkSales.Columns.Add(colMONTHLY_PAYMENT);

                DataColumn colBUDGET_BOOK = new DataColumn("BUDGET_BOOK", typeof(string));
                dtBulkSales.Columns.Add(colBUDGET_BOOK);

                DataColumn colPRICE_LIST = new DataColumn("PRICE_LIST", typeof(string));
                dtBulkSales.Columns.Add(colPRICE_LIST);

                DataColumn colGROSS_HIRE_PRICE = new DataColumn("GROSS_HIRE_PRICE", typeof(Double));
                dtBulkSales.Columns.Add(colGROSS_HIRE_PRICE);

                DataColumn colTILL_NO = new DataColumn("TILL_NO", typeof(string));
                dtBulkSales.Columns.Add(colTILL_NO);

                DataColumn colSALE_LINE_TYPE = new DataColumn("SALE_LINE_TYPE", typeof(string));
                dtBulkSales.Columns.Add(colSALE_LINE_TYPE);

                DataColumn colSALE_LINE_NO = new DataColumn("SALE_LINE_NO", typeof(int));
                dtBulkSales.Columns.Add(colSALE_LINE_NO);

                DataColumn colSURAKSHA_VALUE = new DataColumn("SURAKSHA_VALUE", typeof(Double));
                dtBulkSales.Columns.Add(colSURAKSHA_VALUE);

                DataColumn colSANASUMA_VALUE = new DataColumn("SANASUMA_VALUE", typeof(Double));
                dtBulkSales.Columns.Add(colSANASUMA_VALUE);

                DataColumn colSenasuma_Plus_Value = new DataColumn("Senasuma_Plus_Value", typeof(Double));
                dtBulkSales.Columns.Add(colSenasuma_Plus_Value);

                DataColumn colSenasuma_Plus_Mobile_Value = new DataColumn("Senasuma_Plus_Mobile_Value", typeof(Double));
                dtBulkSales.Columns.Add(colSenasuma_Plus_Mobile_Value);

                DataColumn colSenasuma_Post_Value = new DataColumn("Senasuma_Post_Value", typeof(Double));
                dtBulkSales.Columns.Add(colSenasuma_Post_Value);

                DataColumn colTRANSAACTION_TYPE = new DataColumn("TRANSAACTION_TYPE", typeof(string));
                dtBulkSales.Columns.Add(colTRANSAACTION_TYPE);

                DataColumn colSALES_VARIATION = new DataColumn("SALES_VARIATION", typeof(string));
                dtBulkSales.Columns.Add(colSALES_VARIATION);

                DataColumn colFREE_ISSUE = new DataColumn("FREE_ISSUE", typeof(string));
                dtBulkSales.Columns.Add(colFREE_ISSUE);

                DataColumn colsalesman_code = new DataColumn("salesman_code", typeof(string));
                dtBulkSales.Columns.Add(colsalesman_code);

                DataColumn colVallibel_Introduced = new DataColumn("Vallibel_Introduced", typeof(string));
                dtBulkSales.Columns.Add(colVallibel_Introduced);

                DataColumn colSERIAL_NO = new DataColumn("SERIAL_NO", typeof(string));
                dtBulkSales.Columns.Add(colSERIAL_NO);

                DataColumn colorder_category = new DataColumn("order_category", typeof(string));
                dtBulkSales.Columns.Add(colorder_category);

                DataColumn colsales_tax_value = new DataColumn("sales_tax_value", typeof(Double));
                dtBulkSales.Columns.Add(colsales_tax_value);

                DataColumn colINVENTORY_VALUE = new DataColumn("INVENTORY_VALUE", typeof(Double));
                dtBulkSales.Columns.Add(colINVENTORY_VALUE);

                DataColumn colSALES_GROUPING = new DataColumn("SALES_GROUPING", typeof(string));
                dtBulkSales.Columns.Add(colSALES_GROUPING);

                DataColumn colRET_REASON_CODE = new DataColumn("RET_REASON_CODE", typeof(string));
                dtBulkSales.Columns.Add(colRET_REASON_CODE);

                DataColumn colStatus_586P = new DataColumn("Status_586P", typeof(string));
                dtBulkSales.Columns.Add(colStatus_586P);

                DataColumn colDISCOUNT_TYPE_CODE = new DataColumn("DISCOUNT_TYPE_CODE", typeof(string));
                dtBulkSales.Columns.Add(colDISCOUNT_TYPE_CODE);

                DataColumn colDISCOUNT_REASON_CODE = new DataColumn("DISCOUNT_REASON_CODE", typeof(string));
                dtBulkSales.Columns.Add(colDISCOUNT_REASON_CODE);

                DataColumn colSALESFILETYPE = new DataColumn("SALESFILETYPE", typeof(string));
                dtBulkSales.Columns.Add(colSALESFILETYPE);

                DataColumn colPROMOTION_CODE = new DataColumn("PROMOTION_CODE", typeof(string));
                dtBulkSales.Columns.Add(colPROMOTION_CODE);

                DataColumn colCash_Conversion_Days = new DataColumn("Cash_Conversion_Days", typeof(int));
                dtBulkSales.Columns.Add(colCash_Conversion_Days);

                DataColumn colCash_Conversion_Cal_Days = new DataColumn("Cash_Conversion_Cal_Days", typeof(string));
                dtBulkSales.Columns.Add(colCash_Conversion_Cal_Days);

                DataColumn colAuthorized_Date = new DataColumn("Authorized_Date", typeof(DateTime));
                colAuthorized_Date.AllowDBNull = true;
                dtBulkSales.Columns.Add(colAuthorized_Date);

                DataColumn colUtilized_Date = new DataColumn("Utilized_Date", typeof(DateTime));
                colUtilized_Date.AllowDBNull = true;
                dtBulkSales.Columns.Add(colUtilized_Date);

                DataColumn colAUTHORIZED_NAME = new DataColumn("AUTHORIZED_NAME", typeof(string));
                dtBulkSales.Columns.Add(colAUTHORIZED_NAME);

                DataColumn colLINE_STATUS = new DataColumn("LINE_STATUS", typeof(string));
                dtBulkSales.Columns.Add(colLINE_STATUS);

                DataColumn colREVERT_REVERSED_BAL = new DataColumn("REVERT_REVERSED_BAL", typeof(Double));
                dtBulkSales.Columns.Add(colREVERT_REVERSED_BAL);

                DataColumn colTOTAL_AMOUNT_PAID = new DataColumn("TOTAL_AMOUNT_PAID", typeof(Double));
                dtBulkSales.Columns.Add(colTOTAL_AMOUNT_PAID);

                DataColumn colHPNRET_GROUPING = new DataColumn("HPNRET_GROUPING", typeof(string));
                dtBulkSales.Columns.Add(colHPNRET_GROUPING);


                DataColumn colSALESMAN_TYPE = new DataColumn("SALESMAN_TYPE", typeof(string));
                dtBulkSales.Columns.Add(colSALESMAN_TYPE);

                DataColumn colENTERED_BY = new DataColumn("ENTERED_BY", typeof(string));
                dtBulkSales.Columns.Add(colENTERED_BY);

                DataColumn colENTERED_NAME = new DataColumn("ENTERED_NAME", typeof(string));
                dtBulkSales.Columns.Add(colENTERED_NAME);


                #endregion

                DataTable dtsalesline = new System.Data.DataTable();
                dtsalesline = dtSales;

                if (dtsalesline == null)
                {
                    return 0;
                }

                foreach (DataRow dr in dtsalesline.Rows)
                {
                    try
                    {

                        #region Format variable values

                        SITE_CODE = dr["SITE_CODE"].ToString();
                        SALES_PART = dr["SALES_PART"].ToString();

                        #region UNITS
                        try
                        {
                            UNITS = Convert.ToDouble(dr["units"].ToString());
                        }
                        catch (Exception ex)
                        {
                            UNITS = -1;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, UNITS, "UNITS", dr["units"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region SALES_PRICE
                        try
                        {
                            SALES_PRICE = Convert.ToDouble(dr["SALES_PRICE"].ToString());
                        }
                        catch (Exception ex)
                        {

                            SALES_PRICE = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, SALES_PRICE, "SALES_PRICE", dr["sales_price"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region UNIT_DISCOUNT_AMOUNT
                        string discount = dr["UNIT_DISCOUNT_AMOUNT"].ToString();
                        if (discount == "")
                        {
                            UNIT_DISCOUNT_AMOUNT = 0;
                        }
                        else
                        {
                            try
                            {
                                UNIT_DISCOUNT_AMOUNT = Convert.ToDouble(dr["UNIT_DISCOUNT_AMOUNT"].ToString());
                            }
                            catch (Exception ex)
                            {

                                UNIT_DISCOUNT_AMOUNT = 0.01;
                                LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, UNIT_DISCOUNT_AMOUNT, "UNIT_DISCOUNT_AMOUNT", dr["UNIT_DISCOUNT_AMOUNT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                            }
                        }

                        #endregion

                        #region UNIT_DISCOUNT_PRENTENTAGE
                        string discountPre = dr["UNIT_DISCOUNT_PRENTENTAGE"].ToString();
                        if (discountPre == "")
                        {
                            UNIT_DISCOUNT_PRENTENTAGE = 0;
                        }
                        else
                        {
                            try
                            {
                                UNIT_DISCOUNT_PRENTENTAGE = Convert.ToDouble(dr["UNIT_DISCOUNT_PRENTENTAGE"].ToString());
                            }
                            catch (Exception ex)
                            {

                                UNIT_DISCOUNT_PRENTENTAGE = 0.01;
                                LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, UNIT_DISCOUNT_PRENTENTAGE, "UNIT_DISCOUNT_PRENTENTAGE", dr["UNIT_DISCOUNT_PRENTENTAGE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                            }
                        }

                        #endregion

                        #region VALUE
                        try
                        {
                            VALUE = Convert.ToDouble(dr["VALUE"].ToString());
                        }
                        catch (Exception ex)
                        {

                            VALUE = 0.00;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, VALUE, "VALUE", dr["VALUE"] + dr["ORDER_NO"].ToString()) + dr["SALE_LINE_NO"].ToString(), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region TOTAL_DISCOUNT
                        string totalDiscount = dr["TOTAL_DISCOUNT"].ToString();
                        if (totalDiscount == "")
                        {
                            TOTAL_DISCOUNT = 0;
                        }
                        else
                        {
                            try
                            {
                                TOTAL_DISCOUNT = Convert.ToDouble(dr["TOTAL_DISCOUNT"].ToString());
                            }
                            catch (Exception ex)
                            {

                                TOTAL_DISCOUNT = 0.01;
                                LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, TOTAL_DISCOUNT, "TOTAL_DISCOUNT", dr["TOTAL_DISCOUNT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                            }
                        }

                        #endregion

                        #region LOCATION_NO
                        try
                        {
                            LOCATION_NO = dr["LOCATION_NO"].ToString();
                        }
                        catch (Exception ex)
                        {

                            LOCATION_NO = " ";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, LOCATION_NO, "LOCATION_NO", dr["LOCATION_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        ORDER_NO = dr["ORDER_NO"].ToString();

                        #region TDATE Date
                        try
                        {
                            SALE_TRANS_DATE = Convert.ToDateTime(dr["TDATE"].ToString());
                            string dtformated = HelperClass.DateManipulationWithTime(SALE_TRANS_DATE);
                            dtNew_SALE_TRANS_DATE = Convert.ToDateTime(dtformated.ToString());
                        }
                        catch (Exception ex)
                        {
                            string dtformated = HelperClass.DateManipulationWithTime(dtPreviousDate_BI);//here pass process run date
                            dtNew_SALE_TRANS_DATE = Convert.ToDateTime(dtformated.ToString());
                            SALE_TRANS_DATE = dtNew_SALE_TRANS_DATE;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dtNew_SALE_TRANS_DATE, "TDATE", dr["TDATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region PROMOTER_CODE

                        string ec = dr["PROMOTER_CODE"].ToString();
                        if (ec == "")
                        {
                            PROMOTER_CODE = "N/A";
                        }
                        else
                        {
                            PROMOTER_CODE = dr["PROMOTER_CODE"].ToString();
                        }
                        #endregion

                        #region AUTHORIZED_USER_ID
                        string authu_id = dr["AUTHORIZED_USER_ID"].ToString();
                        if (authu_id == "")
                        {
                            AUTHORIZED_USER_ID = "";
                        }
                        else
                        {
                            try
                            {
                                AUTHORIZED_USER_ID = dr["AUTHORIZED_USER_ID"].ToString();
                            }
                            catch (Exception ex)
                            {

                                AUTHORIZED_USER_ID = " ";
                                LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, AUTHORIZED_USER_ID, "AUTHORIZED_USER_ID", dr["AUTHORIZED_USER_ID"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                            }
                        }

                        #endregion

                        #region STATE
                        string strStatue = dr["STATE"].ToString();
                        if (strStatue == "")
                        {
                            STATE = "";
                        }
                        else
                        {
                            try
                            {
                                STATE = dr["STATE"].ToString();
                            }
                            catch (Exception ex)
                            {
                                STATE = "";
                                LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, STATE, "STATE", dr["STATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                            }
                        }
                        #endregion

                        #region CUSTOMER_NO
                        try
                        {
                            CUSTOMER_NO = dr["CUSTOMER_NO"].ToString();

                        }
                        catch (Exception ex)
                        {

                            CUSTOMER_NO = " ";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, CUSTOMER_NO, "CUSTOMER_NO", dr["CUSTOMER_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region LENGTH_OF_CONTRACT
                        string loc = dr["LENGTH_OF_CONTRACT"].ToString();
                        if (loc == "")
                        {
                            LENGTH_OF_CONTRACT = 0;
                        }
                        else
                        {
                            #region LENGTH_OF_CONTRACT
                            try
                            {
                                LENGTH_OF_CONTRACT = Convert.ToInt32(dr["LENGTH_OF_CONTRACT"].ToString());
                            }
                            catch (Exception ex)
                            {

                                LENGTH_OF_CONTRACT = 0;
                                LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, LENGTH_OF_CONTRACT, "LENGTH_OF_CONTRACT", dr["LENGTH_OF_CONTRACT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                            }
                            #endregion
                        }
                        #endregion

                        #region ADVANCE_PAYMENT_NO
                        string strAdvancePay_no = dr["ADVANCE_PAYMENT_NO"].ToString();
                        if (strAdvancePay_no == "")
                        {
                            ADVANCE_PAYMENT_NO = "";
                        }
                        else
                        {
                            try
                            {
                                ADVANCE_PAYMENT_NO = dr["ADVANCE_PAYMENT_NO"].ToString();
                            }
                            catch (Exception ex)
                            {

                                ADVANCE_PAYMENT_NO = " ";
                                LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, ADVANCE_PAYMENT_NO, "ADVANCE_PAYMENT_NO", dr["ADVANCE_PAYMENT_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                            }
                        }

                        #endregion

                        #region ADVANCE_PAYMENT_AMOUNT
                        try
                        {
                            ADVANCE_PAYMENT_AMOUNT = Convert.ToDouble(dr["ADVANCE_PAYMENT_AMOUNT"].ToString());
                        }
                        catch (Exception ex)
                        {

                            ADVANCE_PAYMENT_AMOUNT = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, ADVANCE_PAYMENT_AMOUNT, "ADVANCE_PAYMENT_AMOUNT", dr["ADVANCE_PAYMENT_AMOUNT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region SALE_TYPE
                        SALE_TYPE = dr["sale_type"].ToString();
                        if (SALE_TYPE == "")
                        {
                            SALE_TYPE = "0";
                        }
                        #endregion

                        #region SALES_LOCATION_NO
                        try
                        {
                            SALES_LOCATION_NO = dr["SALES_LOCATION_NO"].ToString();
                        }
                        catch (Exception ex)
                        {

                            SALES_LOCATION_NO = " ";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, SALES_LOCATION_NO, "SALES_LOCATION_NO", dr["SALES_LOCATION_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region SERVICE_CHARGE
                        try
                        {
                            SERVICE_CHARGE = Convert.ToDouble(dr["SERVICE_CHARGE"].ToString());
                        }
                        catch (Exception ex)
                        {

                            SERVICE_CHARGE = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, SERVICE_CHARGE, "SERVICE_CHARGE", dr["SERVICE_CHARGE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region FIRST_PAYMENT
                        try
                        {
                            FIRST_PAYMENT = Convert.ToDouble(dr["FIRST_PAYMENT"].ToString());
                        }
                        catch (Exception ex)
                        {

                            FIRST_PAYMENT = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, FIRST_PAYMENT, "FIRST_PAYMENT", dr["FIRST_PAYMENT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region BUDGET_BOOK_HIRE_PRICE
                        try
                        {
                            BUDGET_BOOK_HIRE_PRICE = Convert.ToDouble(dr["BUDGET_BOOK_HIRE_PRICE"].ToString());
                        }
                        catch (Exception ex)
                        {

                            BUDGET_BOOK_HIRE_PRICE = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, BUDGET_BOOK_HIRE_PRICE, "BUDGET_BOOK_HIRE_PRICE", dr["BUDGET_BOOK_HIRE_PRICE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region MONTHLY_PAYMENT
                        try
                        {
                            MONTHLY_PAYMENT = Convert.ToDouble(dr["MONTHLY_PAYMENT"].ToString());
                        }
                        catch (Exception ex)
                        {

                            MONTHLY_PAYMENT = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, MONTHLY_PAYMENT, "MONTHLY_PAYMENT", dr["MONTHLY_PAYMENT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region BUDGET_BOOK
                        try
                        {
                            BUDGET_BOOK = dr["BUDGET_BOOK"].ToString();
                        }
                        catch (Exception ex)
                        {

                            BUDGET_BOOK = " ";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, BUDGET_BOOK, "BUDGET_BOOK", dr["BUDGET_BOOK"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region PRICE_LIST
                        try
                        {
                            PRICE_LIST = dr["PRICE_LIST"].ToString();
                        }
                        catch (Exception ex)
                        {

                            PRICE_LIST = " ";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, PRICE_LIST, "PRICE_LIST", dr["PRICE_LIST"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region GROSS_HIRE_PRICE
                        try
                        {
                            GROSS_HIRE_PRICE = Convert.ToDouble(dr["GROSS_HIRE_PRICE"].ToString());
                        }
                        catch (Exception ex)
                        {

                            GROSS_HIRE_PRICE = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, GROSS_HIRE_PRICE, "GROSS_HIRE_PRICE", dr["GROSS_HIRE_PRICE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        TILL_NO = "1";

                        SALE_LINE_TYPE = dr["SALE_LINE_TYPE"].ToString();
                        if (SALE_LINE_TYPE == "")
                        {
                            SALE_LINE_TYPE = "0";
                        }

                        SALE_LINE_NO = Convert.ToInt32(dr["SALE_LINE_NO"].ToString());

                        #region SURAKSHA_VALUE
                        try
                        {
                            SURAKSHA_VALUE = Convert.ToDouble(dr["SURAKSHA_VALUE"].ToString());
                        }
                        catch (Exception ex)
                        {

                            SURAKSHA_VALUE = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, SURAKSHA_VALUE, "SURAKSHA_VALUE", dr["SURAKSHA_VALUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region SANASUMA_VALUE
                        try
                        {
                            SANASUMA_VALUE = Convert.ToDouble(dr["SANASUMA_VALUE"].ToString());
                        }
                        catch (Exception ex)
                        {

                            SANASUMA_VALUE = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, SANASUMA_VALUE, "SANASUMA_VALUE", dr["SANASUMA_VALUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region Senasuma_Plus_Value
                        string strSenasuma_Plu_val = dr["Senasuma_Plus_Value"].ToString();
                        if (strSenasuma_Plu_val == "")
                        {
                            Senasuma_Plus_Value = 0;
                        }
                        else
                        {
                            try
                            {
                                Senasuma_Plus_Value = Convert.ToDouble(dr["Senasuma_Plus_Value"].ToString());
                            }
                            catch (Exception ex)
                            {

                                Senasuma_Plus_Value = 0;
                                LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, Senasuma_Plus_Value, "Senasuma_Plus_Value", dr["Senasuma_Plus_Value"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                            }
                        }

                        #endregion

                        #region Senasuma_Plus_Mobile_Value
                        string strSenasuma_Plus_Mobile_Value = dr["Senasuma_Plus_Mobile_Value"].ToString();
                        if (strSenasuma_Plus_Mobile_Value == "")
                        {
                            Senasuma_Plus_Mobile_Value = 0;
                        }
                        else
                        {
                            try
                            {
                                Senasuma_Plus_Mobile_Value = Convert.ToDouble(dr["Senasuma_Plus_Mobile_Value"].ToString());
                            }
                            catch (Exception ex)
                            {

                                Senasuma_Plus_Mobile_Value = 0;
                                LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, Senasuma_Plus_Mobile_Value, "Senasuma_Plus_Mobile_Value", dr["Senasuma_Plus_Mobile_Value"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                            }
                        }

                        #endregion

                        #region Senasuma_Post_Value
                        string strSenasuma_Post_Value = dr["Senasuma_Post_Value"].ToString();
                        if (strSenasuma_Post_Value == "")
                        {
                            Senasuma_Post_Value = 0;
                        }
                        else
                        {
                            try
                            {
                                Senasuma_Post_Value = Convert.ToDouble(dr["Senasuma_Post_Value"].ToString());
                            }
                            catch (Exception ex)
                            {

                                Senasuma_Post_Value = 0;
                                LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, Senasuma_Post_Value, "Senasuma_Post_Value", dr["Senasuma_Post_Value"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                            }
                        }

                        #endregion

                        #region TRANSAACTION_TYPE
                        try
                        {
                            TRANSAACTION_TYPE = dr["TRANSAACTION_TYPE"].ToString();
                        }
                        catch (Exception ex)
                        {

                            TRANSAACTION_TYPE = " ";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, TRANSAACTION_TYPE, "TRANSAACTION_TYPE", dr["TRANSAACTION_TYPE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region SALES_VARIATION
                        try
                        {
                            SALES_VARIATION = dr["SALES_VARIATION"].ToString();
                        }
                        catch (Exception ex)
                        {

                            SALES_VARIATION = " ";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, SALES_VARIATION, "SALES_VARIATION", dr["SALES_VARIATION"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region FREE_ISSUE
                        try
                        {
                            FREE_ISSUE = dr["FREE_ISSUE"].ToString();
                        }
                        catch (Exception ex)
                        {

                            FREE_ISSUE = " ";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, FREE_ISSUE, "FREE_ISSUE", dr["FREE_ISSUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region salesman_code
                        try
                        {
                            salesman_code = dr["salesman_code"].ToString();
                        }
                        catch (Exception ex)
                        {

                            salesman_code = " ";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, salesman_code, "salesman_code", dr["salesman_code"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region Vallibel_Introduced
                        try
                        {
                            Vallibel_Introduced = dr["Vallibel_Introduced"].ToString();
                        }
                        catch (Exception ex)
                        {

                            Vallibel_Introduced = " ";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, Vallibel_Introduced, "Vallibel_Introduced", dr["Vallibel_Introduced"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region SERIAL_NO
                        try
                        {
                            SERIAL_NO = dr["SERIAL_NO"].ToString();
                        }
                        catch (Exception ex)
                        {

                            SERIAL_NO = " ";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, SERIAL_NO, "SERIAL_NO", dr["SERIAL_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region order_category
                        try
                        {
                            order_category = dr["order_category"].ToString();
                        }
                        catch (Exception ex)
                        {

                            order_category = " ";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, order_category, "order_category", dr["order_category"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region sales_tax_value
                        try
                        {
                            sales_tax_value = Convert.ToDouble(dr["sales_tax_value"].ToString());
                        }
                        catch (Exception ex)
                        {

                            sales_tax_value = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, sales_tax_value, "sales_tax_value", dr["sales_tax_value"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region INVENTORY_VALUE
                        try
                        {
                            INVENTORY_VALUE = (Convert.ToDouble(dr["INVENTORY_VALUE"].ToString()) * Math.Abs(UNITS));
                        }
                        catch (Exception ex)
                        {

                            INVENTORY_VALUE = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, INVENTORY_VALUE, "INVENTORY_VALUE", dr["INVENTORY_VALUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region SALES_GROUPING
                        try
                        {
                            SALES_GROUPING = dr["SALES_GROUPING"].ToString();
                        }
                        catch (Exception ex)
                        {

                            SALES_GROUPING = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, SALES_GROUPING, "SALES_GROUPING", dr["SALES_GROUPING"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region RET_REASON_CODE
                        try
                        {
                            RET_REASON_CODE = dr["RET_REASON_CODE"].ToString();
                        }
                        catch (Exception ex)
                        {

                            RET_REASON_CODE = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, RET_REASON_CODE, "RET_REASON_CODE", dr["RET_REASON_CODE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region Status_586P
                        try
                        {
                            Status_586P = dr["Status_586P"].ToString();
                        }
                        catch (Exception ex)
                        {

                            Status_586P = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, Status_586P, "Status_586P ", dr["Status_586P"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion


                        DISCOUNT_TYPE_CODE = dr["discount_type"].ToString();
                        DISCOUNT_REASON_CODE = dr["discount_reason"].ToString();
                        SALESFILETYPE = dr["SALESFILETYPE"].ToString();

                        #region PROMOTION_CODE
                        string dtc = DISCOUNT_TYPE_CODE;
                        if (dtc == "S")
                        {
                            PROMOTION_CODE = "";
                        }
                        else
                        {
                            PROMOTION_CODE = dr["PROMOTION_CODE"].ToString();
                        }
                        #endregion

                        #region Cash_Conversion_Days
                        try
                        {
                            Cash_Conversion_Days = Convert.ToInt32(dr["Cash_Conversion_Days"].ToString());
                        }
                        catch (Exception ex)
                        {

                            Cash_Conversion_Days = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, Cash_Conversion_Days, "Cash_Conversion_Days", dr["Cash_Conversion_Days"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region Cash_Conversion_Cal_Days
                        try
                        {
                            Cash_Conversion_Cal_Days = dr["Cash_Conversion_Cal_Days"].ToString();
                        }
                        catch (Exception ex)
                        {

                            Cash_Conversion_Cal_Days = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, Cash_Conversion_Cal_Days, "Cash_Conversion_Cal_Days", dr["Cash_Conversion_Cal_Days"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region Authorized_Date
                        DateTime dtNewAuthorized_Date;
                        try
                        {
                            dtNewAuthorized_Date = Convert.ToDateTime(dr["Authorized_Date"].ToString());
                            string dtformated = HelperClass.DateManipulationWithTime(dtNewAuthorized_Date);
                            dtNewAuthorized_Date = Convert.ToDateTime(dtformated.ToString());
                        }
                        catch (Exception ex)
                        {
                            Authorized_Date = null;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, Authorized_Date, "Authorized_Date", dr["Authorized_Date"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region Utilized_Date
                        DateTime dtNewUtilized_Date;
                        try
                        {
                            dtNewUtilized_Date = Convert.ToDateTime(dr["Utilized_Date"].ToString());
                            string dtformated = HelperClass.DateManipulationWithTime(dtNewUtilized_Date);
                            dtNewUtilized_Date = Convert.ToDateTime(dtformated.ToString());
                        }
                        catch (Exception ex)
                        {
                            Utilized_Date = null;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, Utilized_Date, "Utilized_Date", dr["Utilized_Date"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region AUTHORIZED_NAME

                        try
                        {
                            AUTHORIZED_NAME = dr["AUTHORIZED_NAME"].ToString();
                        }
                        catch (Exception ex)
                        {

                            AUTHORIZED_NAME = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, AUTHORIZED_NAME, "AUTHORIZED_NAME", dr["AUTHORIZED_NAME"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region LINE_STATUS

                        try
                        {
                            LINE_STATUS = dr["LINE_STATUS"].ToString();
                        }
                        catch (Exception ex)
                        {

                            LINE_STATUS = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, LINE_STATUS, "LINE_STATUS", dr["LINE_STATUS"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region REVERT_REVERSED_BAL
                        try
                        {
                            REVERT_REVERSED_BAL = Convert.ToDouble(dr["REVERT_REVERSED_BAL"].ToString());
                        }
                        catch (Exception ex)
                        {

                            REVERT_REVERSED_BAL = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, REVERT_REVERSED_BAL, "REVERT_REVERSED_BAL", dr["REVERT_REVERSED_BAL"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region TOTAL_AMOUNT_PAID
                        try
                        {
                            TOTAL_AMOUNT_PAID = Convert.ToDouble(dr["TOTAL_AMOUNT_PAID"].ToString());
                        }
                        catch (Exception ex)
                        {

                            TOTAL_AMOUNT_PAID = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, TOTAL_AMOUNT_PAID, "TOTAL_AMOUNT_PAID", dr["TOTAL_AMOUNT_PAID"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region HPNRET_GROUPING

                        try
                        {
                            HPNRET_GROUPING = dr["HPNRET_GROUPING"].ToString();
                        }
                        catch (Exception ex)
                        {

                            HPNRET_GROUPING = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, HPNRET_GROUPING, "HPNRET_GROUPING", dr["HPNRET_GROUPING"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region SALESMAN_TYPE
                        try
                        {
                            SALESMAN_TYPE = dr["SALESMAN_TYPE"].ToString();
                        }
                        catch (Exception ex)
                        {

                            SALESMAN_TYPE = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, SALESMAN_TYPE, "SALESMAN_TYPE ", dr["SALESMAN_TYPE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region ENTERED_BY
                        try
                        {
                            EnteredBy = dr["EnteredBy"].ToString();
                        }
                        catch (Exception ex)
                        {

                            EnteredBy = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, EnteredBy, "EnteredBy ", dr["EnteredBy"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region ENTERED_NAME

                        try
                        {
                            EnteredName = dr["EnteredName"].ToString();
                        }
                        catch (Exception ex)
                        {

                            EnteredName = "";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, EnteredName, "EnteredName ", dr["EnteredName"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #endregion

                        #region Add Formatted variable values to data table
                        drBulkSales = dtBulkSales.NewRow();

                        drBulkSales["SITE_CODE"] = SITE_CODE;
                        drBulkSales["SALES_PART"] = SALES_PART;
                        drBulkSales["UNITS"] = UNITS;
                        drBulkSales["SALES_PRICE"] = SALES_PRICE;
                        drBulkSales["UNIT_DISCOUNT_AMOUNT"] = UNIT_DISCOUNT_AMOUNT;
                        drBulkSales["UNIT_DISCOUNT_PRENTENTAGE"] = UNIT_DISCOUNT_PRENTENTAGE;
                        drBulkSales["VALUE"] = VALUE;
                        drBulkSales["TOTAL_DISCOUNT"] = TOTAL_DISCOUNT;
                        drBulkSales["LOCATION_NO"] = LOCATION_NO;
                        drBulkSales["ORDER_NO"] = ORDER_NO;
                        drBulkSales["TDATE"] = SALE_TRANS_DATE;
                        drBulkSales["PROMOTER_CODE"] = PROMOTER_CODE;
                        drBulkSales["AUTHORIZED_USER_ID"] = AUTHORIZED_USER_ID;
                        drBulkSales["STATE"] = STATE;
                        drBulkSales["CUSTOMER_NO"] = CUSTOMER_NO;
                        drBulkSales["LENGTH_OF_CONTRACT"] = LENGTH_OF_CONTRACT;
                        drBulkSales["ADVANCE_PAYMENT_NO"] = ADVANCE_PAYMENT_NO;
                        drBulkSales["ADVANCE_PAYMENT_AMOUNT"] = ADVANCE_PAYMENT_AMOUNT;
                        drBulkSales["SALE_TYPE"] = SALE_TYPE;
                        drBulkSales["SALES_LOCATION_NO"] = SALES_LOCATION_NO;
                        drBulkSales["SERVICE_CHARGE"] = SERVICE_CHARGE;
                        drBulkSales["FIRST_PAYMENT"] = FIRST_PAYMENT;
                        drBulkSales["BUDGET_BOOK_HIRE_PRICE"] = BUDGET_BOOK_HIRE_PRICE;
                        drBulkSales["MONTHLY_PAYMENT"] = MONTHLY_PAYMENT;
                        drBulkSales["BUDGET_BOOK"] = BUDGET_BOOK;
                        drBulkSales["PRICE_LIST"] = PRICE_LIST;
                        drBulkSales["GROSS_HIRE_PRICE"] = GROSS_HIRE_PRICE;
                        drBulkSales["TILL_NO"] = TILL_NO;
                        drBulkSales["SALE_LINE_TYPE"] = SALE_LINE_TYPE;
                        drBulkSales["SALE_LINE_NO"] = SALE_LINE_NO;
                        drBulkSales["SURAKSHA_VALUE"] = SURAKSHA_VALUE;
                        drBulkSales["SANASUMA_VALUE"] = SANASUMA_VALUE;
                        drBulkSales["Senasuma_Plus_Value"] = Senasuma_Plus_Value;
                        drBulkSales["Senasuma_Plus_Mobile_Value"] = Senasuma_Plus_Mobile_Value;
                        drBulkSales["Senasuma_Post_Value"] = Senasuma_Post_Value;
                        drBulkSales["TRANSAACTION_TYPE"] = TRANSAACTION_TYPE;
                        drBulkSales["SALES_VARIATION"] = SALES_VARIATION;
                        drBulkSales["FREE_ISSUE"] = FREE_ISSUE;
                        drBulkSales["salesman_code"] = salesman_code;
                        drBulkSales["Vallibel_Introduced"] = Vallibel_Introduced;
                        drBulkSales["SERIAL_NO"] = SERIAL_NO;
                        drBulkSales["order_category"] = order_category;
                        drBulkSales["sales_tax_value"] = sales_tax_value;
                        drBulkSales["INVENTORY_VALUE"] = INVENTORY_VALUE;
                        drBulkSales["SALES_GROUPING"] = SALES_GROUPING;
                        drBulkSales["RET_REASON_CODE"] = RET_REASON_CODE;
                        drBulkSales["Status_586P"] = Status_586P;
                        drBulkSales["DISCOUNT_TYPE_CODE"] = DISCOUNT_TYPE_CODE;
                        drBulkSales["DISCOUNT_REASON_CODE"] = DISCOUNT_REASON_CODE;
                        drBulkSales["SALESFILETYPE"] = SALESFILETYPE;
                        drBulkSales["PROMOTION_CODE"] = PROMOTION_CODE;
                        drBulkSales["Cash_Conversion_Days"] = Cash_Conversion_Days;
                        drBulkSales["Cash_Conversion_Cal_Days"] = Cash_Conversion_Cal_Days;
                        drBulkSales["Authorized_Date"] = Authorized_Date != null ? Authorized_Date : (object)DBNull.Value;
                        drBulkSales["Utilized_Date"] = Utilized_Date != null ? Utilized_Date : (object)DBNull.Value;
                        drBulkSales["AUTHORIZED_NAME"] = AUTHORIZED_NAME;
                        drBulkSales["LINE_STATUS"] = LINE_STATUS;
                        drBulkSales["REVERT_REVERSED_BAL"] = REVERT_REVERSED_BAL;
                        drBulkSales["TOTAL_AMOUNT_PAID"] = TOTAL_AMOUNT_PAID;
                        drBulkSales["HPNRET_GROUPING"] = HPNRET_GROUPING;
                        drBulkSales["SALESMAN_TYPE"] = SALESMAN_TYPE;
                        drBulkSales["ENTERED_BY"] = EnteredBy;
                        drBulkSales["ENTERED_NAME"] = EnteredName;

                        dtBulkSales.Rows.Add(drBulkSales);

                        #endregion
                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Lopping Error - In Data Formatting Values in " + method.Name + " method", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                intSalesMethodEffectedRecordCount_NP = odbConnection.Insert_BI_NeenOpal_SalesLine(dtBulkSales);
                this.InsertToSingerSideBackUpSalesTable(dtBulkSales);//here one by one method pass data will insert to singer side backup table
                return intSalesMethodEffectedRecordCount_NP;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            finally
            {

            }
            return intSalesMethodEffectedRecordCount_NP;
        }

        private void NeenOpal_BI_DeleteRecordsInBackUp_STG_Sale_Line()
        {
            try
            {
                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                odbConnection.NeenOpal_BI_DeleteRecordsInBackUpSaleLineTable();
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

        private int NeenOpal_SalesLine_BackUpDataForcedByExternal(DataTable dtSINBackUpSalesDetails)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            intBackUpSalesLineMethodEffectRecordCount_BI = odbConnection.Insert_BI_NeenOpal_SalesLine(dtSINBackUpSalesDetails);
            this.InsertToSingerSideBackUpSalesTable(dtSINBackUpSalesDetails);//here one by one method pass data will insert to singer side backup table
            return intBackUpSalesLineMethodEffectRecordCount_BI;

        }

        #endregion

        #region Salesman
        private void Salesman()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_DIM_SALESMAN);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_SALESMAN));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        intSalesLocationInsertRecCount_BI = this.SalesmanExtraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Salesman()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int SalesmanExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtSalesman = odbConnection.GetSalesman(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtSalesman.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_SalesmanProcessTo_BI_NTAN_Server(dtSalesman);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int NeenOpal_SalesmanProcessTo_BI_NTAN_Server(DataTable dtSINSalesman)
        {
            try
            {
                int newRecordTotCount = 0;
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                DataTable dtSalesman = new System.Data.DataTable();
                dtSalesman = dtSINSalesman;

                if (dtSalesman == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtSalesman.Rows)
                {
                    try
                    {
                        Salesmen objSalesmen = new Salesmen();
                        objSalesmen.SALESMAN_CODE = dr["SALESMAN_CODE"].ToString();
                        objSalesmen.SALESMAN_NAME = dr["SALESMAN_NAME"].ToString();
                        objSalesmen.SALESMAN_TYPE = dr["SALESMAN_TYPE"].ToString();
                        objSalesmen.BRANCH_ID = dr["BRANCH_ID"].ToString();
                        objSalesmen.STATUS = dr["STATUS"].ToString();


                        bool isNewRecordAdded = this.CheckSalesmanCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(objSalesmen);
                        if (isNewRecordAdded == true)
                        {
                            newRecordTotCount = newRecordTotCount + 1;
                        }

                    }
                    catch (Exception ee)
                    {

                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ee.Message, ee.StackTrace), "For Lopping Error - In Data Formatting Values in  " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                effectedRecCount_BI = newRecordTotCount;
                return effectedRecCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return effectedRecCount_BI;
        }

        private bool CheckSalesmanCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(Salesmen objSalesmen)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckSalesmanCodeExistsIn_BI_NRTAN_ServerAndProcessTo_NRTAN_Server(objSalesmen);
        }

        #endregion

        #region Inventory Adjustment
        private void InventoryAdjustment()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_INV_ADJUSTMENT);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    //this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_INV_ADJUSTMENT));//comment this tempory for historical given process

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        this.InventoryADJExtraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));

                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  InventoryAdjustment()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }

        }

        private int InventoryADJExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtInventoryADJ = odbConnection.GetInventoryADJ(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :-  " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + dtInventoryADJ.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.InventoryADJProcessTo_BI_NRTAN_Server(dtInventoryADJ);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :-  " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int InventoryADJProcessTo_BI_NRTAN_Server(DataTable dtSINInventoryADJ)
        {
            try
            {
                DataTable dtInventoryADJ = new DataTable();
                dtInventoryADJ = dtSINInventoryADJ;

                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Create data table
                DataTable dtBulkInventoryADJ = new DataTable();
                DataRow drBulkInventoryADJ;

                DataColumn colADJ_TRANS_DATE = new DataColumn("ADJ_TRANS_DATE", typeof(DateTime));
                dtBulkInventoryADJ.Columns.Add(colADJ_TRANS_DATE);

                DataColumn colSTORE_CODE = new DataColumn("STORE_CODE", typeof(string));
                dtBulkInventoryADJ.Columns.Add(colSTORE_CODE);

                DataColumn colPRODUCT_CODE = new DataColumn("PRODUCT_CODE", typeof(string));
                dtBulkInventoryADJ.Columns.Add(colPRODUCT_CODE);

                DataColumn colINVENTORY_LOCATION = new DataColumn("INVENTORY_LOCATION", typeof(string));
                dtBulkInventoryADJ.Columns.Add(colINVENTORY_LOCATION);//1

                DataColumn colADJ_TYPE_CODE = new DataColumn("ADJ_TYPE_CODE", typeof(string));
                dtBulkInventoryADJ.Columns.Add(colADJ_TYPE_CODE);

                DataColumn colADJ_TYPE_DESC = new DataColumn("ADJ_TYPE_DESC", typeof(string));
                dtBulkInventoryADJ.Columns.Add(colADJ_TYPE_DESC);//1

                DataColumn colADJ_REASON_CODE = new DataColumn("ADJ_REASON_CODE", typeof(string));
                dtBulkInventoryADJ.Columns.Add(colADJ_REASON_CODE);

                DataColumn colINV_ADJ_QTY = new DataColumn("INV_ADJ_QTY", typeof(double));
                dtBulkInventoryADJ.Columns.Add(colINV_ADJ_QTY);

                DataColumn colINV_ADJ_VAL_AT_PRICE = new DataColumn("INV_ADJ_VAL_AT_PRICE", typeof(double));
                dtBulkInventoryADJ.Columns.Add(colINV_ADJ_VAL_AT_PRICE);

                DataColumn colINV_ADJ_VAL_AT_COST = new DataColumn("INV_ADJ_VAL_AT_COST", typeof(double));
                dtBulkInventoryADJ.Columns.Add(colINV_ADJ_VAL_AT_COST);




                #endregion

                if (dtInventoryADJ == null)
                {
                    return 0;
                }


                foreach (DataRow dr in dtInventoryADJ.Rows)
                {
                    try
                    {
                        #region Format Variables

                        #region ADJ_TRANS_DATE
                        DateTime ADJ_TRANS_DATE;
                        try
                        {
                            ADJ_TRANS_DATE = Convert.ToDateTime(HelperClass.DateManipulationWithTime(Convert.ToDateTime(dr["ADJ_TRANS_DATE"].ToString())));
                        }
                        catch (Exception ex)
                        {
                            ADJ_TRANS_DATE = Convert.ToDateTime(HelperClass.DateManipulationWithTime(dtPreviousDate_BI));
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, ADJ_TRANS_DATE, "ADJ_TRANS_DATE", dr["ADJ_TRANS_DATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        String STORE_CODE = dr["STORE_CODE"].ToString();
                        String PRODUCT_CODE = dr["PRODUCT_CODE"].ToString();

                        #region INVENTORY_LOCATION
                        string INVENTORY_LOCATION = "";
                        if (dr["INVENTORY_LOCATION"].ToString() == "")
                        {
                            INVENTORY_LOCATION = "";
                        }
                        else
                        {
                            INVENTORY_LOCATION = dr["INVENTORY_LOCATION"].ToString();
                        }
                        #endregion

                        String ADJ_TYPE_CODE = dr["ADJ_TYPE_CODE"].ToString();
                        String ADJ_TYPE_DESC = dr["ADJ_TYPE_DESC"].ToString();
                        String ADJ_REASON_CODE = dr["ADJ_REASON_CODE"].ToString();

                        #region INV_ADJ_QTY
                        double INV_ADJ_QTY;
                        try
                        {
                            INV_ADJ_QTY = Convert.ToDouble(dr["INV_ADJ_QTY"].ToString());
                        }
                        catch (Exception ex)
                        {
                            INV_ADJ_QTY = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, INV_ADJ_QTY, "INV_ADJ_QTY", dr["INV_ADJ_QTY"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region INV_ADJ_VAL_AT_PRICE
                        double INV_ADJ_VAL_AT_PRICE;
                        try
                        {
                            INV_ADJ_VAL_AT_PRICE = Convert.ToDouble(dr["INV_ADJ_VAL_AT_PRICE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            INV_ADJ_VAL_AT_PRICE = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, INV_ADJ_VAL_AT_PRICE, "INV_ADJ_VAL_AT_PRICE", dr["INV_ADJ_VAL_AT_PRICE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region INV_ADJ_VAL_AT_COST
                        double INV_ADJ_VAL_AT_COST;
                        try
                        {
                            INV_ADJ_VAL_AT_COST = Convert.ToDouble(dr["INV_ADJ_VAL_AT_COST"].ToString());
                        }
                        catch (Exception ex)
                        {
                            INV_ADJ_VAL_AT_COST = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, INV_ADJ_VAL_AT_COST, "INV_ADJ_VAL_AT_COST", dr["INV_ADJ_VAL_AT_COST"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #endregion

                        #region Add Formatted variable values to data table
                        drBulkInventoryADJ = dtBulkInventoryADJ.NewRow();

                        drBulkInventoryADJ["ADJ_TRANS_DATE"] = ADJ_TRANS_DATE;
                        drBulkInventoryADJ["STORE_CODE"] = STORE_CODE;
                        drBulkInventoryADJ["PRODUCT_CODE"] = PRODUCT_CODE;
                        drBulkInventoryADJ["INVENTORY_LOCATION"] = INVENTORY_LOCATION;
                        drBulkInventoryADJ["ADJ_TYPE_CODE"] = ADJ_TYPE_CODE;
                        drBulkInventoryADJ["ADJ_TYPE_DESC"] = ADJ_TYPE_DESC;
                        drBulkInventoryADJ["ADJ_REASON_CODE"] = ADJ_REASON_CODE;
                        drBulkInventoryADJ["INV_ADJ_QTY"] = INV_ADJ_QTY;
                        drBulkInventoryADJ["INV_ADJ_VAL_AT_PRICE"] = INV_ADJ_VAL_AT_PRICE;
                        drBulkInventoryADJ["INV_ADJ_VAL_AT_COST"] = INV_ADJ_VAL_AT_COST;


                        dtBulkInventoryADJ.Rows.Add(drBulkInventoryADJ);
                        #endregion

                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "Error - In Formatting Values in " + method.Name + " () method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                intInventoryADJInsertRecCount = odbConnection.InsertInventoryADJ_NewVersion(dtBulkInventoryADJ);
                return intInventoryADJInsertRecCount;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return intInventoryADJInsertRecCount;
        }

        #endregion

        #region StockAgeBand
        private void StockAgeBand()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_STOCKAGE_BAND);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_STOCKAGE_BAND));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        this.StockAgeBandExtraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));

                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  StockAgeBand()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int StockAgeBandExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtStockAgeBand = odbConnection.GetStockAgeBand(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :-  " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + dtStockAgeBand.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.StockAgeBandProcessTo_BI_NTANServer(dtStockAgeBand);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :-  " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int StockAgeBandProcessTo_BI_NTANServer(DataTable dtSINStockAgeBand)
        {
            try
            {
                #region Create data table
                DataTable dtBulkStockAgeBand = new DataTable();
                DataRow drBulkStockAgeBand;

                DataColumn colSTOCKAGE_BAND_DESC = new DataColumn("STOCKAGE_BAND_DESC", typeof(string));
                dtBulkStockAgeBand.Columns.Add(colSTOCKAGE_BAND_DESC);

                DataColumn colSTKAGE_PROD_LEVEL1_CODE = new DataColumn("STKAGE_PROD_LEVEL1_CODE", typeof(string));
                dtBulkStockAgeBand.Columns.Add(colSTKAGE_PROD_LEVEL1_CODE);

                DataColumn colSTOCKAGE_BAND_LOWER = new DataColumn("STOCKAGE_BAND_LOWER", typeof(double));
                dtBulkStockAgeBand.Columns.Add(colSTOCKAGE_BAND_LOWER);

                DataColumn colSTOCKAGE_BAND_UPPER = new DataColumn("STOCKAGE_BAND_UPPER", typeof(double));
                dtBulkStockAgeBand.Columns.Add(colSTOCKAGE_BAND_UPPER);

                DataColumn colSTOCKAGE_BAND_SEQ = new DataColumn("STOCKAGE_BAND_SEQ", typeof(int));
                dtBulkStockAgeBand.Columns.Add(colSTOCKAGE_BAND_SEQ);




                #endregion

                DataTable dtStockAgeBand = new System.Data.DataTable();
                dtStockAgeBand = dtSINStockAgeBand;

                var method = System.Reflection.MethodBase.GetCurrentMethod();

                if (dtStockAgeBand == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtStockAgeBand.Rows)
                {
                    try
                    {
                        #region Format Variables
                        string STOCKAGE_BAND_DESC = dr["STOCKAGE_BAND_DESC"].ToString();
                        string STKAGE_PROD_LEVEL1_CODE = dr["STKAGE_PROD_LEVEL1_CODE"].ToString();


                        #region STOCKAGE_BAND_LOWER
                        Double STOCKAGE_BAND_LOWER;
                        try
                        {
                            STOCKAGE_BAND_LOWER = Convert.ToDouble(dr["STOCKAGE_BAND_LOWER"].ToString());

                        }
                        catch (Exception ex)
                        {
                            STOCKAGE_BAND_LOWER = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, STOCKAGE_BAND_LOWER, "STOCKAGE_BAND_LOWER", dr["STOCKAGE_BAND_LOWER"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region STOCKAGE_BAND_UPPER
                        Double STOCKAGE_BAND_UPPER;
                        try
                        {
                            STOCKAGE_BAND_UPPER = Convert.ToDouble(dr["STOCKAGE_BAND_UPPER"].ToString());

                        }
                        catch (Exception ex)
                        {
                            STOCKAGE_BAND_UPPER = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, STOCKAGE_BAND_UPPER, "STOCKAGE_BAND_UPPER", dr["STOCKAGE_BAND_UPPER"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region STOCKAGE_BAND_SEQ
                        int STOCKAGE_BAND_SEQ;
                        try
                        {
                            STOCKAGE_BAND_SEQ = Convert.ToInt32(dr["STOCKAGE_BAND_SEQ"].ToString());

                        }
                        catch (Exception ex)
                        {
                            STOCKAGE_BAND_SEQ = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, STOCKAGE_BAND_SEQ, "STOCKAGE_BAND_SEQ", dr["STOCKAGE_BAND_SEQ"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #endregion

                        #region Add Formatted variable values to data table
                        drBulkStockAgeBand = dtBulkStockAgeBand.NewRow();

                        drBulkStockAgeBand["STOCKAGE_BAND_DESC"] = STOCKAGE_BAND_DESC;
                        drBulkStockAgeBand["STKAGE_PROD_LEVEL1_CODE"] = STKAGE_PROD_LEVEL1_CODE;
                        drBulkStockAgeBand["STOCKAGE_BAND_LOWER"] = STOCKAGE_BAND_LOWER;
                        drBulkStockAgeBand["STOCKAGE_BAND_UPPER"] = STOCKAGE_BAND_UPPER;
                        drBulkStockAgeBand["STOCKAGE_BAND_SEQ"] = STOCKAGE_BAND_SEQ;



                        dtBulkStockAgeBand.Rows.Add(drBulkStockAgeBand);

                        #endregion
                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Looping Error - In Data Formatting Values in  " + method.Name + " method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                intStockAgeBandEffectRecordCount = odbConnection.Insert_BI_StockAgeBand_NewVersionCommon(dtStockAgeBand);
                return intStockAgeBandEffectRecordCount;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return intStockAgeBandEffectRecordCount;
        }
        #endregion

        #region Mapping Branch to Shop

        private void Mapping_Branch_to_Shop()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_MAP_BR_TO_AD);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_MAP_BR_TO_AD));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {

                        intMappingBMtoADRecordCount_BI = this.Mapping_Branch_to_ShopExtraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Mapping Branch to Site()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }


        private int Mapping_Branch_to_ShopExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtMapping = odbConnection.GetMapping(dtPreviousDate_BI);//1
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtMapping.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_MappingProcessTo_BI_NTAN_Server(dtMapping);//2
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }


        private int NeenOpal_MappingProcessTo_BI_NTAN_Server(DataTable dtSINMapping)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Variable
                string BRANCH_ID;
                string SITE_ID;
                #endregion

                #region Create data table
                DataTable dtBulkMapping = new DataTable();
                DataRow drBulkMapping;

                DataColumn colBRANCH_ID = new DataColumn("BRANCH_ID", typeof(string));
                dtBulkMapping.Columns.Add(colBRANCH_ID);

                DataColumn colSITE_ID = new DataColumn("SITE_ID", typeof(string));
                dtBulkMapping.Columns.Add(colSITE_ID);

                #endregion

                DataTable dtMapping = new System.Data.DataTable();
                dtMapping = dtSINMapping;

                if (dtMapping == null)
                {
                    return 0;
                }

                foreach (DataRow dr in dtMapping.Rows)
                {
                    try
                    {

                        #region Format variable values

                        BRANCH_ID = dr["BRANCH_ID"].ToString();
                        SITE_ID = dr["SITE_ID"].ToString();

                        #endregion

                        #region Add Formatted variable values to data table
                        drBulkMapping = dtBulkMapping.NewRow();

                        drBulkMapping["BRANCH_ID"] = BRANCH_ID;
                        drBulkMapping["SITE_ID"] = SITE_ID;

                        dtBulkMapping.Rows.Add(drBulkMapping);

                        #endregion
                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Lopping Error - In Data Formatting Values in " + method.Name + " method", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                intMappingBMtoADRecordCount_BI = odbConnection.Insert_NeenOpal_BI_Mapping(dtBulkMapping);
                return intMappingBMtoADRecordCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            finally
            {

            }
            return intMappingBMtoADRecordCount_BI;
        }


        #endregion

        #region Inventory Transaction

        private void Inventory_Transaction()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_INV_TRANSACTION);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    //this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_INV_TRANSACTION));//this is tempory commented for loading historical data

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {

                        this.Inventory_TransactionReceiveing();
                        this.Inventory_TransactionSupplies();
                        this.Inventory_TransactionRecipts();
                        this.Inventory_TransactionPurchase();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Inventory_Transaction()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int Inventory_TransactionReceiveing()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtReceiving = odbConnection.GetInventoryTransactionReceiveing(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :-  " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + dtReceiving.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.Inventory_TransactionTo_BI_NTANServer(dtReceiving);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :-  " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int Inventory_TransactionSupplies()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtSuppling = odbConnection.GetInventoryTransactionSupplies(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :-  " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + dtSuppling.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.Inventory_TransactionTo_BI_NTANServer(dtSuppling);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :-  " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int Inventory_TransactionRecipts()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtRecipts = odbConnection.GetInventoryTransactionRecipts(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :-  " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + dtRecipts.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.Inventory_TransactionTo_BI_NTANServer(dtRecipts);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :-  " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int Inventory_TransactionPurchase()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtRecipts = odbConnection.GetInventoryTransactionPurchase(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :-  " + method.Name + " () method Get Data Form DB ", " DAL Table Count : " + dtRecipts.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.Inventory_TransactionTo_BI_NTANServer(dtRecipts);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :-  " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;
        }

        private int Inventory_TransactionTo_BI_NTANServer(DataTable dtSINInventoryTransaction)
        {
            try
            {
                #region Create data table

                DataTable dtBulkInventoryTransation = new DataTable();
                DataRow drBulkInventoryTransationd;

                DataColumn colarrival_date = new DataColumn("arrival_date", typeof(DateTime));
                dtBulkInventoryTransation.Columns.Add(colarrival_date);

                DataColumn colpart_no = new DataColumn("part_no", typeof(string));
                dtBulkInventoryTransation.Columns.Add(colpart_no);

                DataColumn colSupplier_Site = new DataColumn("Supplier_Site", typeof(string));
                dtBulkInventoryTransation.Columns.Add(colSupplier_Site);

                DataColumn colReceiving_Site = new DataColumn("Receiving_Site", typeof(string));
                dtBulkInventoryTransation.Columns.Add(colReceiving_Site);

                DataColumn colPO_NO = new DataColumn("PO_NO", typeof(string));
                dtBulkInventoryTransation.Columns.Add(colPO_NO);

                DataColumn colORDER_NO = new DataColumn("ORDER_NO", typeof(string));
                dtBulkInventoryTransation.Columns.Add(colORDER_NO);

                DataColumn colTRANSACTION_TYPE = new DataColumn("TRANSACTION_TYPE", typeof(string));
                dtBulkInventoryTransation.Columns.Add(colTRANSACTION_TYPE);

                DataColumn colTRANSACTION_STATUS = new DataColumn("TRANSACTION_STATUS", typeof(string));
                dtBulkInventoryTransation.Columns.Add(colTRANSACTION_STATUS);

                DataColumn colTRANSANACTION_SUB_TYPE = new DataColumn("TRANSANACTION_SUB_TYPE", typeof(string));
                dtBulkInventoryTransation.Columns.Add(colTRANSANACTION_SUB_TYPE);

                DataColumn colQTY = new DataColumn("QTY", typeof(int));
                dtBulkInventoryTransation.Columns.Add(colQTY);

                DataColumn colLINE_NO = new DataColumn("LINE_NO", typeof(int));
                dtBulkInventoryTransation.Columns.Add(colLINE_NO);

                DataColumn colRELEASE_NO = new DataColumn("RELEASE_NO", typeof(int));
                dtBulkInventoryTransation.Columns.Add(colRELEASE_NO);

                DataColumn colLINE_ITEM_NO = new DataColumn("LINE_ITEM_NO", typeof(int));
                dtBulkInventoryTransation.Columns.Add(colLINE_ITEM_NO);

                #endregion

                DataTable dtInventoryTransaction = new System.Data.DataTable();
                dtInventoryTransaction = dtSINInventoryTransaction;

                var method = System.Reflection.MethodBase.GetCurrentMethod();

                if (dtInventoryTransaction == null)
                {
                    return 0;
                }
                foreach (DataRow dr in dtInventoryTransaction.Rows)
                {
                    try
                    {
                        #region Format Variables

                        #region INV_DATE Date
                        DateTime INV_DATE;
                        DateTime dtNew_SALE_TRANS_DATE;
                        try
                        {
                            INV_DATE = Convert.ToDateTime(dr["arrival_date"].ToString());
                            string dtformated = HelperClass.DateManipulationWithTime(INV_DATE);
                            dtNew_SALE_TRANS_DATE = Convert.ToDateTime(dtformated.ToString());
                        }
                        catch (Exception ex)
                        {
                            string dtformated = HelperClass.DateManipulationWithTime(dtPreviousDate_BI);//here pass process run date
                            dtNew_SALE_TRANS_DATE = Convert.ToDateTime(dtformated.ToString());
                            INV_DATE = dtNew_SALE_TRANS_DATE;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dtNew_SALE_TRANS_DATE, "INV_DATE", dr["INV_DATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        string PRODUCT_CODE = dr["part_no"].ToString();
                        string SUPPLIER_SITE = dr["SUPPLIER_SITE"].ToString();
                        string RECEIVING_SITE = dr["RECEIVING_SITE"].ToString();

                        #region PO_NO
                        string PO_NO;
                        if (dr["PO_NO"].ToString() == "")
                        {
                            PO_NO = "";
                        }
                        else
                        {
                            PO_NO = dr["PO_NO"].ToString();
                        }
                        #endregion

                        string ORDER_NO = dr["ORDER_NO"].ToString();
                        string TRANSACTION_TYPE = dr["TRANSACTION_TYPE"].ToString();
                        string TRANSACTION_STATUS = dr["TRANSACTION_STATUS"].ToString();
                        string TRANSANACTION_SUB_TYPE = dr["Transaction_sub_type"].ToString();

                        #region QTY
                        int QTY;
                        try
                        {
                            QTY = Convert.ToInt32(dr["QTY"].ToString());
                        }
                        catch (Exception ex)
                        {

                            QTY = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, QTY, "QTY", dr["QTY"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        int LINE_NO = Convert.ToInt32(dr["LINE_NO"]);
                        int RELEASE_NO = Convert.ToInt32(dr["RELEASE_NO"]);
                        int LINE_ITEM_NO = Convert.ToInt32(dr["LINE_ITEM_NO"]);

                        #endregion

                        #region Add Formatted variable values to data table
                        drBulkInventoryTransationd = dtBulkInventoryTransation.NewRow();

                        drBulkInventoryTransationd["arrival_date"] = INV_DATE;
                        drBulkInventoryTransationd["part_no"] = PRODUCT_CODE;
                        drBulkInventoryTransationd["Supplier_Site"] = SUPPLIER_SITE;
                        drBulkInventoryTransationd["Receiving_Site"] = RECEIVING_SITE;

                        drBulkInventoryTransationd["PO_NO"] = PO_NO;

                        drBulkInventoryTransationd["ORDER_NO"] = ORDER_NO;
                        drBulkInventoryTransationd["TRANSACTION_TYPE"] = TRANSACTION_TYPE;
                        drBulkInventoryTransationd["TRANSACTION_STATUS"] = TRANSACTION_STATUS;
                        drBulkInventoryTransationd["TRANSANACTION_SUB_TYPE"] = TRANSANACTION_SUB_TYPE;

                        drBulkInventoryTransationd["QTY"] = QTY;

                        drBulkInventoryTransationd["LINE_NO"] = LINE_NO;
                        drBulkInventoryTransationd["RELEASE_NO"] = RELEASE_NO;
                        drBulkInventoryTransationd["LINE_ITEM_NO"] = LINE_ITEM_NO;


                        dtBulkInventoryTransation.Rows.Add(drBulkInventoryTransationd);

                        #endregion
                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Looping Error - In Data Formatting Values in  " + method.Name + " method.", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                intInventoryTransationEffectRecordCount = odbConnection.Insert_BI_Inventory_Transaction(dtBulkInventoryTransation);
                return intInventoryTransationEffectRecordCount;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return intInventoryTransationEffectRecordCount;
        }

        #endregion

        #region Inventory Position

        private void Inventory_Position()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_INVENTORY_POSITION);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    //this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_INVENTORY_POSITION));//THIS IS TEMPORY COMMNETED because of histrical data given case

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {

                        this.Inventory_PositionExtraction();
                        this.Inventory_PositionExtraction_DMD();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From Inventory Position()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int Inventory_PositionExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtInvPosition = odbConnection.GetInvPosition(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtInvPosition.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_InvPositionTo_BI_NTAN_Server(dtInvPosition);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int Inventory_PositionExtraction_DMD()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtInvPosition = odbConnection.GetInvPosition_DMD(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtInvPosition.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_InvPositionTo_BI_NTAN_Server(dtInvPosition);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int NeenOpal_InvPositionTo_BI_NTAN_Server(DataTable dtSINInvPosition)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Variable

                DateTime AS_AT_DATE;
                string PRODUCT_CODE;
                string SITE_ID;
                string INVENTORY_LOCATION;
                double COST;
                double QTY;

                #endregion

                #region Create data table
                DataTable dtBulkInvPosition = new DataTable();
                DataRow dBulkInvPosition;

                DataColumn colAS_AT_DATE = new DataColumn("AS_AT_DATE", typeof(DateTime));
                dtBulkInvPosition.Columns.Add(colAS_AT_DATE);

                DataColumn colPRODUCT_CODE = new DataColumn("PRODUCT_CODE", typeof(string));
                dtBulkInvPosition.Columns.Add(colPRODUCT_CODE);

                DataColumn colSITE_ID = new DataColumn("SITE_ID", typeof(string));
                dtBulkInvPosition.Columns.Add(colSITE_ID);

                DataColumn colINVENTORY_LOCATION = new DataColumn("INVENTORY_LOCATION", typeof(string));
                dtBulkInvPosition.Columns.Add(colINVENTORY_LOCATION);

                DataColumn colCOST = new DataColumn("COST", typeof(double));
                dtBulkInvPosition.Columns.Add(colCOST);

                DataColumn colQTY = new DataColumn("QTY", typeof(double));
                dtBulkInvPosition.Columns.Add(colQTY);

                #endregion

                DataTable dtInvPosition = new System.Data.DataTable();
                dtInvPosition = dtSINInvPosition;

                if (dtInvPosition == null)
                {
                    return 0;
                }

                foreach (DataRow dr in dtInvPosition.Rows)
                {
                    try
                    {

                        #region Format variable values

                        try
                        {

                            AS_AT_DATE = Convert.ToDateTime(dr["AS_AT_DATE"].ToString());

                        }

                        catch (Exception ex)
                        {



                            AS_AT_DATE = Convert.ToDateTime(HelperClass.DateManipulationWithTime(dtPreviousDate_BI));

                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, AS_AT_DATE, "AS_AT_DATE", dr["AS_AT_DATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());


                        }


                        PRODUCT_CODE = dr["PRODUCT_CODE"].ToString();
                        SITE_ID = dr["SITE_ID"].ToString();


                        try
                        {
                            INVENTORY_LOCATION = (dr["INVENTORY_LOCATION"].ToString());
                        }
                        catch (Exception ex)
                        {

                            INVENTORY_LOCATION = "N/A";
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, INVENTORY_LOCATION, "INVENTORY_LOCATION", dr["INVENTORY_LOCATION"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        try
                        {
                            COST = Convert.ToDouble(dr["COST"].ToString());
                        }
                        catch (Exception ex)
                        {

                            COST = 0.01;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, COST, "COST", dr["COST"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        try
                        {
                            QTY = Convert.ToDouble(dr["QTY"].ToString());
                        }
                        catch (Exception ex)
                        {

                            QTY = 0.0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, QTY, "QTY", dr["QTY"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }



                        #endregion

                        #region Add Formatted variable values to data table
                        dBulkInvPosition = dtBulkInvPosition.NewRow();

                        dBulkInvPosition["AS_AT_DATE"] = AS_AT_DATE;
                        dBulkInvPosition["PRODUCT_CODE"] = PRODUCT_CODE;
                        dBulkInvPosition["SITE_ID"] = SITE_ID;
                        dBulkInvPosition["INVENTORY_LOCATION"] = INVENTORY_LOCATION;
                        dBulkInvPosition["COST"] = COST;
                        dBulkInvPosition["QTY"] = QTY;

                        dtBulkInvPosition.Rows.Add(dBulkInvPosition);

                        #endregion
                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Lopping Error - In Data Formatting Values in " + method.Name + " method", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                intInventoryPositionRecordCount_BI = odbConnection.Insert_NeenOpal_BI_Inv_Position(dtBulkInvPosition);
                return intInventoryPositionRecordCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            finally
            {

            }
            return intInventoryPositionRecordCount_BI;
        }

        #endregion

        #region Cash Management

        private void Cash_Management()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_CASH_MANAGEMENT);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_CASH_MANAGEMENT));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {

                        this.Cash_ManagementExtraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From Cash Management()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int Cash_ManagementExtraction()
        {

            //set variables
            string strLiveYearMonth;
            int intYear = dtCurrentDate.Year;
            int intMonth = dtCurrentDate.Month;
            if (intMonth < 10)
            {
                strLiveYearMonth = intYear.ToString() + "/" + "0" + intMonth.ToString();
            }
            else
            {
                strLiveYearMonth = intYear.ToString() + "/" + intMonth.ToString();
            }
            //set variables

            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtCash = odbConnection.GetCashManagement(dtPreviousDate_BI, strLiveYearMonth);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtCash.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_CashManagementTo_BI_NTAN_Server(dtCash);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int NeenOpal_CashManagementTo_BI_NTAN_Server(DataTable dtSINCashManagement)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();
                List<CashManagement> lstCashManagement = new List<CashManagement>();

                DataTable dtCashManagement = new System.Data.DataTable();
                dtCashManagement = dtSINCashManagement;

                if (dtCashManagement == null)
                {
                    return 0;
                }

                foreach (DataRow dr in dtCashManagement.Rows)
                {
                    try
                    {
                        CashManagement objCashManagement = new CashManagement();

                        objCashManagement.SITE_ID = dr["SITE_ID"].ToString();
                        objCashManagement.TRAN_DATE = Convert.ToDateTime(HelperClass.DateManipulation(Convert.ToDateTime(dr["TRAN_DATE"].ToString())));

                        #region OPEN_AMOUNT
                        decimal OPEN_AMOUNT = 0;
                        try
                        {
                            objCashManagement.OPEN_AMOUNT = Convert.ToDecimal(dr["OPEN_AMOUNT"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.OPEN_AMOUNT = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, OPEN_AMOUNT, "OPEN_AMOUNT", dr["OPEN_AMOUNT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region BANKED_TRANS
                        int BANKED_TRANS = 0;
                        try
                        {
                            objCashManagement.BANKED_TRANS = Convert.ToInt32(dr["BANKED_TRANS"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.BANKED_TRANS = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, BANKED_TRANS, "BANKED_TRANS", dr["BANKED_TRANS"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region BANKING_AMOUNT
                        decimal BANKING_AMOUNT = 0;
                        try
                        {
                            objCashManagement.BANKING_AMOUNT = Convert.ToDecimal(dr["BANKING_AMOUNT"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.BANKING_AMOUNT = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, BANKING_AMOUNT, "BANKING_AMOUNT", dr["BANKING_AMOUNT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region ARREARS_TRANS
                        int ARREARS_TRANS = 0;
                        try
                        {
                            objCashManagement.ARREARS_TRANS = Convert.ToInt32(dr["ARREARS_TRANS"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.ARREARS_TRANS = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, ARREARS_TRANS, "ARREARS_TRANS", dr["ARREARS_TRANS"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region ARREARS_VALUE
                        decimal ARREARS_VALUE = 0;
                        try
                        {
                            objCashManagement.ARREARS_VALUE = Convert.ToDecimal(dr["ARREARS_VALUE"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.ARREARS_VALUE = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, ARREARS_VALUE, "ARREARS_VALUE", dr["ARREARS_VALUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region CASH_ADVANCES_TRANS
                        int CASH_ADVANCES_TRANS = 0;
                        try
                        {
                            objCashManagement.CASH_ADVANCES_TRANS = Convert.ToInt32(dr["CASH_ADVANCES_TRANS"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.CASH_ADVANCES_TRANS = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, CASH_ADVANCES_TRANS, "CASH_ADVANCES_TRANS", dr["ARREARS_TRANS"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region CASH_ADVANCES_VALUE
                        decimal CASH_ADVANCES_VALUE = 0;
                        try
                        {
                            objCashManagement.CASH_ADVANCES_VALUE = Convert.ToDecimal(dr["CASH_ADVANCES_VALUE"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.CASH_ADVANCES_VALUE = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, CASH_ADVANCES_VALUE, "CASH_ADVANCES_VALUE", dr["CASH_ADVANCES_VALUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region OVERDUE_CREDIT_SALES
                        decimal OVERDUE_CREDIT_SALES = 0;
                        try
                        {
                            objCashManagement.OVERDUE_CREDIT_SALES = Convert.ToDecimal(dr["OVERDUE_CREDIT_SALES"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.OVERDUE_CREDIT_SALES = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, OVERDUE_CREDIT_SALES, "OVERDUE_CREDIT_SALES", dr["OVERDUE_CREDIT_SALES"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region HP_COLLECTION
                        decimal HP_COLLECTION = 0;
                        try
                        {
                            objCashManagement.HP_COLLECTION = Convert.ToDecimal(dr["HP_COLLECTION"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.HP_COLLECTION = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, HP_COLLECTION, "HP_COLLECTION", dr["HP_COLLECTION"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region OTHER_COLLECTIONS
                        decimal OTHER_COLLECTIONS = 0;
                        try
                        {
                            objCashManagement.OTHER_COLLECTIONS = Convert.ToDecimal(dr["OTHER_COLLECTIONS"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.OTHER_COLLECTIONS = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, OTHER_COLLECTIONS, "OTHER_COLLECTIONS", dr["OTHER_COLLECTIONS"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region ISSUE_VALUE
                        decimal ISSUE_VALUE = 0;
                        try
                        {
                            objCashManagement.ISSUE_VALUE = Convert.ToDecimal(dr["ISSUE_VALUE"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.ISSUE_VALUE = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, ISSUE_VALUE, "ISSUE_VALUE", dr["ISSUE_VALUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region BALANCE_ISSUE_AMOUNT
                        decimal BALANCE_ISSUE_AMOUNT = 0;
                        try
                        {
                            objCashManagement.BALANCE_ISSUE_AMOUNT = Convert.ToDecimal(dr["BALANCE_ISSUE_AMOUNT"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.BALANCE_ISSUE_AMOUNT = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, BALANCE_ISSUE_AMOUNT, "BALANCE_ISSUE_AMOUNT", dr["BALANCE_ISSUE_AMOUNT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region UNREALISED_DEPOSIT
                        decimal UNREALISED_DEPOSIT = 0;
                        try
                        {
                            objCashManagement.UNREALISED_DEPOSIT = Convert.ToDecimal(dr["UNREALISED_DEPOSIT"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.UNREALISED_DEPOSIT = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, UNREALISED_DEPOSIT, "UNREALISED_DEPOSIT", dr["UNREALISED_DEPOSIT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region NO_OF_HP_ACCOUNTS
                        int NO_OF_HP_ACCOUNTS = 0;
                        try
                        {
                            objCashManagement.NO_OF_HP_ACCOUNTS = Convert.ToInt32(dr["NO_OF_HP_ACCOUNTS"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.NO_OF_HP_ACCOUNTS = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, NO_OF_HP_ACCOUNTS, "NO_OF_HP_ACCOUNTS", dr["NO_OF_HP_ACCOUNTS"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region SHORT_REMITTANCE_VALUE
                        decimal SHORT_REMITTANCE_VALUE = 0;
                        try
                        {
                            objCashManagement.SHORT_REMITTANCE_VALUE = Convert.ToDecimal(dr["SHORT_REMITTANCE_VALUE"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.SHORT_REMITTANCE_VALUE = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, SHORT_REMITTANCE_VALUE, "SHORT_REMITTANCE_VALUE", dr["SHORT_REMITTANCE_VALUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region LEASE_ASSET_OUTSTD_BALANCE
                        decimal LEASE_ASSET_OUTSTD_BALANCE = 0;
                        try
                        {
                            objCashManagement.LEASE_ASSET_OUTSTD_BALANCE = Convert.ToDecimal(dr["LEASE_ASSET_OUTSTD_BALANCE"]);
                        }
                        catch (Exception ex)
                        {
                            objCashManagement.LEASE_ASSET_OUTSTD_BALANCE = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, LEASE_ASSET_OUTSTD_BALANCE, "LEASE_ASSET_OUTSTD_BALANCE", dr["LEASE_ASSET_OUTSTD_BALANCE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        lstCashManagement.Add(objCashManagement);
                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Lopping Error - In Data Formatting Values in " + method.Name + " method", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                intCashManageentRecordCount_BI = odbConnection.InsertCashManagementTo_BI_NRTANServerIncrementalProcess(lstCashManagement);
                return intCashManageentRecordCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            finally
            {

            }
            return intInventoryPositionRecordCount_BI;
        }

        #endregion

        #region Marketing Budget
        private void MarketingBudget()
        {
            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_MARKETING_BUDGET);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_MARKETING_BUDGET));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        intMarketingBudgetRecordCount_BI = this.MarketingBudgetExtraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From  Marketing Budget()  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int MarketingBudgetExtraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtMktBudget = odbConnection.GetMktBudget(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dtMktBudget.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_MktBudgetProcessTo_BI_NTAN_Server(dtMktBudget);
            effectedRecCount_BI = this.GetMaster_BI_NRTAN_InsertedRecordCount(PassTableID);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int NeenOpal_MktBudgetProcessTo_BI_NTAN_Server(DataTable dtSINMktBudget)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Variable
                int TRAN_YEAR;
                string BUDGET_SITE;
                string BUDGET_DES;
                double BUDGET;
                double BUDGET_UTILIZED;
                double BALANCE;
                #endregion

                #region Create data table
                DataTable dtBulkMktBudget = new DataTable();
                DataRow drBulkMktBudget;

                DataColumn colTRAN_YEAR = new DataColumn("TRAN_YEAR", typeof(int));
                dtBulkMktBudget.Columns.Add(colTRAN_YEAR);

                DataColumn colBUDGET_SITE = new DataColumn("BUDGET_SITE", typeof(string));
                dtBulkMktBudget.Columns.Add(colBUDGET_SITE);

                DataColumn colBUDGET_DES = new DataColumn("BUDGET_DES", typeof(string));
                dtBulkMktBudget.Columns.Add(colBUDGET_DES);

                DataColumn colBUDGET = new DataColumn("BUDGET", typeof(double));
                dtBulkMktBudget.Columns.Add(colBUDGET);

                DataColumn colBUDGET_UTILIZED = new DataColumn("BUDGET_UTILIZED", typeof(double));
                dtBulkMktBudget.Columns.Add(colBUDGET_UTILIZED);

                DataColumn colBALANCE = new DataColumn("BALANCE", typeof(double));
                dtBulkMktBudget.Columns.Add(colBALANCE);

                #endregion

                DataTable dtMktBudget = new System.Data.DataTable();
                dtMktBudget = dtSINMktBudget;

                if (dtMktBudget == null)
                {
                    return 0;
                }

                foreach (DataRow dr in dtMktBudget.Rows)
                {
                    try
                    {

                        #region Format variable values

                        TRAN_YEAR = Convert.ToInt32(dr["TRAN_YEAR"].ToString());
                        BUDGET_SITE = dr["BUDGET_SITE"].ToString();
                        BUDGET_DES = dr["BUDGET_DES"].ToString();
                        #region BUDGET
                        try
                        {
                            BUDGET = Convert.ToDouble(dr["BUDGET"].ToString());
                        }
                        catch (Exception ex)
                        {
                            BUDGET = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, BUDGET, "BUDGET", dr["BUDGET"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region BUDGET_UTILIZED
                        try
                        {
                            BUDGET_UTILIZED = Convert.ToDouble(dr["BUDGET_UTILIZED"].ToString());
                        }
                        catch (Exception ex)
                        {
                            BUDGET_UTILIZED = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, BUDGET_UTILIZED, "BUDGET_UTILIZED", dr["BUDGET_UTILIZED"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #region BALANCE
                        try
                        {
                            BALANCE = Convert.ToDouble(dr["BALANCE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            BALANCE = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, BALANCE, "BALANCE", dr["BALANCE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        #endregion

                        #region Add Formatted variable values to data table
                        drBulkMktBudget = dtBulkMktBudget.NewRow();

                        drBulkMktBudget["TRAN_YEAR"] = TRAN_YEAR;
                        drBulkMktBudget["BUDGET_SITE"] = BUDGET_SITE;
                        drBulkMktBudget["BUDGET_DES"] = BUDGET_DES;
                        drBulkMktBudget["BUDGET"] = BUDGET;
                        drBulkMktBudget["BUDGET_UTILIZED"] = BUDGET_UTILIZED;
                        drBulkMktBudget["BALANCE"] = BALANCE;

                        dtBulkMktBudget.Rows.Add(drBulkMktBudget);

                        #endregion
                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Lopping Error - In Data Formatting Values in " + method.Name + " method", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                intMarketingBudgetRecordCount_BI = odbConnection.Insert_NeenOpal_Marketing_Budget(dtBulkMktBudget);
                return intMarketingBudgetRecordCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            finally
            {

            }
            return intMarketingBudgetRecordCount_BI;
        }

        #endregion


        #region HP WALK

        #region BI_HP_WALK_MEGA

        private void BI_HP_WALK_MEGA()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_HP_WALK_MEGA);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_HP_WALK_MEGA));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        this.BI_HP_WALK_MEGA_Extraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From " + method.Name + "  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int BI_HP_WALK_MEGA_Extraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_HP_WALK_MEGA = odbConnection.Get_BI_HP_WALK_MEGA(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_HP_WALK_MEGA.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_HP_WALK_MEGA_ProcessTo_BI_NTAN_Server(dt_BI_HP_WALK_MEGA);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int NeenOpal_BI_HP_WALK_MEGA_ProcessTo_BI_NTAN_Server(DataTable dt_BI_HP_WALK_MEGA)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Create data table

                DataTable dt_ = new DataTable();

                DataColumn ORDER_NO = new DataColumn("ORDER_NO", typeof(string));
                dt_.Columns.Add(ORDER_NO);

                DataColumn CONTRACT = new DataColumn("CONTRACT", typeof(string));
                dt_.Columns.Add(CONTRACT);

                DataColumn CUSTOMER_NO = new DataColumn("CUSTOMER_NO", typeof(string));
                dt_.Columns.Add(CUSTOMER_NO);

                DataColumn SOURCE_OF_INCOME = new DataColumn("SOURCE_OF_INCOME", typeof(string));
                dt_.Columns.Add(SOURCE_OF_INCOME);

                DataColumn MONTHLY_INCOME_RANGE = new DataColumn("MONTHLY_INCOME_RANGE", typeof(string));
                dt_.Columns.Add(MONTHLY_INCOME_RANGE);

                DataColumn PLACE_OF_EMPLOYEMENT = new DataColumn("PLACE_OF_EMPLOYEMENT", typeof(string));
                dt_.Columns.Add(PLACE_OF_EMPLOYEMENT);

                DataColumn AFFORDABLE_FIRST_PAYMENT = new DataColumn("AFFORDABLE_FIRST_PAYMENT", typeof(string));
                dt_.Columns.Add(AFFORDABLE_FIRST_PAYMENT);

                DataColumn MONTHLY_PAYMENT_RANGE = new DataColumn("MONTHLY_PAYMENT_RANGE", typeof(string));
                dt_.Columns.Add(MONTHLY_PAYMENT_RANGE);

                DataColumn STATUS = new DataColumn("STATUS", typeof(string));
                dt_.Columns.Add(STATUS);

                DataColumn REASON = new DataColumn("REASON", typeof(string));
                dt_.Columns.Add(REASON);

                DataColumn APPROVER_ID = new DataColumn("APPROVER_ID", typeof(string));
                dt_.Columns.Add(APPROVER_ID);

                DataColumn REMARKS = new DataColumn("REMARKS", typeof(string));
                dt_.Columns.Add(REMARKS);

                DataColumn READ = new DataColumn("READ", typeof(int));
                dt_.Columns.Add(READ);

                DataColumn NO_GUR_REASON = new DataColumn("NO_GUR_REASON", typeof(string));
                dt_.Columns.Add(NO_GUR_REASON);

                DataColumn ACTION_TIME = new DataColumn("ACTION_TIME", typeof(DateTime));
                dt_.Columns.Add(ACTION_TIME);

                DataColumn ROWVERSION = new DataColumn("ROWVERSION", typeof(DateTime));
                dt_.Columns.Add(ROWVERSION);

                #endregion

                if (dt_BI_HP_WALK_MEGA == null)
                {
                    return 0;
                }

                foreach (DataRow dr in dt_BI_HP_WALK_MEGA.Rows)
                {

                    DataRow dr_ = dt_.NewRow();

                    try
                    {

                        #region Format variable values and add to table

                        #region ORDER_NO

                        try
                        {
                            dr_["ORDER_NO"] = dr["ORDER_NO"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["ORDER_NO"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["ORDER_NO"], "ORDER_NO", dr["ORDER_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region CONTRACT

                        try
                        {
                            dr_["CONTRACT"] = dr["CONTRACT"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["CONTRACT"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["CONTRACT"], "CONTRACT", dr["CONTRACT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region CUSTOMER_NO

                        try
                        {
                            dr_["CUSTOMER_NO"] = dr["CUSTOMER_NO"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["CUSTOMER_NO"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["CUSTOMER_NO"], "CUSTOMER_NO", dr["CUSTOMER_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SOURCE_OF_INCOME

                        try
                        {
                            dr_["SOURCE_OF_INCOME"] = dr["SOURCE_OF_INCOME"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["SOURCE_OF_INCOME"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SOURCE_OF_INCOME"], "SOURCE_OF_INCOME", dr["SOURCE_OF_INCOME"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region MONTHLY_INCOME_RANGE

                        try
                        {
                            dr_["MONTHLY_INCOME_RANGE"] = dr["MONTHLY_INCOME_RANGE"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["MONTHLY_INCOME_RANGE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["MONTHLY_INCOME_RANGE"], "MONTHLY_INCOME_RANGE", dr["MONTHLY_INCOME_RANGE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region PLACE_OF_EMPLOYEMENT

                        try
                        {
                            dr_["PLACE_OF_EMPLOYEMENT"] = dr["PLACE_OF_EMPLOYEMENT"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["PLACE_OF_EMPLOYEMENT"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["PLACE_OF_EMPLOYEMENT"], "PLACE_OF_EMPLOYEMENT", dr["PLACE_OF_EMPLOYEMENT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region AFFORDABLE_FIRST_PAYMENT

                        try
                        {
                            dr_["AFFORDABLE_FIRST_PAYMENT"] = dr["AFFORDABLE_FIRST_PAYMENT"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["AFFORDABLE_FIRST_PAYMENT"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["AFFORDABLE_FIRST_PAYMENT"], "AFFORDABLE_FIRST_PAYMENT", dr["AFFORDABLE_FIRST_PAYMENT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region MONTHLY_PAYMENT_RANGE

                        try
                        {
                            dr_["MONTHLY_PAYMENT_RANGE"] = dr["MONTHLY_PAYMENT_RANGE"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["MONTHLY_PAYMENT_RANGE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["MONTHLY_PAYMENT_RANGE"], "MONTHLY_PAYMENT_RANGE", dr["MONTHLY_PAYMENT_RANGE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region STATUS

                        try
                        {
                            dr_["STATUS"] = dr["STATUS"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["STATUS"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["STATUS"], "STATUS", dr["STATUS"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region REASON

                        try
                        {
                            dr_["REASON"] = dr["REASON"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["REASON"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["REASON"], "REASON", dr["REASON"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region APPROVER_ID

                        try
                        {
                            dr_["APPROVER_ID"] = dr["APPROVER_ID"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["APPROVER_ID"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["APPROVER_ID"], "APPROVER_ID", dr["APPROVER_ID"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region REMARKS

                        try
                        {
                            dr_["REMARKS"] = dr["REMARKS"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["REMARKS"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["REMARKS"], "REMARKS", dr["REMARKS"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region READ

                        try
                        {
                            dr_["READ"] = Convert.ToInt32(dr["READ"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["READ"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["READ"], "READ", dr["READ"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region NO_GUR_REASON

                        try
                        {
                            dr_["NO_GUR_REASON"] = dr["NO_GUR_REASON"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["NO_GUR_REASON"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["NO_GUR_REASON"], "NO_GUR_REASON", dr["NO_GUR_REASON"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region ACTION_TIME

                        try
                        {
                            dr_["ACTION_TIME"] = Convert.ToDateTime(dr["ACTION_TIME"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["ACTION_TIME"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["ACTION_TIME"], "ACTION_TIME", dr["ACTION_TIME"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region ROWVERSION

                        try
                        {
                            dr_["ROWVERSION"] = Convert.ToDateTime(dr["ROWVERSION"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["ROWVERSION"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["ROWVERSION"], "ROWVERSION", dr["ROWVERSION"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        dt_.Rows.Add(dr_);

                        #endregion

                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Lopping Error - In Data Formatting Values in " + method.Name + " method", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                int_BI_HP_WALK_MEGA_RecordCount_BI = odbConnection.Insert_NeenOpal_BI_HP_WALK_MEGA(dt_);
                return int_BI_HP_WALK_MEGA_RecordCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            finally
            {

            }
            return int_BI_HP_WALK_MEGA_RecordCount_BI;
        }

        #endregion

        #region BI_WRITE_OFF_AMT

        private void BI_WRITE_OFF_AMT()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_WRITE_OFF_AMT);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_WRITE_OFF_AMT));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        this.BI_WRITE_OFF_AMT_Extraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From " + method.Name + "  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int BI_WRITE_OFF_AMT_Extraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_WRITE_OFF_AMT = odbConnection.Get_BI_WRITE_OFF_AMT(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_WRITE_OFF_AMT.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_WRITE_OFF_AMT_ProcessTo_BI_NTAN_Server(dt_BI_WRITE_OFF_AMT);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int NeenOpal_BI_WRITE_OFF_AMT_ProcessTo_BI_NTAN_Server(DataTable dt_BI_WRITE_OFF_AMT)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Create data table

                DataTable dt_ = new DataTable();

                DataColumn SITE_ID = new DataColumn("SITE_ID", typeof(string));
                dt_.Columns.Add(SITE_ID);

                DataColumn SALES_DATE = new DataColumn("SALES_DATE", typeof(DateTime));
                dt_.Columns.Add(SALES_DATE);

                DataColumn ACCOUNT_NO = new DataColumn("ACCOUNT_NO", typeof(string));
                dt_.Columns.Add(ACCOUNT_NO);

                DataColumn CUSTOMER_NO = new DataColumn("CUSTOMER_NO", typeof(string));
                dt_.Columns.Add(CUSTOMER_NO);

                DataColumn SALES_GROUPING = new DataColumn("SALES_GROUPING", typeof(string));
                dt_.Columns.Add(SALES_GROUPING);

                DataColumn TOTAL_RECIEVABLE = new DataColumn("TOTAL_RECIEVABLE", typeof(double));
                dt_.Columns.Add(TOTAL_RECIEVABLE);

                DataColumn SANASUMA_VALUE = new DataColumn("SANASUMA_VALUE", typeof(double));
                dt_.Columns.Add(SANASUMA_VALUE);

                DataColumn SURAKSHA_VALUE = new DataColumn("SURAKSHA_VALUE", typeof(double));
                dt_.Columns.Add(SURAKSHA_VALUE);

                DataColumn BUDGET_BOOK = new DataColumn("BUDGET_BOOK", typeof(string));
                dt_.Columns.Add(BUDGET_BOOK);

                DataColumn DOWNPAYMENT = new DataColumn("DOWNPAYMENT", typeof(double));
                dt_.Columns.Add(DOWNPAYMENT);

                DataColumn FIRST_PAYMENT = new DataColumn("FIRST_PAYMENT", typeof(double));
                dt_.Columns.Add(FIRST_PAYMENT);

                DataColumn SALESMAN_ID = new DataColumn("SALESMAN_ID", typeof(string));
                dt_.Columns.Add(SALESMAN_ID);

                DataColumn MONTHLY_PAY = new DataColumn("MONTHLY_PAY", typeof(double));
                dt_.Columns.Add(MONTHLY_PAY);

                DataColumn AGREED_TO_PAY_DATE = new DataColumn("AGREED_TO_PAY_DATE", typeof(DateTime));
                dt_.Columns.Add(AGREED_TO_PAY_DATE);

                DataColumn PRODUCT_CODE = new DataColumn("PRODUCT_CODE", typeof(string));
                dt_.Columns.Add(PRODUCT_CODE);

                DataColumn CASH_PRICE = new DataColumn("CASH_PRICE", typeof(double));
                dt_.Columns.Add(CASH_PRICE);

                DataColumn GROSS_HIRE_VALUE = new DataColumn("GROSS_HIRE_VALUE", typeof(double));
                dt_.Columns.Add(GROSS_HIRE_VALUE);

                DataColumn LENGTH_OF_CONTRACT = new DataColumn("LENGTH_OF_CONTRACT", typeof(int));
                dt_.Columns.Add(LENGTH_OF_CONTRACT);

                #endregion

                if (dt_BI_WRITE_OFF_AMT == null)
                {
                    return 0;
                }

                foreach (DataRow dr in dt_BI_WRITE_OFF_AMT.Rows)
                {

                    DataRow dr_ = dt_.NewRow();

                    try
                    {

                        #region Format variable values and add to table

                        #region SITE_ID

                        try
                        {
                            dr_["SITE_ID"] = dr["SITE_ID"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["SITE_ID"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SITE_ID"], "SITE_ID", dr["SITE_ID"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SALES_DATE

                        try
                        {
                            dr_["SALES_DATE"] = Convert.ToDateTime(dr["SALES_DATE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["SALES_DATE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SALES_DATE"], "SALES_DATE", dr["SALES_DATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region ACCOUNT_NO

                        try
                        {
                            dr_["ACCOUNT_NO"] = dr["ACCOUNT_NO"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["ACCOUNT_NO"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["ACCOUNT_NO"], "ACCOUNT_NO", dr["ACCOUNT_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region CUSTOMER_NO

                        try
                        {
                            dr_["CUSTOMER_NO"] = dr["CUSTOMER_NO"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["CUSTOMER_NO"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["CUSTOMER_NO"], "CUSTOMER_NO", dr["CUSTOMER_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SALES_GROUPING

                        try
                        {
                            dr_["SALES_GROUPING"] = dr["SALES_GROUPING"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["SALES_GROUPING"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SALES_GROUPING"], "SALES_GROUPING", dr["SALES_GROUPING"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region TOTAL_RECIEVABLE

                        try
                        {
                            dr_["TOTAL_RECIEVABLE"] = Convert.ToDouble(dr["TOTAL_RECIEVABLE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["TOTAL_RECIEVABLE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["TOTAL_RECIEVABLE"], "TOTAL_RECIEVABLE", dr["TOTAL_RECIEVABLE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SANASUMA_VALUE

                        try
                        {
                            dr_["SANASUMA_VALUE"] = Convert.ToDouble(dr["SANASUMA_VALUE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["SANASUMA_VALUE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SANASUMA_VALUE"], "SANASUMA_VALUE", dr["SANASUMA_VALUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SURAKSHA_VALUE

                        try
                        {
                            dr_["SURAKSHA_VALUE"] = Convert.ToDouble(dr["SURAKSHA_VALUE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["SURAKSHA_VALUE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SURAKSHA_VALUE"], "SURAKSHA_VALUE", dr["SURAKSHA_VALUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region BUDGET_BOOK

                        try
                        {
                            dr_["BUDGET_BOOK"] = dr["BUDGET_BOOK"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["BUDGET_BOOK"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["BUDGET_BOOK"], "BUDGET_BOOK", dr["BUDGET_BOOK"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region DOWNPAYMENT

                        try
                        {
                            dr_["DOWNPAYMENT"] = Convert.ToDouble(dr["DOWNPAYMENT"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["DOWNPAYMENT"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["DOWNPAYMENT"], "DOWNPAYMENT", dr["DOWNPAYMENT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region FIRST_PAYMENT

                        try
                        {
                            dr_["FIRST_PAYMENT"] = Convert.ToDouble(dr["FIRST_PAYMENT"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["FIRST_PAYMENT"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["FIRST_PAYMENT"], "FIRST_PAYMENT", dr["FIRST_PAYMENT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SALESMAN_ID

                        try
                        {
                            dr_["SALESMAN_ID"] = dr["SALESMAN_ID"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["SALESMAN_ID"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SALESMAN_ID"], "SALESMAN_ID", dr["SALESMAN_ID"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region MONTHLY_PAY

                        try
                        {
                            dr_["MONTHLY_PAY"] = Convert.ToDouble(dr["MONTHLY_PAY"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["MONTHLY_PAY"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["MONTHLY_PAY"], "MONTHLY_PAY", dr["MONTHLY_PAY"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region AGREED_TO_PAY_DATE

                        try
                        {
                            dr_["AGREED_TO_PAY_DATE"] = Convert.ToDateTime(dr["AGREED_TO_PAY_DATE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["AGREED_TO_PAY_DATE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["AGREED_TO_PAY_DATE"], "AGREED_TO_PAY_DATE", dr["AGREED_TO_PAY_DATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region PRODUCT_CODE

                        try
                        {
                            dr_["PRODUCT_CODE"] = dr["PRODUCT_CODE"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["PRODUCT_CODE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["PRODUCT_CODE"], "PRODUCT_CODE", dr["PRODUCT_CODE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region CASH_PRICE

                        try
                        {
                            dr_["CASH_PRICE"] = Convert.ToDouble(dr["CASH_PRICE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["CASH_PRICE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["CASH_PRICE"], "CASH_PRICE", dr["CASH_PRICE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region GROSS_HIRE_VALUE

                        try
                        {
                            dr_["GROSS_HIRE_VALUE"] = Convert.ToDouble(dr["GROSS_HIRE_VALUE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["GROSS_HIRE_VALUE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["GROSS_HIRE_VALUE"], "GROSS_HIRE_VALUE", dr["GROSS_HIRE_VALUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region LENGTH_OF_CONTRACT

                        try
                        {
                            dr_["LENGTH_OF_CONTRACT"] = Convert.ToInt32(dr["LENGTH_OF_CONTRACT"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["LENGTH_OF_CONTRACT"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["LENGTH_OF_CONTRACT"], "LENGTH_OF_CONTRACT", dr["LENGTH_OF_CONTRACT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        dt_.Rows.Add(dr_);

                        #endregion

                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Lopping Error - In Data Formatting Values in " + method.Name + " method", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                int_BI_WRITE_OFF_AMT_RecordCount_BI = odbConnection.Insert_NeenOpal_BI_WRITE_OFF_AMT(dt_);
                return int_BI_WRITE_OFF_AMT_RecordCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            finally
            {

            }
            return int_BI_WRITE_OFF_AMT_RecordCount_BI;
        }

        #endregion

        #region BI_ONLINE_HP_ACC

        private void BI_ONLINE_HP_ACC()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_ONLINE_HP_ACC);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_ONLINE_HP_ACC));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        this.BI_ONLINE_HP_ACC_Extraction_Area_13();
                        this.BI_ONLINE_HP_ACC_Extraction_Area_18();
                        this.BI_ONLINE_HP_ACC_Extraction_Area_1374();
                        this.BI_ONLINE_HP_ACC_Extraction_Area_13018();
                        this.BI_ONLINE_HP_ACC_Extraction_Area_17();
                        this.BI_ONLINE_HP_ACC_Extraction_Area_390();
                        this.BI_ONLINE_HP_ACC_Extraction_Area_5955();
                        this.BI_ONLINE_HP_ACC_Extraction_Area_1657();
                        this.BI_ONLINE_HP_ACC_Extraction_Area_14();
                        this.BI_ONLINE_HP_ACC_Extraction_Area_13017();
                        this.BI_ONLINE_HP_ACC_Extraction_Area_15();
                        this.BI_ONLINE_HP_ACC_Extraction_Area_16();
                        this.BI_ONLINE_HP_ACC_Extraction_Area_13016();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From " + method.Name + "  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int BI_ONLINE_HP_ACC_Extraction_Area_13()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_ONLINE_HP_ACC = odbConnection.Get_BI_ONLINE_HP_ACC_Area_13(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_ONLINE_HP_ACC.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(dt_BI_ONLINE_HP_ACC);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int BI_ONLINE_HP_ACC_Extraction_Area_18()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_ONLINE_HP_ACC = odbConnection.Get_BI_ONLINE_HP_ACC_Area_18(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_ONLINE_HP_ACC.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(dt_BI_ONLINE_HP_ACC);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int BI_ONLINE_HP_ACC_Extraction_Area_1374()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_ONLINE_HP_ACC = odbConnection.Get_BI_ONLINE_HP_ACC_Area_1374(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_ONLINE_HP_ACC.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(dt_BI_ONLINE_HP_ACC);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int BI_ONLINE_HP_ACC_Extraction_Area_13018()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_ONLINE_HP_ACC = odbConnection.Get_BI_ONLINE_HP_ACC_Area_13018(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_ONLINE_HP_ACC.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(dt_BI_ONLINE_HP_ACC);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int BI_ONLINE_HP_ACC_Extraction_Area_17()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_ONLINE_HP_ACC = odbConnection.Get_BI_ONLINE_HP_ACC_Area_17(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_ONLINE_HP_ACC.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(dt_BI_ONLINE_HP_ACC);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int BI_ONLINE_HP_ACC_Extraction_Area_390()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_ONLINE_HP_ACC = odbConnection.Get_BI_ONLINE_HP_ACC_Area_390(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_ONLINE_HP_ACC.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(dt_BI_ONLINE_HP_ACC);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int BI_ONLINE_HP_ACC_Extraction_Area_5955()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_ONLINE_HP_ACC = odbConnection.Get_BI_ONLINE_HP_ACC_Area_5955(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_ONLINE_HP_ACC.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(dt_BI_ONLINE_HP_ACC);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int BI_ONLINE_HP_ACC_Extraction_Area_1657()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_ONLINE_HP_ACC = odbConnection.Get_BI_ONLINE_HP_ACC_Area_1657(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_ONLINE_HP_ACC.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(dt_BI_ONLINE_HP_ACC);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int BI_ONLINE_HP_ACC_Extraction_Area_14()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_ONLINE_HP_ACC = odbConnection.Get_BI_ONLINE_HP_ACC_Area_14(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_ONLINE_HP_ACC.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(dt_BI_ONLINE_HP_ACC);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int BI_ONLINE_HP_ACC_Extraction_Area_13017()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_ONLINE_HP_ACC = odbConnection.Get_BI_ONLINE_HP_ACC_Area_13017(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_ONLINE_HP_ACC.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(dt_BI_ONLINE_HP_ACC);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int BI_ONLINE_HP_ACC_Extraction_Area_15()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_ONLINE_HP_ACC = odbConnection.Get_BI_ONLINE_HP_ACC_Area_15(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_ONLINE_HP_ACC.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(dt_BI_ONLINE_HP_ACC);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int BI_ONLINE_HP_ACC_Extraction_Area_16()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_ONLINE_HP_ACC = odbConnection.Get_BI_ONLINE_HP_ACC_Area_16(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_ONLINE_HP_ACC.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(dt_BI_ONLINE_HP_ACC);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int BI_ONLINE_HP_ACC_Extraction_Area_13016()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_ONLINE_HP_ACC = odbConnection.Get_BI_ONLINE_HP_ACC_Area_13016(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_ONLINE_HP_ACC.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(dt_BI_ONLINE_HP_ACC);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int NeenOpal_BI_ONLINE_HP_ACC_ProcessTo_BI_NTAN_Server(DataTable dt_BI_ONLINE_HP_ACC)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Create data table

                DataTable dt_ = new DataTable();

                DataColumn SITE_ID = new DataColumn("SITE_ID", typeof(string));
                dt_.Columns.Add(SITE_ID);

                DataColumn SALES_DATE = new DataColumn("SALES_DATE", typeof(DateTime));
                dt_.Columns.Add(SALES_DATE);

                DataColumn ACCOUNT_NO = new DataColumn("ACCOUNT_NO", typeof(string));
                dt_.Columns.Add(ACCOUNT_NO);

                DataColumn CUSTOMER_NO = new DataColumn("CUSTOMER_NO", typeof(string));
                dt_.Columns.Add(CUSTOMER_NO);

                DataColumn SALES_GROUPING = new DataColumn("SALES_GROUPING", typeof(string));
                dt_.Columns.Add(SALES_GROUPING);

                DataColumn TOTAL_RECIEVABLE = new DataColumn("TOTAL_RECIEVABLE", typeof(double));
                dt_.Columns.Add(TOTAL_RECIEVABLE);

                DataColumn SANASUMA_VALUE = new DataColumn("SANASUMA_VALUE", typeof(double));
                dt_.Columns.Add(SANASUMA_VALUE);

                DataColumn SURAKSHA_VALUE = new DataColumn("SURAKSHA_VALUE", typeof(double));
                dt_.Columns.Add(SURAKSHA_VALUE);

                DataColumn BUDGET_BOOK = new DataColumn("BUDGET_BOOK", typeof(string));
                dt_.Columns.Add(BUDGET_BOOK);

                DataColumn DOWNPAYMENT = new DataColumn("DOWNPAYMENT", typeof(double));
                dt_.Columns.Add(DOWNPAYMENT);

                DataColumn ADDITIONAL_PAYMENT = new DataColumn("ADDITIONAL_PAYMENT", typeof(double));
                dt_.Columns.Add(ADDITIONAL_PAYMENT);

                DataColumn FIRST_PAYMENT = new DataColumn("FIRST_PAYMENT", typeof(double));
                dt_.Columns.Add(FIRST_PAYMENT);

                DataColumn SALESMAN_ID = new DataColumn("SALESMAN_ID", typeof(string));
                dt_.Columns.Add(SALESMAN_ID);

                DataColumn MONTHLY_PAY = new DataColumn("MONTHLY_PAY", typeof(double));
                dt_.Columns.Add(MONTHLY_PAY);

                DataColumn AGREED_TO_PAY_DATE = new DataColumn("AGREED_TO_PAY_DATE", typeof(DateTime));
                dt_.Columns.Add(AGREED_TO_PAY_DATE);

                DataColumn PRODUCT_CODE = new DataColumn("PRODUCT_CODE", typeof(string));
                dt_.Columns.Add(PRODUCT_CODE);

                DataColumn CASH_PRICE = new DataColumn("CASH_PRICE", typeof(double));
                dt_.Columns.Add(CASH_PRICE);

                DataColumn GROSS_HIRE_VALUE = new DataColumn("GROSS_HIRE_VALUE", typeof(double));
                dt_.Columns.Add(GROSS_HIRE_VALUE);

                DataColumn LENGTH_OF_CONTRACT = new DataColumn("LENGTH_OF_CONTRACT", typeof(int));
                dt_.Columns.Add(LENGTH_OF_CONTRACT);

                DataColumn CUSTOMER_ARREARS = new DataColumn("CUSTOMER_ARREARS", typeof(double));
                dt_.Columns.Add(CUSTOMER_ARREARS);

                #endregion

                if (dt_BI_ONLINE_HP_ACC == null)
                {
                    return 0;
                }

                foreach (DataRow dr in dt_BI_ONLINE_HP_ACC.Rows)
                {

                    DataRow dr_ = dt_.NewRow();

                    try
                    {

                        #region Format variable values and add to table

                        #region SITE_ID

                        try
                        {
                            dr_["SITE_ID"] = dr["SITE_ID"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["SITE_ID"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SITE_ID"], "SITE_ID", dr["SITE_ID"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SALES_DATE

                        try
                        {
                            dr_["SALES_DATE"] = Convert.ToDateTime(dr["SALES_DATE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["SALES_DATE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SALES_DATE"], "SALES_DATE", dr["SALES_DATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region ACCOUNT_NO

                        try
                        {
                            dr_["ACCOUNT_NO"] = dr["ACCOUNT_NO"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["ACCOUNT_NO"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["ACCOUNT_NO"], "ACCOUNT_NO", dr["ACCOUNT_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region CUSTOMER_NO

                        try
                        {
                            dr_["CUSTOMER_NO"] = dr["CUSTOMER_NO"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["CUSTOMER_NO"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["CUSTOMER_NO"], "CUSTOMER_NO", dr["CUSTOMER_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SALES_GROUPING

                        try
                        {
                            dr_["SALES_GROUPING"] = dr["SALES_GROUPING"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["SALES_GROUPING"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SALES_GROUPING"], "SALES_GROUPING", dr["SALES_GROUPING"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region TOTAL_RECIEVABLE

                        try
                        {
                            dr_["TOTAL_RECIEVABLE"] = Convert.ToDouble(dr["TOTAL_RECIEVABLE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["TOTAL_RECIEVABLE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["TOTAL_RECIEVABLE"], "TOTAL_RECIEVABLE", dr["TOTAL_RECIEVABLE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SANASUMA_VALUE

                        try
                        {
                            dr_["SANASUMA_VALUE"] = Convert.ToDouble(dr["SANASUMA_VALUE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["SANASUMA_VALUE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SANASUMA_VALUE"], "SANASUMA_VALUE", dr["SANASUMA_VALUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SURAKSHA_VALUE

                        try
                        {
                            dr_["SURAKSHA_VALUE"] = Convert.ToDouble(dr["SURAKSHA_VALUE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["SURAKSHA_VALUE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SURAKSHA_VALUE"], "SURAKSHA_VALUE", dr["SURAKSHA_VALUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region BUDGET_BOOK

                        try
                        {
                            dr_["BUDGET_BOOK"] = dr["BUDGET_BOOK"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["BUDGET_BOOK"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["BUDGET_BOOK"], "BUDGET_BOOK", dr["BUDGET_BOOK"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region DOWNPAYMENT

                        try
                        {
                            dr_["DOWNPAYMENT"] = Convert.ToDouble(dr["DOWNPAYMENT"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["DOWNPAYMENT"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["DOWNPAYMENT"], "DOWNPAYMENT", dr["DOWNPAYMENT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region ADDITIONAL_PAYMENT

                        try
                        {
                            dr_["ADDITIONAL_PAYMENT"] = Convert.ToDouble(dr["ADDITIONAL_PAYMENT"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["ADDITIONAL_PAYMENT"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["ADDITIONAL_PAYMENT"], "ADDITIONAL_PAYMENT", dr["ADDITIONAL_PAYMENT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region FIRST_PAYMENT

                        try
                        {
                            dr_["FIRST_PAYMENT"] = Convert.ToDouble(dr["FIRST_PAYMENT"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["FIRST_PAYMENT"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["FIRST_PAYMENT"], "FIRST_PAYMENT", dr["FIRST_PAYMENT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SALESMAN_ID

                        try
                        {
                            dr_["SALESMAN_ID"] = dr["SALESMAN_ID"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["SALESMAN_ID"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SALESMAN_ID"], "SALESMAN_ID", dr["SALESMAN_ID"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region MONTHLY_PAY

                        try
                        {
                            dr_["MONTHLY_PAY"] = Convert.ToDouble(dr["MONTHLY_PAY"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["MONTHLY_PAY"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["MONTHLY_PAY"], "MONTHLY_PAY", dr["MONTHLY_PAY"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region AGREED_TO_PAY_DATE

                        try
                        {
                            dr_["AGREED_TO_PAY_DATE"] = Convert.ToDateTime(dr["AGREED_TO_PAY_DATE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["AGREED_TO_PAY_DATE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["AGREED_TO_PAY_DATE"], "AGREED_TO_PAY_DATE", dr["AGREED_TO_PAY_DATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region PRODUCT_CODE

                        try
                        {
                            dr_["PRODUCT_CODE"] = dr["PRODUCT_CODE"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["PRODUCT_CODE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["PRODUCT_CODE"], "PRODUCT_CODE", dr["PRODUCT_CODE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region CASH_PRICE

                        try
                        {
                            dr_["CASH_PRICE"] = Convert.ToDouble(dr["CASH_PRICE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["CASH_PRICE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["CASH_PRICE"], "CASH_PRICE", dr["CASH_PRICE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region GROSS_HIRE_VALUE

                        try
                        {
                            dr_["GROSS_HIRE_VALUE"] = Convert.ToDouble(dr["GROSS_HIRE_VALUE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["GROSS_HIRE_VALUE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["GROSS_HIRE_VALUE"], "GROSS_HIRE_VALUE", dr["GROSS_HIRE_VALUE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region LENGTH_OF_CONTRACT

                        try
                        {
                            dr_["LENGTH_OF_CONTRACT"] = Convert.ToInt32(dr["LENGTH_OF_CONTRACT"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["LENGTH_OF_CONTRACT"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["LENGTH_OF_CONTRACT"], "LENGTH_OF_CONTRACT", dr["LENGTH_OF_CONTRACT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region CUSTOMER_ARREARS

                        try
                        {
                            dr_["CUSTOMER_ARREARS"] = Convert.ToDouble(dr["CUSTOMER_ARREARS"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["CUSTOMER_ARREARS"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["CUSTOMER_ARREARS"], "CUSTOMER_ARREARS", dr["CUSTOMER_ARREARS"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        dt_.Rows.Add(dr_);

                        #endregion

                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Lopping Error - In Data Formatting Values in " + method.Name + " method", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                int_BI_ONLINE_HP_ACC_RecordCount_BI = odbConnection.Insert_NeenOpal_BI_ONLINE_HP_ACC(dt_);
                return int_BI_ONLINE_HP_ACC_RecordCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            finally
            {

            }
            return int_BI_ONLINE_HP_ACC_RecordCount_BI;
        }

        #endregion

        #region BI_BLACKLIST_CUST

        private void BI_BLACKLIST_CUST()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_BLACKLIST_CUST);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_BLACKLIST_CUST));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        this.BI_BLACKLIST_CUST_Extraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From " + method.Name + "  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int BI_BLACKLIST_CUST_Extraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_BLACKLIST_CUST = odbConnection.Get_BI_BLACKLIST_CUST(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_BLACKLIST_CUST.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_BLACKLIST_CUST_ProcessTo_BI_NTAN_Server(dt_BI_BLACKLIST_CUST);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int NeenOpal_BI_BLACKLIST_CUST_ProcessTo_BI_NTAN_Server(DataTable dt_BI_BLACKLIST_CUST)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Create data table

                DataTable dt_ = new DataTable();

                DataColumn CUSTOMER_ID = new DataColumn("CUSTOMER_ID", typeof(string));
                dt_.Columns.Add(CUSTOMER_ID);

                #endregion

                if (dt_BI_BLACKLIST_CUST == null)
                {
                    return 0;
                }

                foreach (DataRow dr in dt_BI_BLACKLIST_CUST.Rows)
                {

                    DataRow dr_ = dt_.NewRow();

                    try
                    {

                        #region Format variable values and add to table

                        #region CUSTOMER_ID

                        try
                        {
                            dr_["CUSTOMER_ID"] = dr["CUSTOMER_ID"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["CUSTOMER_ID"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["CUSTOMER_ID"], "CUSTOMER_ID", dr["CUSTOMER_ID"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        dt_.Rows.Add(dr_);

                        #endregion

                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Lopping Error - In Data Formatting Values in " + method.Name + " method", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                int_BI_BLACKLIST_CUST_RecordCount_BI = odbConnection.Insert_NeenOpal_BI_BLACKLIST_CUST(dt_);
                return int_BI_BLACKLIST_CUST_RecordCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            finally
            {

            }
            return int_BI_BLACKLIST_CUST_RecordCount_BI;
        }

        #endregion

        #region BI_BR_MGR_INCEN

        private void BI_BR_MGR_INCEN()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_BR_MGR_INCEN);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    //this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_BR_MGR_INCEN));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        this.BI_BR_MGR_INCEN_Extraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From " + method.Name + "  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int BI_BR_MGR_INCEN_Extraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_BR_MGR_INCEN = odbConnection.Get_BI_BR_MGR_INCEN(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_BR_MGR_INCEN.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_BR_MGR_INCEN_ProcessTo_BI_NTAN_Server(dt_BI_BR_MGR_INCEN);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int NeenOpal_BI_BR_MGR_INCEN_ProcessTo_BI_NTAN_Server(DataTable dt_BI_BR_MGR_INCEN)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Create data table

                DataTable dt_ = new DataTable();

                DataColumn COMPANY = new DataColumn("COMPANY", typeof(string));
                dt_.Columns.Add(COMPANY);

                DataColumn EXP_STATEMENT_ID = new DataColumn("EXP_STATEMENT_ID", typeof(int));
                dt_.Columns.Add(EXP_STATEMENT_ID);

                DataColumn EXP_LUMP_SUM_TRANS_ID = new DataColumn("EXP_LUMP_SUM_TRANS_ID", typeof(int));
                dt_.Columns.Add(EXP_LUMP_SUM_TRANS_ID);

                DataColumn EXP_STATEMENT_NO = new DataColumn("EXP_STATEMENT_NO", typeof(string));
                dt_.Columns.Add(EXP_STATEMENT_NO);

                DataColumn VOUCHER_DATE = new DataColumn("VOUCHER_DATE", typeof(DateTime));
                dt_.Columns.Add(VOUCHER_DATE);

                DataColumn EXPENSE_CODE = new DataColumn("EXPENSE_CODE", typeof(string));
                dt_.Columns.Add(EXPENSE_CODE);

                DataColumn EXPENSE_NAME = new DataColumn("EXPENSE_NAME", typeof(string));
                dt_.Columns.Add(EXPENSE_NAME);

                DataColumn SHOP_ID = new DataColumn("SHOP_ID", typeof(string));
                dt_.Columns.Add(SHOP_ID);

                DataColumn VOUCHER_NO = new DataColumn("VOUCHER_NO", typeof(int));
                dt_.Columns.Add(VOUCHER_NO);

                DataColumn SALESMAN = new DataColumn("SALESMAN", typeof(string));
                dt_.Columns.Add(SALESMAN);

                DataColumn INVOICE_NO = new DataColumn("INVOICE_NO", typeof(string));
                dt_.Columns.Add(INVOICE_NO);

                DataColumn AMOUNT = new DataColumn("AMOUNT", typeof(double));
                dt_.Columns.Add(AMOUNT);

                #endregion

                if (dt_BI_BR_MGR_INCEN == null)
                {
                    return 0;
                }

                foreach (DataRow dr in dt_BI_BR_MGR_INCEN.Rows)
                {

                    DataRow dr_ = dt_.NewRow();

                    try
                    {

                        #region Format variable values and add to table

                        #region COMPANY

                        try
                        {
                            dr_["COMPANY"] = dr["COMPANY"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["COMPANY"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["COMPANY"], "COMPANY", dr["COMPANY"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region EXP_STATEMENT_ID

                        try
                        {
                            dr_["EXP_STATEMENT_ID"] = Convert.ToInt32(dr["EXP_STATEMENT_ID"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["EXP_STATEMENT_ID"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["EXP_STATEMENT_ID"], "EXP_STATEMENT_ID", dr["EXP_STATEMENT_ID"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region EXP_LUMP_SUM_TRANS_ID

                        try
                        {
                            dr_["EXP_LUMP_SUM_TRANS_ID"] = Convert.ToInt32(dr["EXP_LUMP_SUM_TRANS_ID"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["EXP_LUMP_SUM_TRANS_ID"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["EXP_LUMP_SUM_TRANS_ID"], "EXP_LUMP_SUM_TRANS_ID", dr["EXP_LUMP_SUM_TRANS_ID"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region EXP_STATEMENT_NO

                        try
                        {
                            dr_["EXP_STATEMENT_NO"] = dr["EXP_STATEMENT_NO"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["EXP_STATEMENT_NO"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["EXP_STATEMENT_NO"], "EXP_STATEMENT_NO", dr["EXP_STATEMENT_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region VOUCHER_DATE

                        try
                        {
                            dr_["VOUCHER_DATE"] = Convert.ToDateTime(dr["VOUCHER_DATE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["VOUCHER_DATE"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["VOUCHER_DATE"], "VOUCHER_DATE", dr["VOUCHER_DATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region EXPENSE_CODE

                        try
                        {
                            dr_["EXPENSE_CODE"] = dr["EXPENSE_CODE"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["EXPENSE_CODE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["EXPENSE_CODE"], "EXPENSE_CODE", dr["EXPENSE_CODE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region EXPENSE_NAME

                        try
                        {
                            dr_["EXPENSE_NAME"] = dr["EXPENSE_NAME"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["EXPENSE_NAME"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["EXPENSE_NAME"], "EXPENSE_NAME", dr["EXPENSE_NAME"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SHOP_ID

                        try
                        {
                            dr_["SHOP_ID"] = dr["SHOP_ID"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["SHOP_ID"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SHOP_ID"], "SHOP_ID", dr["SHOP_ID"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region VOUCHER_NO

                        try
                        {
                            dr_["VOUCHER_NO"] = Convert.ToInt32(dr["VOUCHER_NO"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["VOUCHER_NO"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["VOUCHER_NO"], "VOUCHER_NO", dr["VOUCHER_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SALESMAN

                        try
                        {
                            dr_["SALESMAN"] = dr["SALESMAN"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["SALESMAN"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SALESMAN"], "SALESMAN", dr["SALESMAN"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region INVOICE_NO

                        try
                        {
                            dr_["INVOICE_NO"] = dr["INVOICE_NO"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["INVOICE_NO"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["INVOICE_NO"], "INVOICE_NO", dr["INVOICE_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region AMOUNT

                        try
                        {
                            dr_["AMOUNT"] = Convert.ToDouble(dr["AMOUNT"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["AMOUNT"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["AMOUNT"], "AMOUNT", dr["AMOUNT"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        dt_.Rows.Add(dr_);

                        #endregion

                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Lopping Error - In Data Formatting Values in " + method.Name + " method", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                int_BI_BR_MGR_INCEN_RecordCount_BI = odbConnection.Insert_NeenOpal_BI_BR_MGR_INCEN(dt_);
                return int_BI_BR_MGR_INCEN_RecordCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            finally
            {

            }
            return int_BI_BR_MGR_INCEN_RecordCount_BI;
        }

        #endregion

        #region BI_RESALE_VAL_ITEM

        private void BI_RESALE_VAL_ITEM()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_RESALE_VAL_ITEM);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    //this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_RESALE_VAL_ITEM));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        this.BI_RESALE_VAL_ITEM_Extraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From " + method.Name + "  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int BI_RESALE_VAL_ITEM_Extraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_RESALE_VAL_ITEM = odbConnection.Get_BI_RESALE_VAL_ITEM(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_RESALE_VAL_ITEM.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_RESALE_VAL_ITEM_ProcessTo_BI_NTAN_Server(dt_BI_RESALE_VAL_ITEM);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int NeenOpal_BI_RESALE_VAL_ITEM_ProcessTo_BI_NTAN_Server(DataTable dt_BI_RESALE_VAL_ITEM)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Create data table

                DataTable dt_ = new DataTable();

                DataColumn LINE_INDEX = new DataColumn("LINE_INDEX", typeof(int));
                dt_.Columns.Add(LINE_INDEX);

                DataColumn RESALE_DATE = new DataColumn("RESALE_DATE", typeof(DateTime));
                dt_.Columns.Add(RESALE_DATE);

                DataColumn AUTHORIZED_USER = new DataColumn("AUTHORIZED_USER", typeof(string));
                dt_.Columns.Add(AUTHORIZED_USER);

                DataColumn AUTHORIZED_BY = new DataColumn("AUTHORIZED_BY", typeof(string));
                dt_.Columns.Add(AUTHORIZED_BY);

                DataColumn SHOP_CODE = new DataColumn("SHOP_CODE", typeof(string));
                dt_.Columns.Add(SHOP_CODE);

                DataColumn ACCOUNT_NO = new DataColumn("ACCOUNT_NO", typeof(string));
                dt_.Columns.Add(ACCOUNT_NO);

                DataColumn RESALE_PRICE = new DataColumn("RESALE_PRICE", typeof(double));
                dt_.Columns.Add(RESALE_PRICE);

                DataColumn PART_NO = new DataColumn("PART_NO", typeof(string));
                dt_.Columns.Add(PART_NO);

                #endregion

                if (dt_BI_RESALE_VAL_ITEM == null)
                {
                    return 0;
                }

                foreach (DataRow dr in dt_BI_RESALE_VAL_ITEM.Rows)
                {

                    DataRow dr_ = dt_.NewRow();

                    try
                    {

                        #region Format variable values and add to table

                        #region LINE_INDEX

                        try
                        {
                            dr_["LINE_INDEX"] = Convert.ToInt32(dr["LINE_INDEX"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["LINE_INDEX"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["LINE_INDEX"], "LINE_INDEX", dr["LINE_INDEX"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region RESALE_DATE

                        try
                        {
                            dr_["RESALE_DATE"] = Convert.ToDateTime(dr["RESALE_DATE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["RESALE_DATE"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["RESALE_DATE"], "RESALE_DATE", dr["RESALE_DATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region AUTHORIZED_USER

                        try
                        {
                            dr_["AUTHORIZED_USER"] = dr["AUTHORIZED_USER"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["AUTHORIZED_USER"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["AUTHORIZED_USER"], "AUTHORIZED_USER", dr["AUTHORIZED_USER"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region AUTHORIZED_BY

                        try
                        {
                            dr_["AUTHORIZED_BY"] = dr["AUTHORIZED_BY"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["AUTHORIZED_BY"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["AUTHORIZED_BY"], "AUTHORIZED_BY", dr["AUTHORIZED_BY"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SHOP_CODE

                        try
                        {
                            dr_["SHOP_CODE"] = dr["SHOP_CODE"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["SHOP_CODE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SHOP_CODE"], "SHOP_CODE", dr["SHOP_CODE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region ACCOUNT_NO

                        try
                        {
                            dr_["ACCOUNT_NO"] = dr["ACCOUNT_NO"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["ACCOUNT_NO"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["ACCOUNT_NO"], "ACCOUNT_NO", dr["ACCOUNT_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region RESALE_PRICE

                        try
                        {
                            dr_["RESALE_PRICE"] = Convert.ToDouble(dr["RESALE_PRICE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["RESALE_PRICE"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["RESALE_PRICE"], "RESALE_PRICE", dr["RESALE_PRICE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region PART_NO

                        try
                        {
                            dr_["PART_NO"] = dr["PART_NO"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["PART_NO"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["PART_NO"], "PART_NO", dr["PART_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        dt_.Rows.Add(dr_);

                        #endregion

                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Lopping Error - In Data Formatting Values in " + method.Name + " method", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                int_BI_RESALE_VAL_ITEM_RecordCount_BI = odbConnection.Insert_NeenOpal_BI_RESALE_VAL_ITEM(dt_);
                return int_BI_RESALE_VAL_ITEM_RecordCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            finally
            {

            }
            return int_BI_RESALE_VAL_ITEM_RecordCount_BI;
        }

        #endregion

        #region BI_GUARANTER_INFO

        private void BI_GUARANTER_INFO()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            DataRow drPassTableID = this.LoadMethodRelatedTableID(BIAdminControlETITableTypes.BI_GUARANTER_INFO);

            if (drPassTableID != null)
            {
                PassTableID = Convert.ToInt32(drPassTableID["TABLEID"]);

                if (this.IsTableRecordInExtractionLogFileAlreadyNotExsists(PassTableID, dtPreviousDate_BI) == true)
                {
                    //this.DeleteRecordsIn_BI_NRTAN_ServerTables(dtPreviousDate_BI, Convert.ToInt32(BIAdminControlETITableTypes.BI_GUARANTER_INFO));

                    bool IsAdminLogInsert = this.Insert_BI_NTAN_ProcessToExtractionLog(PassTableID, 0, dtPreviousDate_BI, Convert.ToInt32(ExtrationLogStatus.Data_Loading_For_Corresponding_Tables_In_Progress), Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), strUserName, strMachineName);
                    if (IsAdminLogInsert == true)
                    {
                        this.BI_GUARANTER_INFO_Extraction();

                        bool isFinalSave = this.EndFlagInBIAdminExtractionLog(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));
                    }
                }
                else
                {
                    LoggingHelper.WriteErrorToFile("Process Already Run For the Date", "Return From " + method.Name + "  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }
            }
        }

        private int BI_GUARANTER_INFO_Extraction()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();

            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dt_BI_GUARANTER_INFO = odbConnection.Get_BI_GUARANTER_INFO(dtPreviousDate_BI);
            LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + "() method Get Data Form DB ", " DAL Table Count : " + dt_BI_GUARANTER_INFO.Rows.Count.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            effectedRecCount_BI = this.NeenOpal_BI_GUARANTER_INFO_ProcessTo_BI_NTAN_Server(dt_BI_GUARANTER_INFO);
            bool isUpdated = this.UpdateBIAdminETLExtractionLogFile(PassTableID, dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), effectedRecCount_BI);
            LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Formatted and Inserted  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            return effectedRecCount_BI;

        }

        private int NeenOpal_BI_GUARANTER_INFO_ProcessTo_BI_NTAN_Server(DataTable dt_BI_GUARANTER_INFO)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                #region Create data table

                DataTable dt_ = new DataTable();

                DataColumn ACCOUNT_NO = new DataColumn("ACCOUNT_NO", typeof(string));
                dt_.Columns.Add(ACCOUNT_NO);

                DataColumn CUSTOMER_NO = new DataColumn("CUSTOMER_NO", typeof(string));
                dt_.Columns.Add(CUSTOMER_NO);

                DataColumn SALES_DATE = new DataColumn("SALES_DATE", typeof(DateTime));
                dt_.Columns.Add(SALES_DATE);

                DataColumn GUARANTOR1 = new DataColumn("GUARANTOR1", typeof(string));
                dt_.Columns.Add(GUARANTOR1);

                DataColumn GUARANTOR2 = new DataColumn("GUARANTOR2", typeof(string));
                dt_.Columns.Add(GUARANTOR2);

                #endregion

                if (dt_BI_GUARANTER_INFO == null)
                {
                    return 0;
                }

                foreach (DataRow dr in dt_BI_GUARANTER_INFO.Rows)
                {

                    DataRow dr_ = dt_.NewRow();

                    try
                    {

                        #region Format variable values and add to table

                        #region ACCOUNT_NO

                        try
                        {
                            dr_["ACCOUNT_NO"] = dr["ACCOUNT_NO"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["ACCOUNT_NO"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["ACCOUNT_NO"], "ACCOUNT_NO", dr["ACCOUNT_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region CUSTOMER_NO

                        try
                        {
                            dr_["CUSTOMER_NO"] = dr["CUSTOMER_NO"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["CUSTOMER_NO"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["CUSTOMER_NO"], "CUSTOMER_NO", dr["CUSTOMER_NO"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region SALES_DATE

                        try
                        {
                            dr_["SALES_DATE"] = Convert.ToDateTime(dr["SALES_DATE"].ToString());
                        }
                        catch (Exception ex)
                        {
                            dr_["SALES_DATE"] = (object)DBNull.Value;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["SALES_DATE"], "SALES_DATE", dr["SALES_DATE"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region GUARANTOR1

                        try
                        {
                            dr_["GUARANTOR1"] = dr["GUARANTOR1"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["GUARANTOR1"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["GUARANTOR1"], "GUARANTOR1", dr["GUARANTOR1"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region GUARANTOR2

                        try
                        {
                            dr_["GUARANTOR2"] = dr["GUARANTOR2"].ToString();
                        }
                        catch (Exception ex)
                        {
                            dr_["GUARANTOR2"] = (object)DBNull.Value;
                            //LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, dr_["GUARANTOR2"], "GUARANTOR2", dr["GUARANTOR2"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        dt_.Rows.Add(dr_);

                        #endregion

                    }
                    catch (Exception ee)
                    {
                        string row = "";
                        for (int j = 0; j < dr.ItemArray.Length; j++)
                        {
                            row += dr.ItemArray[j] + ", ";
                        }
                        LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} -{2} *****", ee.Message, row, ee.StackTrace), "For Lopping Error - In Data Formatting Values in " + method.Name + " method", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                    }
                }

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                int_BI_GUARANTER_INFO_RecordCount_BI = odbConnection.Insert_NeenOpal_BI_GUARANTER_INFO(dt_);
                return int_BI_GUARANTER_INFO_RecordCount_BI;

            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - Last Final A", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            finally
            {

            }
            return int_BI_GUARANTER_INFO_RecordCount_BI;
        }

        #endregion

        #endregion


        #endregion

        #region Master Table Update Methods

        #region Product Master Update

        private void ProductMasterDetailsUpdateProcess()
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();
                LoggingHelper.WriteErrorToFile("----Start Note :-  " + method.Name + " () method Start ", "   ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                DataTable dtChangedProducts = this.GetDailyIFSSystemChangeProductDetails();
                effectedRecCount_BI = ProductUpdateProcessToBI_NRTANServer(dtChangedProducts);
                LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Updated to DB ", " Updated  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - ProductDetailsUpdateProcess ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            bool isFinalSave = this.EndFlagInBIAdminExtractionLog(Convert.ToInt32(BIAdminControlETITableTypes.BI_Product), dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));

        }

        private void BI_DIM_PRODUCT_IV_DetailsUpdateProcess()
        {
            
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();
                LoggingHelper.WriteErrorToFile("----Start Note :-  " + method.Name + " () method Start ", "   ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                DataTable dtChangedProducts = this.Get_BI_DIM_PRODUCT_IV_IFSSystemChangeProductDetails();
                effectedRecCount_BI = BI_DIM_PRODUCT_IV_UpdateProcessToBI_NRTANServer(dtChangedProducts);
                LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Updated to DB ", " Updated  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - ProductDetailsUpdateProcess ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            bool isFinalSave = this.EndFlagInBIAdminExtractionLog(Convert.ToInt32(BIAdminControlETITableTypes.BI_DIM_PRODUCT_IV), dtPreviousDate_BI, Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now)), Convert.ToInt32(ExtrationLogStatus.Data_Loadinig_Completed));

        }

        private DataTable GetDailyIFSSystemChangeProductDetails()
        {
            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtProduct = odbConnection.GetIFSSystemChangedProductCodes(dtPreviousDate_BI);

            return dtProduct;
        }

        private DataTable Get_BI_DIM_PRODUCT_IV_IFSSystemChangeProductDetails()
        {
            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtProduct = odbConnection.Get_BI_DIM_PRODUCT_IV_Changes(dtPreviousDate_BI);

            return dtProduct;
        }
        private int BI_DIM_PRODUCT_IV_UpdateProcessToBI_NRTANServer(DataTable dtSINUpdatedProducts)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                int intUpdatedCount = 0;

                if (dtSINUpdatedProducts != null && dtSINUpdatedProducts.Rows.Count > 0)
                {

                    foreach (DataRow dr in dtSINUpdatedProducts.Rows)
                    {
                        Product objProduct = new Product();
                        objProduct.PRODUCT_CODE = dr["PRODUCT_CODE"].ToString();

                        #region INCREMENTAL_VOLUMN

                        try
                        {
                            objProduct.INCREMENTAL_VOLUMN = Convert.ToDouble(dr["INCREMENTAL_VOLUMN"]);
                        }
                        catch (Exception ex)
                        {

                            objProduct.INCREMENTAL_VOLUMN = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, objProduct.INCREMENTAL_VOLUMN, "INCREMENTAL_VOLUMN", dr["INCREMENTAL_VOLUMN"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        objProduct.BASICTEC1 = dr["BASICTEC1"].ToString();

                        objProduct.BASICTEC2 = dr["BASICTEC2"].ToString();

                        objProduct.BASICTEC3 = dr["BASICTEC3"].ToString();

                        objProduct.BASICTEC4 = dr["BASICTEC4"].ToString();

                        objProduct.BASICTEC5 = dr["BASICTEC5"].ToString();

                        objProduct.COLOURCODE = dr["COLOURCODE"].ToString();

                        objProduct.BASICTEC6 = dr["BASICTEC6"].ToString();
                        
                       

                        bool isNewRecordAdded = this.Check_BI_DIM_PRODUCT_VI_CodeExsistInRTANAndContinueProductUpdateProcess(objProduct);
                        if (isNewRecordAdded == true)
                        {
                            intUpdatedCount = intUpdatedCount + 1;
                        }
                    }
                }
                else
                {
                    return 0;
                }

                effectedRecCount_BI = intUpdatedCount;
                return effectedRecCount_BI;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - ProductUpdateProcessToBI_NRTANServer ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }

            return effectedRecCount_BI;
        }

        private int ProductUpdateProcessToBI_NRTANServer(DataTable dtSINUpdatedProducts)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                int intUpdatedCount = 0;

                if (dtSINUpdatedProducts != null && dtSINUpdatedProducts.Rows.Count > 0)
                {

                    foreach (DataRow dr in dtSINUpdatedProducts.Rows)
                    {
                        Product objProduct = new Product();
                        objProduct.PRODUCT_CODE = dr["PRODUCT_CODE"].ToString();
                        
                        objProduct.PRODUCT_DESC = dr["PRODUCT_DESC"].ToString();

                        objProduct.ACCOUNTING_GROUP_ID = dr["ACCOUNTING_GROUP_ID"].ToString();

                        objProduct.ACCOUNTING_GROUP_DESC = dr["ACCOUNTING_GROUP_DESC"].ToString();

                        objProduct.PRODUCT_FAMILY_ID = dr["PRODUCT_FAMILY_ID"].ToString();

                        objProduct.PRODUCT_FAMILY_DESC = dr["PRODUCT_FAMILY_DESC"].ToString();

                        objProduct.COMMODITY_ID = dr["COMMODITY_ID"].ToString();

                        objProduct.COMMODITY_DESC = dr["COMMODITY_DESC"].ToString();

                        #region BRAND_ID
                        if (dr["BRAND_ID"].ToString() == "")
                        {
                            objProduct.BRAND_ID = "";
                        }
                        else
                        {
                            objProduct.BRAND_ID = dr["BRAND_ID"].ToString();
                        }
                        #endregion

                        #region BRAND_DESC
                        if (dr["BRAND_DESC"].ToString() == "")
                        {
                            objProduct.BRAND_DESC = "";
                        }
                        else
                        {
                            objProduct.BRAND_DESC = dr["BRAND_DESC"].ToString();
                        }
                        #endregion

                        objProduct.PART_STATUS = dr["PART_STATUS"].ToString();

                        #region CBM

                        Double CBM = 0;
                        try
                        {
                            objProduct.CBM = Convert.ToDouble(dr["CBM"]);
                        }
                        catch (Exception ex)
                        {

                            objProduct.CBM = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, CBM, "CBM", dr["CBM"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }

                        #endregion

                        #region PRODUCT_ATTRIBUTE_1
                        if (dr["PRODUCT_ATTRIBUTE_1"].ToString() == "")
                        {
                            objProduct.PRODUCT_ATTRIBUTE_1 = "";
                        }
                        else
                        {
                            objProduct.PRODUCT_ATTRIBUTE_1 = dr["PRODUCT_ATTRIBUTE_1"].ToString();
                        }
                        #endregion

                        #region PROD_ATTRIBUTE2
                        if (dr["PRODUCT_ATTRIBUTE_2"].ToString() == "")
                        {
                            objProduct.PRODUCT_ATTRIBUTE_2 = "";
                        }
                        else
                        {
                            objProduct.PRODUCT_ATTRIBUTE_2 = dr["PRODUCT_ATTRIBUTE_2"].ToString();
                        }
                        #endregion

                        #region PRODUCT_ATTRIBUTE_3
                        if (dr["PRODUCT_ATTRIBUTE_3"].ToString() == "")
                        {
                            objProduct.PRODUCT_ATTRIBUTE_3 = "";
                        }
                        else
                        {
                            objProduct.PRODUCT_ATTRIBUTE_3 = dr["PRODUCT_ATTRIBUTE_3"].ToString();
                        }
                        #endregion

                        #region PRODUCT_ATTRIBUTE_4
                        if (dr["PRODUCT_ATTRIBUTE_4"].ToString() == "")
                        {
                            objProduct.PRODUCT_ATTRIBUTE_4 = "";
                        }
                        else
                        {
                            objProduct.PRODUCT_ATTRIBUTE_4 = dr["PRODUCT_ATTRIBUTE_4"].ToString();
                        }
                        #endregion

                        #region PRODUCT_ATTRIBUTE_5
                        if (dr["PRODUCT_ATTRIBUTE_5"].ToString() == "")
                        {
                            objProduct.PRODUCT_ATTRIBUTE_5 = "";
                        }
                        else
                        {
                            objProduct.PRODUCT_ATTRIBUTE_5 = dr["PRODUCT_ATTRIBUTE_5"].ToString();
                        }
                        #endregion
                        
                        #region SALES_PRICE
                        Double Sales_Price = 0;
                        try
                        {
                            objProduct.Sales_Price = Convert.ToDouble(dr["SALES_PRICE"]);
                        }
                        catch (Exception ex)
                        {

                            objProduct.Sales_Price = 0;
                            LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, Sales_Price, "Sales_Price", dr["Sales_Price"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                        }
                        #endregion

                        bool isNewRecordAdded = this.CheckProductCodeExsistInRTANAndContinueProductUpdateProcess(objProduct);
                        if (isNewRecordAdded == true)
                        {
                            intUpdatedCount = intUpdatedCount + 1;
                        }
                    }
                }
                else
                {
                    return 0;
                }

                effectedRecCount_BI = intUpdatedCount;
                return effectedRecCount_BI;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - ProductUpdateProcessToBI_NRTANServer ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }

            return effectedRecCount_BI;
        }

        private bool CheckProductCodeExsistInRTANAndContinueProductUpdateProcess(Product objProduct)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckProductCodeExsistInBI_NRTANAndContinueProductUpdateProcess(objProduct);
        }

        private bool Check_BI_DIM_PRODUCT_VI_CodeExsistInRTANAndContinueProductUpdateProcess(Product objProduct)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.Check_BI_DIM_PRODUCT_VI_ExsistInBI_NRTANAndContinueProductUpdateProcess(objProduct);
        }
        
        #endregion

        #region Store Master Square Feet Update
        private void StoreMasterSquareFeetUpdateProcess()
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();
                LoggingHelper.WriteErrorToFile("----Start Note :-  " + method.Name + " () method Start ", "   ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                DataTable dtRENTShopCodes = this.GetRentSystemUpdateShopCodes();
                effectedRecCount_BI = StoreSquareFeetUpdateProcessToBI_NRTANerver(dtRENTShopCodes);
                LoggingHelper.WriteErrorToFile("----End Note :- " + method.Name + " () method Formatted Data Updated to DB ", " Updated  Count : " + effectedRecCount_BI.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - StoreMasterSquareFeetUpdateProcess ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
        }

        private DataTable GetRentSystemUpdateShopCodes()
        {
            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtRentShops = odbConnection.GetRentSystemUpdateShopCodes(dtPreviousDate_BI);

            return dtRentShops;
        }

        private int StoreSquareFeetUpdateProcessToBI_NRTANerver(DataTable dtRENTShopCodes)
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                int intUpdatedCount = 0;

                if (dtRENTShopCodes != null && dtRENTShopCodes.Rows.Count > 0)
                {

                    foreach (DataRow dr in dtRENTShopCodes.Rows)
                    {
                        Store objStore = new Store();
                        objStore.SITE_ID = dr["STORE_CODE"].ToString();
                        objStore.CAPACITIES = dr["TotalSquareFeet"].ToString();

                        bool isNewRecordAdded = this.CheckRENTStoreCodeExsistInBI_NRTANAndContinueProductUpdateProcess(objStore);
                        if (isNewRecordAdded == true)
                        {
                            intUpdatedCount = intUpdatedCount + 1;
                        }
                    }
                }
                else
                {
                    return 0;
                }

                effectedRecCount_BI = intUpdatedCount;
                return effectedRecCount_BI;
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - StoreSquareFeetUpdateProcessToBI_NRTANerver ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }

            return effectedRecCount_BI;
        }

        private bool CheckRENTStoreCodeExsistInBI_NRTANAndContinueProductUpdateProcess(Store objStore)
        {
            NeenOpal_BI_DBAccess odbConnectionNew = new NeenOpal_BI_DBAccess();
            return odbConnectionNew.CheckRENTStoreCodeExsistInBI_NRTANAndContinueProductUpdateProcess(objStore);
        }
        #endregion

        #region Missing Product Codes
        private void MissingProductCodes()
        {

            var method = System.Reflection.MethodBase.GetCurrentMethod();

            try
            {
                int newRecordTotCount = 0;
                DataTable dtMissingProductCodes = this.GetMissingProductCodesFromTransaction();
                if (dtMissingProductCodes != null && dtMissingProductCodes.Rows.Count > 0)
                {
                    LoggingHelper.WriteErrorToFile("--------Missing Product Code  Note :- " + method.Name + " . Daily Total Missing Codes Found From Transaction  " + dtMissingProductCodes.Rows.Count.ToString(), "   ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());


                    foreach (DataRow dRow in dtMissingProductCodes.Rows)
                    {
                        Product objProductMissing = new Product
                        {

                            PRODUCT_CODE = dRow["product_code"].ToString()
                        };

                        DataTable dtFormattedProduct = this.GetFormattedMissingProductDetailsFromIFS(objProductMissing);
                        if (dtFormattedProduct != null && dtFormattedProduct.Rows.Count > 0)
                        {
                            DataRow dr = dtFormattedProduct.Rows[1];

                            if (dr != null)
                            {
                                #region Added to DataRow
                                Product objProduct = new Product();

                                objProduct.PRODUCT_CODE = dr["PRODUCT_CODE"].ToString();
                                objProduct.PRODUCT_DESC = dr["PRODUCT_DESC"].ToString();
                                objProduct.ACCOUNTING_GROUP_ID = dr["ACCOUNTING_GROUP_ID"].ToString();
                                objProduct.ACCOUNTING_GROUP_DESC = dr["ACCOUNTING_GROUP_DESC"].ToString();
                                objProduct.PRODUCT_FAMILY_ID = dr["PRODUCT_FAMILY_ID"].ToString();
                                objProduct.PRODUCT_FAMILY_DESC = dr["PRODUCT_FAMILY_DESC"].ToString();
                                objProduct.COMMODITY_ID = dr["COMMODITY_ID"].ToString();
                                objProduct.COMMODITY_DESC = dr["COMMODITY_DESC"].ToString();

                                #region OWNER_ID
                                if (dr["OWNER_ID"].ToString() == "")
                                {
                                    objProduct.OWNER_ID = "";
                                }
                                else
                                {
                                    objProduct.OWNER_ID = dr["OWNER_ID"].ToString();
                                }
                                #endregion

                                #region OWNER_DESC
                                if (dr["OWNER_DESC"].ToString() == "")
                                {
                                    objProduct.OWNER_DESC = "";
                                }
                                else
                                {
                                    objProduct.OWNER_DESC = dr["OWNER_DESC"].ToString();
                                }
                                #endregion

                                #region BRAND_ID
                                if (dr["BRAND_ID"].ToString() == "")
                                {
                                    objProduct.BRAND_ID = "";
                                }
                                else
                                {
                                    objProduct.BRAND_ID = dr["BRAND_ID"].ToString();
                                }
                                #endregion

                                #region BRAND_DESC
                                if (dr["BRAND_DESC"].ToString() == "")
                                {
                                    objProduct.BRAND_DESC = "";
                                }
                                else
                                {
                                    objProduct.BRAND_DESC = dr["BRAND_DESC"].ToString();
                                }
                                #endregion

                                objProduct.PART_STATUS = dr["part_status"].ToString();

                                objProduct.DATE_OF_INTRODUCTION = Convert.ToDateTime(HelperClass.DateManipulation(Convert.ToDateTime(dr["DATE_OF_INTRODUCTION"].ToString())));

                                #region REPLACEMENT_PRODUCT
                                if (dr["REPLACEMENT_PRODUCT"].ToString() == "")
                                {
                                    objProduct.REPLACEMENT_PRODUCT = "";
                                }
                                else
                                {
                                    objProduct.REPLACEMENT_PRODUCT = dr["REPLACEMENT_PRODUCT"].ToString();
                                }
                                #endregion

                                #region CBM
                                Double CBM = 0;
                                try
                                {
                                    objProduct.CBM = Convert.ToDouble(dr["CBM"]);
                                }
                                catch (Exception ex)
                                {

                                    objProduct.CBM = 0;
                                    LoggingHelper.WriteErrorToFile(string.Format("\n" + " {0} - {1} - {2} - {3} ", ex.Message, CBM, "CBM", dr["CBM"]), "Error - In converting Values", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                                }
                                #endregion

                                #region PRODUCT_ATTRIBUTE_1
                                if (dr["PRODUCT_ATTRIBUTE_1"].ToString() == "")
                                {
                                    objProduct.PRODUCT_ATTRIBUTE_1 = "";
                                }
                                else
                                {
                                    objProduct.PRODUCT_ATTRIBUTE_1 = dr["PRODUCT_ATTRIBUTE_1"].ToString();
                                }
                                #endregion

                                #region PROD_ATTRIBUTE2
                                if (dr["PRODUCT_ATTRIBUTE_2"].ToString() == "")
                                {
                                    objProduct.PRODUCT_ATTRIBUTE_2 = "";
                                }
                                else
                                {
                                    objProduct.PRODUCT_ATTRIBUTE_2 = dr["PRODUCT_ATTRIBUTE_2"].ToString();
                                }
                                #endregion

                                #region PRODUCT_ATTRIBUTE_3
                                if (dr["PRODUCT_ATTRIBUTE_3"].ToString() == "")
                                {
                                    objProduct.PRODUCT_ATTRIBUTE_3 = "";
                                }
                                else
                                {
                                    objProduct.PRODUCT_ATTRIBUTE_3 = dr["PRODUCT_ATTRIBUTE_3"].ToString();
                                }
                                #endregion

                                #region PRODUCT_ATTRIBUTE_4
                                if (dr["PRODUCT_ATTRIBUTE_4"].ToString() == "")
                                {
                                    objProduct.PRODUCT_ATTRIBUTE_4 = "";
                                }
                                else
                                {
                                    objProduct.PRODUCT_ATTRIBUTE_4 = dr["PRODUCT_ATTRIBUTE_4"].ToString();
                                }
                                #endregion

                                #region PRODUCT_ATTRIBUTE_5
                                if (dr["PRODUCT_ATTRIBUTE_5"].ToString() == "")
                                {
                                    objProduct.PRODUCT_ATTRIBUTE_5 = "";
                                }
                                else
                                {
                                    objProduct.PRODUCT_ATTRIBUTE_5 = dr["PRODUCT_ATTRIBUTE_5"].ToString();
                                }
                                #endregion

                                #region Catalog Type
                                objProduct.CATALOG_TYPE = dr["CATALOG_TYPE"].ToString();
                                #endregion

                                #endregion

                                bool isNewRecordAdded = this.CheckProductCodeExistsIn_BI_NRTANServerAndProcessToBI_NRTANServer(objProduct);
                                if (isNewRecordAdded == true)
                                {
                                    newRecordTotCount = newRecordTotCount + 1;
                                }
                            }
                        }



                    }

                }

                LoggingHelper.WriteErrorToFile("--------Missing Product Code  Note :- " + method.Name + " . Daily Total Missing Codes Found and Updated Count is  " + newRecordTotCount.ToString(), "   ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            catch (Exception ee)
            {

                LoggingHelper.WriteErrorToFile(string.Format("Error From MissingProductCodeExtraction()", ee.Message, ee.StackTrace), "MissingStoreExtraction()", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
        }

        private DataTable GetMissingProductCodesFromTransaction()
        {
            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtMissingStore = odbConnection.GetMissingProductCodesFromTransaction();
            return dtMissingStore;
        }

        private DataTable GetFormattedMissingProductDetailsFromIFS(Product objProduct)
        {
            NeenOpal_BI_DBAccess odbConnection_New = new NeenOpal_BI_DBAccess();
            return odbConnection_New.GetFormattedMissingProductDetailsFromIFS(objProduct);
        }
        #endregion

        #endregion

        #region Insert BI NeenOpalVsSinger Sales Figure
        public void InsertBIVsIFSSalesFigureValue()
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + " () method Get Data Form DB ", " ---- Get SalesFigures ----  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                decimal decDirectSaleValue = 0;
                decimal decIndirectSaleValue = 0;
                decimal decDMD = 0;
                decimal decBI = 0;

                int intAllSalesMethodRunRowCount = 0;
                int intBackUpSaleTableRowCount = 0;

                decDirectSaleValue = this.GetDirectSalesInNotInPackage_ForComparision();
                decIndirectSaleValue = this.GetIndirectSalesInNotInPackage_ForComparision();
                decDMD = this.GetDMDSalesInNotInPackage_ForComparision();
                decBI = this.GetNeenOpalBackUpSales_ForComparision();

                intAllSalesMethodRunRowCount = this.GetAllSalesMethodRunRowCount(dtPreviousDate_BI);
                intBackUpSaleTableRowCount = this.GetSingerSideSalesBackUpTableRowCount(dtPreviousDate_BI);

                DailyBIVsSingerSalesLog objSalesFigures = new DailyBIVsSingerSalesLog();

                objSalesFigures.Task_Date = Convert.ToDateTime(dtPreviousDate_BI.ToString());
                objSalesFigures.SingerDirectSaleValue = decDirectSaleValue;
                objSalesFigures.SingerIndirectSaleValue = decIndirectSaleValue;
                objSalesFigures.DMDIndirectSaleValue = decDMD;
                objSalesFigures.NeenOpalSaleValue = decBI;
                objSalesFigures.SingerNetSale = (decDirectSaleValue + decIndirectSaleValue + decDMD);
                objSalesFigures.Difference = (decDirectSaleValue + decIndirectSaleValue + decDMD) - (decBI);
                objSalesFigures.EnteredOn = Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now));

                objSalesFigures.NeenOpalAllSaleMethodRowCount = intAllSalesMethodRunRowCount;
                objSalesFigures.BackUpTableRowCount = intBackUpSaleTableRowCount;


                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                bool isSave = odbConnection.InsertNeenOpalVsIFSFigureSalesToTable(objSalesFigures);

                LoggingHelper.WriteErrorToFile("----End  Note :- " + method.Name + " () method Get Data Form DB ", " ---- End Get SalesFigures ----  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - InsertBIVsIFSSalesFigureValue ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
        }

        private decimal GetDirectSalesInNotInPackage_ForComparision()
        {
            decimal decDirectSingerValue = 0;

            try
            {

                var method = System.Reflection.MethodBase.GetCurrentMethod();

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();

                DataRow dr = odbConnection.GetDirectSalesInNotInPackage_ForComparision(dtPreviousDate_BI);
                if (dr != null)
                {
                    decDirectSingerValue = Convert.ToDecimal(dr["NetValue"]);
                    LoggingHelper.WriteErrorToFile("----Singer Direct Sale Figure Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Sale Figure  : " + decDirectSingerValue.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                }

            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - GetIndirectDirectSalesInNotInPackage ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return decDirectSingerValue;

        }

        private decimal GetIndirectSalesInNotInPackage_ForComparision()
        {
            decimal decIndirectSingerValue = 0;

            try
            {

                var method = System.Reflection.MethodBase.GetCurrentMethod();

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();

                DataRow dr = odbConnection.GetIndirectSalesInNotInPackage_ForComparision(dtPreviousDate_BI);
                if (dr != null)
                {
                    decIndirectSingerValue = Convert.ToDecimal(dr["NetValue"]);
                    LoggingHelper.WriteErrorToFile("----Singer Indirect Sale Figure Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Sale Figure  : " + decIndirectSingerValue.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                }

            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - GetIndirectDirectSalesInNotInPackage ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }

            return decIndirectSingerValue;

        }

        private decimal GetDMDSalesInNotInPackage_ForComparision()
        {
            decimal decDMDValue = 0;

            try
            {

                var method = System.Reflection.MethodBase.GetCurrentMethod();

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();

                DataRow dr = odbConnection.GetDMDSalesInNotInPackage_ForComparision(dtPreviousDate_BI);
                if (dr != null)
                {
                    decDMDValue = Convert.ToDecimal(dr["NetValue"]);
                    LoggingHelper.WriteErrorToFile("----DMD Indirect Sale Figure Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Sale Figure  : " + decDMDValue.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                }
            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - GetDMDSalesInNotInPackage ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return decDMDValue;

        }

        private decimal GetNeenOpalBackUpSales_ForComparision()
        {
            decimal decNeenOpal = 0;

            try
            {

                var method = System.Reflection.MethodBase.GetCurrentMethod();

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();

                DataRow dr = odbConnection.GetNeenOpalBackUpSales_ForComparision(dtPreviousDate_BI);
                if (dr != null)
                {
                    decNeenOpal = Convert.ToDecimal(dr["NetValue"]);
                    LoggingHelper.WriteErrorToFile("----NeenOpal BI Sale Figure Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Sale Figure  : " + decNeenOpal.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }

            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - GetManthanBackUpSales ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return decNeenOpal;
        }

        private int GetSingerSideSalesBackUpTableRowCount(DateTime dtSinTskDate)
        {
            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataRow dRow = odbConnection.GetSingerSideDailySaleBKTableRowCount(dtSinTskDate);

            int intBackUpTableDailyRowCount = 0;

            try
            {
                if (dRow != null)
                {
                    intBackUpTableDailyRowCount = Convert.ToInt32(dRow["SIN_BKTableRowCount"]);
                }
            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - GetSingerSideSalesBackUpTableRowCount ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }

            return intBackUpTableDailyRowCount;
        }

        private int GetAllSalesMethodRunRowCount(DateTime dtSinTskDate)
        {
            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataRow dRow = odbConnection.GetAllSalesMethodRunRowCount(dtSinTskDate);

            int intAllSalesMethodRunRowCount = 0;

            try
            {
                if (dRow != null)
                {
                    intAllSalesMethodRunRowCount = Convert.ToInt32(dRow["TotRowCount"]);
                }
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - GetSingerSideSalesBackUpTableRowCount ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }

            return intAllSalesMethodRunRowCount;
        }
        #endregion

        #region Insert BI NeenOpalVsSinger Sales Figure For ETL Run Year and Month
        public void InsertMonthlyBIVsIFSSalesFigureValue()
        {
            try
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();

                LoggingHelper.WriteErrorToFile("----Start Note :- " + method.Name + " () method Get Data Form DB ", " ---- Get SalesFigures ----  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                decimal decMonthly_DirectSaleValue = 0;
                decimal decMonthly_IndirectSaleValue = 0;
                decimal decMonthly_DMD = 0;

                decimal decBI_DirectSaleValue = 0;
                decimal decBI_InDirectSaleValue = 0;
                decimal decBI_DMDSaleValue = 0;
                decimal decMonthly_BI = 0;

                int intYear = dtPreviousDate_BI.Year;
                int intMonth = dtPreviousDate_BI.Month;

                decMonthly_DirectSaleValue = this.GetMonthlyDirectSalesInNotInPackage_ForComparision(intYear, intMonth);//here pass year and month to direct tab file contain year and month coloum.Not for transaction date.
                decMonthly_IndirectSaleValue = this.GetMonthlyIndirectSalesInNotInPackage_ForComparision(intYear, intMonth);//here pass year and month to direct tab file contain year and month coloum.Not for transaction date.
                decMonthly_DMD = this.GetMonthlyDMDSalesInNotInPackage_ForComparision(intYear, intMonth);//here pass year and month to direct tab file contain year and month coloum.Not for transaction date.

                decBI_DirectSaleValue = this.GetMonthlyBI_DirectSales_BackUpSales_ForComparision(intYear, intMonth);
                decBI_InDirectSaleValue = this.GetMonthlyBI_InDirectSales_BackUpSales_ForComparision(intYear, intMonth);
                decBI_DMDSaleValue = this.GetMonthlyBI_DMDSales_BackUpSales_ForComparision(intYear, intMonth);


                MonthlyBIVsSingerSalesLog objMonthlySalesFigures = new MonthlyBIVsSingerSalesLog();

                objMonthlySalesFigures.Task_Date = Convert.ToDateTime(dtPreviousDate_BI.ToString());
                objMonthlySalesFigures.Log_Year = intYear;
                objMonthlySalesFigures.Log_Month = intMonth;

                objMonthlySalesFigures.MonthlySingerDirectSaleValue = decMonthly_DirectSaleValue;
                objMonthlySalesFigures.MonthlySingerIndirectSaleValue = decMonthly_IndirectSaleValue;
                objMonthlySalesFigures.MonthlyDMDIndirectSaleValue = decMonthly_DMD;

                objMonthlySalesFigures.MonthlyBI_Direct_SaleValue = decBI_DirectSaleValue;
                objMonthlySalesFigures.MonthlyBI_Indirect_SingerNetSale = decBI_InDirectSaleValue;
                objMonthlySalesFigures.MonthlyBI_DMD_SingerNetSale = decBI_DMDSaleValue;

                objMonthlySalesFigures.MonthlySinger_NetSale = (decMonthly_DirectSaleValue + decMonthly_IndirectSaleValue + decMonthly_DMD);
                objMonthlySalesFigures.MonthlyBI_NetSale = (decBI_DirectSaleValue + decBI_InDirectSaleValue + decBI_DMDSaleValue);

                objMonthlySalesFigures.Difference = (decMonthly_DirectSaleValue + decMonthly_IndirectSaleValue + decMonthly_DMD) - (decBI_DirectSaleValue + decBI_InDirectSaleValue + decBI_DMDSaleValue);
                objMonthlySalesFigures.EnteredOn = Convert.ToDateTime(HelperClass.DateManipulationWithTime(DateTime.Now));


                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                bool isSave = odbConnection.InsertMonthlyNeenOpalVsIFSFigureSalesToTable(objMonthlySalesFigures);

                LoggingHelper.WriteErrorToFile("----End  Note :- " + method.Name + " () method Get Data Form DB ", " ---- End Get SalesFigures ----  ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - InsertBIVsIFSSalesFigureValue ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
        }

        //SINGER SIDE
        private decimal GetMonthlyDirectSalesInNotInPackage_ForComparision(int intYear, int intMonth)
        {
            decimal decMonthlyDirectSingerValue = 0;

            try
            {

                var method = System.Reflection.MethodBase.GetCurrentMethod();

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();

                DataRow dr = odbConnection.GetMonthlyDirectSalesInNotInPackage_ForComparision(intYear, intMonth);
                if (dr != null)
                {
                    decMonthlyDirectSingerValue = Convert.ToDecimal(dr["NetValue"]);
                    LoggingHelper.WriteErrorToFile("----Singer Direct Sale Figure Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Sale Figure  : " + decMonthlyDirectSingerValue.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                }

            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - GetIndirectDirectSalesInNotInPackage ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return decMonthlyDirectSingerValue;

        }

        //SINGER SIDE
        private decimal GetMonthlyIndirectSalesInNotInPackage_ForComparision(int intYear, int intMonth)
        {
            decimal decMonthlyIndirectSingerValue = 0;

            try
            {

                var method = System.Reflection.MethodBase.GetCurrentMethod();

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();

                DataRow dr = odbConnection.GetMonthlyIndirectSalesInNotInPackage_ForComparision(intYear, intMonth);
                if (dr != null)
                {
                    decMonthlyIndirectSingerValue = Convert.ToDecimal(dr["NetValue"]);
                    LoggingHelper.WriteErrorToFile("----Singer Indirect Sale Figure Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Sale Figure  : " + decMonthlyIndirectSingerValue.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                }

            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - GetIndirectDirectSalesInNotInPackage ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }

            return decMonthlyIndirectSingerValue;

        }

        //SINGER SIDE
        private decimal GetMonthlyDMDSalesInNotInPackage_ForComparision(int intYear, int intMonth)
        {
            decimal decMonthlyDMDValue = 0;

            try
            {

                var method = System.Reflection.MethodBase.GetCurrentMethod();

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();

                DataRow dr = odbConnection.GetMonthlyDMDSalesInNotInPackage_ForComparision(intYear, intMonth);
                if (dr != null)
                {
                    decMonthlyDMDValue = Convert.ToDecimal(dr["NetValue"]);
                    LoggingHelper.WriteErrorToFile("----DMD Indirect Sale Figure Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Sale Figure  : " + decMonthlyDMDValue.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());

                }
            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - GetDMDSalesInNotInPackage ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return decMonthlyDMDValue;

        }

        //BI SIDE
        private decimal GetMonthlyBI_DirectSales_BackUpSales_ForComparision(int intYear, int intMonth)
        {
            decimal decMonthlyBI_Direct = 0;

            try
            {

                var method = System.Reflection.MethodBase.GetCurrentMethod();

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();

                DataRow dr = odbConnection.GetMonthlyBI_DirectSales_BackUpSales_ForComparision(intYear, intMonth);
                if (dr != null)
                {
                    decMonthlyBI_Direct = Convert.ToDecimal(dr["NetValue"]);
                    LoggingHelper.WriteErrorToFile("----NeenOpal BI Sale Figure Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Sale Figure  : " + decMonthlyBI_Direct.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }

            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - GetManthanBackUpSales ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return decMonthlyBI_Direct;
        }

        //BI SIDE
        private decimal GetMonthlyBI_InDirectSales_BackUpSales_ForComparision(int intYear, int intMonth)
        {
            decimal decMonthlyBI_Indirect = 0;

            try
            {

                var method = System.Reflection.MethodBase.GetCurrentMethod();

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();

                DataRow dr = odbConnection.GetMonthlyBI_InDirectSales_BackUpSales_ForComparision(intYear, intMonth);
                if (dr != null)
                {
                    decMonthlyBI_Indirect = Convert.ToDecimal(dr["NetValue"]);
                    LoggingHelper.WriteErrorToFile("----NeenOpal BI Sale Figure Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Sale Figure  : " + decMonthlyBI_Indirect.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }

            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - GetManthanBackUpSales ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return decMonthlyBI_Indirect;
        }

        //BI SIDE
        private decimal GetMonthlyBI_DMDSales_BackUpSales_ForComparision(int intYear, int intMonth)
        {
            decimal decMonthlyBI_DMD = 0;

            try
            {

                var method = System.Reflection.MethodBase.GetCurrentMethod();

                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();

                DataRow dr = odbConnection.GetMonthlyBI_DMDSales_BackUpSales_ForComparision(intYear, intMonth);
                if (dr != null)
                {
                    decMonthlyBI_DMD = Convert.ToDecimal(dr["NetValue"]);
                    LoggingHelper.WriteErrorToFile("----NeenOpal BI Sale Figure Note :- " + method.Name + " () method Formatted Data Insert to DB ", " Sale Figure  : " + decMonthlyBI_DMD.ToString(), System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
                }

            }
            catch (Exception ex)
            {

                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error - GetManthanBackUpSales ", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
            return decMonthlyBI_DMD;
        }

        #endregion

        #endregion

        #region Public Methods

        public void GetPreviousDate()
        {

            DataRow drSINGERServerCurrentDate = this.GetSINGERServerCurrentDate();
            dtCurrentDate = Convert.ToDateTime(drSINGERServerCurrentDate["CURRENTDATE"]);
            dtPreviousDate_BI = Convert.ToDateTime(HelperClass.DateManipulationWithTime(dtCurrentDate.AddDays(-1)));

            strUserName = Environment.UserName;
            strMachineName = Environment.MachineName;

        }

        public void EndOfBIEntireProcessCompletedEmailRecord()
        {
            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            bool isSave = odbConnection.InsertEndofTheBIWholeProcessRecordForEmail(dtPreviousDate_BI);
        }

        public void SingerIFSSalesFileTransferLog()
        {
            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtTransferDetails = odbConnection.GetIFSSalesFilesTransferDetails(dtPreviousDate_BI);

            if (dtTransferDetails != null && dtTransferDetails.Rows.Count > 0)
            {
                this.InsertSingerIFSSalesTransferLogToNRTANServer(dtTransferDetails);
            }
        }

        public void DMDIFSSalesFileTransferLog()
        {
            NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
            DataTable dtTransferDetails = odbConnection.GetIFSS_DMD_SalesFilesTransferDetails(dtPreviousDate_BI);

            if (dtTransferDetails != null && dtTransferDetails.Rows.Count > 0)
            {
                this.InsertSingerIFSSalesTransferLogToNRTANServer(dtTransferDetails);
            }
        }

        private void InsertSingerIFSSalesTransferLogToNRTANServer(DataTable dtSingerIFSSalesFileTranDetails)
        {
            try
            {
                NeenOpal_BI_DBAccess odbConnection = new NeenOpal_BI_DBAccess();
                int intRecSaveCount = odbConnection.InsertSingerIFSSalesFileTransactionDetailsTo_NRTAN_Server(dtSingerIFSSalesFileTranDetails);
            }
            catch (Exception ex)
            {
                LoggingHelper.WriteErrorToFile(string.Format("**** {0} - {1} *****", ex.Message, ex.StackTrace), "Error", System.Reflection.MethodBase.GetCurrentMethod().Name.ToString());
            }
        }

        public void InvokMasterMethods()
        {
            

        }

        public void InvokTransactionMethods()
        {
            this.BI_HP_WALK_MEGA(); //136
            this.BI_WRITE_OFF_AMT(); //137
            this.BI_BLACKLIST_CUST(); //138
            this.BI_BR_MGR_INCEN(); //132
            this.BI_RESALE_VAL_ITEM(); //139
            this.BI_ONLINE_HP_ACC(); //134
            this.BI_GUARANTER_INFO(); //145
        }

        public void InvokeUpdateMethods()
        {
            
        }

        #endregion
    }
  
}

