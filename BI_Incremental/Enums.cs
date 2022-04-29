using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BI_Incremental
{
    public enum ReturnType
    {
        DataTable = 1,
        DataRow = 2,
        DataSet = 3
    };

    public enum BIAdminControlETITableTypes
    {
        BI_Store = 1 
        ,BI_Product = 2
        ,BI_DIM_STATUS = 3
        ,BI_DIM_SALES_TYPE = 4
        ,BI_SALES_PROMOTER = 5
        ,BI_SALES_STATUS = 6 
        ,BI_DIM_INVENTORY_LOCATION = 7
        ,BI_DIM_SALES_LOCATION = 8
        ,BI_DIM_RELAVENT_TYPE = 9
        ,BI_DIM_PROMOTION = 10
        ,BI_DIM_DISCOUNT= 11
        ,BI_DIM_SALESMAN = 12
        ,BI_DIM_RETURN = 13
        ,BI_DIM_CUSTOMER =14
        ,BI_DIM_SALES_LINE = 15
        , BI_INV_ADJUSTMENT = 18
        , BI_STOCKAGE_BAND = 19
        , BI_MAP_BR_TO_AD = 20
        , BI_INV_TRANSACTION = 17
        , BI_INVENTORY_POSITION = 16
        , BI_CASH_MANAGEMENT = 21
        , BI_LIST_OF_ARREARS = 22
        , BI_RETURN_CHEQUE_LIST = 23
        , BI_MARKETING_BUDGET = 24
        , BI_BRANCH_EXPOSURE = 25
        , BI_586P_PLANNED_SALES = 27
        , BI_SALES_EXCLUDED_RELATED = 36
        , BI_DIM_PRODUCT_IV = 95
        , BI_DIM_STORE_RENT = 98
        , BI_USER_RLS = 99
        , BI_PRICE_LIST = 100
        , BI_AUDIT_WEIGHT = 102
        , BI_HPC_CCD = 103
        , BI_ISSUE_SETTLEMENT_CCD = 104
        , BI_ONLINE_HP_ACC = 134
        , BI_HP_WALK_MEGA = 136
        , BI_WRITE_OFF_AMT = 137
        , BI_BLACKLIST_CUST = 138
        , BI_BR_MGR_INCEN = 132
        , BI_RESALE_VAL_ITEM = 139
        , BI_GUARANTER_INFO = 145
        , BI_REVERT_INFO = 146
    };

    public enum ExtrationLogStatus
    {
        Data_Loading_For_Corresponding_Tables_In_Progress = 1
       , Data_Loadinig_Completed = 2
    };
}
