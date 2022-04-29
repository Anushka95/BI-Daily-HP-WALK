using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BI_Incremental.Objects
{
    public class Store
    {

        #region BI_DIM_STORE

        public string SITE_ID { get; set; }
        public string SITE_NAME { get; set; }
        public string CHANNEL_ID { get; set; }
        public string CHANNEL_NAME { get; set; }
        public string AREA_ID { get; set; }
        public string AREA_NAME { get; set; }
        public string DISTRICT_ID { get; set; }
        public string DISTRICT_NAME { get; set; }
        public string AGENCY_ID { get; set; }
        public string COMPANY_ID { get; set; }
        public string STORE_LATITUDE { get; set; }
        public string STORE_LONGITUDE { get; set; }
        public string TYPE_ID { get; set; }
        public string TYPE_NAME { get; set; }
        public string PROVINCE_ID { get; set; }
        public string PROVINCE_NAME { get; set; }
        public string STORE_PROVINCE { get; set; }
        public string MOBILE_NO { get; set; }
        public string QOS { get; set; }
        public string CAPACITIES { get; set; }
        public string AUDIT_GRADING { get; set; }
        public string STORE_ATTRIBUTE_1 { get; set; }
        public string STORE_ATTRIBUTE_2 { get; set; }
        public string STORE_ATTRIBUTE_3 { get; set; }
        public string STORE_ATTRIBUTE_4 { get; set; }
        public string STORE_ATTRIBUTE_5 { get; set; }
        public string PRIMARY_WAREHOUSE { get; set; }
        public Double FLOAT_AMT { get; set; }
        public string SL_DISTRICT_ID { get; set; }
        public string SL_DISTRICT_NAME { get; set; }
        public string STORE_TYPE { get; set; }

        #endregion

        #region BI_DIM_STORE_RENT

        public Double RENT_AMOUNT { get; set; }
        public Double STORE_ATTRIBUTE_6 { get; set; }
        public Double STORE_ATTRIBUTE_7 { get; set; }
        public Double STORE_ATTRIBUTE_8 { get; set; }
        public Double STORE_ATTRIBUTE_9 { get; set; }

        #endregion

    }
}
