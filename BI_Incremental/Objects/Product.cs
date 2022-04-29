using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BI_Incremental.Objects
{
   public class Product
    {
        public string PRODUCT_CODE { get; set; }
        public string PRODUCT_DESC { get; set; }
        public string ACCOUNTING_GROUP_ID { get; set; }
        public string ACCOUNTING_GROUP_DESC { get; set; }
        public string PRODUCT_FAMILY_ID { get; set; }
        public string PRODUCT_FAMILY_DESC { get; set; }
        public string COMMODITY_ID { get; set; }
        public string COMMODITY_DESC { get; set; }
        public string OWNER_ID { get; set; }
        public string OWNER_DESC{ get; set; }
        public string BRAND_ID { get; set; }
        public string BRAND_DESC { get; set; }
        public string PART_STATUS { get; set; }
        public DateTime DATE_OF_INTRODUCTION { get; set; }
        public string REPLACEMENT_PRODUCT { get; set; }
        public Double CBM { get; set; }
        public string PRODUCT_ATTRIBUTE_1 { get; set; }
        public string PRODUCT_ATTRIBUTE_2{ get; set; }
        public string PRODUCT_ATTRIBUTE_3{ get; set; }
        public string PRODUCT_ATTRIBUTE_4{ get; set; }
        public string PRODUCT_ATTRIBUTE_5{ get; set; }
        public string CATALOG_TYPE { get; set; }
        public Double Sales_Price { get; set; }
        public Double INCREMENTAL_VOLUMN { get; set; }
        public string BASICTEC1 { get; set; }
        public string BASICTEC2 { get; set; }
        public string BASICTEC3 { get; set; }
        public string BASICTEC4 { get; set; }
        public string BASICTEC5 { get; set; }
        public string COLOURCODE { get; set; }
        public string BASICTEC6 { get; set; }
       
    }
}
