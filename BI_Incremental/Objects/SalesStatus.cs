using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BI_Incremental.Objects
{
   public class SalesStatus
    {
        public string SALES_STATUS { get; set; }
        public DateTime PLAN_START_DATE { get; set; }
        public DateTime CONFIRM_DATE { get; set; }
        public string TRANSACION_ID { get; set; }
    }
}
