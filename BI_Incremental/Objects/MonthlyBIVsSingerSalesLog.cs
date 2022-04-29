using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BI_Incremental.Objects
{
  public  class MonthlyBIVsSingerSalesLog
    {
        public DateTime Task_Date { get; set; }
        public int Log_Year { get; set; }
        public int Log_Month { get; set; }

        public decimal MonthlySingerDirectSaleValue { get; set; }
        public decimal MonthlySingerIndirectSaleValue { get; set; }
        public decimal MonthlyDMDIndirectSaleValue { get; set; }

        public decimal MonthlyBI_Direct_SaleValue { get; set; }
        public decimal MonthlyBI_Indirect_SingerNetSale { get; set; }
        public decimal MonthlyBI_DMD_SingerNetSale { get; set; }

        public decimal MonthlySinger_NetSale { get; set; }
        public decimal MonthlyBI_NetSale { get; set; }


        public decimal Difference { get; set; }
        public DateTime EnteredOn { get; set; }
    }
}
