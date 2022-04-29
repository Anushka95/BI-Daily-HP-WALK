using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BI_Incremental.Objects
{
  public  class DailyBIVsSingerSalesLog
    {
        public DateTime Task_Date { get; set; }
        public decimal SingerDirectSaleValue { get; set; }
        public decimal SingerIndirectSaleValue { get; set; }
        public decimal DMDIndirectSaleValue { get; set; }
        public decimal NeenOpalSaleValue { get; set; }
        public decimal SingerNetSale { get; set; }
        public decimal Difference { get; set; }
        public DateTime EnteredOn { get; set; }
        public int NeenOpalAllSaleMethodRowCount { get; set; }
        public int BackUpTableRowCount { get; set; }

    }
}
