using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BI_Incremental.Objects
{
    public class BranchExposure
    {
        public string SITE { get; set; }
        public DateTime TRAN_DATE { get; set; }
        public double UNCLAIMED_EXCHANGES { get; set; }
        public double ISSUE_PERCENTAGE { get; set; }
        public string QUALITATIVE_FACTOR { get; set; }
    }
}
