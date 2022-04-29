using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BI_Incremental.Objects
{
   public class Promotion
    {
        public string PROMO_CODE { get; set; }
        public string PROMO_DESC { get; set; }
        public DateTime PROMO_ENTER_DATE { get; set; }
        public DateTime PROMO_ST_DATE { get; set; }
        public DateTime PROMO_END_DATE { get; set; }
        public string PROMOTION_FREE_ITEM { get; set; }
        public string APPLY_SITE { get; set; }
        public string PROMOTION_EFFECT_MAIN_ITEM { get; set; }
        public string PROMO_TYPE { get; set; }
        public string PROMO_TYPE_DESC { get; set; }

    }
}
