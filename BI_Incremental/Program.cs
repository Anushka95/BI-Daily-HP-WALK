using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;

namespace BI_Incremental
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            NeenOpal_BI objNeenOpal_BI = new NeenOpal_BI();
            LoggingHelper.WriteErrorToFile("BI Daily HP WALK System started to Run ", " Log is Started ", System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.ToString());
            objNeenOpal_BI.GetPreviousDate();
            objNeenOpal_BI.InvokMasterMethods();
            objNeenOpal_BI.InvokTransactionMethods();
            objNeenOpal_BI.InvokeUpdateMethods();
            LoggingHelper.WriteErrorToFile("BI Daily HP WALK System started to Run", " Log is Ended  ", System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.ToString());
        }
    }
}
