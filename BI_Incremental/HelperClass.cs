using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BI_Incremental
{
    public class HelperClass
    {
        public static string DateManipulation(DateTime dt)
        {
            //2011/11/29 12:00:00 AM

            string year = dt.Year.ToString();
            string month = dt.Month.ToString();
            string date = dt.Day.ToString();

            //format year/month/date
            return string.Format("{0}/{1}/{2}", year, month, date);
        }
        public static string DateManipulationWithTime(DateTime dt)
        {
            //2011/11/29 12:00:00 AM

            string year = dt.Year.ToString();
            string month = dt.Month.ToString();
            string date = dt.Day.ToString();
            string hour = dt.Hour.ToString();
            string minute = dt.Minute.ToString();
            string second = dt.Second.ToString();

            //format year/month/date/hour/minute/second
            return string.Format("{0}/{1}/{2} {3}:{4}:{5}", year, month, date, hour, minute, second);

        }
    }
}
