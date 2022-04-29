using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BI_Incremental.Objects
{
  public  class CashManagement
    {
      public string SITE_ID { get; set; }
      public DateTime TRAN_DATE { get; set; }        
      public decimal OPEN_AMOUNT { get; set; }
      public int BANKED_TRANS { get; set; }
	  public decimal BANKING_AMOUNT { get; set; }
	  public int ARREARS_TRANS { get; set; }	
	  public decimal ARREARS_VALUE { get; set; }
	  public int CASH_ADVANCES_TRANS { get; set; }	
      public decimal CASH_ADVANCES_VALUE { get; set; }
	  public decimal OVERDUE_CREDIT_SALES { get; set; }	
	  public decimal HP_COLLECTION { get; set; }			
	  public decimal OTHER_COLLECTIONS { get; set; }				
	  public decimal ISSUE_VALUE { get; set; }			 	
	  public decimal BALANCE_ISSUE_AMOUNT { get; set; }			 			
      public decimal UNREALISED_DEPOSIT { get; set; }			
      public int NO_OF_HP_ACCOUNTS { get; set; }			
      public decimal SHORT_REMITTANCE_VALUE { get; set; }				
      public decimal LEASE_ASSET_OUTSTD_BALANCE { get; set; }		
    }
}
