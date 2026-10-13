-- Enable SQLCMD Mode in SSMS (Query → SQLCMD Mode)
!!osql -S AGF-P3SPZ-WS1 -E -i "C:\Users\p3spz.AGF-P3SPZ-WS1\source\repos\SQL Server Scripts1\src\restore-script.sql"
!!osql -S AGF-P3SPZ-WS1 -d Trn_CreditDeliveryDB -E -i "C:\Users\p3spz.AGF-P3SPZ-WS1\source\repos\SQL Server Scripts1\src\trn_clone_data.sql"
!!osql -S AGF-P3SPZ-WS1 -d Trn_CreditDeliveryDB -E -i "C:\Users\p3spz.AGF-P3SPZ-WS1\source\repos\SQL Server Scripts1\src\runner.sql"
