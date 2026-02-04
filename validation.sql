--validation
select * from SSISLoad where result like '%tbltxloan%'
select * from SSISLoad where result like '%customer%' order by statusdt desc
select * from SSISLoad where result like '%applicant%' order by statusdt desc 
SELECT COUNT(*) AS count
FROM TRN_CreditDeliveryDB.[dbo].tblTxLoans
WHERE AssocID='012'
SELECT COUNT(*) AS count
FROM TRN_CreditDeliveryDB.[dbo].tblTxLoans
WHERE AssocID='032'

SELECT COUNT(*) AS count
FROM TRN_CreditDeliveryDB.[dbo].TxERCustomers
WHERE AssocID='012'

SELECT COUNT(*) AS count
FROM TRN_CreditDeliveryDB.[dbo].TxERCustomers
WHERE AssocID='032'

select * from tblTxLoanApplicants where CustAssocID = '012'
select * from tblTxLoanApplicants where CustAssocID = '032'

select COUNT(*) from tblTxLoanApplicants t1 join TxERCustomers t2 on t1.CustomerID = t2.CustomerID where AssocID='012'  
select COUNT(*) from tblTxLoanApplicants t1 join TxERCustomers t2 on t1.CustomerID = t2.CustomerID where AssocID='012'  and CustAssocID=AssocID


