USE order_processing;

Declare @RandomBillID int
Declare @RandomProductID int
Declare @RandomQty int
Declare @RandomSale float
Declare @Cash int

Declare @Lower_Bill int
Declare @Upper_Bill int
SET @Lower_Bill = 1
SET @Upper_Bill = 500

Declare @Lower_ProductID int
Declare @Upper_ProductID int
SET @Lower_ProductID = 1
SET @Upper_ProductID = 1000

Declare @Lower_Qty int
Declare @Upper_Qty int
SET @Lower_Qty = 10
SET @Upper_Qty = 1000

--Declare @Lower_Sale float
Declare @Upper_Sale float
--SET @Lower_Sale = 0.0
SET @Upper_Sale = 0.5

Declare @Count int
Set @Count = 1

WHILE @Count <= 50000
BEGIN

SELECT @RandomBillID = ROUND(((@Upper_Bill - @Lower_Bill) * RAND()) + @Lower_Bill ,0)
SELECT @RandomProductID = ROUND(((@Upper_ProductID - @Lower_ProductID) * RAND()) + @Lower_ProductID ,0)
SELECT @RandomQty = ROUND(((@Upper_Qty - @Lower_Qty) * RAND()) + @Lower_Qty ,0)
SELECT @RandomSale = ROUND(RAND() * @Upper_Sale, 2)
SELECT @Cash = ROUND(RAND(), 0)

INSERT INTO Orders VALUES (@RandomBillID, @RandomProductID, @RandomQty, @RandomSale, @Cash)

SET @Count = @Count + 1
END


SELECT * FROM Orders
WHERE Bill_id < = 5


