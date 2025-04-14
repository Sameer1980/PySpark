
USE order_processing;

Declare @Id int
Set @Id = 1

While @Id < = 1000
Begin
	INSERT INTO Product VALUES ('Name - ' + CAST(@Id as nvarchar(10)),
								ROUND(RAND() * 50,2),
								'Origin - ' + CAST(ROUND(RAND() * 10, 0) as nvarchar(10))
								)
	Set @Id = @Id + 1
End