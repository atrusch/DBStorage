# DBStorage
https://github.com/atrusch/DBStorage.git

Database Access with JSON Serialization/Deserialization. 
You can manipulate table without to know it's complete structure 

# Some example :
## Read data
```
  DBResultSet set = new DBResultSet(conn, "select * from devise");
  while(set.next())
  {
    DBRow row = set.getRow();
  }

```

## Insert data (with MySQL connection)
```
  DBConnection conn = new DBMySQLConnection(ds.getConnection(),false);
  DBTable devise = new DBTable(conn, "DEVISE");
		
  DBRow row = devise.newRow();
  row.getColumn("DEV_CODE").setValue("TT");
  row.getColumn("DEV_SYMBOLE").setValue("ALEX");
		
  conn.insert(devise.getName(), row);
```

## Update data
```
  row.getColumn("DEV_SYMBOLE").setValue("ALEX2");
  conn.update(devise.getName(), row);
```

## Delete a row
```
  conn.delete(devise.getName(), row);
```
