# Project1 StandaloneKV 

## EDITOR
Qilong Li

## GOAL
In this project, you will build a standalone key/value storage [gRPC](https://grpc.io/docs/guides/) service with the support of the column family. 

## SOLUTION
This project has provided low-level code to manipulate data in database, needed implemention is  
* **Implement standalone storage**  
We mainly need to implement *Write(ctx *kvrpcpb.Context, batch []storage.Modify)*, *Reader(ctx *kvrpcpb.Context)* in <u>/kv/storage/standalone_storage/standalone_storage.go/</u>. *Write(ctx *kvrpcpb.Context, batch []storage.Modify)* writes modifications stored in *batch* to physical storage and *Reader(ctx \*kvrpcpb.Context)* creates an iterator to read data singlely or iteratively in physical storage. This two methods could be implemented based on the provided low-level code in <u>/kv/util/engine_util/</u>.
* **Implement high-level service handler**  
In this part, we need to implement user-friendly high-level service handler to simply the use of methods implemented in standalone storage. To the specific, we need to implement <u>*RawGet()*</u>, <u>*RawScan()*</u>, <u>*RawPut()*</u> and <u>*RawDelete()*</u> in <u>/kv/server/raw_api.go</u> using methods implemented in <u>/kv/storage/standalone_storage/standalone_storage.go/</u>.

## TIPS
* Create a transcation in badgerDB via  ***txn := badger.DB.NewTransaction(true)***.
* ***item := iter.Item()*** and ***iter.Next()***  
***iter.Item()*** returns a pointer which points a pointer which points current item. Then if you execute ***iter.Next()***, ***item*** return by the ***iter.Item()*** won't change. However, if you access current item via ***iter.Item()***, you get the next item. You should pay attention to their use.