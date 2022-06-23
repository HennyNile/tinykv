package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	res := new(kvrpcpb.RawGetResponse)
	res.NotFound = false
	storagereader, _ := server.storage.Reader(nil)
	val, _ := storagereader.GetCF(req.Cf, req.Key)
	if val == nil {
		res.NotFound = true
	}
	res.Value = val
	return res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	res := new(kvrpcpb.RawPutResponse)
	var batch []storage.Modify
	batchtmp := storage.Modify{Data: storage.Put{req.Key, req.Value, req.Cf}}
	batch = append(batch, batchtmp)
	err := server.storage.Write(nil, batch)
	return res, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	res := new(kvrpcpb.RawDeleteResponse)
	var batch []storage.Modify
	batchtmp := storage.Modify{Data: storage.Delete{req.Key, req.Cf}}
	batch = append(batch, batchtmp)
	err := server.storage.Write(nil, batch)
	if err != nil {
		return res, err
	}
	return res, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	res := new(kvrpcpb.RawScanResponse)
	storagereader, _ := server.storage.Reader(nil)
	iter := storagereader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	var kvs []*kvrpcpb.KvPair
	for i := 0; i < int(req.Limit); i++ {
		item := iter.Item()
		key := item.Key()
		value, _ := item.Value()
		kvPair := kvrpcpb.KvPair{Key: key, Value: value}
		kvs = append(kvs, &kvPair)
		iter.Next()
		if !iter.Valid() {
			break
		}
	}
	res.Kvs = kvs
	return res, nil
}
