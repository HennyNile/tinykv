package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/storage"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	key, cf, resp := req.Key, req.Cf, &kvrpcpb.RawGetResponse{}

	reader, _ := server.storage.Reader(nil)
	value, _ := reader.GetCF(cf, key)
	resp.Value = value
	if len(value) == 0 {
		resp.NotFound = true
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	key, value, cf, resp := req.Key, req.Value, req.Cf, &kvrpcpb.RawPutResponse{}

	var batch []storage.Modify
	m := storage.Modify{Data: storage.Put{Key: key, Value: value, Cf: cf}}
	batch = append(batch, m)
	
	err := server.storage.Write(nil, batch)

	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	key, cf, resp := req.Key, req.Cf, &kvrpcpb.RawDeleteResponse{}

	var batch []storage.Modify
	m := storage.Modify{Data: storage.Delete{Key: key, Cf: cf}}
	batch = append(batch, m)
	
	server.storage.Write(nil, batch)

	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	startKey, limit, cf := req.StartKey, (int)(req.Limit), req.Cf
	resp := &kvrpcpb.RawScanResponse{}
	var kvPairs []*kvrpcpb.KvPair

	// Reader(ctx *kvrpcpb.Context) (StorageReader, error)
	reader, _ := server.storage.Reader(nil)
	iter := reader.IterCF(cf)
	iter.Seek(startKey)

	for i := 0; i < limit; i++ {
		item := iter.Item()
		key := item.Key()
		value, _ := item.Value()
		kvPair := kvrpcpb.KvPair{Key: key, Value: value}
		kvPairs = append(kvPairs, &kvPair)
		
		iter.Next()
		if !iter.Valid() {
			break
		}
	}   

	resp.Kvs = kvPairs
	return resp, nil
}
