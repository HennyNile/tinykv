package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/Connor1996/badger"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	DB *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		DB: engine_util.CreateDB(conf.DBPath, false),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

type StandAloneStorageReader struct {
	S *StandAloneStorage
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, _ := engine_util.GetCF(sr.S.DB, cf, key)
	return value, nil
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := sr.S.DB.NewTransaction(true)
	iter := engine_util.NewCFIterator(cf, txn)
	return iter
}

func (sr *StandAloneStorageReader) Close() {
	return 
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{S: s}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			data := m.Data.(storage.Put)
			cf, key, value := data.Cf, data.Key, data.Value
			err := engine_util.PutCF(s.DB, cf, key, value)
			if err != nil {
				return err
			}
		case storage.Delete:
			data := m.Data.(storage.Delete)
			cf, key := data.Cf, data.Key
			err := engine_util.DeleteCF(s.DB, cf, key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
