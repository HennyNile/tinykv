package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	DB *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{DB: engine_util.CreateDB(conf.DBPath, true)}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{s, 0}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	writebatch := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			writebatch.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			writebatch.DeleteCF(data.Cf, data.Key)
		}
		err := writebatch.WriteToDB(s.DB)
		if err != nil {
			return err
		}
		writebatch.Reset()
	}
	return nil
}

type StandAloneStorageReader struct {
	inner     *StandAloneStorage
	iterCount int
}

func (standalonestoragereader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, _ := engine_util.GetCF(standalonestoragereader.inner.DB, cf, key)
	return val, nil
}

func (standalonestoragereader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := standalonestoragereader.inner.DB.NewTransaction(true)
	standalonestorageiter := engine_util.NewCFIterator(cf, txn)
	return standalonestorageiter
}

func (standalonestoragereader *StandAloneStorageReader) Close() {

}
