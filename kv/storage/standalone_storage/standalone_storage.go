package standalone_storage

import (
	"log"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/Connor1996/badger"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db     *badger.DB
	dbPath string
}

//NewStandAloneStorage tt
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{dbPath: conf.DBPath}
}

//Start tt
func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	options := badger.DefaultOptions
	options.Dir = s.dbPath
	options.ValueDir = s.dbPath
	db, err := badger.Open(options)
	if err != nil {
		log.Fatal(err)
		return err
	}
	s.db = db
	db.NewTransaction(true)
	return nil
}

//Stop tt
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.db != nil {
		err := s.db.Close()
		s.db = nil
		return err
	}
	return nil
}

//MyReader tt
type MyReader struct {
	txn *badger.Txn
}

//GetCF tt
func (r *MyReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(r.txn, cf, key)
}

//IterCF tt
func (r *MyReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

//Close tt
func (r *MyReader) Close() {
	r.txn.Discard()
}

// Reader ttt
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return &MyReader{txn: txn}, nil
}

//Write tt
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	var err error
	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			err = txn.Set(engine_util.KeyWithCF(data.Cf, data.Key), data.Value)
		case storage.Delete:
			err = txn.Delete(engine_util.KeyWithCF(data.Cf, data.Key))
		}
		if err != nil {
			return err
		}
	}

	return txn.Commit()
}
