// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package backupccl

import (
	"context"
	fmt "fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	descpb "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	hlc "github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestMaxImportBatchSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		importBatchSize int64
		maxCommandSize  int64
		expected        int64
	}{
		{importBatchSize: 2 << 20, maxCommandSize: 64 << 20, expected: 2 << 20},
		{importBatchSize: 128 << 20, maxCommandSize: 64 << 20, expected: 63 << 20},
		{importBatchSize: 64 << 20, maxCommandSize: 64 << 20, expected: 63 << 20},
		{importBatchSize: 63 << 20, maxCommandSize: 64 << 20, expected: 63 << 20},
	}
	for i, testCase := range testCases {
		st := cluster.MakeTestingClusterSettings()
		importBatchSize.Override(&st.SV, testCase.importBatchSize)
		kvserver.MaxCommandSize.Override(&st.SV, testCase.maxCommandSize)
		if e, a := MaxImportBatchSize(st), testCase.expected; e != a {
			t.Errorf("%d: expected max batch size %d, but got %d", i, e, a)
		}
	}
}

func slurpSSTablesLatestKey(
	t *testing.T, dir string, paths []string, oldPrefix, newPrefix []byte,
) []storage.MVCCKeyValue {
	start, end := storage.MVCCKey{Key: keys.MinKey}, storage.MVCCKey{Key: keys.MaxKey}
	kr := prefixRewriter{{OldPrefix: oldPrefix, NewPrefix: newPrefix}}

	e := storage.NewDefaultInMem()
	defer e.Close()
	batch := e.NewBatch()
	defer batch.Close()

	for _, path := range paths {
		sst, err := storage.NewSSTIterator(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}
		defer sst.Close()

		sst.SeekGE(start)
		for {
			if valid, err := sst.Valid(); !valid || err != nil {
				if err != nil {
					t.Fatal(err)
				}
				break
			}
			if !sst.UnsafeKey().Less(end) {
				break
			}
			var ok bool
			var newKv storage.MVCCKeyValue
			key := sst.UnsafeKey()
			newKv.Value = append(newKv.Value, sst.UnsafeValue()...)
			newKv.Key.Key = append(newKv.Key.Key, key.Key...)
			newKv.Key.Timestamp = key.Timestamp
			newKv.Key.Key, ok = kr.rewriteKey(newKv.Key.Key)
			if !ok {
				t.Fatalf("could not rewrite key: %s", newKv.Key.Key)
			}
			v := roachpb.Value{RawBytes: newKv.Value}
			v.ClearChecksum()
			v.InitChecksum(newKv.Key.Key)
			// TODO(sumeer): this will not be correct with the separated
			// lock table. We should iterate using EngineKey on the sst,
			// and expose a PutEngine method to write directly.
			if newKv.Key.Timestamp.IsEmpty() {
				if err := batch.PutUnversioned(newKv.Key.Key, v.RawBytes); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := batch.PutMVCC(newKv.Key, v.RawBytes); err != nil {
					t.Fatal(err)
				}
			}
			sst.Next()
		}
	}

	var kvs []storage.MVCCKeyValue
	it := batch.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: roachpb.KeyMax})
	defer it.Close()
	for it.SeekGE(start); ; it.NextKey() {
		if ok, err := it.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok || !it.UnsafeKey().Less(end) {
			break
		}
		kvs = append(kvs, storage.MVCCKeyValue{Key: it.Key(), Value: it.Value()})
	}
	return kvs
}

func clientKVsToEngineKVs(kvs []kv.KeyValue) []storage.MVCCKeyValue {
	var ret []storage.MVCCKeyValue
	for _, kv := range kvs {
		if kv.Value == nil {
			continue
		}
		k := storage.MVCCKey{
			Key:       kv.Key,
			Timestamp: kv.Value.Timestamp,
		}
		ret = append(ret, storage.MVCCKeyValue{Key: k, Value: kv.Value.RawBytes})
	}
	return ret
}

func mustMarshalDesc(t *testing.T, tableDesc *descpb.TableDescriptor) []byte {
	desc := tabledesc.NewImmutable(*tableDesc).DescriptorProto()
	// Set the timestamp to a non-zero value.
	descpb.TableFromDescriptor(desc, hlc.Timestamp{WallTime: 1})
	bytes, err := protoutil.Marshal(desc)
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

func TestImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("batch=default", func(t *testing.T) {
		runTestImport(t, func(_ *cluster.Settings) {})
	})
	t.Run("batch=1", func(t *testing.T) {
		// The test normally doesn't trigger the batching behavior, so lower
		// the threshold to force it.
		init := func(st *cluster.Settings) {
			importBatchSize.Override(&st.SV, 1)
		}
		runTestImport(t, init)
	})
}

func runTestImport(t *testing.T, init func(*cluster.Settings)) {
	defer leaktest.AfterTest(t)()

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	if err := os.Mkdir(filepath.Join(dir, "foo"), 0755); err != nil {
		t.Fatal(err)
	}

	const (
		oldID   = 51
		indexID = 1
	)

	srcPrefix := MakeKeyRewriterPrefixIgnoringInterleaved(oldID, indexID)
	var keys []roachpb.Key
	for i := 0; i < 8; i++ {
		key := append([]byte(nil), srcPrefix...)
		key = encoding.EncodeStringAscending(key, fmt.Sprintf("k%d", i))
		keys = append(keys, key)
	}

	writeSST := func(t *testing.T, offsets []int) string {
		path := strconv.FormatInt(hlc.UnixNano(), 10)

		sstFile := &storage.MemFile{}
		sst := storage.MakeBackupSSTWriter(sstFile)
		defer sst.Close()
		ts := hlc.NewClock(hlc.UnixNano, time.Nanosecond).Now()
		value := roachpb.MakeValueFromString("bar")
		for _, idx := range offsets {
			key := keys[idx]
			value.ClearChecksum()
			value.InitChecksum(key)
			if err := sst.Put(storage.MVCCKey{Key: key, Timestamp: ts}, value.RawBytes); err != nil {
				t.Fatalf("%+v", err)
			}
		}
		if err := sst.Finish(); err != nil {
			t.Fatalf("%+v", err)
		}
		if err := ioutil.WriteFile(filepath.Join(dir, "foo", path), sstFile.Data(), 0644); err != nil {
			t.Fatalf("%+v", err)
		}
		return path
	}

	// Make the first few WriteBatch/AddSSTable calls return
	// AmbiguousResultError. Import should be resilient to this.
	const initialAmbiguousSubReqs = 3
	remainingAmbiguousSubReqs := int64(initialAmbiguousSubReqs)
	knobs := base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
		EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
			TestingEvalFilter: func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
				switch filterArgs.Req.(type) {
				case *roachpb.WriteBatchRequest, *roachpb.AddSSTableRequest:
				// No-op.
				default:
					return nil
				}
				r := atomic.AddInt64(&remainingAmbiguousSubReqs, -1)
				if r < 0 {
					return nil
				}
				return roachpb.NewError(roachpb.NewAmbiguousResultError(strconv.Itoa(int(r))))
			},
		},
	}}

	ctx := context.Background()
	args := base.TestServerArgs{Knobs: knobs, ExternalIODir: dir}
	// TODO(dan): This currently doesn't work with AddSSTable on in-memory
	// stores because RocksDB's InMemoryEnv doesn't support NewRandomRWFile
	// (which breaks the global-seqno rewrite used when the added sstable
	// overlaps with existing data in the RocksDB instance). #16345.
	args.StoreSpecs = []base.StoreSpec{{InMemory: false, Path: filepath.Join(dir, "testserver")}}
	s, _, kvDB := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)
	init(s.ClusterSettings())

	evalCtx := tree.EvalContext{Settings: s.ClusterSettings()}
	flowCtx := execinfra.FlowCtx{Cfg: &execinfra.ServerConfig{DB: kvDB,
		ExternalStorage: func(ctx context.Context, dest roachpb.ExternalStorage) (cloud.ExternalStorage, error) {
			return cloudimpl.MakeExternalStorage(ctx, dest, base.ExternalIODirConfig{},
				nil, blobs.TestBlobServiceClient(s.ClusterSettings().ExternalIODir), nil, nil)
		}}}

	storage, err := cloudimpl.ExternalStorageConfFromURI("nodelocal://0/foo", security.RootUserName())
	if err != nil {
		t.Fatalf("%+v", err)
	}

	const splitKey1, splitKey2 = 3, 5
	// Each test case consists of some number of batches of keys, represented as
	// ints [0, 8). Splits are at 3 and 5.
	for i, testCase := range [][][]int{
		// Simple cases, no spanning splits, try first, last, middle, etc in each.
		// r1
		{{0}},
		{{1}},
		{{2}},
		{{0, 1, 2}},
		{{0}, {1}, {2}},

		// r2
		{{3}},
		{{4}},
		{{3, 4}},
		{{3}, {4}},

		// r3
		{{5}},
		{{5, 6, 7}},
		{{6}},

		// batches exactly matching spans.
		{{0, 1, 2}, {3, 4}, {5, 6, 7}},

		// every key, in its own batch.
		{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}},

		// every key in one big batch.
		{{0, 1, 2, 3, 4, 5, 6, 7}},

		// Look for off-by-ones on and around the splits.
		{{2, 3}},
		{{1, 3}},
		{{2, 4}},
		{{1, 4}},
		{{1, 5}},
		{{2, 5}},

		// Mixture of split-aligned and non-aligned batches.
		{{1}, {5}, {6}},
		{{1, 2, 3}, {4, 5}, {6, 7}},
		{{0}, {2, 3, 5}, {7}},
		{{0, 4}, {5, 7}},
		{{0, 3}, {4}},
	} {
		t.Run(fmt.Sprintf("%d-%v", i, testCase), func(t *testing.T) {
			newID := descpb.ID(100 + i)
			newPrefix := MakeKeyRewriterPrefixIgnoringInterleaved(newID, indexID)
			kr := prefixRewriter{{OldPrefix: srcPrefix, NewPrefix: newPrefix}}
			rekeys := []execinfrapb.TableRekey{
				{
					OldID: oldID,
					NewDesc: mustMarshalDesc(t, &descpb.TableDescriptor{
						ID: newID,
						PrimaryIndex: descpb.IndexDescriptor{
							ID: indexID,
						},
					}),
				},
			}

			first := keys[testCase[0][0]]
			last := keys[testCase[len(testCase)-1][len(testCase[len(testCase)-1])-1]]

			reqStartKey, ok := kr.rewriteKey(append([]byte(nil), keys[0]...))
			if !ok {
				t.Fatalf("failed to rewrite key: %s", reqStartKey)
			}
			reqEndKey, ok := kr.rewriteKey(append([]byte(nil), keys[len(keys)-1].PrefixEnd()...))
			if !ok {
				t.Fatalf("failed to rewrite key: %s", reqEndKey)
			}
			reqMidKey1, ok := kr.rewriteKey(append([]byte(nil), keys[splitKey1]...))
			if !ok {
				t.Fatalf("failed to rewrite key: %s", reqMidKey1)
			}
			reqMidKey2, ok := kr.rewriteKey(append([]byte(nil), keys[splitKey2]...))
			if !ok {
				t.Fatalf("failed to rewrite key: %s", reqMidKey2)
			}

			if err := kvDB.AdminSplit(ctx, reqMidKey1, hlc.MaxTimestamp /* expirationTime */); err != nil {
				t.Fatal(err)
			}
			if err := kvDB.AdminSplit(ctx, reqMidKey2, hlc.MaxTimestamp /* expirationTime */); err != nil {
				t.Fatal(err)
			}

			atomic.StoreInt64(&remainingAmbiguousSubReqs, initialAmbiguousSubReqs)

			mockRestoreDataSpec := execinfrapb.RestoreDataSpec{
				Rekeys: rekeys,
			}
			restoreSpanEntry := execinfrapb.RestoreSpanEntry{
				Span: roachpb.Span{Key: first, EndKey: last.PrefixEnd()},
			}

			var slurp []string
			for ks := range testCase {
				f := writeSST(t, testCase[ks])
				slurp = append(slurp, f)
				restoreSpanEntry.Files = append(restoreSpanEntry.Files,
					execinfrapb.RestoreFileSpec{Dir: storage, Path: f})
			}
			expectedKVs := slurpSSTablesLatestKey(t, filepath.Join(dir, "foo"), slurp, srcPrefix,
				newPrefix)

			mockRestoreDataProcessor, err := newTestingRestoreDataProcessor(ctx, &evalCtx,
				&flowCtx, mockRestoreDataSpec)
			require.NoError(t, err)

			_, err = mockRestoreDataProcessor.ProcessRestoreSpanEntry(restoreSpanEntry, reqStartKey)
			require.NoError(t, err)

			clientKVs, err := kvDB.Scan(ctx, reqStartKey, reqEndKey, 0)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			kvs := clientKVsToEngineKVs(clientKVs)

			if !reflect.DeepEqual(kvs, expectedKVs) {
				for i := 0; i < len(kvs) || i < len(expectedKVs); i++ {
					if i < len(expectedKVs) {
						t.Logf("expected %d\t%v\t%v", i, expectedKVs[i].Key, expectedKVs[i].Value)
					}
					if i < len(kvs) {
						t.Logf("got      %d\t%v\t%v", i, kvs[i].Key, kvs[i].Value)
					}
				}
				t.Fatalf("got %+v expected %+v", kvs, expectedKVs)
			}

			if r := atomic.LoadInt64(&remainingAmbiguousSubReqs); r > 0 {
				t.Errorf("expected ambiguous sub-requests to be depleted got %d", r)
			}
		})
	}
}
