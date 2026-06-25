// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type fakeObjectStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newFakeObjectStore() *fakeObjectStore {
	return &fakeObjectStore{objects: map[string][]byte{}}
}

func objectKey(bucket, object string) string {
	return bucket + "/" + object
}

func (f *fakeObjectStore) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	content, ok := f.objects[objectKey(bucket, object)]
	if !ok {
		return nil, fmt.Errorf("object %s not found", objectKey(bucket, object))
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func (f *fakeObjectStore) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	content, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[objectKey(bucket, object)] = content
	return nil
}

func (f *fakeObjectStore) setGSURL(gsURL string, content []byte) {
	trimmed := strings.TrimPrefix(gsURL, "gs://")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		panic(fmt.Sprintf("invalid fake gs URL %q", gsURL))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[objectKey(parts[0], parts[1])] = content
}

type fakePullCache struct {
	tar []byte
}

func (f *fakePullCache) Fetch(ctx context.Context, ref string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.tar)), nil
}

type recordingAteomDialer struct {
	client  *fakeAteomClient
	lastUID string
	conn    *grpc.ClientConn
}

func newRecordingAteomDialer(t *testing.T) *recordingAteomDialer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening for fake ateom: %v", err)
	}
	svr := grpc.NewServer()
	d := &recordingAteomDialer{client: &fakeAteomClient{}}
	ateompb.RegisterAteomServer(svr, d.client)
	go func() {
		_ = svr.Serve(lis)
	}()
	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("creating fake ateom client connection: %v", err)
	}
	d.conn = conn
	t.Cleanup(func() {
		conn.Close()
		svr.Stop()
		lis.Close()
	})
	return d
}

func (d *recordingAteomDialer) DialAteomPod(ctx context.Context, targetAteomUid string) (*grpc.ClientConn, error) {
	d.lastUID = targetAteomUid
	return d.conn, nil
}

type fakeAteomClient struct {
	ateompb.UnimplementedAteomServer

	runReq        *ateompb.RunWorkloadRequest
	checkpointReq *ateompb.CheckpointWorkloadRequest
	restoreReq    *ateompb.RestoreWorkloadRequest
}

func (f *fakeAteomClient) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (*ateompb.RunWorkloadResponse, error) {
	f.runReq = req
	return &ateompb.RunWorkloadResponse{}, nil
}

func (f *fakeAteomClient) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	f.checkpointReq = req
	checkpointDir := checkpointStateDir(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())
	for _, fileName := range []string{"checkpoint.img", "pages.img", "pages_meta.img"} {
		if err := os.WriteFile(filepath.Join(checkpointDir, fileName), []byte(fileName), 0o600); err != nil {
			return nil, err
		}
	}
	return &ateompb.CheckpointWorkloadResponse{}, nil
}

func (f *fakeAteomClient) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (*ateompb.RestoreWorkloadResponse, error) {
	f.restoreReq = req
	return &ateompb.RestoreWorkloadResponse{}, nil
}

// ateomWorkloadReq is the set of fields atelet must propagate identically onto
// every ateom workload request; all three request types satisfy it.
type ateomWorkloadReq interface {
	GetActorTemplateNamespace() string
	GetActorTemplateName() string
	GetActorId() string
	GetRunscPath() string
}

func assertAteomReq(t *testing.T, rpc string, got ateomWorkloadReq, wantNS, wantTmpl, wantID, wantRunsc string) {
	t.Helper()
	if g := got.GetActorTemplateNamespace(); g != wantNS {
		t.Errorf("%s ActorTemplateNamespace = %q, want %q", rpc, g, wantNS)
	}
	if g := got.GetActorTemplateName(); g != wantTmpl {
		t.Errorf("%s ActorTemplateName = %q, want %q", rpc, g, wantTmpl)
	}
	if g := got.GetActorId(); g != wantID {
		t.Errorf("%s ActorId = %q, want %q", rpc, g, wantID)
	}
	if g := got.GetRunscPath(); g != wantRunsc {
		t.Errorf("%s RunscPath = %q, want %q", rpc, g, wantRunsc)
	}
}

func TestWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "actor-id")

	// One shared write over an existing value, as happens on every resume;
	// each subtest checks one postcondition.
	if err := os.WriteFile(target, []byte("golden-id"), 0o600); err != nil {
		t.Fatalf("seeding target: %v", err)
	}
	if err := writeFileAtomic(target, []byte("counter-1"), 0o644); err != nil {
		t.Fatalf("writeFileAtomic: %v", err)
	}

	t.Run("replaces content", func(t *testing.T) {
		got, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("reading target: %v", err)
		}
		if string(got) != "counter-1" {
			t.Errorf("content = %q, want %q", got, "counter-1")
		}
	})

	t.Run("sets permissions", func(t *testing.T) {
		info, err := os.Stat(target)
		if err != nil {
			t.Fatalf("stat target: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o644 {
			t.Errorf("perm = %o, want 644", perm)
		}
	})

	t.Run("leaves no temp files", func(t *testing.T) {
		// The directory is visible inside the actor.
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading dir: %v", err)
		}
		if len(entries) != 1 {
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}
			t.Errorf("leftover files in identity dir: %v", names)
		}
	})
}

func TestValidateActorRequest(t *testing.T) {
	const okNS, okTmpl, okID, okUID = "ate-demo", "counter", "counter-1", "422938ba-8860-4983-a25d-d6bcb0a69d4e"
	okSpec := &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}}

	tests := []struct {
		name              string
		ns, tmpl, id, uid string
		spec              *ateletpb.WorkloadSpec
		wantErr           bool
	}{
		{"all valid", okNS, okTmpl, okID, okUID, okSpec, false},
		{"bad namespace", "../x", okTmpl, okID, okUID, okSpec, true},
		{"bad actor id", okNS, okTmpl, "../x", okUID, okSpec, true},
		{"bad uid", okNS, okTmpl, okID, "../x", okSpec, true},
		{"bad container", okNS, okTmpl, okID, okUID, &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "../x"}}}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateActorRequest(tc.ns, tc.tmpl, tc.id, tc.uid, tc.spec); (err != nil) != tc.wantErr {
				t.Errorf("validateActorRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// validRunRequest, validCheckpointRequest, and validRestoreRequest build
// requests whose every field passes validation; the per-request tests below
// break one field per case.
func validRunRequest() *ateletpb.RunRequest {
	return &ateletpb.RunRequest{
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		ActorId:                "counter-1",
		TargetAteomUid:         "422938ba-8860-4983-a25d-d6bcb0a69d4e",
		Spec: &ateletpb.WorkloadSpec{
			PauseImage: "example.com/pause:latest",
			Containers: []*ateletpb.Container{{Name: "worker", Image: "example.com/worker:latest", Command: []string{"/worker"}}},
		},
	}
}

func validCheckpointRequest() *ateletpb.CheckpointRequest {
	return &ateletpb.CheckpointRequest{
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		ActorId:                "counter-1",
		TargetAteomUid:         "422938ba-8860-4983-a25d-d6bcb0a69d4e",
		Spec: &ateletpb.WorkloadSpec{
			PauseImage: "example.com/pause:latest",
			Containers: []*ateletpb.Container{{Name: "worker", Image: "example.com/worker:latest", Command: []string{"/worker"}}},
		},
		Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.CheckpointRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
				SnapshotUriPrefix: "gs://bucket/actors/1/snapshots/2/",
			},
		},
	}
}

func validRestoreRequest() *ateletpb.RestoreRequest {
	return &ateletpb.RestoreRequest{
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		ActorId:                "counter-1",
		TargetAteomUid:         "422938ba-8860-4983-a25d-d6bcb0a69d4e",
		Spec: &ateletpb.WorkloadSpec{
			PauseImage: "example.com/pause:latest",
			Containers: []*ateletpb.Container{{Name: "worker", Image: "example.com/worker:latest", Command: []string{"/worker"}}},
		},
		Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.RestoreRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
				SnapshotUriPrefix: "gs://bucket/actors/1/snapshots/2/",
			},
		},
	}
}

func withTempAteomPath(t *testing.T) {
	t.Helper()
	basePath := t.TempDir()
	origActorPath := actorPath
	actorPath = func(actorTemplateNamespace, actorTemplateName, actorID string) string {
		return filepath.Join(basePath, "actors", actorTemplateNamespace+":"+actorTemplateName+":"+actorID)
	}
	origStaticFilesDir := ateompath.StaticFilesDir
	ateompath.StaticFilesDir = filepath.Join(basePath, "static-files")
	t.Cleanup(func() {
		actorPath = origActorPath
		ateompath.StaticFilesDir = origStaticFilesDir
	})
}

func testTar(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, hdr := range []*tar.Header{
		{Name: "bin", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "bin/worker", Typeflag: tar.TypeReg, Mode: 0o755, Size: int64(len("worker"))},
	} {
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("writing tar header: %v", err)
		}
		if hdr.Typeflag == tar.TypeReg {
			if _, err := tw.Write([]byte("worker")); err != nil {
				t.Fatalf("writing tar body: %v", err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("closing tar writer: %v", err)
	}
	return buf.Bytes()
}

func zstdBytes(t *testing.T, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatalf("creating zstd writer: %v", err)
	}
	if _, err := zw.Write(content); err != nil {
		t.Fatalf("writing zstd content: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("closing zstd writer: %v", err)
	}
	return buf.Bytes()
}

func testService(t *testing.T) (*AteomHerder, *recordingAteomDialer, *fakeObjectStore, string) {
	t.Helper()
	withTempAteomPath(t)
	runscContent := []byte("runsc")
	runscSum := sha256.Sum256(runscContent)
	runscHash := hex.EncodeToString(runscSum[:])
	anonStorage := newFakeObjectStore()
	stateStorage := newFakeObjectStore()
	anonStorage.setGSURL("gs://runtime/runsc", runscContent)
	dialer := newRecordingAteomDialer(t)
	s := newAteomHerder(dialer, &fakePullCache{tar: testTar(t)}, anonStorage, stateStorage)
	return s, dialer, stateStorage, runscHash
}

func sandboxAssets(runscHash string) *ateletpb.SandboxAssets {
	return &ateletpb.SandboxAssets{
		SandboxClass: "gvisor",
		Assets: map[string]*ateletpb.ArchAssets{
			runtime.GOARCH: {
				Files: map[string]*ateletpb.AssetFile{
					"runsc": {Url: "gs://runtime/runsc", Sha256: runscHash},
				},
			},
		},
	}
}

func TestRPCBoundariesAcceptHappyPathsWithFakes(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, s *AteomHerder, dialer *recordingAteomDialer, stateStorage *fakeObjectStore, runscHash string)
	}{
		{
			name: "Run",
			run: func(t *testing.T, s *AteomHerder, dialer *recordingAteomDialer, stateStorage *fakeObjectStore, runscHash string) {
				req := validRunRequest()
				req.SandboxAssets = sandboxAssets(runscHash)

				if _, err := s.Run(context.Background(), req); err != nil {
					t.Fatalf("Run returned error: %v", err)
				}
				assertDialedAteom(t, dialer, req.GetTargetAteomUid())
				if dialer.client.runReq == nil {
					t.Fatal("Run did not call ateom RunWorkload")
				}
				assertAteomReq(t, "Run", dialer.client.runReq, req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId(), ateompath.RunSCBinaryPath(runscHash))
				assertIdentityFile(t, req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())
			},
		},
		{
			name: "Checkpoint external",
			run: func(t *testing.T, s *AteomHerder, dialer *recordingAteomDialer, stateStorage *fakeObjectStore, runscHash string) {
				req := validCheckpointRequest()
				writeTestSandboxRecord(t, req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId(), runscHash)
				if err := resetActorDirs(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()); err != nil {
					t.Fatalf("creating actor dirs: %v", err)
				}

				if _, err := s.Checkpoint(context.Background(), req); err != nil {
					t.Fatalf("Checkpoint returned error: %v", err)
				}
				assertDialedAteom(t, dialer, req.GetTargetAteomUid())
				if dialer.client.checkpointReq == nil {
					t.Fatal("Checkpoint did not call ateom CheckpointWorkload")
				}
				assertAteomReq(t, "Checkpoint", dialer.client.checkpointReq, req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId(), ateompath.RunSCBinaryPath(runscHash))
				assertUploadedObjects(t, stateStorage, "bucket", []string{"actors/1/snapshots/2/checkpoint.img.zstd", "actors/1/snapshots/2/pages.img.zstd", "actors/1/snapshots/2/pages_meta.img.zstd", "actors/1/snapshots/2/manifest.json"})
			},
		},
		{
			name: "Restore external",
			run: func(t *testing.T, s *AteomHerder, dialer *recordingAteomDialer, stateStorage *fakeObjectStore, runscHash string) {
				req := validRestoreRequest()
				seedExternalSnapshot(t, stateStorage, runscHash)

				if _, err := s.Restore(context.Background(), req); err != nil {
					t.Fatalf("Restore returned error: %v", err)
				}
				assertDialedAteom(t, dialer, req.GetTargetAteomUid())
				if dialer.client.restoreReq == nil {
					t.Fatal("Restore did not call ateom RestoreWorkload")
				}
				assertAteomReq(t, "Restore", dialer.client.restoreReq, req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId(), ateompath.RunSCBinaryPath(runscHash))
				assertRecordedRunscHash(t, req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId(), runscHash)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, dialer, stateStorage, runscHash := testService(t)
			tt.run(t, s, dialer, stateStorage, runscHash)
		})
	}
}

func assertDialedAteom(t *testing.T, dialer *recordingAteomDialer, wantUID string) {
	t.Helper()
	if dialer.lastUID != wantUID {
		t.Errorf("dialed ateom UID = %q, want %q", dialer.lastUID, wantUID)
	}
}

func assertIdentityFile(t *testing.T, ns, tmpl, actorID string) {
	t.Helper()
	identityFile := filepath.Join(actorIdentityDirPath(ns, tmpl, actorID), ActorIDFileName)
	gotID, err := os.ReadFile(identityFile)
	if err != nil {
		t.Fatalf("reading identity file: %v", err)
	}
	if string(gotID) != actorID {
		t.Errorf("identity file = %q, want %q", gotID, actorID)
	}
}

func assertUploadedObjects(t *testing.T, storage *fakeObjectStore, bucket string, objects []string) {
	t.Helper()
	storage.mu.Lock()
	defer storage.mu.Unlock()
	for _, object := range objects {
		if _, ok := storage.objects[objectKey(bucket, object)]; !ok {
			t.Errorf("expected upload of gs://%s/%s", bucket, object)
		}
	}
}

func assertRecordedRunscHash(t *testing.T, ns, tmpl, actorID, wantHash string) {
	t.Helper()
	gotRec, err := readSandboxRecord(ns, tmpl, actorID)
	if err != nil {
		t.Fatalf("reading sandbox record: %v", err)
	}
	if got := gotRec.Assets["runsc"].SHA256; got != wantHash {
		t.Errorf("recorded runsc sha256 = %q, want %q", got, wantHash)
	}
}

func writeTestSandboxRecord(t *testing.T, ns, tmpl, actorID, runscHash string) {
	t.Helper()
	rec := &sandboxAssetsRecord{SandboxClass: "gvisor", Assets: map[string]assetEntry{"runsc": {URL: "gs://runtime/runsc", SHA256: runscHash}}}
	if err := writeSandboxRecord(ns, tmpl, actorID, rec); err != nil {
		t.Fatalf("writing sandbox record: %v", err)
	}
}

func seedExternalSnapshot(t *testing.T, storage *fakeObjectStore, runscHash string) {
	t.Helper()
	manifest := fmt.Sprintf(`{"sandboxClass":"gvisor","assets":{"runsc":{"url":"gs://runtime/runsc","sha256":%q}}}`, runscHash)
	storage.setGSURL("gs://bucket/actors/1/snapshots/2/manifest.json", []byte(manifest))
	for _, fileName := range []string{"checkpoint.img", "pages.img", "pages_meta.img"} {
		storage.setGSURL("gs://bucket/actors/1/snapshots/2/"+fileName+".zstd", zstdBytes(t, []byte(fileName)))
	}
}

// TestRPCBoundariesLocalCheckpointRestoreRoundTripWithFakes exercises the LOCAL
// checkpoint and restore branches end to end (moveLocalCheckpoint /
// copyLocalCheckpoint), which the EXTERNAL happy paths above do not cover. It
// checkpoints to a local prefix, asserts the manifest and images land under
// LocalCheckpointsDir/<prefix>, then restores from the same prefix.
func TestRPCBoundariesLocalCheckpointRestoreRoundTripWithFakes(t *testing.T) {
	s, dialer, _, runscHash := testService(t)
	const prefix = "counter-1-2026-06-24T00:00:00Z-abcd"
	ns, tmpl, id := "ate-demo", "counter", "counter-1"

	// --- Checkpoint (LOCAL) ---
	cpReq := validCheckpointRequest()
	cpReq.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
	cpReq.Config = &ateletpb.CheckpointRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotPrefix: prefix}}

	rec := &sandboxAssetsRecord{SandboxClass: "gvisor", Assets: map[string]assetEntry{"runsc": {URL: "gs://runtime/runsc", SHA256: runscHash}}}
	if err := writeSandboxRecord(ns, tmpl, id, rec); err != nil {
		t.Fatalf("writing sandbox record: %v", err)
	}
	if err := resetActorDirs(ns, tmpl, id); err != nil {
		t.Fatalf("creating actor dirs: %v", err)
	}

	if _, err := s.Checkpoint(context.Background(), cpReq); err != nil {
		t.Fatalf("local Checkpoint returned error: %v", err)
	}

	localDir := filepath.Join(localCheckpointsDir(ns, tmpl, id), prefix)
	manifestBytes, err := os.ReadFile(filepath.Join(localDir, sandboxManifestName))
	if err != nil {
		t.Fatalf("reading local manifest: %v", err)
	}
	gotRec, err := unmarshalSandboxRecord(manifestBytes)
	if err != nil {
		t.Fatalf("parsing local manifest: %v", err)
	}
	if got := gotRec.Assets["runsc"].SHA256; got != runscHash {
		t.Errorf("local manifest runsc sha256 = %q, want %q", got, runscHash)
	}
	for _, fileName := range []string{"checkpoint.img", "pages.img", "pages_meta.img"} {
		if _, err := os.Stat(filepath.Join(localDir, fileName)); err != nil {
			t.Errorf("expected %s under local checkpoint dir: %v", fileName, err)
		}
	}

	// --- Restore (LOCAL) reusing the same prefix ---
	rsReq := validRestoreRequest()
	rsReq.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
	rsReq.Config = &ateletpb.RestoreRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotPrefix: prefix}}

	if _, err := s.Restore(context.Background(), rsReq); err != nil {
		t.Fatalf("local Restore returned error: %v", err)
	}
	if dialer.client.restoreReq == nil {
		t.Fatal("local Restore did not call ateom RestoreWorkload")
	}
	assertAteomReq(t, "Restore", dialer.client.restoreReq, ns, tmpl, id, ateompath.RunSCBinaryPath(runscHash))

	restoreDir := restoreStateDir(ns, tmpl, id)
	for _, fileName := range []string{"checkpoint.img", "pages.img", "pages_meta.img"} {
		if _, err := os.Stat(filepath.Join(restoreDir, fileName)); err != nil {
			t.Errorf("expected %s copied into restore-state dir: %v", fileName, err)
		}
	}
	gotRec, err = readSandboxRecord(ns, tmpl, id)
	if err != nil {
		t.Fatalf("reading sandbox record after local Restore: %v", err)
	}
	if got := gotRec.Assets["runsc"].SHA256; got != runscHash {
		t.Errorf("recorded runsc sha256 after local Restore = %q, want %q", got, runscHash)
	}
}

func TestValidateRunRequest(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ateletpb.RunRequest)
		wantErr bool
	}{
		{"valid", func(*ateletpb.RunRequest) {}, false},
		{"invalid ateom uid", func(r *ateletpb.RunRequest) { r.TargetAteomUid = "../escape" }, true},
		{"invalid actor id", func(r *ateletpb.RunRequest) { r.ActorId = "../escape" }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validRunRequest()
			tc.mutate(req)
			if err := validateRunRequest(req); (err != nil) != tc.wantErr {
				t.Errorf("validateRunRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// Checkpoint and Restore must reject a bad snapshot URI prefix even when
// every common field is valid.
func TestValidateCheckpointRequest(t *testing.T) {
	makeReq := func(opts ...func(*ateletpb.CheckpointRequest)) *ateletpb.CheckpointRequest {
		r := validCheckpointRequest()
		for _, opt := range opts {
			opt(r)
		}
		return r
	}
	localPrefix := func(p string) func(*ateletpb.CheckpointRequest) {
		return func(r *ateletpb.CheckpointRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.CheckpointRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotPrefix: p}}
		}
	}

	tests := []struct {
		name    string
		req     *ateletpb.CheckpointRequest
		wantErr bool
	}{
		{"valid", makeReq(), false},
		{"empty snapshot uri", makeReq(func(r *ateletpb.CheckpointRequest) { r.GetExternalConfig().SnapshotUriPrefix = "" }), true},
		{"bucketless snapshot uri", makeReq(func(r *ateletpb.CheckpointRequest) { r.GetExternalConfig().SnapshotUriPrefix = "relative/path" }), true},
		{"invalid ateom uid", makeReq(func(r *ateletpb.CheckpointRequest) { r.TargetAteomUid = "../escape" }), true},
		{"empty local snapshot prefix", makeReq(localPrefix("")), true},
		{"valid local snapshot prefix", makeReq(localPrefix("counter-1-2026-06-24T00:00:00Z-abcd")), false},
		{"unspecified snapshot type", makeReq(func(r *ateletpb.CheckpointRequest) { r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_UNSPECIFIED }), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateCheckpointRequest(tc.req); (err != nil) != tc.wantErr {
				t.Errorf("validateCheckpointRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateRestoreRequest(t *testing.T) {
	makeReq := func(opts ...func(*ateletpb.RestoreRequest)) *ateletpb.RestoreRequest {
		r := validRestoreRequest()
		for _, opt := range opts {
			opt(r)
		}
		return r
	}
	localPrefix := func(p string) func(*ateletpb.RestoreRequest) {
		return func(r *ateletpb.RestoreRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.RestoreRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotPrefix: p}}
		}
	}

	tests := []struct {
		name    string
		req     *ateletpb.RestoreRequest
		wantErr bool
	}{
		{"valid", makeReq(), false},
		{"empty snapshot uri", makeReq(func(r *ateletpb.RestoreRequest) { r.GetExternalConfig().SnapshotUriPrefix = "" }), true},
		{"bucketless snapshot uri", makeReq(func(r *ateletpb.RestoreRequest) { r.GetExternalConfig().SnapshotUriPrefix = "relative/path" }), true},
		{"invalid ateom uid", makeReq(func(r *ateletpb.RestoreRequest) { r.TargetAteomUid = "../escape" }), true},
		{"empty local snapshot prefix", makeReq(localPrefix("")), true},
		{"valid local snapshot prefix", makeReq(localPrefix("counter-1-2026-06-24T00:00:00Z-abcd")), false},
		{"unspecified snapshot type", makeReq(func(r *ateletpb.RestoreRequest) { r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_UNSPECIFIED }), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateRestoreRequest(tc.req); (err != nil) != tc.wantErr {
				t.Errorf("validateRestoreRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestFetchAssetRejectsBadHash confirms fetchAsset validates the asset hash
// before the cache-hit os.Stat/early-return, not merely "at some point". To
// prove the ordering, it plants a real file at the exact path an invalid hash
// resolves to: a correctly-ordered fetchAsset validates first and returns an
// error, while a regression that stats first would find this file and return it
// with a nil error, failing the test. StaticFilesDir is redirected to a temp dir
// so the cache dir is writable and isolated.
func TestFetchAssetRejectsBadHash(t *testing.T) {
	withTempAteomPath(t)
	if err := os.MkdirAll(ateompath.StaticFilesDir, 0o755); err != nil {
		t.Fatalf("creating static files dir: %v", err)
	}

	// Invalid (8 chars, not 64) but separator-free, so it resolves to a normal
	// filename inside the temp StaticFilesDir.
	const badHash = "deadbeef"
	if err := os.WriteFile(ateompath.RunSCBinaryPath(badHash), []byte("planted"), 0o755); err != nil {
		t.Fatalf("planting cache file: %v", err)
	}

	s := &AteomHerder{}
	if _, err := s.fetchAsset(context.Background(), assetEntry{SHA256: badHash}); err == nil {
		t.Error("fetchAsset returned a cache hit for an invalid hash; validation must run before the os.Stat early return")
	}
}

// fakeObjectStorage serves fixed bytes for GetObject so fetchAsset can be tested.
type fakeObjectStorage struct {
	data []byte
	err  error
}

func (f fakeObjectStorage) GetObject(_ context.Context, _, _ string) (io.ReadCloser, error) {
	if f.err != nil {
		return nil, f.err
	}
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

func (fakeObjectStorage) PutObject(_ context.Context, _, _ string, _ io.Reader) error { return nil }

// TestFetchAssetStreaming covers the streamed download: good asset cached,
// over-cap rejected, hash mismatch rejected (failures leave no cache file).
func TestFetchAssetStreaming(t *testing.T) {
	origCap := maxAssetBytes
	t.Cleanup(func() { maxAssetBytes = origCap })

	content := []byte("micro-vm kernel bytes")
	goodHash := fmt.Sprintf("%x", sha256.Sum256(content))
	const url = "gs://test-bucket/asset"

	t.Run("good asset is cached", func(t *testing.T) {
		withTempAteomPath(t)
		if err := os.MkdirAll(ateompath.StaticFilesDir, 0o755); err != nil {
			t.Fatalf("creating static files dir: %v", err)
		}
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		path, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: goodHash})
		if err != nil {
			t.Fatalf("fetchAsset: %v", err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading cached asset: %v", err)
		}
		if !bytes.Equal(got, content) {
			t.Errorf("cached bytes = %q, want %q", got, content)
		}
	})

	t.Run("over-cap asset rejected, cache not written", func(t *testing.T) {
		withTempAteomPath(t)
		if err := os.MkdirAll(ateompath.StaticFilesDir, 0o755); err != nil {
			t.Fatalf("creating static files dir: %v", err)
		}
		maxAssetBytes = 4 // content is longer than this
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		if _, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: goodHash}); err == nil {
			t.Fatal("fetchAsset accepted an over-cap asset")
		}
		if _, err := os.Stat(ateompath.RunSCBinaryPath(goodHash)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("over-cap download left a file at the cache path (stat err = %v)", err)
		}
	})

	t.Run("hash mismatch rejected, cache not written", func(t *testing.T) {
		withTempAteomPath(t)
		if err := os.MkdirAll(ateompath.StaticFilesDir, 0o755); err != nil {
			t.Fatalf("creating static files dir: %v", err)
		}
		maxAssetBytes = origCap
		wrongHash := strings.Repeat("a", 64) // valid 64-hex format, wrong value
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		if _, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: wrongHash}); err == nil {
			t.Fatal("fetchAsset accepted a hash mismatch")
		}
		if _, err := os.Stat(ateompath.RunSCBinaryPath(wrongHash)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("mismatched download left a file at the cache path (stat err = %v)", err)
		}
	})
}

// TestRPCBoundariesReject confirms each of the three RPCs validates path inputs
// before touching its (here nil) dependencies. A traversal value must be
// rejected as InvalidArgument rather than panicking or surfacing as
// Internal. Guards against a future removal or reordering of the validation
// call at any boundary.
func TestRPCBoundariesReject(t *testing.T) {
	s := &AteomHerder{}
	ctx := context.Background()
	badUID := "../escape" // valid actor ref, invalid ateom UID
	const okNS, okTmpl, okID = "ate-demo", "counter", "counter-1"
	okSpec := &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}}

	wantInvalidArgument := func(t *testing.T, rpc string, err error) {
		t.Helper()
		if err == nil {
			t.Errorf("%s accepted an invalid target ateom UID", rpc)
			return
		}
		if code := status.Code(err); code != codes.InvalidArgument {
			t.Errorf("%s returned code %v, want InvalidArgument", rpc, code)
		}
	}

	t.Run("Run", func(t *testing.T) {
		_, err := s.Run(ctx, &ateletpb.RunRequest{
			ActorTemplateNamespace: okNS, ActorTemplateName: okTmpl, ActorId: okID,
			TargetAteomUid: badUID, Spec: okSpec,
		})
		wantInvalidArgument(t, "Run", err)
	})
	t.Run("Checkpoint", func(t *testing.T) {
		_, err := s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
			ActorTemplateNamespace: okNS, ActorTemplateName: okTmpl, ActorId: okID,
			TargetAteomUid: badUID, Spec: okSpec,
		})
		wantInvalidArgument(t, "Checkpoint", err)
	})
	t.Run("Restore", func(t *testing.T) {
		_, err := s.Restore(ctx, &ateletpb.RestoreRequest{
			ActorTemplateNamespace: okNS, ActorTemplateName: okTmpl, ActorId: okID,
			TargetAteomUid: badUID, Spec: okSpec,
		})
		wantInvalidArgument(t, "Restore", err)
	})
}
