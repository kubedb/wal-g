package storagetools

import (
	"bytes"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func newListTestFolder(t *testing.T) storage.Folder {
	folder := memory.NewFolder("test/", memory.NewKVS())
	for name, content := range map[string]string{
		"top":          "1",
		"a/one":        "22",
		"a/b/two":      "333",
		"a/b/c/three":  "4444",
		"x/b/four":     "55555",
		"x/other/five": "666666",
	} {
		require.NoError(t, folder.PutObject(name, bytes.NewBufferString(content)))
	}
	return folder
}

func decodeListing(t *testing.T, data []byte) map[string]listElementJSON {
	var list []listElementJSON
	require.NoError(t, json.Unmarshal(data, &list))
	out := map[string]listElementJSON{}
	for _, e := range list {
		out[e.Name] = e
	}
	require.Len(t, out, len(list))
	return out
}

func names(m map[string]listElementJSON) []string {
	var out []string
	for n := range m {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func TestWriteFolderListJSON(t *testing.T) {
	folder := newListTestFolder(t)

	var flat bytes.Buffer
	require.NoError(t, writeFolderListJSON(folder, false, &flat))
	got := decodeListing(t, flat.Bytes())
	assert.Equal(t, []string{"a/", "top", "x/"}, names(got))
	assert.Equal(t, Directory, got["a/"].Type)
	assert.Empty(t, got["a/"].Modified)
	assert.Equal(t, Object, got["top"].Type)
	assert.Equal(t, int64(1), got["top"].Size)
	modified, err := time.Parse(time.RFC3339Nano, got["top"].Modified)
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), modified, time.Minute)

	var deep bytes.Buffer
	require.NoError(t, writeFolderListJSON(folder.GetSubFolder("a"), true, &deep))
	got = decodeListing(t, deep.Bytes())
	assert.Equal(t, []string{"b/c/three", "b/two", "one"}, names(got))
	assert.Equal(t, int64(4), got["b/c/three"].Size)
	for _, e := range got {
		assert.Equal(t, Object, e.Type)
	}
}

func TestWriteFolderListWithGlobJSON(t *testing.T) {
	folder := newListTestFolder(t)

	var out bytes.Buffer
	require.NoError(t, writeFolderListWithGlobJSON(folder, "*/b", true, &out))
	got := decodeListing(t, out.Bytes())
	assert.Equal(t, []string{"a/b/c/three", "a/b/two", "x/b/four"}, names(got))
	assert.Equal(t, int64(5), got["x/b/four"].Size)

	out.Reset()
	require.NoError(t, writeFolderListWithGlobJSON(folder, "nothing*", false, &out))
	assert.Equal(t, "[]\n", out.String())
}

func TestWriteObjectsListJSONOfEmptyList(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, WriteObjectsListJSON(nil, &out))
	assert.Equal(t, "[]\n", out.String())
}
