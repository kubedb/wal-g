module github.com/wal-g/wal-g

go 1.25.0

require (
	cloud.google.com/go/storage v1.51.0
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.20.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.13.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.3
	github.com/RoaringBitmap/roaring v0.4.21
	github.com/aws/aws-sdk-go v1.55.6
	github.com/blang/semver v3.5.1+incompatible
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/cucumber/godog v0.12.5
	github.com/cyberdelia/lzo v0.0.0-20171006181345-d85071271a6f
	github.com/denisenkom/go-mssqldb v0.10.0
	github.com/docker/docker v28.5.2+incompatible
	github.com/go-mysql-org/go-mysql v1.14.1-0.20260227075927-498f8104b8ff
	github.com/go-sql-driver/mysql v1.9.3
	github.com/gofrs/flock v0.8.0
	github.com/golang/mock v1.4.4
	github.com/google/uuid v1.6.0
	github.com/greenplum-db/gp-common-go-libs v1.0.22
	github.com/hashicorp/golang-lru v1.0.2
	github.com/jackc/pglogrepl v0.0.0-20250325003938-6dfcde87cfc7
	github.com/jedib0t/go-pretty v4.3.0+incompatible
	github.com/magiconair/properties v1.8.1 // indirect
	github.com/minio/sio v0.2.0
	github.com/pierrec/lz4/v4 v4.1.21
	github.com/pkg/errors v0.9.1
	github.com/pkg/sftp v1.13.1
	github.com/spf13/cobra v1.10.1
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.10
	github.com/spf13/viper v1.7.0
	github.com/stretchr/testify v1.11.1
	github.com/ulikunitz/xz v0.5.15
	github.com/wal-g/json v0.3.1
	github.com/wal-g/tracelog v0.1.1
	github.com/yandex-cloud/go-genproto v0.0.0-20230918115514-93a99045c9de
	github.com/yandex-cloud/go-sdk v0.0.0-20230918120620-9e95f0816d79
	go.mongodb.org/mongo-driver v1.17.1
	golang.org/x/crypto v0.47.0
	golang.org/x/sync v0.19.0
	golang.org/x/time v0.14.0
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da
	google.golang.org/api v0.255.0
	gopkg.in/ini.v1 v1.67.0
)

require (
	github.com/ProtonMail/go-crypto v1.3.0
	github.com/aliyun/alibabacloud-oss-go-sdk-v2 v1.2.3
	github.com/aliyun/credentials-go v1.4.5
	github.com/cactus/go-statsd-client/v5 v5.0.0
	github.com/google/brotli/go/cbrotli v0.0.0-20220110100810-f4153a09f87c
	github.com/klauspost/compress v1.18.4
	github.com/mongodb/mongo-tools v0.0.0-20240724183527-6d4f001be3fc
	github.com/ncw/directio v1.0.5
	github.com/ncw/swift/v2 v2.0.2
	github.com/pkg/profile v1.6.0
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/client_model v0.6.2
	github.com/redis/go-redis/v9 v9.7.3
	golang.org/x/mod v0.32.0
	golang.org/x/net v0.49.0
	golang.org/x/sys v0.40.0
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/apimachinery v0.35.3
	k8s.io/client-go v0.35.3
	k8s.io/klog/v2 v2.130.1
	kmodules.xyz/client-go v0.34.3
	kubedb.dev/apimachinery v0.63.1-0.20260402121059-f57d49cdeb54
	kubestash.dev/apimachinery v0.26.1-0.20260406104729-ca5351d933aa
	sigs.k8s.io/controller-runtime v0.22.4
)

require (
	cel.dev/expr v0.25.1 // indirect
	cloud.google.com/go/auth v0.17.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/iam v1.5.2 // indirect
	cloud.google.com/go/monitoring v1.24.2 // indirect
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.30.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.51.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.51.0 // indirect
	github.com/Masterminds/semver/v3 v3.4.0 // indirect
	github.com/alibabacloud-go/debug v1.0.1 // indirect
	github.com/cert-manager/cert-manager v1.19.4 // indirect
	github.com/cncf/xds/go v0.0.0-20251210132809-ee656c7534f5 // indirect
	github.com/containerd/errdefs v1.0.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/cyphar/filepath-securejoin v0.6.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/emicklei/go-restful/v3 v3.13.0 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.36.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.0 // indirect
	github.com/evanphx/json-patch v5.9.11+incompatible // indirect
	github.com/evanphx/json-patch/v5 v5.9.11 // indirect
	github.com/fatih/structs v1.1.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fxamacker/cbor/v2 v2.9.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.3 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v0.22.1 // indirect
	github.com/go-openapi/jsonreference v0.21.2 // indirect
	github.com/go-openapi/swag v0.25.1 // indirect
	github.com/go-openapi/swag/cmdutils v0.25.1 // indirect
	github.com/go-openapi/swag/conv v0.25.1 // indirect
	github.com/go-openapi/swag/fileutils v0.25.1 // indirect
	github.com/go-openapi/swag/jsonname v0.25.1 // indirect
	github.com/go-openapi/swag/jsonutils v0.25.1 // indirect
	github.com/go-openapi/swag/loading v0.25.1 // indirect
	github.com/go-openapi/swag/mangling v0.25.1 // indirect
	github.com/go-openapi/swag/netutils v0.25.1 // indirect
	github.com/go-openapi/swag/stringutils v0.25.1 // indirect
	github.com/go-openapi/swag/typeutils v0.25.1 // indirect
	github.com/go-openapi/swag/yamlutils v0.25.1 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.0 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/gnostic-models v0.7.0 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/go-containerregistry v0.20.7 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.6 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.14.3 // indirect
	github.com/jackc/pgproto3/v2 v2.3.3 // indirect
	github.com/jackc/pgtype v1.14.0 // indirect
	github.com/jackc/pgx/v4 v4.18.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.2.5 // indirect
	github.com/kubernetes-csi/external-snapshotter/client/v8 v8.4.0 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/sys/sequential v0.6.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/morikuni/aec v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/onsi/gomega v1.38.2 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/pingcap/log v1.1.1-0.20241212030209-7e3ff8601a2a // indirect
	github.com/pingcap/tidb/pkg/parser v0.0.0-20260219190905-9b9281fa8d6d // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring v0.87.1 // indirect
	github.com/prometheus-operator/prometheus-operator/pkg/client v0.87.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/sergi/go-diff v1.3.1 // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/yudai/gojsondiff v1.0.0 // indirect
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.39.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.61.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.64.0 // indirect
	go.opentelemetry.io/otel v1.40.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.39.0 // indirect
	go.opentelemetry.io/otel/metric v1.40.0 // indirect
	go.opentelemetry.io/otel/sdk v1.40.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.40.0 // indirect
	go.opentelemetry.io/otel/trace v1.40.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	go.virtual-secrets.dev/apimachinery v0.0.1 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	gomodules.xyz/encoding v0.0.8 // indirect
	gomodules.xyz/jsonpatch/v2 v2.5.0 // indirect
	gomodules.xyz/mergo v0.3.13 // indirect
	gomodules.xyz/pointer v0.1.0 // indirect
	gomodules.xyz/sets v0.2.1 // indirect
	gomodules.xyz/x v0.0.17 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	gopkg.in/evanphx/json-patch.v4 v4.13.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gotest.tools/v3 v3.5.2 // indirect
	k8s.io/api v0.35.3 // indirect
	k8s.io/apiextensions-apiserver v0.34.3 // indirect
	k8s.io/apiserver v0.34.3 // indirect
	k8s.io/kube-aggregator v0.34.3 // indirect
	k8s.io/kube-openapi v0.0.0-20250910181357-589584f1c912 // indirect
	k8s.io/metrics v0.34.3 // indirect
	k8s.io/utils v0.0.0-20251002143259-bc988d571ff4 // indirect
	kmodules.xyz/apiversion v0.2.0 // indirect
	kmodules.xyz/custom-resources v0.34.0 // indirect
	kmodules.xyz/monitoring-agent-api v0.34.1 // indirect
	kmodules.xyz/objectstore-api v0.34.0 // indirect
	kmodules.xyz/offshoot-api v0.34.0 // indirect
	kmodules.xyz/prober v0.34.0 // indirect
	kmodules.xyz/resource-metadata v0.42.4 // indirect
	kubeops.dev/csi-driver-cacerts v0.5.1-0.20260407041214-38ba0daf59e3 // indirect
	kubeops.dev/operator-shard-manager v0.0.5 // indirect
	kubeops.dev/petset v0.0.15 // indirect
	kubeops.dev/sidekick v0.0.12 // indirect
	open-cluster-management.io/api v1.2.0 // indirect
	sigs.k8s.io/gateway-api v1.4.0 // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.3.0 // indirect
	sigs.k8s.io/yaml v1.6.0 // indirect
	stash.appscode.dev/apimachinery v0.42.1 // indirect
)

require (
	cloud.google.com/go v0.120.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.2 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.6.0 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/asaskevich/govalidator v0.0.0-20210307081110-f21760c49a8d // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudflare/circl v1.6.3 // indirect
	github.com/cucumber/gherkin-go/v19 v19.0.3 // indirect
	github.com/cucumber/messages-go/v16 v16.0.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/docker/go-connections v0.5.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20190901134440-81cf024a9e0a // indirect
	github.com/go-openapi/errors v0.19.3 // indirect
	github.com/go-openapi/strfmt v0.19.4 // indirect
	github.com/gofrs/uuid v4.4.0+incompatible // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/googleapis/gax-go/v2 v2.15.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/go-memdb v1.3.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-version v1.7.0
	github.com/hashicorp/hcl v1.0.1-vault-7 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.9.1
	github.com/jessevdk/go-flags v1.6.1 // indirect
	github.com/jmespath/go-jmespath v0.4.1-0.20220621161143-b0104c826a24 // indirect
	github.com/jmoiron/sqlx v1.4.0 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/pelletier/go-toml v1.7.0 // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pingcap/errors v0.11.5-0.20250523034308-74f78ae071ee // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.17.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/spf13/afero v1.10.0 // indirect
	github.com/spf13/cast v1.7.0
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	github.com/tinylib/msgp v1.1.0 // indirect
	github.com/willf/bitset v1.1.10 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/oauth2 v0.34.0 // indirect
	golang.org/x/term v0.39.0 // indirect
	golang.org/x/text v0.34.0
	google.golang.org/genproto v0.0.0-20250603155806-513f23925822 // indirect
	google.golang.org/grpc v1.79.3 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
