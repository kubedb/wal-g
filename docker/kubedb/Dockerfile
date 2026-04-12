# Copyright AppsCode Inc. and Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Multi-arch image build for KubeDB releases (matches wal-g main/<db>/ layout).
ARG GO_VERSION=1.25.8
ARG RUNTIME=debian:bookworm-slim

FROM golang:{GO_VERSION}-bookworm AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG DB=mongo
ARG BUILD_TAGS=

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
  && apt-get install -y --no-install-recommends apt-transport-https ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
  go build -mod vendor -tags "${BUILD_TAGS}" -o /wal-g ./main/${DB}/main.go

FROM ${RUNTIME}

ARG DB=mongo
LABEL org.opencontainers.image.source="https://github.com/kubedb/wal-g"

COPY --from=builder /wal-g /wal-g
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

ENTRYPOINT ["/wal-g"]
