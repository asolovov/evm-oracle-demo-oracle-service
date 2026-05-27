# Application name and entrypoint.
APP        := evm-oracle-demo-oracle-service
APP_ENTRY  := cmd/$(APP).go
BUILD_OUT  := ./bin
GITVER_PKG := github.com/asolovov/evm-oracle-demo-oracle-service/pkg/version

# Pinned codegen tools. Per architecture rule 9 — never @latest.
BUF_VERSION                 := v1.55.0
PROTOC_GEN_GO_VERSION       := v1.36.0
PROTOC_GEN_GO_GRPC_VERSION  := v1.5.1

# Build metadata (best-effort; missing values are tolerated).
GOOS       := $(shell go env GOOS)
GOARCH     := $(shell go env GOARCH)
TAG        := $(shell git describe --abbrev=0 --tags 2>/dev/null || true)
COMMIT     := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
BRANCH     ?= $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)
REMOTE     := $(shell git config --get remote.origin.url 2>/dev/null || echo "")
BUILD_DATE := $(shell date +'%Y-%m-%dT%H:%M:%SZ%Z')
RELEASE    := $(if $(TAG),$(TAG),$(COMMIT))

LDFLAGS += -X $(GITVER_PKG).ServiceName=$(APP)
LDFLAGS += -X $(GITVER_PKG).CommitTag=$(TAG)
LDFLAGS += -X $(GITVER_PKG).CommitSHA=$(COMMIT)
LDFLAGS += -X $(GITVER_PKG).CommitBranch=$(BRANCH)
LDFLAGS += -X $(GITVER_PKG).OriginURL=$(REMOTE)
LDFLAGS += -X $(GITVER_PKG).BuildDate=$(BUILD_DATE)
LDFLAGS += -X $(GITVER_PKG).Release=$(RELEASE)

# Migrations config
MIGRATIONS_DIR    := ./db/migrations
DATABASE_HOST     ?= localhost
DATABASE_PORT     ?= 5432
DATABASE_USER     ?= oracle_user
DATABASE_PASSWORD ?= oracle_pass
DATABASE_NAME     ?= evm_oracle
DATABASE_SSL_MODE ?= disable
DATABASE_URL      := postgres://$(DATABASE_USER):$(DATABASE_PASSWORD)@$(DATABASE_HOST):$(DATABASE_PORT)/$(DATABASE_NAME)?sslmode=$(DATABASE_SSL_MODE)

.PHONY: all
all: tidy proto-gen build test

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: update
update:
	go get -u ./...

# ---------------------------------------------------------------------------
# Proto codegen (service-owned per architecture rule 9)
# ---------------------------------------------------------------------------

.PHONY: proto-install
proto-install:
	@which buf > /dev/null || (echo "Installing buf $(BUF_VERSION)..." && go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION))
	@echo "Installing protoc-gen-go $(PROTOC_GEN_GO_VERSION)..."
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	@echo "Installing protoc-gen-go-grpc $(PROTOC_GEN_GO_GRPC_VERSION)..."
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

.PHONY: proto-gen
proto-gen:
	@mkdir -p internal/genproto
	@buf generate

.PHONY: proto-clean
proto-clean:
	@rm -rf internal/genproto

# ---------------------------------------------------------------------------
# Build / test / lint
# ---------------------------------------------------------------------------

.PHONY: build
build: proto-gen
	@mkdir -p $(BUILD_OUT)
	@env CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
		go build -ldflags="-w -s $(LDFLAGS)" -o $(BUILD_OUT)/$(APP) $(APP_ENTRY)

.PHONY: run
run: proto-gen
	@go run -race $(APP_ENTRY) serve

.PHONY: test
test: proto-gen
	@go test ./...

.PHONY: test-integration
test-integration: proto-gen
	@go test -tags=integration ./...

.PHONY: test-coverage
test-coverage: proto-gen
	@go test -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out

.PHONY: lint
lint: proto-gen
	@golangci-lint run ./...

.PHONY: lint-install
lint-install:
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest)

.PHONY: clean
clean:
	@rm -rf $(BUILD_OUT) internal/genproto coverage.out

# ---------------------------------------------------------------------------
# Migrations (golang-migrate)
# ---------------------------------------------------------------------------

.PHONY: migrate-install
migrate-install:
	@which migrate > /dev/null || (echo "Installing golang-migrate..." && \
		go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest)

.PHONY: migrate-create
migrate-create:
ifndef NAME
	@echo "Error: NAME parameter is required (e.g. make migrate-create NAME=add_index)"
	@exit 1
endif
	@migrate create -ext sql -dir $(MIGRATIONS_DIR) -seq $(NAME)

.PHONY: migrate-up
migrate-up:
	@migrate -path $(MIGRATIONS_DIR) -database "$(DATABASE_URL)" up

.PHONY: migrate-down
migrate-down:
	@migrate -path $(MIGRATIONS_DIR) -database "$(DATABASE_URL)" down 1

.PHONY: migrate-version
migrate-version:
	@migrate -path $(MIGRATIONS_DIR) -database "$(DATABASE_URL)" version

.PHONY: migrate-force
migrate-force:
ifndef VERSION
	@echo "Error: VERSION parameter is required"
	@exit 1
endif
	@migrate -path $(MIGRATIONS_DIR) -database "$(DATABASE_URL)" force $(VERSION)

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

.PHONY: compose-up
compose-up:
	@docker compose up -d

.PHONY: compose-down
compose-down:
	@docker compose down

.PHONY: compose-restart
compose-restart:
	@docker compose down && docker compose up -d

# ---------------------------------------------------------------------------
# Rename helper (allows dots in NEW_NAME so github.com/... paths work)
# ---------------------------------------------------------------------------

.PHONY: rename
rename:
ifndef NEW_NAME
	@echo "Error: NEW_NAME parameter is required"
	@exit 1
endif
	@bash scripts/rename.sh "$(NEW_NAME)"
