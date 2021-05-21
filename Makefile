SEVERITIES = HIGH,CRITICAL

ifeq ($(ARCH),)
ARCH=$(shell go env GOARCH)
endif

ORG ?= rancher
TAG ?= v0
REPO=opni-preprocessing-service

ifneq ($(DRONE_TAG),)
TAG := $(DRONE_TAG)
endif

.PHONY: image-build
image-build:
	git submodule update --init
	docker build \
		--pull \
		--build-arg PKG=$(PKG) \
		--build-arg SRC=$(SRC) \
		--build-arg TAG=$(TAG:$(BUILD_META)=) \
		--tag $(ORG)/$(REPO):$(TAG) \
		--tag $(ORG)/$(REPO):$(TAG)-$(ARCH) \
	.

.PHONY: image-push
image-push:
	docker push $(ORG)/$(REPO):$(TAG)-$(ARCH)

.PHONY: image-manifest
image-manifest:
	DOCKER_CLI_EXPERIMENTAL=enabled docker manifest create --amend \
		$(ORG)/$(REPO):$(TAG) \
		$(ORG)/$(REPO):$(TAG)-$(ARCH)
	DOCKER_CLI_EXPERIMENTAL=enabled docker manifest push \
		$(ORG)/$(REPO):$(TAG)

.PHONY: image-scan
image-scan:
	trivy --severity $(SEVERITIES) --no-progress --ignore-unfixed $(ORG)/$(REPO):$(TAG)
