STEAMPIPE_INSTALL_DIR ?= ~/.steampipe
BUILD_TAGS = netgo

build:
	go build -tags "${BUILD_TAGS}" ./...

install:
	go build -o $(STEAMPIPE_INSTALL_DIR)/plugins/hub.steampipe.io/plugins/turbot/alicloud@latest/steampipe-plugin-alicloud.plugin -tags "${BUILD_TAGS}" *.go