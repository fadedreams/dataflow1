# Variables
BASE_IMAGE_NAME=base1-image
BASE_DOCKERFILE=dockerfile.base
IMAGE_TAG=latest

# Default target to build the base image
.PHONY: build-base-image
build-base-image:
	@echo "Building the custom base image: $(BASE_IMAGE_NAME):$(IMAGE_TAG)"
	docker build -f $(BASE_DOCKERFILE) -t $(BASE_IMAGE_NAME):$(IMAGE_TAG) .

# Clean up dangling images (optional)
.PHONY: clean
clean:
	@echo "Cleaning up dangling images..."
	docker image prune -f

# Help target to display available targets
.PHONY: help
help:
	@echo "Makefile targets:"
	@echo "  build-base-image   - Build the base image from dockerfile.base"
	@echo "  clean              - Clean up dangling images"
