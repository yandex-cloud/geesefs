.PHONY: build run run-host stop clean logs

IMAGE = samba-s3
CONTAINER = samba-s3

build:
	docker build -t $(IMAGE) .

# Run with port mapping (default: 9445->445, 9139->139)
# Use this if port 445 is already in use (e.g., by SSH on macOS)
run: build
	docker run -d --name $(CONTAINER) --privileged \
		-p $(or $(PORT),9445):445 \
		-p 9139:139 \
		-e S3_ACCESS_KEY_ID=$(S3_ACCESS_KEY_ID) \
		-e S3_SECRET_ACCESS_KEY=$(S3_SECRET_ACCESS_KEY) \
		-e S3_BUCKET=$(S3_BUCKET) \
		-e S3_ENDPOINT=$(S3_ENDPOINT) \
		-e SAMBA_USER=$(SAMBA_USER) \
		-e SAMBA_PASSWORD=$(SAMBA_PASSWORD) \
		-e SAMBA_AD_MODE=$(SAMBA_AD_MODE) \
		-e SAMBA_REALM=$(SAMBA_REALM) \
		-e SAMBA_WORKGROUP=$(SAMBA_WORKGROUP) \
		-e SAMBA_KDC=$(SAMBA_KDC) \
		-e SAMBA_AD_PASSWORD=$(SAMBA_AD_PASSWORD) \
		-e SAMBA_GROUP=$(SAMBA_GROUP) \
		$(IMAGE)

# Run with host network (requires port 445/139 to be free)
# Use this on Linux or when ports are available
run-host: build
	docker run -d --name $(CONTAINER) --privileged --network host \
		-e S3_ACCESS_KEY_ID=$(S3_ACCESS_KEY_ID) \
		-e S3_SECRET_ACCESS_KEY=$(S3_SECRET_ACCESS_KEY) \
		-e S3_BUCKET=$(S3_BUCKET) \
		-e S3_ENDPOINT=$(S3_ENDPOINT) \
		-e SAMBA_USER=$(SAMBA_USER) \
		-e SAMBA_PASSWORD=$(SAMBA_PASSWORD) \
		-e SAMBA_AD_MODE=$(SAMBA_AD_MODE) \
		-e SAMBA_REALM=$(SAMBA_REALM) \
		-e SAMBA_WORKGROUP=$(SAMBA_WORKGROUP) \
		-e SAMBA_KDC=$(SAMBA_KDC) \
		-e SAMBA_AD_PASSWORD=$(SAMBA_AD_PASSWORD) \
		-e SAMBA_GROUP=$(SAMBA_GROUP) \
		$(IMAGE)

stop:
	docker stop $(CONTAINER) || true
	docker rm $(CONTAINER) || true

clean: stop
	docker rmi $(IMAGE) || true

logs:
	docker logs $(CONTAINER)

# Example usage:
# make run S3_BUCKET=my-bucket S3_ENDPOINT=https://storage.yandexcloud.net SAMBA_PASSWORD=secret
# make run S3_BUCKET=my-bucket S3_ENDPOINT=https://storage.yandexcloud.net SAMBA_PASSWORD=secret PORT=9445
# make run-host S3_BUCKET=my-bucket S3_ENDPOINT=https://storage.yandexcloud.net SAMBA_PASSWORD=secret
