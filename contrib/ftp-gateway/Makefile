IMAGE=cr.yandex/crp9ftr22d26age3hulg/ftp-s3-gateway
IMAGE2=cr.il.nebius.cloud/crll7us9n6i5j3v4n92m/ftp-s3-gateway
VERSION=1.0

build:
	docker build -t $(IMAGE):$(VERSION) .

push: build
	docker tag $(IMAGE):$(VERSION) $(IMAGE):latest
	docker push $(IMAGE):latest
	docker push $(IMAGE):$(VERSION)
