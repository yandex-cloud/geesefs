# samba-s3-gateway

__samba-s3-gateway__ - Samba сервер для предоставления доступа к бакетам S3 через протокол SMB/CIFS.

Реализован в формате Docker-образа, в котором запускаются Samba сервер и реализация FUSE S3 файловой системы [geesefs](https://github.com/yandex-cloud/geesefs).

## Сборка образа

```bash
docker build -t samba-s3 .
```

Или с помощью Makefile:

```bash
make build
```

## Настройки

Ключи доступа S3 передаются через переменные окружения.

### Доступ к бакету

Бакет и путь в бакете выбирается с помощью переменной окружения S3_BUCKET.
Endpoint указывается в S3_ENDPOINT (по умолчанию https://storage.yandexcloud.net).

Для доступа нужно получить статический ключ и передать его через переменные окружения:

```bash
-e S3_ACCESS_KEY_ID=KEY-ID -e S3_SECRET_ACCESS_KEY=MY-SECRET-KEY
```

### Доступ по SMB

По умолчанию включён. Имя пользователя задаётся с помощью переменной окружения
SAMBA_USER (по умолчанию "geesefs"). Пароль задаётся с помощью переменной
SAMBA_PASSWORD (обязателен в standalone-режиме, без AD).

### Active Directory

Для интеграции с Active Directory установите SAMBA_AD_MODE=YES и укажите
параметры домена:

* SAMBA_REALM - полное имя домена (например, CORP.DOMAIN.COM)
* SAMBA_WORKGROUP - NetBIOS имя домена (например, CORP)
* SAMBA_KDC - адрес KDC сервера (например, dc.corp.domain.com)
* SAMBA_AD_PASSWORD - пароль администратора домена для join
* SAMBA_GROUP - группа AD для доступа к шаре (по умолчанию "Domain Users")

## Все настройки

### S3

* S3_BUCKET - название бакета и путь в нём (bucket или bucket/path)
* S3_ENDPOINT - url к s3 (по умолчанию https://storage.yandexcloud.net)

### Samba

* SAMBA_USER - пользователь (по умолчанию "geesefs")
* SAMBA_PASSWORD - пароль для пользователя (обязателен без AD)
* SAMBA_AD_MODE (YES/NO) - режим Active Directory (по умолчанию NO)
* SAMBA_REALM - имя realm для AD
* SAMBA_WORKGROUP - имя workgroup для AD
* SAMBA_KDC - адрес KDC для AD
* SAMBA_AD_PASSWORD - пароль для join к AD
* SAMBA_GROUP - группа AD для доступа

## Запуск контейнера

### Makefile (рекомендуется)

```bash
make run \
    S3_ACCESS_KEY_ID=KEY-ID \
    S3_SECRET_ACCESS_KEY=SECRET-KEY \
    S3_BUCKET=my-bucket \
    S3_ENDPOINT=https://storage.yandexcloud.net \
    SAMBA_PASSWORD=secret
```

### Docker run

Если порт 445 занят, можно использовать проброс на 9445:

```bash
docker run --privileged -d \
    -p 9445:445 -p 9139:139 \
    -e S3_ACCESS_KEY_ID=KEY-ID \
    -e S3_SECRET_ACCESS_KEY=SECRET-KEY \
    -e S3_BUCKET=my-bucket \
    -e S3_ENDPOINT=https://storage.yandexcloud.net \
    -e SAMBA_PASSWORD=secret \
    --name samba-s3 samba-s3
```

### Запуск с Active Directory

```bash
make run \
    S3_ACCESS_KEY_ID=KEY-ID \
    S3_SECRET_ACCESS_KEY=SECRET-KEY \
    S3_BUCKET=my-bucket \
    SAMBA_AD_MODE=YES \
    SAMBA_REALM=CORP.DOMAIN.COM \
    SAMBA_WORKGROUP=CORP \
    SAMBA_KDC=dc.corp.domain.com \
    SAMBA_AD_PASSWORD=admin_password
```

## Подключение

### macOS

Протестировать подключение можно с помощью утилиты `mount_smbfs`:

```bash
$ mkdir -p ~/s3-mount
$ mount_smbfs //geesefs:secret@localhost:9445/geesefs ~/s3-mount
$ ls ~/s3-mount/
```

Отмонтировать:

```bash
$ umount ~/s3-mount
```

### Linux

```bash
$ mount -t cifs //localhost:9445/geesefs /mnt -o user=geesefs,pass=YOUR_PASSWORD,port=9445
```

Замените `YOUR_PASSWORD` на значение переменной `SAMBA_PASSWORD`.

### Windows

```
\\localhost:9445\geesefs
```

Введите:
- **Пользователь**: `geesefs`
- **Пароль**: значение `SAMBA_PASSWORD`
