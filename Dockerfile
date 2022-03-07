FROM registry-mirror.pingcap.net/library/golang:1.15 as builder

RUN git clone https://github.com/PingCAP-QE/amend-random.git \
 && cd amend-random \
 && go build -o /amend-random ./

FROM registry-mirror.pingcap.net/library/debian:buster

RUN apt -y update && apt -y install wget curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /amend-random /usr/local/bin/amend-random
