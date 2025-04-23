# Flexa Waitfree API

## 환경별 배포방법

### 사전 준비

Docker로 nginx와 letsencrypt를 배포해주어야합니다.

```shell
# [1] Let's Encrypt에서 SSL 인증서를 발급받기 위한 도메인의 소유 증명 (반드시 EC2 환경에서 실행해야합니다.)
# run --rm certbot: certbot 컨테이너를 일회성으로 실행하고 끝나면 삭제
# certonly: 인증서만 발급 받고, 설치는 안 함 (우린 nginx에서 직접 경로 지정했으니까)
# --webroot -w /var/www/certbot: 인증 파일을 /var/www/certbot 경로에 저장해서 도메인 소유 확인
# --email: 인증 관련 공지 받을 이메일
# --agree-tos: Let's Encrypt 서비스 이용 약관 동의
# --no-eff-email: EFF 이메일 구독 안 받겠다는 뜻
# -d ...: 인증서를 발급받을 도메인들 나열
docker-compose run --rm certbot certonly \
  --webroot -w /var/www/certbot \
  --email your-email@example.com \
  --agree-tos \
  --no-eff-email \
  -d api.flexa.expert -d api-stag.flexa.expert -d api-dev.flexa.expert

# [2] Docker network 생성
$ docker network create flexa-shared-net
```

### Development

///

### Staging

///

### Production

///
