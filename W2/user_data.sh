#!/bin/bash
dnf update -y
dnf install -y docker aws-cli
systemctl start docker
systemctl enable docker

export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws ecr get-login-password --region ap-northeast-2 \
| docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.ap-northeast-2.amazonaws.com


docker pull ${ACCOUNT_ID}.dkr.ecr.ap-northeast-2.amazonaws.com/my-jupyter:latest

docker run --rm -p 8888:8888 ${ACCOUNT_ID}.dkr.ecr.ap-northeast-2.amazonaws.com/my-jupyter:latest
