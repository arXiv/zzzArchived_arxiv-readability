#!/bin/bash

ENVIRONMENT=$1
TOKEN_NAME=USER_TOKEN_$(echo $ENVIRONMENT | awk '{print toupper($0)}')
SA_NAME=USER_SA_$(echo $ENVIRONMENT | awk '{print toupper($0)}')
DEPLOYMENT_HOSTNAME_VAR=DEPLOYMENT_DOMAIN_$(echo $ENVIRONMENT | awk '{print toupper($0)}')
USER_TOKEN=${!TOKEN_NAME}
USER_SA=${!SA_NAME}
HELM_RELEASE=${CHART_NAME}-${ENVIRONMENT}
DEPLOYMENT_HOSTNAME=${!DEPLOYMENT_HOSTNAME_VAR}

# Install kubectl & Helm
curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.9.2/bin/linux/amd64/kubectl
chmod +x ./kubectl
sudo mv ./kubectl /usr/local/bin/kubectl
echo "Intalled kubectl"

curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get > get_helm.sh
chmod 700 get_helm.sh
sudo ./get_helm.sh -v v2.8.0 || echo "Helm already installed"
echo "Intalled Helm"

# Configure Kubernetes & Helm
echo $CA_CERT | base64 --decode > ${HOME}/ca.crt

kubectl config set-cluster $CLUSTER_NAME --embed-certs=true --server=$CLUSTER_ENDPOINT --certificate-authority=${HOME}/ca.crt
kubectl config set-credentials $USER_SA --token=$(echo $USER_TOKEN | base64 --decode)
kubectl config set-context travis --cluster=$CLUSTER_NAME --user=$USER_SA --namespace=$ENVIRONMENT
kubectl config use-context travis
kubectl config current-context
echo "Configured kubectl"

helm init --client-only --tiller-namespace $ENVIRONMENT
echo "Set up helm client"

# Add S3 repo. Requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to be set
# in the environment.
helm plugin install https://github.com/hypnoglow/helm-s3.git || echo "Helm S3 already installed"
helm repo add arxiv $HELM_REPOSITORY
helm repo update
echo "Updated Helm repo"
