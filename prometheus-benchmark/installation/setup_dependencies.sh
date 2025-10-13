#!/bin/bash

set -e

echo "Setting up Kubernetes dependencies..."

# Update package list
echo "Updating package list..."
sudo apt-get update

# Install dependencies
echo "Installing required dependencies..."
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg

# Install kubectl
echo "Installing kubectl..."
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# Install minikube
echo "Installing minikube..."
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
rm minikube-linux-amd64

# Install helm
echo "Installing helm..."
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

echo "Verifying installations..."
kubectl version --client
minikube version
helm version
