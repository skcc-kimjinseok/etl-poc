#!/bin/bash

# Variables
ACR_NAME="youracrname" # Azure Container Registry name
ACR_LOGIN_SERVER="$ACR_NAME.azurecr.io"
IMAGE_NAME="etl-06-pipeline" # Docker image name
IMAGE_TAG="v1" # Docker image tag
AKS_RESOURCE_GROUP="youraksresourcegroup" # Resource group containing AKS
AKS_CLUSTER_NAME="youraksclustername" # AKS cluster name
NAMESPACE="prefect" # Kubernetes namespace (optional)
DEPLOYMENT_NAME="etl-06-pipeline" # Kubernetes deployment name
SECRET_NAME="etl-06-secret"
DB_PASSWORD="your_db_password"
DB_NAME="database_name"
DB_HOST="db_host"
DB_PORT="dp_port"
DB_USER="db_user"

# Build Docker image
echo "Building Docker image..."
docker build -t $IMAGE_NAME:$IMAGE_TAG .


# Login to ACR
echo "Logging in to ACR..."
az acr login --name $ACR_NAME

# Tag Docker image
echo "Tagging Docker image..."
docker tag $IMAGE_NAME:$IMAGE_TAG $ACR_LOGIN_SERVER/$IMAGE_NAME:$IMAGE_TAG

# Push Docker image to ACR
echo "Pushing Docker image to ACR..."
docker push $ACR_LOGIN_SERVER/$IMAGE_NAME:$IMAGE_TAG

# Get AKS credentials
echo "Getting AKS credentials..."
sudo az aks get-credentials --resource-group $AKS_RESOURCE_GROUP --name $AKS_CLUSTER_NAME

# Create namespace if it doesn't exist (optional)
kubectl get namespace $NAMESPACE || kubectl create namespace $NAMESPACE

# Create Kubernetes Secret
echo "Creating Kubernetes secret..."
kubectl create secret generic $SECRET_NAME --from-literal=db-password=$DB_PASSWORD --namespace=$NAMESPACE

# Deploy to AKS
echo "Deploying to AKS..."
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $DEPLOYMENT_NAME
  namespace: $NAMESPACE
spec:
  replicas: 1
  selector:
    matchLabels:
      flow: $DEPLOYMENT_NAME
  template:
    metadata:
      labels:
        flow: $DEPLOYMENT_NAME
    spec:
      containers:
      - name: $IMAGE_NAME
        image: $ACR_LOGIN_SERVER/$IMAGE_NAME:$IMAGE_TAG
        env:
        - name: PREFECT_API_URL
          value: http://prefect-server/api
        - name: DB_USER
          value: $DB_USER
        - name: DB_HOST
          value: $DB_HOST
        - name: DB_PORT
          value: "$DB_PORT"
        
        - name: DB_NAME
          value: $DB_NAME
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: $SECRET_NAME
              key: db-password
EOF

echo "Deployment completed successfully."