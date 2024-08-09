#!/bin/bash# 

# Variables 
ACR_NAME=<your-acr-name>
ACR_LOGIN_SERVER=$ACR_NAME.azurecr.io
IMAGE_NAME=<your-image-name>
CONTAINER_NAME=<your-container-name>
IMAGE_TAG=<your-tag-name>
NAMESPACE=<your-namespace-name>

# SCENARIO 1
SERVER_IP_1=<your-server-ip>
SERVER_USERNAME_1=<your-server-username>
SERVER_PASSWORD_1=<your-server-password>
SERVER_PORT_1=<your-server-port>

SERVER_IP_2=<your-server-name>
SERVER_USERNAME_2=<your-server-username>
SERVER_PASSWORD_2=<your-server-password>
SERVER_PORT_2=<your-server-port>

# INFO ORACLE DB
ORACLE_HOST=<your-database-host>
ORACLE_PORT=<your-database-port>
ORACLE_SID=<your-database-sid>
ORACLE_USER=<your-database-user>
ORACLE_PASSWORD=<your-database-password>


# Build the Docker image 
echo "Building Docker image..."
docker build -t $IMAGE_NAME:$IMAGE_TAG .
# Tag the Docker image
echo "Tagging Docker image..."
docker tag $IMAGE_NAME:$IMAGE_TAG $ACR_LOGIN_SERVER/$IMAGE_NAME:$IMAGE_TAG
# Log in to Azure
echo "Logging in to Azure..."
# az login
# Log in to ACR
echo "Logging in to ACR..."
az acr login --name $ACR_NAME
# Push the Docker image to ACR
echo "Pushing Docker image to ACR..."
docker push $ACR_LOGIN_SERVER/$IMAGE_NAME:$IMAGE_TAG

# Deploy to AKS
echo "Deploying to AKS..."

kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $IMAGE_NAME
  namespace: $NAMESPACE

spec:
  replicas: 1
  selector:
    matchLabels:
      flow: $IMAGE_NAME
  template:
    metadata:
      labels:
        flow: $IMAGE_NAME
    spec:
      containers:
        - name: $CONTAINER_NAME
          image: $ACR_NAME.azurecr.io/$IMAGE_NAME:$IMAGE_TAG
          env:
            - name: PREFECT_API_URL
              value: http://prefect-server/api          
            - name: SERVER_IP_1
              value: $SERVER_IP_1
            - name: SERVER_USERNAME_1
              value: $SERVER_USERNAME_1
            - name: SERVER_PASSWORD_1
              value: $SERVER_PASSWORD_1
            - name: SERVER_PORT_1
              value: "$SERVER_PORT_1"
            - name: SERVER_IP_2
              value: $SERVER_IP_2         
            - name: SERVER_USERNAME_2
              value: $SERVER_USERNAME_2
            - name: SERVER_PASSWORD_2
              value: $SERVER_PASSWORD_2
            - name: SERVER_PORT_2
              value: "$SERVER_PORT_2"
            - name: ORACLE_HOST
              value: $ORACLE_HOST
            - name: ORACLE_PORT
              value: "$ORACLE_PORT"
            - name: ORACLE_SID
              value: $ORACLE_SID
            - name: ORACLE_USER
              value: $ORACLE_USER
            - name: ORACLE_PASSWORD
              value: $ORACLE_PASSWORD

EOF

echo "Deployment completed successfully."

