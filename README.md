# etl-poc

### Set up prefect environment

1. Access the file named `Guide_to_setup_a_new_AKS_cluster_for_developing_environment.docx` and follow the guidelines.

2. Then, in the terminal, type the following commands and press enter:

Create namespace
```
$ kubectl apply -f prefect-namespace.yaml
```
Create postgres database
```
$kubectl apply -f postgres-deployment.yaml
```
Create prefect server
```
$kubectl apply -f prefect-server-deployment.yaml
```

Type `kubectl get services` to get external ip of prefect server

### How to deploy code

1. Go to each code directory that contains code and deployment files.
2. Open `deploy.sh` file and fill in the necessary values.
3. In the terminal, type bash deploy.sh and enter.
