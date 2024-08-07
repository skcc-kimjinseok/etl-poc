FROM prefecthq/prefect:2.19.4-python3.9
WORKDIR /flows 
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY install.sh /flows/install.sh

# Make the bash.sh script executable
RUN chmod +x /flows/install.sh
RUN /flows/install.sh
COPY flows /flows

# Run our flow script when the container starts
CMD ["python3", "etl_04.py"]