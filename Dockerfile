# use Python 3.11 base image
FROM python:3.11-slim

# set the wd inside the container to /app.
WORKDIR /app

# copy requirements.txt file into the container at /app.
COPY requirements.txt .

# install the Python dependencies listed in requirements.txt.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire contents of the current directory (Flask app) into the container at /app.
# This copies app.py, the static and templates folders, the models folder, and the database file.
COPY . .

# set port environment variable for cloud run 
ENV PORT 8080

# define the command that will be executed when the container starts.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app