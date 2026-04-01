FROM mcr.microsoft.com/cbl-mariner/base/python:3

# CBL-Mariner uses python3, create symlink for compatibility
RUN ln -sf /usr/bin/python3 /usr/bin/python

WORKDIR /app

# Install ODBC Driver 18 for SQL Server (needed for runtime index refresh)
ENV ACCEPT_EULA=Y
RUN tdnf install -y msodbcsql18 unixODBC krb5-libs && \
    tdnf clean all

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

COPY app.py .
COPY dicom_index.json .

EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "120", "app:app"]
