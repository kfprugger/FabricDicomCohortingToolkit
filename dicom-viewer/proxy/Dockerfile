FROM mcr.microsoft.com/cbl-mariner/base/python:3@sha256:d1dd9c1296ec1a311efc2dd35d74a9c8f2be3c5e264d9a26c847b1123807312c

# CBL-Mariner uses python3, create symlink for compatibility
RUN ln -sf /usr/bin/python3 /usr/bin/python

WORKDIR /app

# Install ODBC Driver 18 for SQL Server (needed for runtime index refresh)
ENV ACCEPT_EULA=Y
RUN tdnf install -y msodbcsql18 unixODBC krb5-libs && \
    tdnf clean all

COPY requirements.lock .
RUN pip install --no-cache-dir --require-hashes -r requirements.lock

COPY app.py .
COPY dicom_index.json .
COPY ohif-dist /app/ohif-dist

EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "120", "app:app"]
