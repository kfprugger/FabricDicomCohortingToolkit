# Azure Linux 3.0 (CBL-Mariner successor) with Python 3.12; pydicom >= 2.4.5 requires Python >= 3.10.
FROM mcr.microsoft.com/azurelinux/base/python:3.12@sha256:b006d366ab67c2170e5ed5b7d34500e0291373df7faedc1d1ca2472944f6c7b2

# The base image ships python3 only; keep the conventional `python` name available
RUN ln -sf /usr/bin/python3 /usr/bin/python

WORKDIR /app

# Install ODBC Driver 18 for SQL Server (needed for runtime index refresh)
ENV ACCEPT_EULA=Y
RUN tdnf install -y msodbcsql18 unixODBC krb5 && \
    tdnf clean all

# Networks that block files.pythonhosted.org can pass a mirror with
# --build-arg PIP_INDEX_URL=<index>; --require-hashes still pins every artifact.
ARG PIP_INDEX_URL=https://pypi.org/simple
COPY requirements.lock .
RUN pip install --no-cache-dir --require-hashes -r requirements.lock

COPY app.py .
COPY dicom_index.json .
COPY ohif-dist /app/ohif-dist

EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "120", "app:app"]
