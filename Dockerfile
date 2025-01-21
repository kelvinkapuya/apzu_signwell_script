FROM python
WORKDIR /signwell_script
COPY . /signwell_script
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "main.py"]