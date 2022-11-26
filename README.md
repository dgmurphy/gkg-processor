# GKG Processor
Scripts for downloading and processing GDELT-GKG data.

# Usage
Start docker:

```systemctl start docker```

Start MongoDB:

```
cd mongo
docker compose up
```

Check MongoUI:

http://localhost:8081/

Run the processor:

```
source venv/bin/activate
python gkg-downloader.py
```


