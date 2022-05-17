# Setup Instructions
  
  * Build the Docker image:  

```console
[sudo] docker build . -t unipi-ais
```

  * Run the Docker image:

``` console
sudo docker run -d -p 5006:5006 -e PYTHONUNBUFFERED=1 --restart unless-stopped unipi-ais
```

