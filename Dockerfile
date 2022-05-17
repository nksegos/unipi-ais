FROM python:3.7

WORKDIR /unipi-ais

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY ./static ./static
COPY ./st_visions ./st_visions
COPY ./server.ini ./server.ini
COPY ./main.py ./main.py
COPY ./vessel_positions_json.py ./vessel_positions_json.py

EXPOSE 5006

CMD ["python", "-m", "bokeh", "serve", ".", "--port", "5006", "--use-xheaders", "--prefix", "/unipi-ais", "--allow-websocket-origin", "<LOCAL_IP_ADDRESS_HERE>:5006", "--allow-websocket-origin", "<LOCAL_IP_ADDRESS_HERE>"]
