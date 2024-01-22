# Import libraries
import subprocess
import RPi.GPIO as GPIO
import serial
import time
from datetime import datetime
import paho.mqtt.client as paho
from paho import mqtt
from dotenv import load_dotenv
import os
import pytz

load_dotenv()

# Global variables
SERIAL_BAUD_RATE = 115200
SERIAL_DEVICE = '/dev/ttyS0'
POWER_KEY = 6
PIR_MOTION_PIN = 16
kmh_per_knot = 1.85200

# MQTT settings
publish_delay = 5
identifier = "1ZRG83"
hive_username = os.getenv('hive_username')
hive_password = os.getenv('hive_password')
hive_cluster_url = os.getenv('hive_cluster_url')
hive_port = int(os.getenv('hive_port'))

# Video Settings
rtmp_server_url = os.getenv('rtmp_server_url')
video_width = 1280
video_height = 720
video_framerate = 24
video_bitrate = "1000k"

# Initialize serial connection & Clear input buffer of serial connection
serial = serial.Serial(port=SERIAL_DEVICE, baudrate=SERIAL_BAUD_RATE)
serial.flushInput()


def main():
    mqtt_client = system_setup()
    last_gps_fetch_time = time.time()
    ffmpeg_process = None
    stream = False
    loopId = 0
    
    libcamera_cmd = [
        "libcamera-vid",
        "-t", "0",
        "--inline",
        "--output", "-",
    ]
    
    ffmpeg_cmd = [
        "ffmpeg",
        "-i", "pipe:",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-tune", "zerolatency",
        "-b:v", video_bitrate,
        "-maxrate", video_bitrate,
        "-an",
        "-bufsize", "500k",
        "-vf", f"scale={video_width}:{video_height}",
        "-r", str(video_framerate),
        "-f", "flv",
        f"{rtmp_server_url}/{identifier}"
    ]
    
    try:
        while True:
            motion_detected = get_motion_detected()
            current_time = time.time()
            
            if current_time - last_gps_fetch_time >= publish_delay:
                gps_position = get_gps_position()
                print('\n')

                if motion_detected:
                    if gps_position:
                        print(f'{loopId}: Motion detected, GPS: {gps_position}')
                        publish_to_mqtt_broker(mqtt_client, str(gps_position))
                    else:
                        print(f'{loopId}: Motion detected, GPS: Skipping')
                        
                    if not stream:
                        print(f'{loopId}: Motion detected, VIDEO: Starting stream now')
                        stream = True
                        libcamera_process = subprocess.Popen(libcamera_cmd, stdout=subprocess.PIPE)
                        ffmpeg_process = subprocess.Popen(ffmpeg_cmd, stdin=libcamera_process.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    else:
                        print(f'{loopId}: Motion detected, VIDEO: Stream already on')               
                else:
                    print(f'{loopId}: No motion detected, GPS & VIDEO: Skipping')
                    if stream:
                        print(f'{loopId}: No motion detected, VIDEO: Stopping stream now')
                        stream = False
                        libcamera_process.terminate()
                        ffmpeg_process.terminate()
                        libcamera_process.wait()
                        ffmpeg_process.wait()

                last_gps_fetch_time = current_time
                loopId += 1
    except Exception as e:
        print(f"An error occurred: {e}")


# Returns True if motion detected from PIR sensor
def get_motion_detected():
    return bool(GPIO.input(PIR_MOTION_PIN))


# Writes 'AT command' to serial and returns response
def serial_write_at(command, timeout):
    serial.write((command+'\r\n').encode())
    time.sleep(timeout)
    
    if serial.inWaiting():
        return serial.read(serial.inWaiting())


# Sends 'AT command' and checks received response
def send_at(command, expected_result, timeout):    
    rec_buff = serial_write_at(command, timeout)

    if rec_buff is None:
        print('Module is not ready')
        return
    
    if expected_result not in rec_buff.decode():
        print(command + ' did not return expected result: ' + expected_result)
        return
        
    return rec_buff.decode()


# Transforms and returns CGPSINFO data into a usable array format
def transform_gps_data(gps_data):
    if gps_data is None:
        return {}
    
    if ',,,,,,,' in gps_data:
        return {}
    
    try:
        nmea_sentence = gps_data.split(':')[1].strip()
        sentence_parts = nmea_sentence.split(',')

        latitude_degrees = sentence_parts[0][:2]
        latitude_minutes_raw = sentence_parts[0][2:]
        latitude_minutes = latitude_minutes_raw if latitude_minutes_raw else '0'
        latitude_direction = sentence_parts[1]

        longitude_degrees = sentence_parts[2][:3]
        longitude_minutes_raw = sentence_parts[2][3:]
        longitude_minutes = longitude_minutes_raw if longitude_minutes_raw else '0'
        longitude_direction = sentence_parts[3]
                    
        date = sentence_parts[4]
        time = sentence_parts[5]
        combined_date_time = f"{date} {time}"
        combined_date_time_formatted = datetime.strptime(combined_date_time, "%d%m%y %H%M%S.%f")
        aware_combined_date_time_formatted = combined_date_time_formatted.replace(tzinfo=pytz.utc)
        speed_in_knots = float(sentence_parts[7])
                                    
        final_latitude = float(latitude_degrees) + (float(latitude_minutes) / 60)
        final_longitude = float(longitude_degrees) + (float(longitude_minutes) / 60)

        if latitude_direction == 'S':
            final_latitude = -final_latitude
        if longitude_direction == 'W':
            final_longitude = -final_longitude
                        
        final_datetime_in_utc = aware_combined_date_time_formatted.isoformat()
        final_speed_in_kmh = round(speed_in_knots * kmh_per_knot, 1)
        
        return {
            "latitude": final_latitude,
            "longitude": final_longitude,
            "datetime": final_datetime_in_utc,
            "speed": final_speed_in_kmh
        }
    except:
        return {}
                    

# Sends AT command and returns array of GPS position information or empty array if module not ready
def get_gps_position():
    gps_data = send_at('AT+CGPSINFO', '+CGPSINFO: ', 0.5)
    gps_position = transform_gps_data(gps_data)
    return gps_position


# Method to power on the module
def system_setup():
    print('System is setting up')

    # Configure GPIO
    gpio_configure()
    
    # Connect to MQTT broker
    mqtt_client = connect_to_mqtt_broker()

    # Serial communication flush
    serial.flushInput()

    print('System is ready\n----------')
    return mqtt_client


# Method for configuring GPIO settings
def gpio_configure():
    GPIO.setmode(GPIO.BCM)
    GPIO.setwarnings(False)
    GPIO.setup(POWER_KEY, GPIO.OUT)
    GPIO.setup(PIR_MOTION_PIN, GPIO.IN)


# setting callbacks for different events to see if it works, print the message etc.
def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)


# with this callback you can see if your publish was successful
# def on_publish(client, userdata, mid, properties=None):
#     print(f"mid (message id): {mid}")


# Publishes a message to mqtt broker
def publish_to_mqtt_broker(client, message):
    client.publish(f"taxi/{identifier}", payload=message, qos=1)


# Connects to mqtt client and returns client
def connect_to_mqtt_broker():
    client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
    client.on_connect = on_connect

    client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS, ca_certs='certificates/isrgrootx1.pem')
    client.username_pw_set(hive_username, hive_password)
    client.connect(hive_cluster_url, hive_port)

    # client.on_publish = on_publish
    client.loop_start()
    return client

# Run main method
main()

