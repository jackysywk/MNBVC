import requests
import concurrent.futures
import warnings
warnings.filterwarnings('ignore')

def test_ip_speed(hostname: str, ip: str):
    try:
        r = requests.head(f"https://{ip}", headers={"host": hostname}, verify=False, timeout=5)
        if r.status_code < 500:
            return {'ip': ip, 'speed': r.elapsed.microseconds, 'is_connected': True}
        else:
            return {'ip': ip, 'speed': r.elapsed.microseconds, 'is_connected': False}
    except:
        return {'ip': ip, 'speed': float('inf'), 'is_connected': False}

def find_fastest_ip():
    ips = [
    "20.201.28.148",
    "20.205.243.168",
    "20.87.245.6",
    "20.248.137.49",
    "20.207.73.85",
    "20.27.177.116",
    "20.200.245.245",
    "20.175.192.149",
    "20.233.83.146",
    "20.29.134.17",
    "20.199.39.228",
    "4.208.26.200",
    "20.26.156.210"
]
    domain = "github.com"
    speeds = list()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_ip = {executor.submit(test_ip_speed, domain, ip): ip for ip in ips}
        for future in concurrent.futures.as_completed(future_to_ip):
            speed = future.result()
            if speed['is_connected']:
                speeds.append(speed)

    speeds.sort(key=lambda x: x['speed'])
    return speeds
    # if len(speeds) > 0:
    #     speeds.sort(key=lambda x: x['speed'])
    #     return speeds[0]['ip'], speeds, None
    # else:
    #     return '', [], Exception('all IPs are not reachable')
    
if __name__ == "__main__":
    ip_set = find_fastest_ip()
