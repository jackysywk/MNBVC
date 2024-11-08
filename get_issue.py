import json
import os
import time
import traceback

import requests
from datetime import datetime
# from utils import find_fastest_ip
from urllib.parse import urlparse
import sys

BASE_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
TARGET_PATH = TARGET_PATH = os.path.join(BASE_DIR, 'output')  # 目的路径
TMP_PATH = os.path.join(BASE_DIR, 'tmp')  # 存放爬取记录
MAX_FILE_SIZE =  500 * 1024 * 1024


def get_jsonl_filenames(SOURCE_PATH):
    # 获取目录下以jsonl结尾的文件，返回相对路径列表
    jsonl_filenames = []
    for filename in os.listdir(SOURCE_PATH):
        if filename.endswith('.jsonl'):
            jsonl_filenames.append(filename)
    return jsonl_filenames


def get_data_from_file(filename):
    # 369507628, https://github.com/1KomalRani/Komal.github.git
    # 369507636, https://github.com/T1moB/RTS_Test_Game.git
    # 解析为列表[{"id":369507628,"url":"https://github.com/T1moB/RTS_Test_Game.git"}]
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            try:
                id = line.split(",")[0]
                url = line.split(",")[1].strip().replace("https://github.com/", "https://api.github.com/repos/")
                if url.endswith(".git"):
                    url = url[:-4]
                yield {"id": id, "url": url}
            except Exception as e:
                print(e)


def get_data(filename):
    # 解析jsonl, 把每一行json 记录yield 
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            try:
                yield json.loads(line)
            except Exception as e:
                print(e)


def get(url, params=None,ips=[]):
    # requests.get， 并防止token超限
    print(f'get {url}')
    current_timestamp=time.time()
    for i in range(3):
        try:
            host = urlparse(url).hostname
            headers['host'] = host
            # current_ip=None
            # for ip in ips:
            #     if 'limit_time' not in ip:
            #         ip['limit_time']=0
            #     if ip['limit_time'] < current_timestamp and ip['is_connected']:
            #         current_ip=ip
            # if current_ip:
            #     url = url.replace(host, current_ip['ip'], 1)
            # elif not current_ip: #没有可用ip，等1分钟
            #     print("没有可用ip，等待一分钟")
            #     time.sleep(60)
            #     continue
            response = requests.get(url, headers=headers, params=params, stream=True, verify=False, timeout=5 * 60)
            limit = response.headers.get("X-RateLimit-Limit")
            remaining = response.headers.get("X-RateLimit-Remaining")
            reset = response.headers.get("X-RateLimit-Reset")
            if not (limit and remaining and reset):
                continue
            # if current_ip:
            #     print(limit,remaining,reset)
            #     current_ip['limit_time']=int(reset)
            current_timestamp = int(time.time())
            time_diff = int(reset) - current_timestamp
            if response.status_code == 200:
                data = response.json()
            else:
                data = []
            if int(remaining) <= 20:
                print('sleeping', time_diff)
            return data
        except Exception as e:
            traceback.print_exc()
            continue


def get_issues(metadata, record,ips):
    if str(metadata['id']) in record.keys():
        return  False# 如果已经爬过了，就return
    if os.path.exists(os.path.join(TARGET_PATH, f'{metadata["id"]}.jsonl')):
        os.remove(os.path.join(TARGET_PATH, f'{metadata["id"]}.jsonl'))
    page = 1
    has_data=False
    while True:
        try:
            # metadata['url']格式 https://api.github.com/repos/<username>/<repo>
            issues = get(metadata['url'] + '/issues', params={"page": page, "state": "all"},ips=ips)
            if issues:
                add_comments(metadata, issues)
                page += 1
                has_data=True
            else:
                break
        except Exception as e:
            traceback.print_exc()
            break
    return has_data

def add_comments(metadata, issues):
    # 增加评论

    for issue in issues:
        comment_dict = {}
        # issue example = issue.example.json
        issue['comments_data'] = []
        if issue['comments']:
            comments_url = issue['comments_url']
            comments_data = get(comments_url,ips=[])
            if comments_data:
                issue['comments_data'].extend(comments_data)
        comment_dict["ID"] = issue["id"]
        comment_dict['ID'] = issue['id']
        comment_dict['主题'] = issue['title']
        comment_dict['来源'] = 'Github Issue'
        comment_dict['元数据'] = {
            '发帖时间': format_date(issue['created_at']),
            '回复数': int(issue['comments']) + 1,
            '扩展字段': str({"url": issue['html_url']})
        }
        comment_dict['回复'] = []

        # add the first post
        comment_dict['回复'].append(extract_comment(id=1,
                                                    content=issue['body'],
                                                    user=issue['user']['login'],
                                                    time=format_date(issue['created_at'])))
        for id, comment in enumerate(issue['comments_data'], 2):
            comment_dict['回复'].append(extract_comment(id=id,
                                                        content=comment['body'],
                                                        user=comment['user']['login'],
                                                        time=format_date(comment['created_at'])))
        write_to_file(metadata, comment_dict)


def format_date(input_date):
    # Parsing the date-time string to a datetime object
    # The 'Z' indicates UTC time and can be handled by replacing it with '+00:00'
    parsed_date = datetime.strptime(input_date, '%Y-%m-%dT%H:%M:%SZ')

    # Formatting the datetime object to the desired format
    formatted_date = parsed_date.strftime('%Y%m%d %H:%M:%S')
    return formatted_date


def extract_comment(id, content, user, time, comment_id=None):
    if comment_id:
        extended_string = str({"回复人": user, "回复时间": time, "Github_comment_id": comment_id})
    else:
        extended_string = str({"回复人": user, "回复时间": time})
    return {
        "楼ID": id,
        "回复": content,
        "扩展字段": extended_string
    }


def get_next_filename(base_filename):
    """
    Generate the next filename based on existing files.
    """
    i = 1
    while True:
        if i == 1:
            new_filename = os.path.join(TARGET_PATH, f'{base_filename}.jsonl')
        else:
            new_filename = os.path.join(TARGET_PATH, f'{base_filename}_{i}.jsonl')
        if not os.path.exists(new_filename):
            return new_filename
        else:
            file_size = os.path.getsize(new_filename)
            if file_size < MAX_FILE_SIZE:
                return new_filename
        i += 1


def write_to_file(metadata, issue):
    base_filename = 'Github_issue'
    filename = get_next_filename(base_filename)
    with open(filename, 'a', encoding='utf-8') as f:
        f.write(json.dumps(issue, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    # parser = argparse.ArgumentParser(
    #         prog='python get_issue.py',
    #         description='GET MNBVC ISSUES',
    #         epilog='Text at the bottom of help')
    # parser.add_argument('-p', '--path', help='meta jsonl path', required=False)
    # parser.add_argument('-t', '--token', help='github token like github_pat_*', required=True)
    #
    
    # SOURCE_PATH = "repos_list.txt"
    token_path = os.path.join(BASE_DIR, 'token.txt')
    SOURCE_PATH = os.path.join(BASE_DIR, 'repos_list1.txt')
    with open(token_path, 'r') as f:
        print("main")
        github_token = f.read()
    headers = {"Authorization": f"token {github_token}","X-GitHub-Api-Version": "2022-11-28"}
    # print("Finding Fasting IPs")
    # ips = find_fastest_ip()
    # print(f"Found fastest IPs is .",ips)

    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    record_file =  "data.json"
    record = {}
    if os.path.exists(record_file):  # 检查在tmp 路径 的record 文件是否存在
        with open(record_file, 'r') as f:
            try:
                record = json.load(f)
            except Exception as e:
                pass
    for metadata in get_data_from_file(SOURCE_PATH):
        try:
            has_data=get_issues(metadata, record,[])
            record[metadata['id']] = {'get_time': time.time()}
            if has_data:
                with open(record_file, 'w') as f:
                    json.dump(record, f)
        except Exception as e:
            traceback.print_exc()
