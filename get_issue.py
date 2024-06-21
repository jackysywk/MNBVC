import json
import os
import time
import requests
import argparse
from datetime import datetime
from utils import find_fastest_ip
from urllib.parse import urlparse
import sys
#SOURCE_PATH = '../20230301/github.20230301.1.代码元数据/'  # 源路径,但是程序会使用arg 覆盖，所以这不需要
TARGET_PATH = 'output'  # 目的路径
TMP_PATH = 'tmp' # 存放爬取记录


MAX_FILE_SIZE = 500 * 1024 * 1024

def get_jsonl_filenames(SOURCE_PATH):
    # 获取目录下以jsonl结尾的文件，返回相对路径列表
    jsonl_filenames = []
    for filename in os.listdir(SOURCE_PATH):
        if filename.endswith('.jsonl'):
            jsonl_filenames.append(filename)
    return jsonl_filenames


def get_data(filename):
    # 解析jsonl, 把每一行json 记录yield 
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            try:
                yield json.loads(line)
            except Exception as e:
                print(e)

def get(url, params=None):
    # requests.get， 并防止token超限
    #print(f'get {url}')
    for i in range(3):
        try:
            host = urlparse(url).hostname
            headers['host'] = host
            if fastest_ip:
                url = url.replace(host,fastest_ip, 1)

            response = requests.get(url, headers=headers, params=params,stream=True, verify=False, timeout=5*60)
            limit = response.headers.get("X-RateLimit-Limit")
            remaining = response.headers.get("X-RateLimit-Remaining")
            reset = response.headers.get("X-RateLimit-Reset")
            current_timestamp = int(time.time())
            time_diff = int(reset) - current_timestamp
            if response.status_code == 200:

                data = response.json()
            else:
                data = []
            if int(remaining) <= 20:
                print('sleeping',time_diff)
                time.sleep(time_diff)       
            return data
        except Exception as e:
            print(e)
            continue

def get_issues(metadata, record_file):
    record = {}
    if os.path.exists(record_file): #检查在tmp 路径 的record 文件是否存在
        with open(record_file, 'r') as f:
            try:
                record = json.load(f)
            except Exception as e:
                print(e)
        
    if str(metadata['id']) in record.keys():
        return #如果已经爬过了，就return

    if os.path.exists(os.path.join(TARGET_PATH, f'{metadata["id"]}.jsonl')):
        os.remove(os.path.join(TARGET_PATH, f'{metadata["id"]}.jsonl'))
        #删除output 的JSONL 文件
    page = 1
    while True:
        try:
            #metadata['url']格式 https://api.github.com/repos/<username>/<repo>
            issues = get(metadata['url'] + '/issues', params={"page": page, "state": "all"})
            add_comments(metadata, issues)
            if issues:
                page += 1
            else:
                break
        except Exception as e:
            print(e)
            break

    record[metadata['id']] = {'get_time': time.time()}
    with open(record_file, 'w') as f:
        json.dump(record, f)



def add_comments(metadata, issues):
    # 增加评论

    for issue in issues:
        comment_dict ={}
        # issue example = issue.example.json
        issue['comments_data'] = []
        if issue['comments']:
            comments_url = issue['comments_url']
            comments_data = get(comments_url)
            if comments_data:
                issue['comments_data'].extend(comments_data)
        comment_dict["ID"] = issue["id"]
        comment_dict['ID'] = issue['id']
        comment_dict['主题'] = issue['title']
        comment_dict['来源'] = 'Github Issue'
        comment_dict['元数据'] = {
            '发帖时间':format_date(issue['created_at']),
            '回复数':int(issue['comments'])+1,
            '扩展字段':str({"url":issue['html_url']})
        }
        comment_dict['回复'] = []
        
        #add the first post
        comment_dict['回复'].append(extract_comment(id=1,
                                            content=issue['body'],
                                            user = issue['user']['login'],
                                            time = format_date(issue['created_at'])))
        for id, comment in enumerate(issue['comments_data'],2):
            comment_dict['回复'].append(extract_comment(id=id,
                                                    content=comment['body'],
                                                    user = comment['user']['login'],
                                                    time = format_date(comment['created_at'])))
        write_to_file(metadata, comment_dict)

def format_date(input_date):

    # Parsing the date-time string to a datetime object
    # The 'Z' indicates UTC time and can be handled by replacing it with '+00:00'
    parsed_date = datetime.strptime(input_date, '%Y-%m-%dT%H:%M:%SZ')

    # Formatting the datetime object to the desired format
    formatted_date = parsed_date.strftime('%Y%m%d %H:%M:%S')
    return formatted_date

def extract_comment(id, content, user, time, comment_id= None):
    if comment_id:
        extended_string = str({"回复人":user, "回复时间": time,"Github_comment_id":comment_id})
    else:
        extended_string = str({"回复人":user, "回复时间": time})
    return {
        "楼ID":id,
        "回复":content,
        "扩展字段":extended_string
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
    parser = argparse.ArgumentParser(
            prog='python get_issue.py',
            description='GET MNBVC ISSUES',
            epilog='Text at the bottom of help')
    parser.add_argument('-p', '--path', help='meta jsonl path', required=True)
    parser.add_argument('-t', '--token', help='github token like github_pat_*', required=True)


    args = parser.parse_args()
    if args.path:
        SOURCE_PATH = args.path
    else:
        SOURCE_PATH = "../../20230301/github.20230301.1.代码元数据"
    if args.token:
        github_token = args.token        
    headers = {"Authorization": f"token {github_token}"}
    print("Finding Fasting IPs")
    fastest_ip, _, _ = find_fastest_ip()
    print(f"Found fastest IP is {fastest_ip}.")

    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    if not os.path.exists(TMP_PATH):
        os.makedirs(TMP_PATH)

    jsonl_filenames = get_jsonl_filenames(SOURCE_PATH)  # 获取JSONL的相对路径文件名
    for filename in jsonl_filenames:
        abs_filename = os.path.join(SOURCE_PATH, filename) #把相对路径名字改成绝对路径
        record_file = os.path.join(TMP_PATH, filename.replace('jsonl', 'json')) #在TMP路径 以jsonl 的名字创建 同名的json文件
        for metadata in get_data(abs_filename): #每一行的json 数据 (metadata.example.json)
            try:    
                get_issues(metadata, record_file)
            except Exception as e:
                print(e)