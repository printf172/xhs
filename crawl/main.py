import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))
import pandas as pd
import numpy as np
import requests
from crawler.xhs.xhs import XhsClient
from common.utils import tools
import traceback
import json
import time
from tencentcloud.common import credential
from tencentcloud.common.profile.client_profile import ClientProfile
from tencentcloud.common.profile.http_profile import HttpProfile
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.iai.v20200303 import iai_client, models
import cv2
import easyocr
from common.logs import set_logger
import threading
import queue
from concurrent.futures import ThreadPoolExecutor

# 设置日志
log_path = str(os.path.dirname(os.path.abspath(__file__)))
logger = set_logger(log_path, "startCrawl")

# 配置文件
COOKIE_FILE = "crawler/xhs/crawl/cookie.txt"
RESULT_DIR = "crawler/xhs/crawl/results"
MAX_THREADS = 1  # 最大线程数

# 确保结果目录存在
if not os.path.exists(RESULT_DIR):
    os.makedirs(RESULT_DIR)

# 线程安全的Cookie管理
cookie_lock = threading.Lock()
cookie_queue = queue.Queue()

def load_cookie():
    """从文件加载cookie，并在读取后删除文件内容"""
    with cookie_lock:
        if not os.path.exists(COOKIE_FILE):
            logger.error(f"Cookie文件不存在: {COOKIE_FILE}")
            return None
        
        try:
            with open(COOKIE_FILE, 'r') as file:
                cookie = file.read().strip()
            
            # 读取后清空文件
            if cookie:
                with open(COOKIE_FILE, 'w') as file:
                    file.write("")
                logger.info("Cookie已加载并从文件中删除")
                return cookie
            else:
                logger.error("Cookie文件为空")
                return None
        except Exception as e:
            logger.error(f"读取Cookie文件出错: {e}")
            return None

def wait_for_cookie():
    """等待新的cookie"""
    logger.info("等待新的Cookie... (请将Cookie放入cookie.txt文件)")
    while True:
        cookie = load_cookie()
        if cookie:
            logger.info("已获取新Cookie，继续执行")
            return cookie
        time.sleep(10)  # 每10秒检查一次

# 保留原有功能函数
def detect_face(url):
    try:
        logger.info(f"开始进行人脸识别:{url}")
        # 实例化一个认证对象，入参需要传入腾讯云账户 SecretId 和 SecretKey，此处还需注意密钥对的保密
        cred = credential.Credential("", "")
        # 实例化一个http选项，可选的，没有特殊需求可以跳过
        httpProfile = HttpProfile()
        httpProfile.endpoint = "iai.tencentcloudapi.com"

        # 实例化一个client选项，可选的，没有特殊需求可以跳过
        clientProfile = ClientProfile()
        clientProfile.httpProfile = httpProfile
        # 实例化要请求产品的client对象,clientProfile是可选的
        client = iai_client.IaiClient(cred, "ap-beijing", clientProfile)

        # 实例化一个请求对象,每个接口都会对应一个request对象
        req = models.DetectFaceRequest()
        params = {
            "Image": "",
            "Url": url,
            "MaxFaceNum": 5,
            "NeedFaceAttributes": 1,
            "NeedQualityDetection": 1,
        }
        req.from_json_string(json.dumps(params))

        # 返回的resp是一个DetectFaceResponse的实例，与请求对象对应
        resp = client.DetectFace(req)
        # 输出json格式的字符串回包
        return resp.to_json_string()

    except TencentCloudSDKException as err:
        logger.info(err)
        return ''


def convert_numpy_types(obj):
    """递归转换NumPy数据类型为Python原生类型"""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_numpy_types(item) for item in obj)
    else:
        return obj

def read_text(img_path, compress_factor: int = 1, lang_list=None):
    if lang_list is None:
        lang_list = ['en']  # default to English if no languages are provided

    img = cv2.imread(img_path)
    if img is None:
        raise ValueError(f"Could not read the image from path: {img_path}")
    
    if compress_factor > 1:
        img = cv2.resize(img, (int(img.shape[1] / compress_factor), int(img.shape[0] / compress_factor)))

    reader = easyocr.Reader(lang_list)
    ocr_results = reader.readtext(img)
    converted_results = convert_numpy_types(ocr_results)
    return converted_results


def download_image(url, save_path):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            # Ensure the directory exists
            directory = os.path.dirname(save_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
                
            with open(save_path, 'wb') as file:
                file.write(response.content)
                logger.info(f'图片保存成功')
                return True
        else:
            logger.error(f'Failed to download image, status code: {response.status_code}')
            return False
    except Exception:
        logger.error(f'download_image出现异常: {traceback.format_exc()}')
        return False

def sign(uri, data=None, a1="", web_session=""):
    # 填写自己的 flask 签名服务端口地址
    for i in range(20):
        try:
            res = requests.post("http://localhost:5005/sign",
                                json={"uri": uri, "data": data, "a1": a1, "web_session": web_session})
            signs = res.json()
            return {
                "x-s": signs["x-s"],
                "x-t": signs["x-t"]
            }
        except Exception:
            pass

headers = {
    'Authorization': 'test'
}

def summarize_with_ernie(content, max_retries=2000, backoff_factor=2):
    API_KEY = ""
    SECRET_KEY = ""
    def get_access_token():
        """
        使用 AK，SK 生成鉴权签名（Access Token）
        :return: access_token，或是None(如果错误)
        """
        url = "https://aip.baidubce.com/oauth/2.0/token"
        params = {"grant_type": "client_credentials", "client_id": API_KEY, "client_secret": SECRET_KEY}
        return str(requests.post(url, params=params).json().get("access_token"))

    url = "https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/completions?access_token=" + get_access_token()
    
    payload = json.dumps({
        "messages": [
            {
                "role": "user",
                "content": content
            }
        ],
        "temperature": 0.95,
        "top_p": 0.8,
        "penalty_score": 1,
        "enable_system_memory": False,
        "disable_search": False,
        "enable_citation": False,
        "response_format": "text"
    }, ensure_ascii=False)
    headers = {
        'Content-Type': 'application/json'
    }
    
    retries = 0
    while retries < max_retries:
        try:
            start_time = time.time()
            response = requests.request("POST", url, headers=headers, data=payload.encode("utf-8"))
            elapsed_time = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json().get('result')
                logger.info(f"Request successful. Elapsed time: {elapsed_time:.2f} seconds")
                logger.info(f"Result: {result}")
                if result:
                    return result
            else:
                logger.error(f"Error: {response.status_code} - {response.text}")
                retries += 1
                time.sleep(backoff_factor ** retries)
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            retries += 1
            time.sleep(backoff_factor ** retries)
    
    logger.error("Max retries reached. Failed to get summary from ERNIE.")
    return None

def get_user_notes(data, cookie):
    user_works = []
    user_id = data['user_id']
    xsec_token = data['xsec_token']
    logger.info(f'开始获取用户作品：{user_id}')
    
    # 使用传入的cookie创建XhsClient实例
    xhs_client = XhsClient(cookie, sign=sign)
    
    try:
        res = xhs_client.get_user_notes(user_id, xsec_token)
        notes = tools.safe_get(res, ['notes'])
        if not notes:
            logger.error(f'获取用户作品异常:{user_id}')
            return ''
        
        logger.info(f"共找到{len(notes)}篇笔记")
        cursor = tools.safe_get(res, ['cursor'])
        for note in notes:
            try:
                note_id = tools.safe_get(note, ['note_id'])
                xsec_token = tools.safe_get(note, ['xsec_token'])
                note_detail = xhs_client.get_note_by_id(note_id, xsec_token)
                type = tools.safe_get(note_detail, 'type')
                note_desc = tools.safe_get(note_detail, 'desc')
                if type == 'video':
                    logger.info(f'当前笔记是视频:{type}')
                    continue
                image_list = tools.safe_get(note_detail, 'image_list')
                logger.info(f"共找到{len(image_list)}张图片")
                for i, img in enumerate(image_list):
                    live_photo = tools.safe_get(img, 'live_photo')
                    if live_photo:
                        logger.info(f'当前图片是动图：{live_photo}')
                        continue
                    img_url = tools.safe_get(img, 'url_default')
                    img_path = f'crawler/xhs/crawl/img/{note_id}/{tools.get_current_timestamp()}.png'
                    download_image(img_url, img_path)
                    img['text'] = read_text(img_path)
                    
                    url = 'http://116.62.236.232/ai-huanzhuang-sys/file/uploadFile'
                    files = {'file': open(img_path, 'rb')}
                    response = tools.get_html(url, files=files, headers=headers)
                    if response['code'] == 0:
                        oss_path = response['data']
                        img['oss_path'] = oss_path
                    else:
                        raise Exception("阿里云上传图片异常")
                    
                    face_info = detect_face(img_url)
                    face_info = tools.get_json(face_info)
                    if face_info:
                        face_infos = tools.safe_get(face_info, ['FaceInfos'])
                        img['face_count'] = len(face_infos)
                        
                        img['face_info'] = face_info
                        image_list[i] = img

                note_detail['image_list'] = image_list
                note['note_detail'] = note_detail
                user_works.append(note)
                logger.info(f'***************完成笔记{note_id}的采集***************')
            except Exception:
                logger.error(traceback.format_exc())
                time.sleep(2)
        
        # 请求成功，将cookie放回队列
        cookie_queue.put(cookie)
        return user_works
    except Exception as e:
        logger.error(f"获取用户笔记出错: {e}")
        # 请求失败，可能是cookie问题，不返回cookie到队列
        return ''
import time
import traceback
from queue import Queue

def search(user_id, max_retries=20, retry_delay=1):
    """
    搜索用户，支持重试机制
    
    参数:
        user_id: 用户ID
        max_retries: 最大重试次数，默认20次
        retry_delay: 初始重试延迟(秒)，默认1秒
    
    返回:
        (id, xsec_token) 元组，如果搜索失败则返回 ('', '')
    """
    retry_count = 0
    
    while retry_count <= max_retries:  # 使用 <= 确保总共尝试 max_retries + 1 次
        if retry_count > 0:
            logger.info(f'搜索用户 {user_id} 第 {retry_count} 次重试')
            # 指数退避策略，每次重试延迟增加
            sleep_time = min(retry_delay * (2 ** (retry_count - 1)), 30)  # 最大延迟30秒
            time.sleep(sleep_time)
        
        logger.info(f'开始搜索：{user_id}')
        
        cookie = None
        try:
            # 获取可用的cookie
            if cookie_queue.empty():
                logger.warning(f"Cookie队列为空，等待新cookie...")
                cookie = wait_for_cookie()
            else:
                cookie = cookie_queue.get()
                
            # 使用传入的cookie创建XhsClient实例
            xhs_client = XhsClient(cookie, sign=sign)
            
            res = xhs_client.get_user_by_keyword(user_id)
            if not res:
                logger.error(f'搜索异常:{user_id}')
                if cookie:
                    cookie_queue.put(cookie)
                    cookie = None
                
                retry_count += 1
                continue  # 重试
                
            users = tools.safe_get(res, ['users'])
            find = False
            for user in users:
                red_id = tools.safe_get(user, ['red_id'])
                if user_id == red_id:
                    id = tools.safe_get(user, ['id'])  # 注意：这里从当前user获取，而不是固定用index 0
                    xsec_token = tools.safe_get(user, ['xsec_token'])
                    find = True
                    
                    # 请求成功，将cookie放回队列
                    if cookie:
                        cookie_queue.put(cookie)
                        cookie = None
                    
                    logger.info(f'成功找到用户 {user_id}')
                    return id, xsec_token
                    
            if not find:    
                logger.error(f'未搜索到相关用户 {user_id}')
                
                # 请求成功但未找到用户，将cookie放回队列
                if cookie:
                    cookie_queue.put(cookie)
                    cookie = None
                
                # 这种情况不需要重试，直接返回空结果
                return '', ''
                
        except Exception as e:
            error_msg = traceback.format_exc()
            logger.error(f"搜索用户出错: {error_msg}")
            
            # 无论出错原因，都将cookie放回队列
            if cookie:
                cookie_queue.put(cookie)
                cookie = None
            
            retry_count += 1
            # 如果已经达到最大重试次数，则退出循环
            if retry_count > max_retries:
                logger.error(f"搜索用户 {user_id} 失败，已达到最大重试次数 {max_retries}")
                break
        finally:
            # 确保在任何情况下都归还cookie
            if cookie:
                cookie_queue.put(cookie)
    
    # 所有重试都失败了
    return '', ''


def get_comments(data, user_work, cookie):
    user_id = data['user_id']
    xsec_token = tools.safe_get(user_work, ['xsec_token'])
    note_id = tools.safe_get(user_work, ['note_id'])
    
    # 使用传入的cookie创建XhsClient实例
    xhs_client = XhsClient(cookie, sign=sign)
    
    try:
        comments = xhs_client.get_note_all_comments(note_id, xsec_token)
        # 只保留作者或者包含作者参与的评论
        # This is a summary of the chat history as a recap: 的评论
        filtered_comments = []

        for comment in comments:
            comment_user_id = tools.safe_get(comment, ['user_info', 'user_id'])
            sub_comments = tools.safe_get(comment, ['sub_comments'], [])

            # 检查主评论是否由作者参与
            if comment_user_id == user_id:
                filtered_comments.append(comment)
                continue

            # 检查子评论是否由作者参与
            author_in_sub_comments = False
            for sub_comment in sub_comments:
                sub_comment_user_id = tools.safe_get(sub_comment, ['user_info', 'user_id'])
                if sub_comment_user_id == user_id:
                    author_in_sub_comments = True
                    break

            # 如果主评论或子评论中包含作者的评论，将整个主评论和所有子评论保留
            if author_in_sub_comments:
                filtered_comments.append(comment)

        user_work['note_detail']['comments'] = filtered_comments
        
        if filtered_comments:
            # 拼接评论内容
            comments_content = '\n'.join([tools.safe_get(comment, 'content', '') for comment in filtered_comments])
            note_desc = tools.safe_get(user_work, ['note_detail', 'desc'])
            prompt = f"""
                以下是小红书的一篇笔记内容和评论，请根据这些内容生成一个总结，介绍这篇笔记的主要内容、地理位置（如果有提到）、以及其他重要信息：

                笔记内容:
                {note_desc}

                评论内容:
                {comments_content}
                """
            logger.info(prompt)
            user_work['note_detail']['summary'] = summarize_with_ernie(prompt)
        
        # 请求成功，将cookie放回队列
        cookie_queue.put(cookie)
        return user_work
    except Exception as e:
        logger.error(f"获取评论出错: {e}")
        # 请求失败，可能是cookie问题，不返回cookie到队列
        return user_work

def is_save(image_list, blogger_gender=None):
    """
    分析图片列表，决定图片是否符合要求1或要求2
    返回: 1 (符合要求1), 2 (符合要求2), [1, 2] (两者都符合), 或 False (都不符合)
    """
    if not image_list:
        return False
    
    result = []
    
    for img in image_list:
        face_count = tools.safe_get(img, ['face_count'], 0)
        text = img.get('text', '')
        
        # 如果图片有较多文字，跳过
        if len(text) > 3:
            continue
        
        # 检查是否符合基本条件：单人脸
        if face_count != 1:
            continue
        
        # 获取人脸信息
        face_info = tools.safe_get(img, ['face_info', 'FaceInfos', 0], {})
        face_attributes = tools.safe_get(face_info, ['FaceAttributesInfo'], {})
        
        # 获取人脸角度
        pitch = tools.safe_get(face_attributes, ['Pitch'], 0)
        yaw = tools.safe_get(face_attributes, ['Yaw'], 0)
        
        # 获取年龄和性别
        age = tools.safe_get(face_attributes, ['Age'], 25)
        gender = tools.safe_get(face_attributes, ['Gender'], '')
        
        # 获取人脸在图片中的位置和大小信息
        face_rect = tools.safe_get(face_info, ['FaceRect'], {})
        face_width = tools.safe_get(face_rect, ['Width'], 0)
        face_height = tools.safe_get(face_rect, ['Height'], 0)
        face_x = tools.safe_get(face_rect, ['X'], 0)
        face_y = tools.safe_get(face_rect, ['Y'], 0)
        
        # 获取图片尺寸
        img_width = tools.safe_get(img, ['width'], 1)
        img_height = tools.safe_get(img, ['height'], 1)
        
        # 计算人脸占图片的比例
        face_area_ratio = (face_width * face_height) / (img_width * img_height)
        
        # 计算人脸中心点与图片中心点的距离
        img_center_x = img_width / 2
        img_center_y = img_height / 2
        face_center_x = face_x + face_width / 2
        face_center_y = face_y + face_height / 2
        
        # 归一化距离（相对于图片尺寸）
        distance_from_center = (((face_center_x - img_center_x) / img_width) ** 2 + 
                               ((face_center_y - img_center_y) / img_height) ** 2) ** 0.5
        
        # 检查要求1：人物大且居中
        is_requirement1 = False
        if face_area_ratio > 0.15 and distance_from_center < 0.15:
            is_requirement1 = True
            if 1 not in result:
                result.append(1)
        
        # 检查要求2：脸部倾斜度低，年龄15-40岁
        is_requirement2 = False
        if (abs(pitch) < 25 and abs(yaw) < 25 and 
            15 <= age <= 40 and 
            (blogger_gender is None or gender == blogger_gender)):
            is_requirement2 = True
            if 2 not in result:
                result.append(2)
    
    # 如果没有符合任何要求的图片，返回False
    if not result:
        return False
    
    # 如果只符合一个要求，返回该要求编号
    if len(result) == 1:
        return result[0]
    
    # 如果两个要求都符合，返回列表[1, 2]
    return result

def process_data_item(data, output_path, retry_limit=3):
    """处理单个数据项"""
    try:
        retries = 0
        while retries < retry_limit:
            try:
                # 获取可用的cookie
                if cookie_queue.empty():
                    cookie = wait_for_cookie()
                else:
                    cookie = cookie_queue.get()
                
                # 如果需要搜索用户ID
                if data['id'] and not data.get('user_id'):
                    id, xsec_token = search(data['id'], cookie)
                    data["user_id"] = id
                    data["xsec_token"] = xsec_token
                
                if data.get('user_id') and data.get('xsec_token'):
                    user_works = get_user_notes(data, cookie)
                    data["user_works"] = user_works
                    
                    for i, user_work in enumerate(user_works):
                        image_list = tools.safe_get(user_work,['note_detail','image_list'])
                        for j, image in enumerate(image_list):
                            result = is_save([image])
                            if result:
                                image['dataTypeArr'] = result
                            else:
                                del image_list[j]
                                
                        if image_list:
                            ret = get_comments(data, user_work, cookie)
                            user_works[i] = ret
                            
                            data["user_works"] = user_works
                            
                            url = "http://116.62.236.232/ai-huanzhuang-sys/huanzhuang/reportData"
                            response = tools.get_html(url, json_data={"jsonData": json.dumps(data)}, headers=headers)
                            if response['code'] == 0:
                                logger.info('数据录入成功')
                            else:
                                raise Exception("数据录入异常")
                    
                    # 将处理后的数据写入输出文件
                    with open(output_path, 'a', encoding='utf-8') as outfile:
                        outfile.write(json.dumps(data, ensure_ascii=False) + '\n')

                    logger.info(f'数据已写入 {output_path}')
                    with open("crawler/xhs/crawl/id.txt", 'w', encoding='utf-8') as last_id_file:
                        last_id_file.write(data['id'])
                
                # 处理成功，跳出重试循环
                break
                
            except Exception as e:
                retries += 1
                logger.error(f"处理数据项出错: {e}")
                logger.error(traceback.format_exc())
                if retries < retry_limit:
                    logger.error(f"重试 ({retries}/{retry_limit})...")
                    time.sleep(2)  # 等待2秒后重试
                else:
                    logger.error(f"重试{retry_limit}次后失败，跳过ID {data['id']}。")
    except Exception as e:
        logger.error(f"处理数据项时发生未处理的异常: {e}")
        logger.error(traceback.format_exc())


def process_excel(file_path, output_path):
    # 读取Excel文件，并将第一行和第二行作为列名
    df = pd.read_excel(file_path, header=[0, 0])
    
    # 检查是否包含所需的列
    if df.shape[1] != 8:
        raise ValueError("Excel文件应包含八列")
    
    # 创建一个列表来存储所有JSON对象
    json_objects = []

    # 遍历每列数据（从第一列开始，每两列为一组：用户ID和性别）
    for i in range(0, 8, 2):
        city = df.columns[i][0]  # 获取城市名称
        user_ids = df.iloc[:, i].tolist()  # 获取所有用户ID，包括第一行
        genders = df.iloc[:, i + 1].tolist()  # 获取所有性别，包括第一行
        
        if len(user_ids) != len(genders):
            raise ValueError(f"城市 {city} 的用户ID和性别列长度不一致")
        
        for user_id, gender in zip(user_ids, genders):
            user_id = str(user_id).strip()  # 去除用户ID前后的空格
            json_object = {
                "city": city,
                "id": user_id,
                "gender": gender
            }
            json_objects.append(json_object)
    
    # 将JSON对象写入输出文件
    with open(output_path, 'w', encoding='utf-8') as file:
        for json_object in json_objects:
            file.write(json.dumps(json_object, ensure_ascii=False) + '\n')


def process_json(file_path, output_json_file, retry_limit=3):
    """多线程处理JSON文件"""
    unique_ids = set()
    last_processed_id = None
    
    # 读取最后处理的ID
    try:
        with open("crawler/xhs/crawl/id.txt", 'r', encoding='utf-8') as last_id_file:
            last_processed_id = last_id_file.read().strip()
    except FileNotFoundError:
        pass

    start_processing = False if last_processed_id else True
    
    # 初始化线程池
    executor = ThreadPoolExecutor(max_workers=MAX_THREADS)
    futures = []
    
    # 初始化cookie队列
    initial_cookie = load_cookie()
    if initial_cookie:
        cookie_queue.put(initial_cookie)
    else:
        logger.warning("未找到初始Cookie，将在需要时等待新Cookie")
    
    # 读取JSON文件并处理每一行
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            try:
                data = json.loads(line.strip())
                if not start_processing:
                    if data['id'] == last_processed_id:
                        start_processing = True
                    else:
                        continue
                
                if data['id'] not in unique_ids:
                    unique_ids.add(data['id'])
                    
                    # 采集xsec_token
                    # if data['id']:
                    #     id, xsec_token = search(data['id'])
                    #     data["user_id"] = id
                    #     data["xsec_token"] = xsec_token
                        
                    #     # 将处理后的数据写入输出文件
                    #     with open(output_json_file, 'a', encoding='utf-8') as outfile:
                    #         outfile.write(json.dumps(data, ensure_ascii=False) + '\n')

                    #     logger.info(f'数据已写入{output_json_file}')

                    if data['user_id'] and data['xsec_token']:
                        # 提交任务到线程池
                        future = executor.submit(process_data_item, data, f'{RESULT_DIR}/data.json', retry_limit)
                        futures.append(future)
            except Exception as e:
                logger.error(f"解析JSON行出错: {e}")
                logger.error(traceback.format_exc())
    
    # 等待所有任务完成
    for future in futures:
        try:
            future.result()
        except Exception as e:
            logger.error(f"线程执行出错: {e}")
            logger.error(traceback.format_exc())
    
    # 关闭线程池
    executor.shutdown()

if __name__ == "__main__":
    try:
        input_file = "crawler/xhs/crawl/4城市（修改后去重）.xlsx"  # 输入文件路径
        output_file = "crawler/xhs/crawl/4城市.json"  # 输出文件路径
        output_json_file = "crawler/xhs/crawl/ori_data.json"  # 输出文件路径
        
        # process_excel(input_file, output_file)
        process_json(output_json_file, output_json_file)
    except Exception:
        logger.error(traceback.format_exc())