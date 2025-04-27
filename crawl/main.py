import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))
import pandas as pd
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

log_path = str(os.path.dirname(os.path.abspath(__file__)))
logger = set_logger(log_path, "startCrawl")

def detect_face(url):
    try:
        logger.info(f"开始进行人脸识别:{url}")
        # 实例化一个认证对象，入参需要传入腾讯云账户 SecretId 和 SecretKey，此处还需注意密钥对的保密
        # 代码泄露可能会导致 SecretId 和 SecretKey 泄露，并威胁账号下所有资源的安全性。以下代码示例仅供参考，建议采用更安全的方式来使用密钥，请参见：https://cloud.tencent.com/document/product/1278/85305
        # 密钥可前往官网控制台 https://console.cloud.tencent.com/cam/capi 进行获取
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
            # "MinFaceSize": 34,
            "NeedFaceAttributes": 1,
            "NeedQualityDetection": 1,
            # "NeedRotateDetection": 1
        }
        req.from_json_string(json.dumps(params))

        # 返回的resp是一个DetectFaceResponse的实例，与请求对象对应
        resp = client.DetectFace(req)
        # 输出json格式的字符串回包
        return resp.to_json_string()

    except TencentCloudSDKException as err:
        logger.info(err)
        return ''
    

def read_text(img_path, compress_factor: int = 1, lang_list=None):
    if lang_list is None:
        lang_list = ['en']  # default to English if no languages are provided

    img = cv2.imread(img_path)
    if img is None:
        raise ValueError(f"Could not read the image from path: {img_path}")
    
    if compress_factor > 1:
        img = cv2.resize(img, (int(img.shape[1] / compress_factor), int(img.shape[0] / compress_factor)))

    reader = easyocr.Reader(lang_list)
    return reader.readtext(img)

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
    res = requests.post("http://localhost:5005/sign",
                        json={"uri": uri, "data": data, "a1": a1, "web_session": web_session})
    signs = res.json()
    return {
        "x-s": signs["x-s"],
        "x-t": signs["x-t"]
    }


cookie = "abRequestId=122db771-4363-5033-8560-5e4c927b1725; a1=190a55c8149bjjte6pipi627kkp29c9vxdmyavr6v30000247366; webId=ca267d989dae0a7532ed28fc3e8bed1b; gid=yj8022SY2dCiyj8022SYyfhF4jDVV9dKU3U36KhKJWTTUvq8JjSjh0888J4WqKK8JJJdYKdj; ads-portal_worker_plugin_uuid=1c7d604b00a14acba62501f557c5fa60; x-user-id-ruzhu.xiaohongshu.com=5ba367b89f64dc0001e1cf9e; customerClientId=070900957451639; x-user-id-creator.xiaohongshu.com=5ba367b89f64dc0001e1cf9e; webBuild=4.62.3; _did=169AC6EF; unread={%22ub%22:%2267da2e220000000009015f69%22%2C%22ue%22:%2267ca6b900000000028035023%22%2C%22uc%22:20}; web_session=040069b6fe74d368b05adc52ca354b75573956; xsecappid=xhs-pc-web; acw_tc=0a00dcc017457359812634157ef46e1eb9a6e2087f81bcd19f924498492883; loadts=1745735981623; acw_tc=0a0b147c17457359821348060e7a417399ae178f645a4ee5e622c1cf6ae592; websectiga=f47eda31ec99545da40c2f731f0630efd2b0959e1dd10d5fedac3dce0bd1e04d; sec_poison_id=5b3e24f9-034a-4ade-8a4e-ca276ebf85b6"
xhs_client = XhsClient(cookie, sign=sign)
headers = {
    'Authorization': 'test'
}

def summarize_with_ernie(content, max_retries=3, backoff_factor=2):
    API_KEY = "nzmhDqH4wycCGUeQBqpHoBBZ"
    SECRET_KEY = "vRj0EyyzlfPjxDrbTiwM8lCgCKJGsXI5"
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

def get_user_notes(data):
    user_works = []
    user_id = data['user_id']
    xsec_token = data['xsec_token']
    logger.info(f'开始获取用户作品：{user_id}')
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
                img_path = f'crawler/xhs/crawl/img/{tools.get_current_timestamp()}.png'
                download_image(img_url, img_path)
                res = read_text(img_path)
                if res:
                    img['text'] = 1
                else:
                    img['text'] = 0
                
                url = 'http://116.62.236.232/ai-huanzhuang-sys/file/uploadFile'
                files = {'file': open(img_path, 'rb')}
                response = tools.get_html(url, files=files, headers=headers)
                if response['code'] == 0:
                    oss_path = response['data']
                    img['oss_path'] = oss_path
                else:
                    raise "阿里云上传图片异常"
                
                face_info = detect_face(img_url)
                face_info = tools.get_json(face_info)
                if face_info:
                    face_infos = tools.safe_get(face_info, ['FaceInfos'])
                    img['face_count'] = len(face_infos)
                    
                    img['face_info'] = face_info
                    image_list[i] = img
                    break

            note_detail['image_list'] = image_list
            note['note_detail'] = note_detail
            user_works.append(note)
            logger.info(f'***************完成笔记{note_id}的采集***************')
            break
        except Exception:
            logger.error(traceback.format_exc())
            time.sleep(2)
            
    return user_works
    

def search(user_id):    
    logger.info(f'开始搜索：{user_id}')
    res = xhs_client.get_user_by_keyword(user_id)
    if not res:
        logger.error(f'搜索异常:{user_id}')
        return '', ''
    users = tools.safe_get(res, ['users'])
    find = False
    for user in users:
        red_id = tools.safe_get(user, ['red_id'])
        if user_id == red_id:
            id = tools.safe_get(res, ['users', 0, 'id'])
            xsec_token = tools.safe_get(res, ['users', 0, 'xsec_token'])
            find = True
            return id, xsec_token
    if not find:    
        logger.error(f'未搜索到相关用户')
        return '', ''


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


def do_huanzhuang(data, img):
    city = data['city']
    gender = data['gender']
    url = 'http://116.62.236.232/ai-huanzhuang-sys/huanzhuang/doHuanzhuan'
    json_data = {
        "personImageUrl": img['oss_path'],
        "personGender": 1 if gender == "男" else 0,
        "city": city,
        "day": "2024-11-11",
    }
    response = tools.get_html(url, json_data=json_data, headers=headers, timeout=50)
    if response['code'] == 0:
        huanzhuang_url = response['data']['url']
        img['huanzhuang_url'] = huanzhuang_url
    else:
        raise "换装异常"
    return img

def get_comments(data, user_work):
    user_id = data['user_id']
    xsec_token = data['xsec_token']
    note_id = tools.safe_get(user_work, ['note_id'])
    comments = xhs_client.get_note_all_comments(note_id, xsec_token)
    # 只保留作者或者包含作者参与的评论
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
    
    # 拼接评论内容
    comments_content = '\n'.join([tools.safe_get(comment, 'content', '') for comment in filtered_comments])
    note_desc = tools.safe_get(user_work, ['desc','note_detail'])
    prompt = f"""
        以下是小红书的一篇笔记内容和评论，请根据这些内容生成一个总结，介绍这篇笔记的主要内容、地理位置（如果有提到）、以及其他重要信息：

        笔记内容:
        {note_desc}

        评论内容:
        {comments_content}
        """
    logger.info(prompt)
    user_work['note_detail']['summary'] = summarize_with_ernie(prompt)
    return user_work


def is_save(image_list, blogger_gender=None):
    """
    分析图片列表，决定是否保留笔记及其中的图片
    """
    if not image_list:
        return {'save_note': False, 'filtered_images': [], 'face_images': [], 'non_face_images': []}
    
    # 分类图片
    face_images = []  # 带脸照片
    multi_face_images = []  # 多人脸照片(>=2)
    non_face_images = []  # 不带脸照片
    problematic_face_images = []  # 有问题的人脸照片(年龄不符/性别不符/角度过大/有文字)
    
    # 遍历图片列表，进行分类
    for img in image_list:
        face_count = tools.safe_get(img, ['face_count'], 0)
        
        text = img['text']
        if len(text)>3:
            continue
        
        if face_count == 0:
            # 不带脸的图片
            non_face_images.append(img)
            continue
            
        if face_count >= 2:
            # 多人脸图片
            multi_face_images.append(img)
            continue
            
        # 单人脸图片，检查是否有问题
        pitch = tools.safe_get(img, ['face_info', 'FaceInfos', 0, "FaceAttributesInfo", "Pitch"], 0)
        yaw = tools.safe_get(img, ['face_info', 'FaceInfos', 0, "FaceAttributesInfo", "Yaw"], 0)
        age = tools.safe_get(img, ['face_info', 'FaceInfos', 0, 'FaceAttributesInfo', 'Age'], 25)
        gender = tools.safe_get(img, ['face_info', 'FaceInfos', 0, 'FaceAttributesInfo', 'Gender'], '')
        
        # 检查是否为问题图片
        if (age < 15 or age > 40 or 
            (blogger_gender and gender != blogger_gender) or 
            abs(pitch) > 25 or abs(yaw) > 25):
            problematic_face_images.append(img)
        else:
            # 合格的单人脸图片
            face_images.append(img)
    
    # 规则1: 如果一篇笔记中一张带脸照片都没有，则整篇笔记不要
    total_face_images = len(face_images) + len(multi_face_images) + len(problematic_face_images)
    if total_face_images == 0:
        return False
    
    # 规则2: 如果一篇笔记中只有一张带脸照片且这张图人脸数大于等于2，则整篇笔记不要
    if total_face_images == 1 and len(multi_face_images) == 1:
        return False
    
    # 规则3: 特殊情况判断
    valid_face_images = face_images.copy()
    if len(multi_face_images) > 0:
        # 如果去除多人脸照片后，只剩一张带脸照片且有问题
        if len(face_images) == 1 and len(problematic_face_images) > 0:
            return False
    
    # 过滤后的图片列表 = 合格的单人脸图片 + 不带脸的图片
    filtered_images = valid_face_images + non_face_images
    
    # 为每张图片添加标签
    for img in valid_face_images:
        img['is_template'] = True  # 标记为模板照片
    
    # 如果过滤后没有图片，则不保留笔记
    if not filtered_images:
        return False
    
    return True


def process_json(file_path, output_path, retry_limit=3):
    unique_ids = set()
    last_processed_id = None
    
    # 读取最后处理的ID
    try:
        with open("crawler/xhs/crawl/id.txt", 'r', encoding='utf-8') as last_id_file:
            last_processed_id = last_id_file.read().strip()
    except FileNotFoundError:
        pass

    start_processing = False if last_processed_id else True
        
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
                    retries = 0
                    while retries < retry_limit:
                        try:
                            # if data['id'] and not data['user_id']:
                                # id, xsec_token = search(data['id'])
                                # data["user_id"] = id
                                # data["xsec_token"] = xsec_token
                            if data['user_id'] and data['xsec_token']:
                                user_works = get_user_notes(data)
                                data["user_works"] = user_works
                                # 将处理后的数据写入输出文件
                                with open(output_path, 'a', encoding='utf-8') as outfile:
                                    outfile.write(json.dumps(data, ensure_ascii=False) + '\n')

                                logger.info('数据已写入', output_path)
                                
                                for i, user_work in enumerate(user_works):
                                    image_list = tools.safe_get(user_work,['note_detail','image_list'])
                                    for i, image in enumerate(image_list):
                                        if is_save([image]):
                                            img = do_huanzhuang(data, image)
                                            image_list[i] = img
                                        else:
                                            del image_list[i]
                                            
                                    if image_list:
                                        user_work = get_comments(data, user_work)
                                        user_works[i] = user_work
                                        
                                        data["user_works"] = user_works
                                        
                                        url = "http://116.62.236.232/ai-huanzhuang-sys/huanzhuang/reportData"
                                        response = tools.get_html(url, json_data= {"jsonData": json.dumps(data)}, headers=headers)
                                        if response['code'] == 0:
                                            logger.info('数据录入成功')
                                        else:
                                            raise "数据录入异常"
                                with open("crawler/xhs/crawl/id.txt", 'w', encoding='utf-8') as last_id_file:
                                    last_id_file.write(data['id'])
                        except Exception:
                            retries += 1
                            logger.error(f"Error during search for id {data['id']}: {traceback.format_exc()}")
                            if retries < retry_limit:
                                logger.error(f"Retrying ({retries}/{retry_limit})...")
                                time.sleep(2)  # 等待2秒后重试
                            else:
                                logger.error(f"Failed after {retry_limit} retries. Skipping id {data['id']}.")
            except Exception:
                logger.error(traceback.format_exc())
    

if __name__ == "__main__":
    try:
        input_file = "/Users/wangjie/Downloads/4城市.xlsx"  # 输入文件路径
        output_file = "crawler/xhs/crawl/4城市.json"  # 输出文件路径
        # output_json_file = "crawler/xhs/crawl/4城市2.json"  # 输出文件路径
        output_json_file = "crawler/xhs/crawl/data.json"  # 输出文件路径
        
        # process_excel(input_file, output_file)
        process_json(output_file, output_json_file)
    except Exception:
        logger.error(traceback.format_exc())

