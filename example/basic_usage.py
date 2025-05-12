import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))
import datetime
import json
from time import sleep

from playwright.sync_api import sync_playwright

from crawler.xhs.xhs import DataFetchError, XhsClient, help


def sign(uri, data=None, a1="", web_session=""):
    for _ in range(10):
        try:
            with sync_playwright() as playwright:
                stealth_js_path = "crawler/MediaCrawler/libs/stealth.min.js"
                chromium = playwright.chromium

                # 如果一直失败可尝试设置成 False 让其打开浏览器，适当添加 sleep 可查看浏览器状态
                browser = chromium.launch(headless=True)

                browser_context = browser.new_context()
                browser_context.add_init_script(path=stealth_js_path)
                context_page = browser_context.new_page()
                context_page.goto("https://www.xiaohongshu.com")
                browser_context.add_cookies([
                    {'name': 'a1', 'value': a1, 'domain': ".xiaohongshu.com", 'path': "/"}]
                )
                context_page.reload()
                # 这个地方设置完浏览器 cookie 之后，如果这儿不 sleep 一下签名获取就失败了，如果经常失败请设置长一点试试
                sleep(1)
                encrypt_params = context_page.evaluate("([url, data]) => window._webmsxyw(url, data)", [uri, data])
                return {
                    "x-s": encrypt_params["X-s"],
                    "x-t": str(encrypt_params["X-t"])
                }
        except Exception:
            import traceback
            print(traceback.format_exc())
            # 这儿有时会出现 window._webmsxyw is not a function 或未知跳转错误，因此加一个失败重试趴
            pass
    raise Exception("重试了这么多次还是无法签名成功，寄寄寄")


if __name__ == '__main__':
    cookie = "abRequestId=122db771-4363-5033-8560-5e4c927b1725; a1=190a55c8149bjjte6pipi627kkp29c9vxdmyavr6v30000247366; webId=ca267d989dae0a7532ed28fc3e8bed1b; gid=yj8022SY2dCiyj8022SYyfhF4jDVV9dKU3U36KhKJWTTUvq8JjSjh0888J4WqKK8JJJdYKdj; ads-portal_worker_plugin_uuid=1c7d604b00a14acba62501f557c5fa60; x-user-id-ruzhu.xiaohongshu.com=5ba367b89f64dc0001e1cf9e; customerClientId=070900957451639; x-user-id-creator.xiaohongshu.com=5ba367b89f64dc0001e1cf9e; webBuild=4.62.3; _did=169AC6EF; web_session=040069b6fe74d368b05adc52ca354b75573956; xsecappid=xhs-pc-web; acw_tc=0a00d7ab17465863013078825e0e2cfa936a136ab4159c349d4e2db99c68ec; loadts=1746586302531; acw_tc=0a4a293017465863039407623e7536d6ad65c3311b99e0165d9f396f1f0d55; unread={%22ub%22:%226814bded000000002202d624%22%2C%22ue%22:%22681a0cda000000002301453c%22%2C%22uc%22:25}; websectiga=2a3d3ea002e7d92b5c9743590ebd24010cf3710ff3af8029153751e41a6af4a3; sec_poison_id=d95711ec-6135-4ebd-81e9-03398b54bf51"

    xhs_client = XhsClient(cookie, sign=sign)
    print(datetime.datetime.now())

    for _ in range(10):
        # 即便上面做了重试，还是有可能会遇到签名失败的情况，重试即可
        try:
            # 初始化统计变量
            liked_counts = []
            collected_counts = []
            comment_counts = []
            shared_counts = []
            total_notes = 0
            filtered_notes = 0
            
            # 遍历10页数据
            for j in range(10):
                note = xhs_client.get_note_comments("谁懂啊！中考数学几何模型帮我提高了30分", page=j+1)
                for item in note.get("items", []):
                    try:
                        interact_info = item['note_card']['interact_info']
                        liked_count = int(interact_info['liked_count'])
                        collected_count = int(interact_info['collected_count'])
                        comment_count = int(interact_info['comment_count'])
                        shared_count = int(interact_info['shared_count'])
                        
                        total_notes += 1
                        
                        # 过滤掉过千的数据
                        if (liked_count < 1000 and collected_count < 1000 and 
                            comment_count < 1000 and shared_count < 1000):
                            liked_counts.append(liked_count)
                            collected_counts.append(collected_count)
                            comment_counts.append(comment_count)
                            shared_counts.append(shared_count)
                        else:
                            filtered_notes += 1
                            
                    except Exception as e:
                        continue
            
            # 计算平均值
            valid_notes = len(liked_counts)
            if valid_notes > 0:
                avg_liked_count = sum(liked_counts) / valid_notes
                avg_collected_count = sum(collected_counts) / valid_notes
                avg_comment_count = sum(comment_counts) / valid_notes
                avg_shared_count = sum(shared_counts) / valid_notes
                
                print(f"平均点赞数: {avg_liked_count:.2f}")
                print(f"平均收藏数: {avg_collected_count:.2f}")
                print(f"平均评论数: {avg_comment_count:.2f}")
                print(f"平均分享数: {avg_shared_count:.2f}")
                print(f"有效统计笔记数: {valid_notes}")
                print(f"过滤掉的笔记数(数据过千): {filtered_notes}")
                print(f"总计获取笔记数: {total_notes}")
                
                # 输出中位数统计，更能反映普通笔记的表现
                print("\n中位数统计:")
                print(f"点赞数中位数: {sorted(liked_counts)[valid_notes//2]}")
                print(f"收藏数中位数: {sorted(collected_counts)[valid_notes//2]}")
                print(f"评论数中位数: {sorted(comment_counts)[valid_notes//2]}")
                print(f"分享数中位数: {sorted(shared_counts)[valid_notes//2]}")
            else:
                print("没有符合条件的有效数据")
            
            break  # 成功获取数据后跳出重试循环
        except DataFetchError as e:
            print(f"数据获取失败: {e}")
            print("失败重试中...")


