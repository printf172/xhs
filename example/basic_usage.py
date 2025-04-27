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
    cookie = "abRequestId=8290b1ce-7bf9-52bf-ab75-4f8e492f0f8f; webBuild=4.55.1; xsecappid=xhs-pc-web; a1=194ba5bf51ddjarywdch0ed3n4sdzym2mpi4zm7p430000371440; webId=00044acbdbdefc8ef02af10ebd0fdab6; gid=yj4D02S8SS6Yyj4D02Di2U6WyffV0FvEfSx83U4dfqI4M4q8fuv6Ju888qWy4488JDyi8W0S; web_session=0400697d145f2fea9fc8ff09a9354b6220fab9; unread={%22ub%22:%2267960437000000001703bbf8%22%2C%22ue%22:%22679c40af000000002901707d%22%2C%22uc%22:29}; acw_tc=0a4a82e817382952013481610e09729c2bf7590399dba9e05c76a64afe3bca; websectiga=2845367ec3848418062e761c09db7caf0e8b79d132ccdd1a4f8e64a11d0cac0d; sec_poison_id=cbcbc416-0080-41c6-983f-471da29a64a3"

    xhs_client = XhsClient(cookie, sign=sign)
    print(datetime.datetime.now())

    for _ in range(10):
        # 即便上面做了重试，还是有可能会遇到签名失败的情况，重试即可
        try:
            note = xhs_client.get_note_comments("6786b1180000000018012df9", "ABTCv00Mw0wBvsnFfeMyddo4EYM9NEKTB0YI5F-NThHjs=")
            print(json.dumps(note, indent=4))
            print(help.get_imgs_url_from_note(note))
            break
        except DataFetchError as e:
            print(e)
            print("失败重试一下下")
