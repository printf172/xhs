import datetime
import json

import requests

import xhs.help
from xhs import XhsClient


def sign(uri, data=None, a1="", web_session=""):
    # 填写自己的 flask 签名服务端口地址
    res = requests.post("http://localhost:5005/sign",
                        json={"uri": uri, "data": data, "a1": a1, "web_session": web_session})
    signs = res.json()
    return {
        "x-s": signs["x-s"],
        "x-t": signs["x-t"]
    }


if __name__ == '__main__':
    cookie = "abRequestId=122db771-4363-5033-8560-5e4c927b1725; a1=190a55c8149bjjte6pipi627kkp29c9vxdmyavr6v30000247366; webId=ca267d989dae0a7532ed28fc3e8bed1b; gid=yj8022SY2dCiyj8022SYyfhF4jDVV9dKU3U36KhKJWTTUvq8JjSjh0888J4WqKK8JJJdYKdj; x-user-id-ruzhu.xiaohongshu.com=5ba367b89f64dc0001e1cf9e; customerClientId=070900957451639; x-user-id-creator.xiaohongshu.com=5ba367b89f64dc0001e1cf9e; customer-sso-sid=68c517460357811226663950cea818f09310bb78; access-token-creator.xiaohongshu.com=customer.creator.AT-68c517460357811226826619rsblqspujumxnpkg; galaxy_creator_session_id=r6x0xJGmcJzPFFyHkZOz1u8FLe1scMBgzOrg; galaxy.creator.beaker.session.id=1736999911727004896951; xsecappid=xhs-pc-web; webBuild=4.55.1; unread={%22ub%22:%22676d8733000000000b00c9c4%22%2C%22ue%22:%22676ef997000000000b02015a%22%2C%22uc%22:25}; web_session=040069b649e83c72d07b9c7dbb354bc0509cdc; acw_tc=0a4a9a7a17374311399685540ecccb7c3bf88a2aa6a09eb4d110355db9f2ec; websectiga=7750c37de43b7be9de8ed9ff8ea0e576519e8cd2157322eb972ecb429a7735d4; sec_poison_id=c592b9da-2469-4ace-ac5c-5c397bd2dc71"
    xhs_client = XhsClient(cookie, sign=sign)
    # get note info
    note_info = xhs_client.get_user_by_keyword("63db8819000000001a01ead1")
    print(datetime.datetime.now())
    print(json.dumps(note_info, indent=2))
    print(xhs.help.get_imgs_url_from_note(note_info))
