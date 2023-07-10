"""
Date: 2023/7/6 10:33
Desc: 
"""
import uuid
import requests
import base64

fe_host = "xxx.xx.xx.xxx"
fe_http_port = "xxxx"
db_name = "db_name"
username = "username"
password = "password"


def stream_load_csv(file_path, label, table_name):
    """
    通过starrocks提供的StreamLoad导入csv格式数据
    :param file_path: 数据源文件路径
    :param label: 导入作业label
    :param table_name: 目标表名
    :return:
	:tips: columns->如果源数据文件中的列与 StarRocks 表中的列按顺序一一对应，则不需要指定 columns 参数。
	例如，目标表中有三列，按顺序依次为 col1、col2 和 col3；源数据文件中也有三列，按顺序依次对应目标表中
	的 col3、col2 和 col1。这种情况下，需要指定 COLUMNS(col3, col2, col1)。
    """
    api = 'http://%s:%s/api/%s/%s/_stream_load' % (fe_host, fe_http_port, db_name, table_name)
    headers = {
        "Expect": "100-continue",
        "label": label,
        "column_separator": ",",
        "skip_header": "1",
        "columns": "col1, col2, col3",
        "Content-Type": "application/octet-stream",
        "max_filter_ratio": "0.2",
        "Authorization": "Basic " + base64.b64encode((username + ":" + password).encode()).decode()
    }
    response = requests.put(url=api, headers=headers, data=open(file_path, "rb"))
    print(response.status_code)
    print(response.content.decode())


if __name__ == "__main__":
    stream_load_csv("file_path", uuid.uuid4().hex, "table_name")
    