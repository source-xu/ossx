import csv
import time
import warnings
import xml.etree.ElementTree as ET

import requests
import urllib3

# 忽略InsecureRequestWarning警告
warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

# 用来统计所有key的列表
totoal_keys = []


# 获取存储桶页面默认显示条数max-keys,默认最大不超过1000
def get_info(url):
    response = requests.get(url, verify=False)
    # 解析XML内容
    xml_content = response.content
    # 解析XML
    root = ET.fromstring(xml_content)
    maxkey = root.findtext(f".//MaxKeys")
    nextmarker = root.find(f".//NextMarker")
    xpath_expr = ".//Contents"
    # 检查是否存在命名空间，存在命名空间的索引写法需要改变
    has_namespace = root.tag.startswith("{")
    if has_namespace:
        # 获取命名空间
        namespace = root.tag.split('}')[0].strip('{')
        xpath_expr = f".//{{{namespace}}}Contents"
        maxkey = root.findtext(f".//{{{namespace}}}MaxKeys")
        nextmarker = root.find(f".//{{{namespace}}}NextMarker")
    # 获取所有子标签的名称
    child_tags = set()
    for contents_element in root.findall(xpath_expr):
        for child_element in contents_element:
            if has_namespace:
                child_tags.add(child_element.tag.replace(f"{{{namespace}}}", ""))
            else:
                child_tags.add(child_element.tag)
    # 创建csv文件写入表头也就是各列名称
    filename = write_csv_header(child_tags)
    # 返回PageSize、下一页索引、创建的CSV文件名称、以及列名集合
    return maxkey, nextmarker, filename, child_tags


def getdata(baseurl, max_keys, csv_filename, child_tags, marker='', page=0):
    if int(max_keys) < 1000:
        max_keys = 1000
    baseurl = baseurl
    url = baseurl + f'?max-keys={max_keys}&marker={marker}'
    response = requests.get(url, verify=False)
    xml_content = response.content
    root = ET.fromstring(xml_content)
    # 检查是否存在命名空间
    namespace = ''
    xpath_expr = ".//Contents"
    nextmarker = root.findtext(f".//NextMarker")
    has_namespace = root.tag.startswith("{")
    if has_namespace:
        # 获取命名空间
        namespace = root.tag.split('}')[0].strip('{')
        xpath_expr = f".//{{{namespace}}}Contents"
        nextmarker = root.findtext(f".//{{{namespace}}}NextMarker")
    datas = root.findall(xpath_expr)
    # 写入数据
    nums, is_repeate, repeate_nums, total_nums = write_csv_content(csv_filename, datas, has_namespace, namespace,
                                                                   child_tags)
    page += 1
    print(f"[+] 第{page}页检测到{nums}条数据,共计发现{total_nums}个文件")
    # 是否存在nextmarker存在则说明还有下一页需要迭代进行遍历，不存在则说明以及遍历完成退出
    if nextmarker is None or is_repeate == 1:
        print(f"[√] 数据结果已写入文件：{csv_filename}，请查看😀")
        return
    getdata(baseurl, max_keys, csv_filename, child_tags, nextmarker, page)


def write_csv_header(child_tags):
    # 获取当前时间戳
    timestamp = int(time.time())
    # 将时间戳转换为字符串
    timestamp_str = str(timestamp)
    # 创建CSV文件并写入数据
    csv_filename = f'xml_data{timestamp_str}.csv'
    with open(csv_filename, 'w', newline='') as csv_file:
        # 写入表头，另外增加完整的url和文件类型列
        writer = csv.writer(csv_file)
        list_tags = list(child_tags)
        list_tags.append("url")
        list_tags.append("filetype")
        writer.writerow(list_tags)
        return csv_filename


def write_csv_content(csv_filename, datas, has_namespace, namespace, child_tags):
    # 提取数据并写入CSV文件
    with open(csv_filename, 'a', newline='') as csv_file:
        nums = 0
        repeate_nums = 0
        is_repeate = 0
        # 写入数据
        for contents_element in datas:
            if has_namespace:
                row = [contents_element.findtext(f"{{{namespace}}}{tag}") for tag in child_tags]
                key = contents_element.findtext(f"{{{namespace}}}Key")
            else:
                row = [contents_element.findtext(tag) for tag in child_tags]
                key = contents_element.findtext(f"Key")
            if str(key) not in totoal_keys:
                nums += 1
                totoal_keys.append(key)
                url = str(baseUrl) + str(key)
                parts = str(key).split(".")
                if len(parts) > 1:
                    # 如果分割后的列表长度大于1，说明存在文件后缀名
                    file_extension = parts[-1]
                else:
                    # 否则，文件后缀名不存在
                    file_extension = ""
                row.append(url)
                row.append(file_extension)
                writer = csv.writer(csv_file)
                writer.writerow(row)
            else:
                repeate_nums += 1
        if repeate_nums > 2:
            is_repeate = 1

        return nums, is_repeate, repeate_nums, len(totoal_keys)


if __name__ == '__main__':
    # 发送HTTP请求获取响应
    url = input("[*] 请输入存储桶遍历url：").strip()
    baseUrl = input("[*] 请输入存储桶根路径(不输入则和上述url保持一致)：").strip()
    if baseUrl == '':
        baseUrl = url
    if not baseUrl.endswith('/'):
        baseUrl += '/'
    # 获取存储桶基本信息包括默认的PageSize、下一页索引，同时创建csv文件根据字段写表头
    try:
        maxkey, nextmarker, csv_filename, child_tags = get_info(url)
        if len(child_tags) != 0:
            print("[+] xml数据提取成功！✨")
            # 未指定maxkey则默认1000
            if maxkey == None:
                maxkey = 1000
            print(f"[o] 该存储桶默认每页显示{maxkey}条数据")
            if nextmarker == None:
                print("[-] 该存储桶不支持Web翻页遍历😢")
            else:
                print("[+] 该存储桶支持遍历,正在获取文件及数量😀")
            getdata(url, max_keys=maxkey, child_tags=child_tags, csv_filename=csv_filename)
        else:
            print("[-] 该存储桶不支持遍历,或检查网址是否有误！")
    except Exception as e:
        print(e)
        print("[-] XML解析有误，无法遍历😢")
